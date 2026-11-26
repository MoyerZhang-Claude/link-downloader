// Package quark 处理夸克网盘分享链接：校验提取码、列出文件（不需要登录）；
// 下载时自动“临时转存 → 获取下载地址 → 下载完删除”（夸克只允许下载自己网盘里的文件）。
package quark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"pandl/internal/account"
	"pandl/internal/share"
)

const needLoginNote = "夸克网盘需要登录后才能下载（会自动转存到你网盘的临时文件夹，下载完自动删除），请先扫码登录"

// Provider 实现 share.Provider。
type Provider struct {
	client   *http.Client
	accounts *account.Store
}

// New 创建夸克网盘平台。accounts 用来判断是否已登录。
func New(client *http.Client, accounts *account.Store) *Provider {
	return &Provider{client: client, accounts: accounts}
}

// ID 返回平台 ID。
func (p *Provider) ID() string { return share.Quark }

// fileInfo 是转存时需要的信息。
type fileInfo struct {
	FID        string
	ShareToken string
	PdirFID    string // 文件在分享里所在文件夹的 fid（根目录为 "0"）。转存时必须对得上，否则校验令牌失败（41020）
}

type session struct {
	p        *Provider
	pwdID    string
	passcode string
	stoken   string
	title    string
	mu       sync.Mutex          // 保护 title、dirs、files（可以同时读取多个文件夹）
	dirs     map[string]string   // 文件夹 fid -> 相对路径
	files    map[string]fileInfo // fid -> 转存需要的信息
	pages    chan struct{}       // 限制同时在读的页数（整个分享共用，几个文件夹一起读时也不超过）
}

const (
	pageSize     = 100 // 夸克一页最多给 100 条（_size 填再大也只给 100）
	pageWorkers  = 8   // 同时读取的页数：4000 多个文件的文件夹一页一页读要 30 秒，8 页一起读 5 秒左右
	pageAttempts = 3   // 一页最多试几次
)

// pageTimeout：一页超过这么久没读完就换个连接重试（以前卡住一页要等 60 秒，然后整个文件夹失败）。测试时改短。
var pageTimeout = 20 * time.Second

// shareItem 是分享列表里的一项。
type shareItem struct {
	FID        string `json:"fid"`
	Name       string `json:"file_name"`
	Size       int64  `json:"size"`
	Dir        bool   `json:"dir"`
	UpdatedAt  int64  `json:"updated_at"`
	ShareToken string `json:"share_fid_token"`
}

// listSharePage 列出分享里某个文件夹的一页。cookie 为空表示不登录。
func listSharePage(ctx context.Context, client *http.Client, cookie, pwdID, stoken, pdir string, page int) ([]shareItem, int, error) {
	q := url.Values{
		"pr": {"ucpro"}, "fr": {"pc"}, "pwd_id": {pwdID}, "stoken": {stoken}, "pdir_fid": {pdir},
		"force": {"0"}, "_page": {strconv.Itoa(page)}, "_size": {strconv.Itoa(pageSize)},
		"_fetch_banner": {"0"}, "_fetch_share": {"0"}, "_fetch_total": {"1"},
		"_sort": {"file_type:asc,file_name:asc"},
	}
	var r apiResp
	if err := doJSON(ctx, client, http.MethodGet, apiBase+"/share/sharepage/detail?"+q.Encode(), nil, cookie, "", &r); err != nil {
		return nil, 0, err
	}
	if r.Code != 0 {
		return nil, 0, fmt.Errorf("夸克：%s（%d）", r.Message, r.Code)
	}
	var d struct {
		List []shareItem `json:"list"`
	}
	if err := json.Unmarshal(r.Data, &d); err != nil {
		return nil, 0, errors.New("夸克返回的文件列表无法识别")
	}
	return d.List, r.Meta.Total, nil
}

// getStoken 用提取码换取分享令牌 stoken。cookie 为空表示不登录。
func getStoken(ctx context.Context, client *http.Client, cookie, pwdID, passcode string) (string, string, error) {
	var r apiResp
	if err := doJSON(ctx, client, http.MethodPost, apiBase+"/share/sharepage/token?pr=ucpro&fr=pc",
		map[string]string{"pwd_id": pwdID, "passcode": passcode}, cookie, "", &r); err != nil {
		return "", "", err
	}
	if r.Code != 0 {
		switch {
		case r.Code == 41006 || r.Status == http.StatusNotFound:
			return "", "", fmt.Errorf("%w（%s）", share.ErrNotFound, r.Message)
		case r.Code == 41007 || r.Code == 41008 || strings.Contains(r.Message, "提取码") || strings.Contains(r.Message, "密码"):
			return "", "", &share.PasswordError{Label: "提取码", Wrong: passcode != ""}
		default:
			return "", "", fmt.Errorf("夸克：%s（%d）", r.Message, r.Code)
		}
	}
	var d struct {
		Stoken string `json:"stoken"`
		Title  string `json:"title"`
	}
	if err := json.Unmarshal(r.Data, &d); err != nil || d.Stoken == "" {
		return "", "", errors.New("夸克没有返回分享令牌")
	}
	return d.Stoken, d.Title, nil
}

// Open 用提取码换取 stoken。
func (p *Provider) Open(ctx context.Context, link share.Link, pwd string) (share.Session, error) {
	pwd = strings.TrimSpace(pwd)
	stoken, title, err := getStoken(ctx, p.client, "", link.ID, pwd)
	if err != nil {
		return nil, err
	}
	return &session{p: p, pwdID: link.ID, passcode: pwd, stoken: stoken, title: title,
		dirs: map[string]string{"0": ""}, files: map[string]fileInfo{}, pages: make(chan struct{}, pageWorkers)}, nil
}

func (s *session) Title() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.title
}

// DownloadNote：登录后为空（可以下载），未登录时提示去登录。
func (s *session) DownloadNote() string {
	if acc, ok := s.p.accounts.Get(share.Quark); ok && len(acc.Cookies) > 0 {
		return ""
	}
	return needLoginNote
}

func (s *session) List(ctx context.Context, dirID string) ([]share.Entry, error) {
	if dirID == "" {
		dirID = "0"
	}
	s.mu.Lock()
	parentRel, ok := s.dirs[dirID]
	s.mu.Unlock()
	if !ok {
		return nil, errors.New("目录不存在")
	}

	// 先读第一页，拿到总条数；剩下的页同时读（按页号放回原来的位置，顺序不变）
	first, total, err := s.listPage(ctx, dirID, 1)
	if err != nil {
		return nil, err
	}
	pages := [][]shareItem{first}
	if len(first) >= pageSize {
		share.ReportListProgress(ctx, len(first), total, s.toEntries(first, dirID, parentRel))
		if n := (total + pageSize - 1) / pageSize; n > 1 {
			rest, err := s.listPages(ctx, dirID, 2, n, len(first), total)
			if err != nil {
				return nil, err
			}
			pages = append(pages, rest...)
		}
		// 没给总数（或总数不准）：最后一页还是满的就接着往后读
		count := func() (n int) {
			for _, l := range pages {
				n += len(l)
			}
			return
		}
		for page := len(pages) + 1; len(pages[len(pages)-1]) >= pageSize && (total <= 0 || count() < total); page++ {
			list, _, err := s.listPage(ctx, dirID, page)
			if err != nil {
				return nil, err
			}
			pages = append(pages, list)
		}
	}

	var out []share.Entry
	seen := map[string]bool{}
	for _, list := range pages {
		for _, e := range s.toEntries(list, dirID, parentRel) {
			if !seen[e.ID] { // 读的过程中文件有变动时，同一项可能出现在相邻两页
				seen[e.ID] = true
				out = append(out, e)
			}
		}
	}
	s.mu.Lock()
	if dirID == "0" && s.title == "" && len(out) > 0 {
		s.title = out[0].Name
	}
	s.mu.Unlock()
	return out, nil
}

// toEntries 把一页的内容转成 share.Entry，并记下转存需要的信息。
func (s *session) toEntries(list []shareItem, dirID, parentRel string) []share.Entry {
	out := make([]share.Entry, 0, len(list))
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, it := range list {
		rel := it.Name
		if parentRel != "" {
			rel = parentRel + "/" + it.Name
		}
		e := share.Entry{ID: it.FID, Name: it.Name, IsDir: it.Dir, Size: it.Size, Path: rel, ModTime: it.UpdatedAt / 1000}
		if it.Dir {
			e.Size = -1
			s.dirs[it.FID] = rel
		}
		s.files[it.FID] = fileInfo{FID: it.FID, ShareToken: it.ShareToken, PdirFID: dirID}
		out = append(out, e)
	}
	return out
}

// listPages 同时读取第 from～to 页（最多 pageWorkers 页一起读），按页号顺序返回。
// 每读完一页报告一次进度；有一页怎么都读不出来就整个失败。
func (s *session) listPages(ctx context.Context, dirID string, from, to, loaded, total int) ([][]shareItem, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	out := make([][]shareItem, to-from+1)
	next := make(chan int)
	go func() {
		defer close(next)
		for p := from; p <= to; p++ {
			select {
			case next <- p:
			case <-ctx.Done():
				return
			}
		}
	}()
	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)
	for w := 0; w < pageWorkers && w <= to-from; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range next {
				list, _, err := s.listPage(ctx, dirID, p)
				if err != nil {
					cancel(err)
					continue
				}
				mu.Lock()
				out[p-from] = list
				loaded += len(list)
				n := loaded
				mu.Unlock()
				share.ReportListProgress(ctx, n, total, nil)
			}
		}()
	}
	wg.Wait()
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// listPage 读取一页。网络卡住、断开或夸克一时出错时，等一下换个连接再试（最多 pageAttempts 次）。
func (s *session) listPage(ctx context.Context, dirID string, page int) ([]shareItem, int, error) {
	var err error
	for attempt := 0; attempt < pageAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(time.Duration(attempt) * time.Second):
			case <-ctx.Done():
				return nil, 0, context.Cause(ctx)
			}
		}
		if s.pages != nil {
			select {
			case s.pages <- struct{}{}:
			case <-ctx.Done():
				return nil, 0, context.Cause(ctx)
			}
		}
		actx, cancel := context.WithTimeout(ctx, pageTimeout)
		var list []shareItem
		var total int
		list, total, err = listSharePage(actx, s.p.client, "", s.pwdID, s.stoken, dirID, page)
		cancel()
		if s.pages != nil {
			<-s.pages
		}
		if err == nil {
			return list, total, nil
		}
		if ctx.Err() != nil {
			return nil, 0, context.Cause(ctx)
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		err = errors.New("夸克网盘响应太慢，读取文件夹超时，请稍后重试")
	}
	return nil, 0, err
}

func (s *session) TaskSource(e share.Entry) (json.RawMessage, error) {
	s.mu.Lock()
	f, ok := s.files[e.ID]
	s.mu.Unlock()
	if !ok || e.IsDir {
		return nil, errors.New("找不到这个文件的信息，请重新解析")
	}
	return json.Marshal(taskSource{PwdID: s.pwdID, Passcode: s.passcode, FID: f.FID, ShareToken: f.ShareToken,
		PdirFID: f.PdirFID, Size: e.Size, Name: e.Name})
}
