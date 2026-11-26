package quark

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pandl/internal/account"
	"pandl/internal/share"
)

// fakeBigShare 是一个文件夹里有 n 个文件的分享（一页 100 条）。
// fail[页号] 次数：这一页前几次返回错误；stall[页号] 次数：这一页前几次一直不回应（直到请求被取消）。
type fakeBigShare struct {
	n         int
	withTotal bool
	mu        sync.Mutex
	fail      map[int]int
	stall     map[int]int
	requests  map[int]int
	inflight  atomic.Int32
	maxFlight atomic.Int32
}

func (f *fakeBigShare) start(t *testing.T) {
	js := func(w http.ResponseWriter, v any) { json.NewEncoder(w).Encode(v) }
	mux := http.NewServeMux()
	mux.HandleFunc("/1/clouddrive/share/sharepage/token", func(w http.ResponseWriter, r *http.Request) {
		js(w, map[string]any{"status": 200, "code": 0, "data": map[string]any{"stoken": "ST", "title": "游戏"}})
	})
	mux.HandleFunc("/1/clouddrive/share/sharepage/detail", func(w http.ResponseWriter, r *http.Request) {
		cur := f.inflight.Add(1)
		defer f.inflight.Add(-1)
		for {
			m := f.maxFlight.Load()
			if cur <= m || f.maxFlight.CompareAndSwap(m, cur) {
				break
			}
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("_page"))
		size, _ := strconv.Atoi(r.URL.Query().Get("_size"))
		if size > 100 { // 和真实夸克一致：一页最多 100 条
			size = 100
		}
		f.mu.Lock()
		f.requests[page]++
		fail, stall := f.fail[page] > 0, f.stall[page] > 0 && f.fail[page] == 0
		if fail {
			f.fail[page]--
		} else if stall {
			f.stall[page]--
		}
		f.mu.Unlock()
		if stall {
			<-r.Context().Done()
			return
		}
		if fail {
			js(w, map[string]any{"status": 500, "code": 41099, "message": "服务器繁忙"})
			return
		}
		var list []map[string]any
		for i := (page - 1) * size; i < page*size && i < f.n; i++ {
			list = append(list, map[string]any{"fid": fmt.Sprintf("F%04d", i), "file_name": fmt.Sprintf("游戏%04d", i),
				"dir": true, "share_fid_token": "T"})
		}
		resp := map[string]any{"status": 200, "code": 0, "data": map[string]any{"list": list}}
		if f.withTotal {
			resp["metadata"] = map[string]any{"_total": f.n}
		}
		time.Sleep(20 * time.Millisecond) // 让几页的请求有机会同时在进行
		js(w, resp)
	})
	srv := httptest.NewServer(mux)
	old := apiBase
	apiBase = srv.URL + "/1/clouddrive"
	t.Cleanup(func() { apiBase = old; srv.Close() })
}

func openBig(t *testing.T, f *fakeBigShare) share.Session {
	t.Helper()
	f.start(t)
	store, err := account.Open(filepath.Join(t.TempDir(), "a.dat"), nil)
	if err != nil {
		t.Fatal(err)
	}
	p := New(&http.Client{Timeout: 10 * time.Second}, store)
	sess, err := p.Open(context.Background(), share.Link{Provider: share.Quark, ID: "PWD"}, "")
	if err != nil {
		t.Fatal(err)
	}
	return sess
}

func checkOrder(t *testing.T, got []share.Entry, n int) {
	t.Helper()
	if len(got) != n {
		t.Fatalf("读到 %d 项，应为 %d 项", len(got), n)
	}
	for i, e := range got {
		if want := fmt.Sprintf("F%04d", i); e.ID != want || e.Path != fmt.Sprintf("游戏%04d", i) || !e.IsDir {
			t.Fatalf("第 %d 项是 %+v，应为 %s（顺序乱了？）", i, e, want)
		}
	}
}

// 4000 多项的文件夹：第一页读完马上报告（带内容），剩下的几页同时读，顺序不乱；某一页出错会重试。
func TestListReadsPagesInParallel(t *testing.T) {
	f := &fakeBigShare{n: 4236, withTotal: true, fail: map[int]int{3: 1}, stall: map[int]int{}, requests: map[int]int{}}
	sess := openBig(t, f)

	var mu sync.Mutex
	var firstLen, firstTotal, reports, last int
	ctx := share.WithListProgress(context.Background(), func(loaded, total int, first []share.Entry) {
		mu.Lock()
		defer mu.Unlock()
		reports++
		if first != nil {
			if reports != 1 {
				t.Errorf("第一页的内容应该是第一次报告")
			}
			firstLen, firstTotal = len(first), total
		}
		if loaded > last {
			last = loaded
		}
	})
	start := time.Now()
	got, err := sess.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	checkOrder(t, got, 4236)
	if firstLen != 100 || firstTotal != 4236 || last != 4236 || reports != 43 {
		t.Fatalf("进度报告不对：第一页 %d 项 / 总数 %d，最后 %d，报告 %d 次", firstLen, firstTotal, last, reports)
	}
	if m := f.maxFlight.Load(); m < 2 || m > pageWorkers {
		t.Fatalf("同时进行的请求最多 %d 个，应在 2～%d 之间", m, pageWorkers)
	}
	if f.requests[3] != 2 {
		t.Fatalf("第 3 页请求了 %d 次，出错后应该重试一次", f.requests[3])
	}
	if len(f.requests) != 43 {
		t.Fatalf("请求了 %d 页，应为 43 页（不能多读）", len(f.requests))
	}
	t.Logf("43 页用了 %v，最多同时 %d 个请求", time.Since(start), f.maxFlight.Load())
}

// 某一页卡住不回应：超时后重试，不再整个文件夹失败。
func TestListRetriesStalledPage(t *testing.T) {
	old := pageTimeout
	pageTimeout = 300 * time.Millisecond
	defer func() { pageTimeout = old }()
	f := &fakeBigShare{n: 250, withTotal: true, fail: map[int]int{}, stall: map[int]int{2: 1}, requests: map[int]int{}}
	sess := openBig(t, f)
	got, err := sess.List(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	checkOrder(t, got, 250)
	if f.requests[2] != 2 {
		t.Fatalf("第 2 页请求了 %d 次，卡住后应该重试一次", f.requests[2])
	}
}

// 一页一直出错：试 3 次后报错，不会一直等下去。
func TestListGivesUpAfterAttempts(t *testing.T) {
	f := &fakeBigShare{n: 250, withTotal: true, fail: map[int]int{2: 10}, stall: map[int]int{}, requests: map[int]int{}}
	sess := openBig(t, f)
	if _, err := sess.List(context.Background(), ""); err == nil {
		t.Fatal("应该报错")
	}
	if f.requests[2] != pageAttempts {
		t.Fatalf("第 2 页请求了 %d 次，应为 %d 次", f.requests[2], pageAttempts)
	}
}

// 夸克没给总数时：一页一页往后读，直到某页不满。
func TestListWithoutTotal(t *testing.T) {
	f := &fakeBigShare{n: 250, fail: map[int]int{}, stall: map[int]int{}, requests: map[int]int{}}
	sess := openBig(t, f)
	got, err := sess.List(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	checkOrder(t, got, 250)
	if len(f.requests) != 3 {
		t.Fatalf("请求了 %d 页，应为 3 页", len(f.requests))
	}
}

// 正好整页（300 项）且给了总数：不多读一页空的。
func TestListExactPages(t *testing.T) {
	f := &fakeBigShare{n: 300, withTotal: true, fail: map[int]int{}, stall: map[int]int{}, requests: map[int]int{}}
	sess := openBig(t, f)
	got, err := sess.List(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	checkOrder(t, got, 300)
	if len(f.requests) != 3 {
		t.Fatalf("请求了 %d 页，应为 3 页", len(f.requests))
	}
}
