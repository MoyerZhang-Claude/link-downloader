package share

import "context"

// ListProgressFunc 报告读取大文件夹的进度（一个文件夹要分很多页读取时）：
// loaded 是已经读到的条数，total 是总条数（网盘没给时为 0）；
// first 只在第一次报告时给出，是第一页的内容，界面可以先显示出来，不用等全部读完。
// 可能从多个 goroutine 同时调用。
type ListProgressFunc func(loaded, total int, first []Entry)

type listProgressKey struct{}

// WithListProgress 让 ctx 带上进度回调，Session.List 读到多页时会调用它。
func WithListProgress(ctx context.Context, f ListProgressFunc) context.Context {
	return context.WithValue(ctx, listProgressKey{}, f)
}

// ReportListProgress 报告读取进度；ctx 里没有回调时什么也不做。
func ReportListProgress(ctx context.Context, loaded, total int, first []Entry) {
	if f, _ := ctx.Value(listProgressKey{}).(ListProgressFunc); f != nil {
		f(loaded, total, first)
	}
}
