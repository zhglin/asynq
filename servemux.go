// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ServeMux is a multiplexer for asynchronous tasks.
// It matches the type of each task against a list of registered patterns
// and calls the handler for the pattern that most closely matches the
// task's type name.
//
// Longer patterns take precedence over shorter ones, so that if there are
// handlers registered for both "images" and "images:thumbnails",
// the latter handler will be called for tasks with a type name beginning with
// "images:thumbnails" and the former will receive tasks with type name beginning
// with "images".
// ServeMux是用于异步任务的多路复用器。
// 它根据一个已注册模式列表匹配每个任务的类型，并调用与任务类型名称最匹配的模式的处理程序。
// 长模式优先于短的,所以如果有处理程序注册为“images”和“images:thumbnails”,
// 后一个处理程序将被调用用于类型名以"images:thumbnails"开头的任务，而前者将接收类型名以"images"开头的任务。
type ServeMux struct {
	mu  sync.RWMutex
	m   map[string]muxEntry
	es  []muxEntry       // slice of entries sorted from longest to shortest.
	mws []MiddlewareFunc // handler的中间件
}

// handler匹配实例
type muxEntry struct {
	h       Handler
	pattern string
}

// MiddlewareFunc is a function which receives an asynq.Handler and returns another asynq.Handler.
// Typically, the returned handler is a closure which does something with the context and task passed
// to it, and then calls the handler passed as parameter to the MiddlewareFunc.
// MiddlewareFunc是一个接收asynq.Handler返回另一个asynq.Handler的函数。
// 通常，返回的处理器是一个闭包，它对传递给它的上下文和任务做一些事情，然后调用作为参数传递给MiddlewareFunc的处理器。
type MiddlewareFunc func(Handler) Handler

// NewServeMux allocates and returns a new ServeMux.
// 分配并返回一个新的ServeMux。
func NewServeMux() *ServeMux {
	return new(ServeMux)
}

// ProcessTask dispatches the task to the handler whose
// pattern most closely matches the task type.
// ProcessTask将任务分派给最匹配任务类型的处理器。
func (mux *ServeMux) ProcessTask(ctx context.Context, task *Task) error {
	h, _ := mux.Handler(task)
	return h.ProcessTask(ctx, task)
}

// Handler returns the handler to use for the given task.
// It always return a non-nil handler.
//
// Handler also returns the registered pattern that matches the task.
//
// If there is no registered handler that applies to the task,
// handler returns a 'not found' handler which returns an error.
// Handler返回给定任务的处理程序。
// 它总是返回一个非空处理器。
// Handler也返回与任务匹配的已注册模式。
// 如果没有注册的处理程序应用于该任务，handler返回一个'not found'处理程序，该处理程序返回一个错误。
func (mux *ServeMux) Handler(t *Task) (h Handler, pattern string) {
	mux.mu.RLock()
	defer mux.mu.RUnlock()

	// 不存在指定的handler，返回默认的NotFoundHandler
	h, pattern = mux.match(t.Type())
	if h == nil {
		h, pattern = NotFoundHandler(), ""
	}
	// 依次应用Middleware
	for i := len(mux.mws) - 1; i >= 0; i-- {
		h = mux.mws[i](h)
	}
	return h, pattern
}

// Find a handler on a handler map given a typename string.
// Most-specific (longest) pattern wins.
// 在给定typename字符串的处理程序映射上查找一个处理程序。
// 最特定(最长)的模式获胜。
func (mux *ServeMux) match(typename string) (h Handler, pattern string) {
	// Check for exact match first. 首先检查是否完全匹配。
	v, ok := mux.m[typename]
	if ok {
		return v.h, v.pattern
	}

	// Check for longest valid match.
	// mux.es contains all patterns from longest to shortest.
	// 检查最长的有效匹配。Mux.es包含从最长到最短的所有模式。
	for _, e := range mux.es {
		if strings.HasPrefix(typename, e.pattern) {
			return e.h, e.pattern
		}
	}
	return nil, ""

}

// Handle registers the handler for the given pattern.
// If a handler already exists for pattern, Handle panics.
// Handle为给定的模式注册处理程序。
// 重复添加pattern会panic。
func (mux *ServeMux) Handle(pattern string, handler Handler) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	if strings.TrimSpace(pattern) == "" {
		panic("asynq: invalid pattern")
	}
	if handler == nil {
		panic("asynq: nil handler")
	}
	if _, exist := mux.m[pattern]; exist {
		panic("asynq: multiple registrations for " + pattern)
	}

	// 延迟创建
	if mux.m == nil {
		mux.m = make(map[string]muxEntry)
	}
	e := muxEntry{h: handler, pattern: pattern}
	mux.m[pattern] = e
	mux.es = appendSorted(mux.es, e)
}

// 按长度顺序添加匹配规则，从长到短
func appendSorted(es []muxEntry, e muxEntry) []muxEntry {
	n := len(es)
	i := sort.Search(n, func(i int) bool {
		return len(es[i].pattern) < len(e.pattern)
	})
	if i == n {
		return append(es, e)
	}
	// we now know that i points at where we want to insert.
	es = append(es, muxEntry{}) // try to grow the slice in place, any entry works. 扩容
	copy(es[i+1:], es[i:])      // shift shorter entries down.空间不够，上面append的空元素copy不进去
	es[i] = e                   // 设置新增加的元素
	return es
}

// HandleFunc registers the handler function for the given pattern.
// 为给定的模式注册处理程序函数。
func (mux *ServeMux) HandleFunc(pattern string, handler func(context.Context, *Task) error) {
	if handler == nil {
		panic("asynq: nil handler")
	}
	mux.Handle(pattern, HandlerFunc(handler))
}

// Use appends a MiddlewareFunc to the chain.
// Middlewares are executed in the order that they are applied to the ServeMux.
// 追加一个或多个MiddlewareFunc。
// 中间件按照它们被应用到ServeMux的顺序执行。
func (mux *ServeMux) Use(mws ...MiddlewareFunc) {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	for _, fn := range mws {
		mux.mws = append(mux.mws, fn)
	}
}

// NotFound returns an error indicating that the handler was not found for the given task.
// 返回一个错误，指示没有为给定的任务找到处理程序。
func NotFound(ctx context.Context, task *Task) error {
	return fmt.Errorf("handler not found for task %q", task.Type())
}

// NotFoundHandler returns a simple task handler that returns a ``not found`` error.
// 返回一个简单的任务处理程序，该处理程序返回“未找到”错误。
func NotFoundHandler() Handler { return HandlerFunc(NotFound) }
