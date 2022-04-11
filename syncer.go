// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/log"
)

// syncer is responsible for queuing up failed requests to redis and retry
// those requests to sync state between the background process and redis.
// syncer负责将失败的请求排队到redis，然后重试这些请求，在后台进程和redis之间同步状态。
type syncer struct {
	logger *log.Logger

	// 接收的channel
	requestsCh <-chan *syncRequest

	// channel to communicate back to the long running "syncer" goroutine.
	done chan struct{}

	// interval between sync operations.
	// 间隔时间 默认5秒
	interval time.Duration
}

// 异步请求结构体
type syncRequest struct {
	fn       func() error // sync operation	操作
	errMsg   string       // error message  错误消息
	deadline time.Time    // request should be dropped if deadline has been exceeded 如果超过了截止日期，请求应该被删除
}

type syncerParams struct {
	logger     *log.Logger
	requestsCh <-chan *syncRequest
	interval   time.Duration
}

func newSyncer(params syncerParams) *syncer {
	return &syncer{
		logger:     params.logger,
		requestsCh: params.requestsCh,
		done:       make(chan struct{}),
		interval:   params.interval,
	}
}

// 停止
func (s *syncer) shutdown() {
	s.logger.Debug("Syncer shutting down...")
	// Signal the syncer goroutine to stop.
	s.done <- struct{}{}
}

func (s *syncer) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		var requests []*syncRequest // 收集request
		for {
			select {
			case <-s.done:
				// Try sync one last time before shutting down.
				// 在关闭之前尝试最后一次同步。
				for _, req := range requests {
					if err := req.fn(); err != nil {
						s.logger.Error(req.errMsg)
					}
				}
				s.logger.Debug("Syncer done")
				return
			case req := <-s.requestsCh: // 收集request
				requests = append(requests, req)
			case <-time.After(s.interval): // 定时处理收集到的请求
				var temp []*syncRequest // 失败的sync
				for _, req := range requests {
					if req.deadline.Before(time.Now()) {
						continue // drop stale request 放弃陈旧的请求
					}
					if err := req.fn(); err != nil {
						temp = append(temp, req)
					}
				}
				requests = temp // 重置成失败的request，下次一起重试
			}
		}
	}()
}
