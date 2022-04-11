// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

// A forwarder is responsible for moving scheduled and retry tasks to pending state
// so that the tasks get processed by the workers.
// 转发器负责将scheduled任务和retry任务转移到pending状态，以便由worker处理这些任务。
type forwarder struct {
	logger *log.Logger
	broker base.Broker

	// channel to communicate back to the long running "forwarder" goroutine.
	done chan struct{}

	// list of queue names to check and enqueue.
	// 要检查和排队的队列名称列表。
	queues []string

	// poll interval on average
	// 平均轮询间隔
	avgInterval time.Duration
}

type forwarderParams struct {
	logger   *log.Logger
	broker   base.Broker
	queues   []string
	interval time.Duration
}

func newForwarder(params forwarderParams) *forwarder {
	return &forwarder{
		logger:      params.logger,
		broker:      params.broker,
		done:        make(chan struct{}),
		queues:      params.queues,
		avgInterval: params.interval,
	}
}

func (f *forwarder) shutdown() {
	f.logger.Debug("Forwarder shutting down...")
	// Signal the forwarder goroutine to stop polling.
	f.done <- struct{}{}
}

// start starts the "forwarder" goroutine.
func (f *forwarder) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-f.done:
				f.logger.Debug("Forwarder done")
				return
			case <-time.After(f.avgInterval):
				f.exec()
			}
		}
	}()
}

// 执行状态转换
func (f *forwarder) exec() {
	if err := f.broker.ForwardIfReady(f.queues...); err != nil {
		f.logger.Errorf("Failed to forward scheduled tasks: %v", err)
	}
}
