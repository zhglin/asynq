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

// healthchecker is responsible for pinging broker periodically
// and call user provided HeathCheckFunc with the ping result.
// healthchecker负责定期ping代理，并调用用户提供的HeathCheckFunc的ping结果。
type healthchecker struct {
	logger *log.Logger
	broker base.Broker

	// channel to communicate back to the long running "healthchecker" goroutine.
	done chan struct{}

	// interval between healthchecks.
	// 心跳检查的间隔时间
	interval time.Duration

	// function to call periodically.
	// 心跳检查函数
	healthcheckFunc func(error)
}

// 参数
type healthcheckerParams struct {
	logger          *log.Logger
	broker          base.Broker
	interval        time.Duration
	healthcheckFunc func(error)
}

// 根据参数构建healthchecker实例
func newHealthChecker(params healthcheckerParams) *healthchecker {
	return &healthchecker{
		logger:          params.logger,
		broker:          params.broker,
		done:            make(chan struct{}),
		interval:        params.interval,
		healthcheckFunc: params.healthcheckFunc,
	}
}

// 停止
func (hc *healthchecker) shutdown() {
	if hc.healthcheckFunc == nil {
		return
	}

	hc.logger.Debug("Healthchecker shutting down...")
	// Signal the healthchecker goroutine to stop.
	// 通知健康检查程序goroutine停止。
	hc.done <- struct{}{}
}

// 开启
func (hc *healthchecker) start(wg *sync.WaitGroup) {
	if hc.healthcheckFunc == nil {
		return
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		timer := time.NewTimer(hc.interval) // 定时器
		for {
			select {
			case <-hc.done:
				hc.logger.Debug("Healthchecker done")
				timer.Stop()
				return
			case <-timer.C:
				err := hc.broker.Ping() // broker的ping函数
				hc.healthcheckFunc(err) // ping结果传递给healthcheckFunc
				timer.Reset(hc.interval)
			}
		}
	}()
}
