// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
)

// 对租约过期的task进行回收
// 租约过期说明server已经收到消息但是未处理完成并且不在处理。server异常下线
type recoverer struct {
	logger *log.Logger
	broker base.Broker
	// 重试时间计算函数
	retryDelayFunc RetryDelayFunc
	// 用户层校验task是否处理失败的函数
	isFailureFunc func(error) bool

	// channel to communicate back to the long running "recoverer" goroutine.
	done chan struct{}

	// list of queues to check for deadline.
	// 需要进行recover的队列名
	queues []string

	// poll interval.
	// 间隔时间 默认1分钟
	interval time.Duration
}

type recovererParams struct {
	logger         *log.Logger
	broker         base.Broker
	queues         []string
	interval       time.Duration
	retryDelayFunc RetryDelayFunc
	isFailureFunc  func(error) bool
}

func newRecoverer(params recovererParams) *recoverer {
	return &recoverer{
		logger:         params.logger,
		broker:         params.broker,
		done:           make(chan struct{}),
		queues:         params.queues,
		interval:       params.interval,
		retryDelayFunc: params.retryDelayFunc,
		isFailureFunc:  params.isFailureFunc,
	}
}

func (r *recoverer) shutdown() {
	r.logger.Debug("Recoverer shutting down...")
	// Signal the recoverer goroutine to stop polling.
	r.done <- struct{}{}
}

func (r *recoverer) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.recover() // 定时器之前先执行一次
		timer := time.NewTimer(r.interval)
		for {
			select {
			case <-r.done:
				r.logger.Debug("Recoverer done")
				timer.Stop()
				return
			case <-timer.C:
				r.recover()
				timer.Reset(r.interval)
			}
		}
	}()
}

// ErrLeaseExpired error indicates that the task failed because the worker working on the task
// could not extend its lease due to missing heartbeats. The worker may have crashed or got cutoff from the network.
// 表示任务失败，因为工作在该任务上的worker由于心跳缺失无法延长其租期。工作人员可能已经崩溃或被切断了网络。
var ErrLeaseExpired = errors.New("asynq: task lease expired")

func (r *recoverer) recover() {
	// Get all tasks which have expired 30 seconds ago or earlier to accomodate certain amount of clock skew.
	// 获取30秒前或更早过期的所有任务，以适应一定程度的时钟偏差。
	cutoff := time.Now().Add(-30 * time.Second) // 30秒之前的时间戳
	msgs, err := r.broker.ListLeaseExpired(cutoff, r.queues...)
	if err != nil {
		r.logger.Warn("recoverer: could not list lease expired tasks")
		return
	}

	// 对租约过期的task进行重试或者归档
	for _, msg := range msgs {
		if msg.Retried >= msg.Retry {
			r.archive(msg, ErrLeaseExpired)
		} else {
			r.retry(msg, ErrLeaseExpired)
		}
	}
}

// 重试
func (r *recoverer) retry(msg *base.TaskMessage, err error) {
	delay := r.retryDelayFunc(msg.Retried, err, NewTask(msg.Type, msg.Payload))
	retryAt := time.Now().Add(delay)
	if err := r.broker.Retry(context.Background(), msg, retryAt, err.Error(), r.isFailureFunc(err)); err != nil {
		r.logger.Warnf("recoverer: could not retry lease expired task: %v", err)
	}
}

// 归档
func (r *recoverer) archive(msg *base.TaskMessage, err error) {
	if err := r.broker.Archive(context.Background(), msg, err.Error()); err != nil {
		r.logger.Warnf("recoverer: could not move task to archive: %v", err)
	}
}
