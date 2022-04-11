// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

// 订阅取消任务队列，进行任务取消
type subscriber struct {
	logger *log.Logger
	broker base.Broker

	// channel to communicate back to the long running "subscriber" goroutine.
	done chan struct{}

	// cancelations hold cancel functions for all active tasks.
	// 所有active的task的取消函数
	cancelations *base.Cancelations

	// time to wait before retrying to connect to redis.
	// 重新连接redis之前的等待时间。
	retryTimeout time.Duration
}

type subscriberParams struct {
	logger       *log.Logger
	broker       base.Broker
	cancelations *base.Cancelations
}

func newSubscriber(params subscriberParams) *subscriber {
	return &subscriber{
		logger:       params.logger,
		broker:       params.broker,
		done:         make(chan struct{}),
		cancelations: params.cancelations,
		retryTimeout: 5 * time.Second,
	}
}

func (s *subscriber) shutdown() {
	s.logger.Debug("Subscriber shutting down...")
	// Signal the subscriber goroutine to stop.
	s.done <- struct{}{}
}

func (s *subscriber) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		var (
			pubsub *redis.PubSub
			err    error
		)
		// Try until successfully connect to Redis.
		// 尝试直到成功连接Redis。进行消息订阅
		for {
			pubsub, err = s.broker.CancelationPubSub()
			if err != nil {
				s.logger.Errorf("cannot subscribe to cancelation channel: %v", err)
				select {
				case <-time.After(s.retryTimeout):
					continue
				case <-s.done:
					s.logger.Debug("Subscriber done")
					return
				}
			}
			break
		}
		cancelCh := pubsub.Channel()
		for {
			select {
			case <-s.done:
				pubsub.Close()
				s.logger.Debug("Subscriber done")
				return
			case msg := <-cancelCh: // 从队列中获取消息
				cancel, ok := s.cancelations.Get(msg.Payload)
				if ok {
					cancel() // 取消正在执行的消息
				}
			}
		}
	}()
}
