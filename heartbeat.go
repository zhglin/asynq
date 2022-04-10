// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/timeutil"
)

// heartbeater is responsible for writing process info to redis periodically to
// indicate that the background worker process is up.
// heartbeater负责定期向redis写入进程信息，以表明后台工作进程已启动。
type heartbeater struct {
	logger *log.Logger
	broker base.Broker    // broker实例
	clock  timeutil.Clock // 时钟

	// channel to communicate back to the long running "heartbeater" goroutine.
	// 关闭通知
	done chan struct{}

	// interval between heartbeats.
	// 间隔时间
	interval time.Duration

	// following fields are initialized at construction time and are immutable.
	// 以下字段在构造时初始化，并且是不可变的。
	host           string         // 当前节点Hostname
	pid            int            // 进程id
	serverID       string         // 生成的唯一id
	concurrency    int            // 并发度
	queues         map[string]int // 队列优先级
	strictPriority bool           // 是否应该严格对待队列优先级

	// following fields are mutable and should be accessed only by the
	// heartbeater goroutine. In other words, confine these variables
	// to this goroutine only.
	// 下面的字段是可变的，应该只被heartbeater goroutine访问。换句话说，只将这些变量限制在这个goroutine例程中。
	started time.Time // 启动时间
	workers map[string]*workerInfo

	// state is shared with other goroutine but is concurrency safe.
	// state与其他goroutine共享，但是并发安全。
	state *serverState

	// channels to receive updates on active workers.
	// 接收当前workers更新的通道。
	starting <-chan *workerInfo
	finished <-chan *base.TaskMessage
}

// 构建heartbeater的参数
type heartbeaterParams struct {
	logger         *log.Logger
	broker         base.Broker
	interval       time.Duration
	concurrency    int
	queues         map[string]int
	strictPriority bool
	state          *serverState
	starting       <-chan *workerInfo
	finished       <-chan *base.TaskMessage
}

func newHeartbeater(params heartbeaterParams) *heartbeater {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}

	return &heartbeater{
		logger:   params.logger,
		broker:   params.broker,
		clock:    timeutil.NewRealClock(),
		done:     make(chan struct{}),
		interval: params.interval,

		host:           host,
		pid:            os.Getpid(),
		serverID:       uuid.New().String(),
		concurrency:    params.concurrency,
		queues:         params.queues,
		strictPriority: params.strictPriority,

		state:    params.state,
		workers:  make(map[string]*workerInfo),
		starting: params.starting,
		finished: params.finished,
	}
}

// 终止
func (h *heartbeater) shutdown() {
	h.logger.Debug("Heartbeater shutting down...")
	// Signal the heartbeater goroutine to stop.
	h.done <- struct{}{}
}

// A workerInfo holds an active worker information.
type workerInfo struct {
	// the task message the worker is processing.
	msg *base.TaskMessage
	// the time the worker has started processing the message.
	started time.Time
	// deadline the worker has to finish processing the task by.
	deadline time.Time
	// lease the worker holds for the task.
	lease *base.Lease
}

func (h *heartbeater) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		h.started = h.clock.Now()

		h.beat()

		timer := time.NewTimer(h.interval)
		for {
			select {
			case <-h.done:
				h.broker.ClearServerState(h.host, h.pid, h.serverID)
				h.logger.Debug("Heartbeater done")
				timer.Stop()
				return

			case <-timer.C:
				h.beat()
				timer.Reset(h.interval)

			case w := <-h.starting:
				h.workers[w.msg.ID] = w

			case msg := <-h.finished:
				delete(h.workers, msg.ID)
			}
		}
	}()
}

// beat extends lease for workers and writes server/worker info to redis.
func (h *heartbeater) beat() {
	h.state.mu.Lock()
	srvStatus := h.state.value.String()
	h.state.mu.Unlock()

	info := base.ServerInfo{
		Host:              h.host,
		PID:               h.pid,
		ServerID:          h.serverID,
		Concurrency:       h.concurrency,
		Queues:            h.queues,
		StrictPriority:    h.strictPriority,
		Status:            srvStatus,
		Started:           h.started,
		ActiveWorkerCount: len(h.workers),
	}

	var ws []*base.WorkerInfo
	idsByQueue := make(map[string][]string)
	for id, w := range h.workers {
		ws = append(ws, &base.WorkerInfo{
			Host:     h.host,
			PID:      h.pid,
			ServerID: h.serverID,
			ID:       id,
			Type:     w.msg.Type,
			Queue:    w.msg.Queue,
			Payload:  w.msg.Payload,
			Started:  w.started,
			Deadline: w.deadline,
		})
		// Check lease before adding to the set to make sure not to extend the lease if the lease is already expired.
		if w.lease.IsValid() {
			idsByQueue[w.msg.Queue] = append(idsByQueue[w.msg.Queue], id)
		} else {
			w.lease.NotifyExpiration() // notify processor if the lease is expired
		}
	}

	// Note: Set TTL to be long enough so that it won't expire before we write again
	// and short enough to expire quickly once the process is shut down or killed.
	if err := h.broker.WriteServerState(&info, ws, h.interval*2); err != nil {
		h.logger.Errorf("Failed to write server state data: %v", err)
	}

	for qname, ids := range idsByQueue {
		expirationTime, err := h.broker.ExtendLease(qname, ids...)
		if err != nil {
			h.logger.Errorf("Failed to extend lease for tasks %v: %v", ids, err)
			continue
		}
		for _, id := range ids {
			if l := h.workers[id].lease; !l.Reset(expirationTime) {
				h.logger.Warnf("Lease reset failed for %s; lease deadline: %v", id, l.Deadline())
			}
		}
	}
}
