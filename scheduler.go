// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/robfig/cron/v3"
)

// A Scheduler kicks off tasks at regular intervals based on the user defined schedule.
//
// Schedulers are safe for concurrent use by multiple goroutines.
// A Scheduler kicks off tasks at regular intervals based on the user defined schedule.
// Schedulers are safe for concurrent use by multiple goroutines.
// Scheduler根据用户定义的时间表，定期启动任务。
// 调度器可以被多个goroutines同时使用。
// Scheduler根据用户定义的时间表，定期启动任务。
// 调度器可以被多个goroutines同时使用。
type Scheduler struct {
	id string // 唯一标识

	state *serverState // 状态

	logger     *log.Logger
	client     *Client        // redis客户端
	rdb        *rdb.RDB       // 存储
	cron       *cron.Cron     // 定时任务
	location   *time.Location // 时区
	done       chan struct{}
	wg         sync.WaitGroup
	errHandler func(task *Task, opts []Option, err error)

	// guards idmap
	mu sync.Mutex
	// idmap maps Scheduler's entry ID to cron.EntryID
	// to avoid using cron.EntryID as the public API of
	// the Scheduler.
	idmap map[string]cron.EntryID
}

// NewScheduler returns a new Scheduler instance given the redis connection option.
// The parameter opts is optional, defaults will be used if opts is set to nil
func NewScheduler(r RedisConnOpt, opts *SchedulerOpts) *Scheduler {
	c, ok := r.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		panic(fmt.Sprintf("asynq: unsupported RedisConnOpt type %T", r))
	}
	if opts == nil {
		opts = &SchedulerOpts{}
	}

	logger := log.NewLogger(opts.Logger)
	loglevel := opts.LogLevel
	if loglevel == level_unspecified {
		loglevel = InfoLevel
	}
	logger.SetLevel(toInternalLogLevel(loglevel))

	loc := opts.Location
	if loc == nil {
		loc = time.UTC
	}

	return &Scheduler{
		id:         generateSchedulerID(),
		state:      &serverState{value: srvStateNew},
		logger:     logger,
		client:     NewClient(r),
		rdb:        rdb.NewRDB(c),
		cron:       cron.New(cron.WithLocation(loc)),
		location:   loc,
		done:       make(chan struct{}),
		errHandler: opts.EnqueueErrorHandler,
		idmap:      make(map[string]cron.EntryID),
	}
}

// scheduler标识
func generateSchedulerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s:%d:%v", host, os.Getpid(), uuid.New())
}

// SchedulerOpts specifies scheduler options.
// 配置项
type SchedulerOpts struct {
	// Logger specifies the logger used by the scheduler instance.
	//
	// If unset, the default logger is used.
	Logger Logger

	// LogLevel specifies the minimum log level to enable.
	//
	// If unset, InfoLevel is used by default.
	LogLevel LogLevel

	// Location specifies the time zone location.
	//
	// If unset, the UTC time zone (time.UTC) is used.
	// Location时区位置。
	// 如果不设置，则使用UTC时区(time.UTC)。
	Location *time.Location

	// EnqueueErrorHandler gets called when scheduler cannot enqueue a registered task
	// due to an error.
	// 当调度程序由于错误无法对已注册的任务进行排队时，调用EnqueueErrorHandler。
	EnqueueErrorHandler func(task *Task, opts []Option, err error)
}

// enqueueJob encapsulates the job of enqueing a task and recording the event.
// enqueueJob封装了对任务进行排队和记录事件的任务。
type enqueueJob struct {
	id         uuid.UUID // 生成的id  一个cron任务的id是一样的
	cronspec   string
	task       *Task
	opts       []Option
	location   *time.Location
	logger     *log.Logger
	client     *Client
	rdb        *rdb.RDB
	errHandler func(task *Task, opts []Option, err error)
}

// Run cron调用
func (j *enqueueJob) Run() {
	info, err := j.client.Enqueue(j.task, j.opts...)
	if err != nil {
		j.logger.Errorf("scheduler could not enqueue a task %+v: %v", j.task, err)
		if j.errHandler != nil {
			j.errHandler(j.task, j.opts, err)
		}
		return
	}
	// 记录调度的日志
	j.logger.Debugf("scheduler enqueued a task: %+v", info)
	event := &base.SchedulerEnqueueEvent{
		TaskID:     info.ID,
		EnqueuedAt: time.Now().In(j.location),
	}
	err = j.rdb.RecordSchedulerEnqueueEvent(j.id.String(), event)
	if err != nil {
		j.logger.Errorf("scheduler could not record enqueue event of enqueued task %+v: %v", j.task, err)
	}
}

// Register registers a task to be enqueued on the given schedule specified by the cronspec.
// It returns an ID of the newly registered entry.
// Register将一个任务注册到cronspec指定的时间队列中。
// 返回新注册条目的ID。
func (s *Scheduler) Register(cronspec string, task *Task, opts ...Option) (entryID string, err error) {
	job := &enqueueJob{
		id:         uuid.New(),
		cronspec:   cronspec,
		task:       task,
		opts:       opts,
		location:   s.location,
		client:     s.client,
		rdb:        s.rdb,
		logger:     s.logger,
		errHandler: s.errHandler,
	}
	cronID, err := s.cron.AddJob(cronspec, job)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.idmap[job.id.String()] = cronID
	s.mu.Unlock()
	return job.id.String(), nil
}

// Unregister removes a registered entry by entry ID.
// Unregister returns a non-nil error if no entries were found for the given entryID.
// Unregister通过条目ID删除已注册条目。
// Unregister返回一个非nil错误，如果没有找到给定的entryID的条目。
func (s *Scheduler) Unregister(entryID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cronID, ok := s.idmap[entryID]
	if !ok {
		return fmt.Errorf("asynq: no scheduler entry found")
	}
	delete(s.idmap, entryID)
	s.cron.Remove(cronID)
	return nil
}

// Run starts the scheduler until an os signal to exit the program is received.
// It returns an error if scheduler is already running or has been shutdown.
// 运行调度器，直到接收到退出程序的os信号。
// 如果调度程序已经运行或已经关闭，它返回一个错误。
func (s *Scheduler) Run() error {
	if err := s.Start(); err != nil {
		return err
	}
	s.waitForSignals()
	s.Shutdown()
	return nil
}

// Start starts the scheduler.
// It returns an error if the scheduler is already running or has been shutdown.
// Start启动调度程序。
// 如果调度程序已经运行或已经关闭，则返回一个错误。
func (s *Scheduler) Start() error {
	if err := s.start(); err != nil {
		return err
	}
	s.logger.Info("Scheduler starting")
	s.logger.Infof("Scheduler timezone is set to %v", s.location)
	s.cron.Start() // 启动cron
	s.wg.Add(1)
	go s.runHeartbeater() // 同步状态到rdb
	return nil
}

// Checks server state and returns an error if pre-condition is not met.
// Otherwise it sets the server state to active.
// 检查服务器状态，如果不满足先决条件，返回一个错误。
// 否则，它设置服务器状态为活动。
func (s *Scheduler) start() error {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	switch s.state.value {
	case srvStateActive:
		return fmt.Errorf("asynq: the scheduler is already running")
	case srvStateClosed:
		return fmt.Errorf("asynq: the scheduler has already been stopped")
	}
	s.state.value = srvStateActive
	return nil
}

// Shutdown stops and shuts down the scheduler.
// 停止并关闭调度程序。
func (s *Scheduler) Shutdown() {
	s.state.mu.Lock()
	if s.state.value == srvStateNew || s.state.value == srvStateClosed {
		// scheduler is not running, do nothing and return.
		s.state.mu.Unlock()
		return
	}
	s.state.value = srvStateClosed
	s.state.mu.Unlock()

	s.logger.Info("Scheduler shutting down")
	close(s.done)        // signal heartbeater to stop
	ctx := s.cron.Stop() // 停止cron
	<-ctx.Done()
	s.wg.Wait() // wait

	s.clearHistory() // 清空调度日志
	s.client.Close()
	s.rdb.Close()
	s.logger.Info("Scheduler stopped")
}

// 同步状态
func (s *Scheduler) runHeartbeater() {
	defer s.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-s.done:
			s.logger.Debugf("Scheduler heatbeater shutting down")
			s.rdb.ClearSchedulerEntries(s.id) // 删除
			return
		case <-ticker.C:
			s.beat() // 写入cron信息
		}
	}
}

// beat writes a snapshot of entries to redis.
// 将cron条目的快照写入redis。
func (s *Scheduler) beat() {
	var entries []*base.SchedulerEntry
	for _, entry := range s.cron.Entries() {
		job := entry.Job.(*enqueueJob)
		e := &base.SchedulerEntry{
			ID:      job.id.String(),
			Spec:    job.cronspec,
			Type:    job.task.Type(),
			Payload: job.task.Payload(),
			Opts:    stringifyOptions(job.opts),
			Next:    entry.Next,
			Prev:    entry.Prev,
		}
		entries = append(entries, e)
	}
	s.logger.Debugf("Writing entries %v", entries)
	if err := s.rdb.WriteSchedulerEntries(s.id, entries, 5*time.Second); err != nil {
		s.logger.Warnf("Scheduler could not write heartbeat data: %v", err)
	}
}

func stringifyOptions(opts []Option) []string {
	var res []string
	for _, opt := range opts {
		res = append(res, opt.String())
	}
	return res
}

// 删除调度日志
func (s *Scheduler) clearHistory() {
	for _, entry := range s.cron.Entries() {
		job := entry.Job.(*enqueueJob)
		if err := s.rdb.ClearSchedulerHistory(job.id.String()); err != nil {
			s.logger.Warnf("Could not clear scheduler history for entry %q: %v", job.id.String(), err)
		}
	}
}
