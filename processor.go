// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	asynqcontext "github.com/hibiken/asynq/internal/context"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/timeutil"
	"golang.org/x/time/rate"
)

type processor struct {
	logger *log.Logger
	broker base.Broker
	clock  timeutil.Clock

	handler   Handler                // task处理函数
	baseCtxFn func() context.Context // 上下文

	// 队列优先级
	queueConfig map[string]int

	// orderedQueues is set only in strict-priority mode.
	// 只能在strict-priority模式下配置。 按优先级排好的队列名
	orderedQueues []string

	// 计算重试时间函数
	retryDelayFunc RetryDelayFunc
	// 判断Handler返回的错误是否为失败的谓词函数。业务是否处理失败
	isFailureFunc func(error) bool

	// 接收并处理task消费异常
	errHandler ErrorHandler

	// 停止时的延后时间
	shutdownTimeout time.Duration

	// channel via which to send sync requests to syncer.
	// 发送异步请求的通道。
	syncRequestCh chan<- *syncRequest

	// rate limiter to prevent spamming logs with a bunch of errors.
	// 速率限制，以防止垃圾日志与一堆错误。
	errLogLimiter *rate.Limiter

	// sema is a counting semaphore to ensure the number of active workers
	// does not exceed the limit.
	// sema是一个计数信号量，以确保活动的工人的数量不超过限制。带缓冲
	sema chan struct{}

	// channel to communicate back to the long running "processor" goroutine.
	// once is used to send value to the channel only once.
	done chan struct{}
	once sync.Once

	// quit channel is closed when the shutdown of the "processor" goroutine starts.
	// 退出processor的task消费
	quit chan struct{}

	// abort channel communicates to the in-flight worker goroutines to stop.
	// 中止
	abort chan struct{}

	// cancelations is a set of cancel functions for all active tasks.
	// 是一组用于所有活动任务的取消函数。
	cancelations *base.Cancelations

	// task开始处理的通知
	starting chan<- *workerInfo
	// task成功处理完成的通知
	finished chan<- *base.TaskMessage
}

type processorParams struct {
	logger          *log.Logger
	broker          base.Broker
	baseCtxFn       func() context.Context
	retryDelayFunc  RetryDelayFunc
	isFailureFunc   func(error) bool
	syncCh          chan<- *syncRequest
	cancelations    *base.Cancelations
	concurrency     int
	queues          map[string]int
	strictPriority  bool // 是否严格按队列优先级消费
	errHandler      ErrorHandler
	shutdownTimeout time.Duration
	starting        chan<- *workerInfo
	finished        chan<- *base.TaskMessage
}

// newProcessor constructs a new processor.
func newProcessor(params processorParams) *processor {
	queues := normalizeQueues(params.queues)
	orderedQueues := []string(nil)
	// 严格按优先级
	if params.strictPriority {
		orderedQueues = sortByPriority(queues)
	}
	return &processor{
		logger:          params.logger,
		broker:          params.broker,
		baseCtxFn:       params.baseCtxFn,
		clock:           timeutil.NewRealClock(),
		queueConfig:     queues,
		orderedQueues:   orderedQueues,
		retryDelayFunc:  params.retryDelayFunc,
		isFailureFunc:   params.isFailureFunc,
		syncRequestCh:   params.syncCh,
		cancelations:    params.cancelations,
		errLogLimiter:   rate.NewLimiter(rate.Every(3*time.Second), 1),
		sema:            make(chan struct{}, params.concurrency),
		done:            make(chan struct{}),
		quit:            make(chan struct{}),
		abort:           make(chan struct{}),
		errHandler:      params.errHandler,
		handler:         HandlerFunc(func(ctx context.Context, t *Task) error { return fmt.Errorf("handler not set") }),
		shutdownTimeout: params.shutdownTimeout,
		starting:        params.starting,
		finished:        params.finished,
	}
}

// Note: stops only the "processor" goroutine, does not stop workers.
// It's safe to call this method multiple times.
// 注意:只停止“processor”的goroutine，不停止workers。多次调用这个方法是安全的。
// 停止消费redis中任务
func (p *processor) stop() {
	p.once.Do(func() {
		p.logger.Debug("Processor shutting down...")
		// Unblock if processor is waiting for sema token.
		close(p.quit)
		// Signal the processor goroutine to stop processing tasks
		// from the queue.
		p.done <- struct{}{}
	})
}

// NOTE: once shutdown, processor cannot be re-started.
// 关闭 等待当前active的task处理完
func (p *processor) shutdown() {
	p.stop()

	// 等待
	time.AfterFunc(p.shutdownTimeout, func() { close(p.abort) })

	p.logger.Info("Waiting for all workers to finish...")
	// block until all workers have released the token
	// 阻塞，直到所有worker释放token
	for i := 0; i < cap(p.sema); i++ {
		p.sema <- struct{}{}
	}
	p.logger.Info("All workers have finished")
}

func (p *processor) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-p.done:
				p.logger.Debug("Processor done")
				return
			default:
				p.exec()
			}
		}
	}()
}

// exec pulls a task out of the queue and starts a worker goroutine to
// process the task.
// 从队列中取出一个任务，并启动一个worker goroutine来处理该任务。
func (p *processor) exec() {
	select {
	case <-p.quit:
		return
	case p.sema <- struct{}{}: // acquire token 获得令牌
		// 从所有队列中获取task
		qnames := p.queues()
		msg, leaseExpirationTime, err := p.broker.Dequeue(qnames...)
		switch {
		case errors.Is(err, errors.ErrNoProcessableTask):
			p.logger.Debug("All queues are empty")
			// Queues are empty, this is a normal behavior.
			// Sleep to avoid slamming redis and let scheduler move tasks into queues.
			// Note: We are not using blocking pop operation and polling queues instead.
			// This adds significant load to redis.
			// 队列是空的，这是正常的行为。
			// Sleep避免关闭redis，让调度程序将任务移动到队列中。
			// 注意:我们不使用阻塞pop操作和轮询队列代替。
			// 这将增加redis的显著负载。
			time.Sleep(time.Second)
			<-p.sema // release token 释放令牌
			return
		case err != nil: // 限制日志记录速率
			if p.errLogLimiter.Allow() {
				p.logger.Errorf("Dequeue error: %v", err)
			}
			<-p.sema // release token 释放令牌
			return
		}

		// 租约 30秒
		lease := base.NewLease(leaseExpirationTime)
		// 任务截止时间
		deadline := p.computeDeadline(msg)
		// 写入开始处理任务
		p.starting <- &workerInfo{msg, time.Now(), deadline, lease}
		go func() {
			defer func() {
				p.finished <- msg
				<-p.sema // release token 释放令牌
			}()

			// 构建上下文
			ctx, cancel := asynqcontext.New(p.baseCtxFn(), msg, deadline)
			// 添加到task取消函数中
			p.cancelations.Add(msg.ID, cancel)
			defer func() {
				cancel()
				p.cancelations.Delete(msg.ID)
			}()

			// check context before starting a worker goroutine.
			// 在启动一个worker gorroutine之前检查上下文。
			select {
			case <-ctx.Done():
				// already canceled (e.g. deadline exceeded).
				// 已经取消(例如超时)。
				p.handleFailedMessage(ctx, lease, msg, ctx.Err())
				return
			default:
			}

			resCh := make(chan error, 1) // task处理结果
			go func() {
				task := newTask(
					msg.Type,
					msg.Payload,
					&ResultWriter{
						id:     msg.ID,
						qname:  msg.Queue,
						broker: p.broker,
						ctx:    ctx,
					},
				)
				resCh <- p.perform(ctx, task) // 处理task
			}()

			select {
			case <-p.abort: // shutdown
				// time is up, push the message back to queue and quit this worker goroutine.
				// 关闭等待时间到了，将消息推回队列并退出这个worker goroutine。
				p.logger.Warnf("Quitting worker. task id=%s", msg.ID)
				p.requeue(lease, msg)
				return
			case <-lease.Done(): // lease
				cancel()
				p.handleFailedMessage(ctx, lease, msg, ErrLeaseExpired)
				return
			case <-ctx.Done():
				p.handleFailedMessage(ctx, lease, msg, ctx.Err())
				return
			case resErr := <-resCh: // 业务处理失败
				if resErr != nil {
					p.handleFailedMessage(ctx, lease, msg, resErr)
					return
				}
				// task处理成功
				p.handleSucceededMessage(lease, msg)
			}
		}()
	}
}

// task重新写入队列
func (p *processor) requeue(l *base.Lease, msg *base.TaskMessage) {
	if !l.IsValid() {
		// If lease is not valid, do not write to redis; Let recoverer take care of it.
		return
	}
	ctx, _ := context.WithDeadline(context.Background(), l.Deadline())
	err := p.broker.Requeue(ctx, msg) // 重新写入pending队列
	if err != nil {
		p.logger.Errorf("Could not push task id=%s back to queue: %v", msg.ID, err)
	} else {
		p.logger.Infof("Pushed task id=%s back to queue", msg.ID)
	}
}

// task成功处理
func (p *processor) handleSucceededMessage(l *base.Lease, msg *base.TaskMessage) {
	if msg.Retention > 0 {
		p.markAsComplete(l, msg)
	} else {
		p.markAsDone(l, msg)
	}
}

// 记录成功信息
func (p *processor) markAsComplete(l *base.Lease, msg *base.TaskMessage) {
	if !l.IsValid() {
		// If lease is not valid, do not write to redis; Let recoverer take care of it.
		return
	}
	ctx, _ := context.WithDeadline(context.Background(), l.Deadline())
	err := p.broker.MarkAsComplete(ctx, msg)
	if err != nil {
		errMsg := fmt.Sprintf("Could not move task id=%s type=%q from %q to %q:  %+v",
			msg.ID, msg.Type, base.ActiveKey(msg.Queue), base.CompletedKey(msg.Queue), err)
		p.logger.Warnf("%s; Will retry syncing", errMsg)
		// 异步记录
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return p.broker.MarkAsComplete(ctx, msg)
			},
			errMsg:   errMsg,
			deadline: l.Deadline(),
		}
	}
}

// 处理成功，清理数据
func (p *processor) markAsDone(l *base.Lease, msg *base.TaskMessage) {
	if !l.IsValid() {
		// If lease is not valid, do not write to redis; Let recoverer take care of it.
		return
	}
	ctx, _ := context.WithDeadline(context.Background(), l.Deadline())
	err := p.broker.Done(ctx, msg)
	if err != nil {
		errMsg := fmt.Sprintf("Could not remove task id=%s type=%q from %q err: %+v", msg.ID, msg.Type, base.ActiveKey(msg.Queue), err)
		p.logger.Warnf("%s; Will retry syncing", errMsg)
		// 异步处理
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return p.broker.Done(ctx, msg)
			},
			errMsg:   errMsg,
			deadline: l.Deadline(),
		}
	}
}

// SkipRetry is used as a return value from Handler.ProcessTask to indicate that
// the task should not be retried and should be archived instead.
// SkipRetry被用作Handler的返回值。processstask，指示该任务不应该重试，而是应该被归档。
var SkipRetry = errors.New("skip retry for the task")

// task消息失败或者超时
func (p *processor) handleFailedMessage(ctx context.Context, l *base.Lease, msg *base.TaskMessage, err error) {
	// 异常处理函数
	if p.errHandler != nil {
		p.errHandler.HandleError(ctx, NewTask(msg.Type, msg.Payload), err)
	}
	// 校验err是否是task处理失败 非业务处理失败 重试
	if !p.isFailureFunc(err) {
		// retry the task without marking it as failed
		// 重试任务而不标记为失败
		p.retry(l, msg, err, false /*isFailure*/)
		return
	}
	// 处理失败 超过重试次数 或者跳过重试
	if msg.Retried >= msg.Retry || errors.Is(err, SkipRetry) {
		p.logger.Warnf("Retry exhausted for task id=%s", msg.ID)
		p.archive(l, msg, err) // 存档
	} else {
		p.retry(l, msg, err, true /*isFailure*/) //重试
	}
}

// 重试
func (p *processor) retry(l *base.Lease, msg *base.TaskMessage, e error, isFailure bool) {
	if !l.IsValid() {
		// If lease is not valid, do not write to redis; Let recoverer take care of it.
		// 如果lease是无效的，不要写redis;让recoverer来处理吧。
		return
	}
	ctx, _ := context.WithDeadline(context.Background(), l.Deadline())
	// 计算重试时间
	d := p.retryDelayFunc(msg.Retried, e, NewTask(msg.Type, msg.Payload))
	retryAt := time.Now().Add(d)
	// 写入重试队列
	err := p.broker.Retry(ctx, msg, retryAt, e.Error(), isFailure)
	if err != nil {
		errMsg := fmt.Sprintf("Could not move task id=%s from %q to %q", msg.ID, base.ActiveKey(msg.Queue), base.RetryKey(msg.Queue))
		p.logger.Warnf("%s; Will retry syncing", errMsg)
		// 写入失败，异步重试
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return p.broker.Retry(ctx, msg, retryAt, e.Error(), isFailure)
			},
			errMsg:   errMsg,
			deadline: l.Deadline(),
		}
	}
}

// 失败task进行归档
func (p *processor) archive(l *base.Lease, msg *base.TaskMessage, e error) {
	if !l.IsValid() {
		// If lease is not valid, do not write to redis; Let recoverer take care of it.
		return
	}
	ctx, _ := context.WithDeadline(context.Background(), l.Deadline())
	err := p.broker.Archive(ctx, msg, e.Error())
	if err != nil {
		errMsg := fmt.Sprintf("Could not move task id=%s from %q to %q", msg.ID, base.ActiveKey(msg.Queue), base.ArchivedKey(msg.Queue))
		p.logger.Warnf("%s; Will retry syncing", errMsg)
		// 异步处理
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return p.broker.Archive(ctx, msg, e.Error())
			},
			errMsg:   errMsg,
			deadline: l.Deadline(),
		}
	}
}

// queues returns a list of queues to query.
// Order of the queue names is based on the priority of each queue.
// Queue names is sorted by their priority level if strict-priority is true.
// If strict-priority is false, then the order of queue names are roughly based on
// the priority level but randomized in order to avoid starving low priority queues.
// queues返回要查询的队列列表。
// 队列名的顺序是基于每个队列的优先级。
// 如果strict-priority为true，则按队列的优先级排序。
// 如果strict-priority为false，则队列名的顺序大致基于优先级，但为了避免低优先级队列挨饿，队列名的顺序是随机的。
func (p *processor) queues() []string {
	// skip the overhead of generating a list of queue names
	// if we are processing one queue.
	// 如果我们正在处理一个队列，则跳过生成一个队列名称列表的开销。
	if len(p.queueConfig) == 1 {
		for qname := range p.queueConfig {
			return []string{qname}
		}
	}
	if p.orderedQueues != nil {
		return p.orderedQueues
	}
	var names []string
	for qname, priority := range p.queueConfig {
		for i := 0; i < priority; i++ {
			names = append(names, qname)
		}
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
	return uniq(names, len(p.queueConfig))
}

// perform calls the handler with the given task.
// If the call returns without panic, it simply returns the value,
// otherwise, it recovers from panic and returns an error.
// execute调用给定任务的处理程序。
// 如果调用没有panic返回，它只是简单地返回值，否则，它从panic中恢复并返回一个错误。
func (p *processor) perform(ctx context.Context, task *Task) (err error) {
	defer func() {
		if x := recover(); x != nil {
			p.logger.Errorf("recovering from panic. See the stack trace below for details:\n%s", string(debug.Stack()))
			_, file, line, ok := runtime.Caller(1) // skip the first frame (panic itself)
			if ok && strings.Contains(file, "runtime/") {
				// The panic came from the runtime, most likely due to incorrect
				// map/slice usage. The parent frame should have the real trigger.
				_, file, line, ok = runtime.Caller(2)
			}

			// Include the file and line number info in the error, if runtime.Caller returned ok.
			if ok {
				err = fmt.Errorf("panic [%s:%d]: %v", file, line, x)
			} else {
				err = fmt.Errorf("panic: %v", x)
			}
		}
	}()
	return p.handler.ProcessTask(ctx, task)
}

// uniq dedupes elements and returns a slice of unique names of length l.
// Order of the output slice is based on the input list.
// uniq重复数据删除元素并返回长度为l的唯一名称片。
// 输出片的顺序基于输入列表。
func uniq(names []string, l int) []string {
	var res []string
	seen := make(map[string]struct{})
	for _, s := range names {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			res = append(res, s)
		}
		if len(res) == l {
			break
		}
	}
	return res
}

// sortByPriority returns a list of queue names sorted by
// their priority level in descending order.
// sortByPriority返回按优先级降序排序的队列名称列表。
func sortByPriority(qcfg map[string]int) []string {
	var queues []*queue
	for qname, n := range qcfg {
		queues = append(queues, &queue{qname, n})
	}
	sort.Sort(sort.Reverse(byPriority(queues)))
	var res []string
	for _, q := range queues {
		res = append(res, q.name)
	}
	return res
}

type queue struct {
	name     string
	priority int
}

type byPriority []*queue

func (x byPriority) Len() int           { return len(x) }
func (x byPriority) Less(i, j int) bool { return x[i].priority < x[j].priority }
func (x byPriority) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// normalizeQueues divides priority numbers by their greatest common divisor.
// normalizeQueues将优先级数除以其最大公约数。
func normalizeQueues(queues map[string]int) map[string]int {
	var xs []int
	for _, x := range queues {
		xs = append(xs, x)
	}
	d := gcd(xs...)
	res := make(map[string]int)
	for q, x := range queues {
		res[q] = x / d
	}
	return res
}

// 最大公约数
func gcd(xs ...int) int {
	fn := func(x, y int) int {
		for y > 0 {
			x, y = y, x%y
		}
		return x
	}
	res := xs[0]
	for i := 0; i < len(xs); i++ {
		res = fn(xs[i], res)
		if res == 1 {
			return 1
		}
	}
	return res
}

// computeDeadline returns the given task's deadline,
// 返回给定任务的截止日期，
func (p *processor) computeDeadline(msg *base.TaskMessage) time.Time {
	if msg.Timeout == 0 && msg.Deadline == 0 {
		p.logger.Errorf("asynq: internal error: both timeout and deadline are not set for the task message: %s", msg.ID)
		return p.clock.Now().Add(defaultTimeout)
	}
	if msg.Timeout != 0 && msg.Deadline != 0 {
		deadlineUnix := math.Min(float64(p.clock.Now().Unix()+msg.Timeout), float64(msg.Deadline))
		return time.Unix(int64(deadlineUnix), 0)
	}
	if msg.Timeout != 0 {
		return p.clock.Now().Add(time.Duration(msg.Timeout) * time.Second)
	}
	return time.Unix(msg.Deadline, 0)
}
