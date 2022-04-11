// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rdb"
)

// Server is responsible for task processing and task lifecycle management.
//
// Server pulls tasks off queues and processes them.
// If the processing of a task is unsuccessful, server will schedule it for a retry.
//
// A task will be retried until either the task gets processed successfully
// or until it reaches its max retry count.
//
// If a task exhausts its retries, it will be moved to the archive and
// will be kept in the archive set.
// Note that the archive size is finite and once it reaches its max size,
// oldest tasks in the archive will be deleted.
// Server负责任务处理和任务生命周期管理。
// 服务器从队列中提取任务并处理它们。
// 如果一个任务处理不成功，服务器将调度它重试。
// 任务将被重试，直到任务被成功处理或达到最大重试次数。
// 如果一个任务重试完成，它将被移动到存档集，并保留在存档集中。
// 归档文件的大小是有限的，一旦达到最大大小，归档文件中最老的任务将被删除。
type Server struct {
	logger *log.Logger

	broker base.Broker // 存储

	state *serverState // 状态

	// wait group to wait for all goroutines to finish.
	wg            sync.WaitGroup
	forwarder     *forwarder     // scheduled/retry队列转移到pending队列
	processor     *processor     // task消费处理
	syncer        *syncer        // 异步请求
	heartbeater   *heartbeater   // server状态上报
	subscriber    *subscriber    // 订阅taskId，取消正在执行的task
	recoverer     *recoverer     // 回收租约过期的task
	healthchecker *healthchecker // broker心跳检查
	janitor       *janitor       // 清理已完成的task
}

type serverState struct {
	mu    sync.Mutex
	value serverStateValue
}

type serverStateValue int

const (
	// StateNew represents a new server. Server begins in
	// this state and then transition to StatusActive when
	// Start or Run is callled.
	// 表示新创建的server。server开始在这个状态，然后过渡到StatusActive时，Start或Run被调用。
	srvStateNew serverStateValue = iota

	// StateActive indicates the server is up and active.
	// StateActive表示服务器处于激活状态。
	srvStateActive

	// StateStopped indicates the server is up but no longer processing new tasks.
	// StateStopped 表示服务器已启动，但不再处理新任务。
	srvStateStopped

	// StateClosed indicates the server has been shutdown.
	// StateClosed表示服务器已关闭。
	srvStateClosed
)

var serverStates = []string{
	"new",
	"active",
	"stopped",
	"closed",
}

func (s serverStateValue) String() string {
	if srvStateNew <= s && s <= srvStateClosed {
		return serverStates[s]
	}
	return "unknown status"
}

// Config specifies the server's background-task processing behavior.
// 配置指定服务器的后台任务处理行为。
type Config struct {
	// Maximum number of concurrent processing of tasks.
	//
	// If set to a zero or negative value, NewServer will overwrite the value
	// to the number of CPUs usable by the current process.
	// 最大并发处理数。
	// 如果设置为0或负值，NewServer会将该值覆盖为当前进程可用的cpu数量。
	Concurrency int

	// BaseContext optionally specifies a function that returns the base context for Handler invocations on this server.
	//
	// If BaseContext is nil, the default is context.Background().
	// If this is defined, then it MUST return a non-nil context
	// BaseContext可选地指定一个函数，该函数返回此服务器上Handler调用的基上下文。
	// 如果BaseContext为nil，则默认为context.Background()。
	// 如果这个定义了，那么它必须返回一个非空上下文
	BaseContext func() context.Context

	// Function to calculate retry delay for a failed task.
	//
	// By default, it uses exponential backoff algorithm to calculate the delay.
	// 计算失败任务重试延迟的函数。
	// 默认使用指数后退算法计算时延。
	RetryDelayFunc RetryDelayFunc

	// Predicate function to determine whether the error returned from Handler is a failure.
	// If the function returns false, Server will not increment the retried counter for the task,
	// and Server won't record the queue stats (processed and failed stats) to avoid skewing the error
	// rate of the queue.
	//
	// By default, if the given error is non-nil the function returns true.
	// 判断Handler返回的错误是否为失败的谓词函数。
	// 如果函数返回false, Server将不会增加任务的重试计数器，并且Server将不会记录队列的统计信息(处理和失败的统计信息)，以避免队列的错误率倾斜。
	// 默认情况下，如果给定的错误是非nil，函数返回true。
	IsFailure func(error) bool

	// List of queues to process with given priority value. Keys are the names of the
	// queues and values are associated priority value.
	//
	// If set to nil or not specified, the server will process only the "default" queue.
	//
	// Priority is treated as follows to avoid starving low priority queues.
	//
	// Example:
	//
	//     Queues: map[string]int{
	//         "critical": 6,
	//         "default":  3,
	//         "low":      1,
	//     }
	//
	// With the above config and given that all queues are not empty, the tasks
	// in "critical", "default", "low" should be processed 60%, 30%, 10% of
	// the time respectively.
	//
	// If a queue has a zero or negative priority value, the queue will be ignored.
	// 指定优先级值的队列列表。键是队列的名称，值是关联的优先级值。
	// 如果设置为nil或未指定，服务器将只处理"default"队列。
	// 优先级被如下处理，以避免低优先级队列被饿死。
	// Example:
	//     Queues: map[string]int{
	//         "critical": 6,
	//         "default":  3,
	//         "low":      1,
	//     }
	// 在上面的配置中，假设所有队列都不是空的，任务
	// 在“critical”，“default”，“low”中应该分别处理60%，30%，10%的时间。
	// 如果一个队列的优先级为0或负数，则该队列将被忽略。
	Queues map[string]int

	// StrictPriority indicates whether the queue priority should be treated strictly.
	//
	// If set to true, tasks in the queue with the highest priority is processed first.
	// The tasks in lower priority queues are processed only when those queues with
	// higher priorities are empty.
	// StrictPriority是否应该严格对待队列优先级。
	// 如果设置为true，则优先队列中优先级最高的任务优先处理;
	// 低优先级队列的任务只有在高优先级队列为空时才会被处理。
	StrictPriority bool

	// ErrorHandler handles errors returned by the task handler.
	//
	// HandleError is invoked only if the task handler returns a non-nil error.
	//
	// Example:
	//
	//     func reportError(ctx context, task *asynq.Task, err error) {
	//         retried, _ := asynq.GetRetryCount(ctx)
	//         maxRetry, _ := asynq.GetMaxRetry(ctx)
	//     	   if retried >= maxRetry {
	//             err = fmt.Errorf("retry exhausted for task %s: %w", task.Type, err)
	//     	   }
	//         errorReportingService.Notify(err)
	//     })
	//
	//     ErrorHandler: asynq.ErrorHandlerFunc(reportError)
	// ErrorHandler处理任务处理程序返回的错误。
	// HandleError仅在任务处理程序返回一个非nil错误时被调用。
	ErrorHandler ErrorHandler

	// Logger specifies the logger used by the server instance.
	//
	// If unset, default logger is used.
	// Logger指定服务器实例使用的记录器。
	// 如果不设置，则使用默认记录器。
	Logger Logger

	// LogLevel specifies the minimum log level to enable.
	//
	// If unset, InfoLevel is used by default.
	// LogLevel要启用的最小日志级别。
	// unset表示默认使用infollevel。
	LogLevel LogLevel

	// ShutdownTimeout specifies the duration to wait to let workers finish their tasks
	// before forcing them to abort when stopping the server.
	//
	// If unset or zero, default timeout of 8 seconds is used.
	// ShutdownTimeout指定当服务器停止时，等待workers完成任务的时间，然后强制他们中止。
	// 如果unset或0，默认超时8秒。
	ShutdownTimeout time.Duration

	// HealthCheckFunc is called periodically with any errors encountered during ping to the
	// connected redis server.
	// HealthCheckFunc在ping连接的redis服务器期间遇到任何错误被周期性地调用。
	HealthCheckFunc func(error)

	// HealthCheckInterval specifies the interval between healthchecks.
	//
	// If unset or zero, the interval is set to 15 seconds.
	// HealthCheckInterval健康检查间隔时间。
	// unset或0表示时间间隔为15秒。
	HealthCheckInterval time.Duration

	// DelayedTaskCheckInterval specifies the interval between checks run on 'scheduled' and 'retry'
	// tasks, and forwarding them to 'pending' state if they are ready to be processed.
	//
	// If unset or zero, the interval is set to 5 seconds.
	// DelayedTaskCheckInterval指定在'scheduled'和'retry'任务上执行检查的时间间隔，并在它们准备好被处理时将它们转发到'pending'状态。
	// 如果unset或0，则设置为5秒。
	DelayedTaskCheckInterval time.Duration
}

// An ErrorHandler handles an error occured during task processing.
// ErrorHandler处理任务处理过程中发生的错误。
type ErrorHandler interface {
	HandleError(ctx context.Context, task *Task, err error)
}

// The ErrorHandlerFunc type is an adapter to allow the use of  ordinary functions as a ErrorHandler.
// If f is a function with the appropriate signature, ErrorHandlerFunc(f) is a ErrorHandler that calls f.
// ErrorHandlerFunc类型是一个适配器，允许使用普通函数作为ErrorHandler。
// 如果f是一个具有适当签名的函数，ErrorHandlerFunc(f)是一个调用f的ErrorHandler。
type ErrorHandlerFunc func(ctx context.Context, task *Task, err error)

// HandleError calls fn(ctx, task, err)
func (fn ErrorHandlerFunc) HandleError(ctx context.Context, task *Task, err error) {
	fn(ctx, task, err)
}

// RetryDelayFunc calculates the retry delay duration for a failed task given
// the retry count, error, and the task.
//
// n is the number of times the task has been retried.
// e is the error returned by the task handler.
// t is the task in question.
// RetryDelayFunc计算失败任务的重试延迟时间，给出重试次数、错误和任务。
// n为重试次数。
// e是任务处理程序返回的错误。
// t是正在讨论的任务。
type RetryDelayFunc func(n int, e error, t *Task) time.Duration

// Logger supports logging at various log levels.
type Logger interface {
	// Debug logs a message at Debug level.
	Debug(args ...interface{})

	// Info logs a message at Info level.
	Info(args ...interface{})

	// Warn logs a message at Warning level.
	Warn(args ...interface{})

	// Error logs a message at Error level.
	Error(args ...interface{})

	// Fatal logs a message at Fatal level
	// and process will exit with status set to 1.
	Fatal(args ...interface{})
}

// LogLevel represents logging level.
//
// It satisfies flag.Value interface.
type LogLevel int32

const (
	// Note: reserving value zero to differentiate unspecified case.
	level_unspecified LogLevel = iota

	// DebugLevel is the lowest level of logging.
	// Debug logs are intended for debugging and development purposes.
	DebugLevel

	// InfoLevel is used for general informational log messages.
	InfoLevel

	// WarnLevel is used for undesired but relatively expected events,
	// which may indicate a problem.
	WarnLevel

	// ErrorLevel is used for undesired and unexpected events that
	// the program can recover from.
	ErrorLevel

	// FatalLevel is used for undesired and unexpected events that
	// the program cannot recover from.
	FatalLevel
)

// String is part of the flag.Value interface.
func (l *LogLevel) String() string {
	switch *l {
	case DebugLevel:
		return "debug"
	case InfoLevel:
		return "info"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	case FatalLevel:
		return "fatal"
	}
	panic(fmt.Sprintf("asynq: unexpected log level: %v", *l))
}

// Set is part of the flag.Value interface.
func (l *LogLevel) Set(val string) error {
	switch strings.ToLower(val) {
	case "debug":
		*l = DebugLevel
	case "info":
		*l = InfoLevel
	case "warn", "warning":
		*l = WarnLevel
	case "error":
		*l = ErrorLevel
	case "fatal":
		*l = FatalLevel
	default:
		return fmt.Errorf("asynq: unsupported log level %q", val)
	}
	return nil
}

func toInternalLogLevel(l LogLevel) log.Level {
	switch l {
	case DebugLevel:
		return log.DebugLevel
	case InfoLevel:
		return log.InfoLevel
	case WarnLevel:
		return log.WarnLevel
	case ErrorLevel:
		return log.ErrorLevel
	case FatalLevel:
		return log.FatalLevel
	}
	panic(fmt.Sprintf("asynq: unexpected log level: %v", l))
}

// DefaultRetryDelayFunc is the default RetryDelayFunc used if one is not specified in Config.
// It uses exponential back-off strategy to calculate the retry delay.
// DefaultRetryDelayFunc是默认的RetryDelayFunc，如果配置中没有指定。
// 采用指数后退策略计算重试延迟。
func DefaultRetryDelayFunc(n int, e error, t *Task) time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Formula taken from https://github.com/mperham/sidekiq.
	s := int(math.Pow(float64(n), 4)) + 15 + (r.Intn(30) * (n + 1))
	return time.Duration(s) * time.Second
}

func defaultIsFailureFunc(err error) bool { return err != nil }

var defaultQueueConfig = map[string]int{
	base.DefaultQueueName: 1,
}

const (
	defaultShutdownTimeout = 8 * time.Second

	defaultHealthCheckInterval = 15 * time.Second

	defaultDelayedTaskCheckInterval = 5 * time.Second
)

// NewServer returns a new Server given a redis connection option
// and server configuration.
// NewServer返回一个新的服务器，给出redis连接选项和服务器配置。
func NewServer(r RedisConnOpt, cfg Config) *Server {
	// redis链接
	c, ok := r.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		panic(fmt.Sprintf("asynq: unsupported RedisConnOpt type %T", r))
	}
	// 上下文
	baseCtxFn := cfg.BaseContext
	if baseCtxFn == nil {
		baseCtxFn = context.Background
	}
	// 并发度
	n := cfg.Concurrency
	if n < 1 {
		n = runtime.NumCPU()
	}
	// 重试延迟函数
	delayFunc := cfg.RetryDelayFunc
	if delayFunc == nil {
		delayFunc = DefaultRetryDelayFunc
	}
	// 是否失败函数
	isFailureFunc := cfg.IsFailure
	if isFailureFunc == nil {
		isFailureFunc = defaultIsFailureFunc
	}
	// 优先级配置
	queues := make(map[string]int)
	for qname, p := range cfg.Queues {
		if err := base.ValidateQueueName(qname); err != nil {
			continue // ignore invalid queue names
		}
		if p > 0 {
			queues[qname] = p
		}
	}
	if len(queues) == 0 {
		queues = defaultQueueConfig
	}
	// 队列名
	var qnames []string
	for q := range queues {
		qnames = append(qnames, q)
	}
	// 终止时间
	shutdownTimeout := cfg.ShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = defaultShutdownTimeout
	}
	// 心跳检查间隔时间
	healthcheckInterval := cfg.HealthCheckInterval
	if healthcheckInterval == 0 {
		healthcheckInterval = defaultHealthCheckInterval
	}
	// 日志
	logger := log.NewLogger(cfg.Logger)
	loglevel := cfg.LogLevel
	if loglevel == level_unspecified {
		loglevel = InfoLevel
	}
	logger.SetLevel(toInternalLogLevel(loglevel))

	rdb := rdb.NewRDB(c)
	starting := make(chan *workerInfo)       // 已消费到的task 未完成
	finished := make(chan *base.TaskMessage) // 已完成的task
	syncCh := make(chan *syncRequest)        // 异步请求
	// 服务状态
	srvState := &serverState{value: srvStateNew}
	// 所有active的task的取消函数
	cancels := base.NewCancelations()
	// 异步请求处理
	syncer := newSyncer(syncerParams{
		logger:     logger,
		requestsCh: syncCh,
		interval:   5 * time.Second,
	})
	// server状态上报
	heartbeater := newHeartbeater(heartbeaterParams{
		logger:         logger,
		broker:         rdb,
		interval:       5 * time.Second,
		concurrency:    n,
		queues:         queues,
		strictPriority: cfg.StrictPriority,
		state:          srvState,
		starting:       starting,
		finished:       finished,
	})
	delayedTaskCheckInterval := cfg.DelayedTaskCheckInterval
	if delayedTaskCheckInterval == 0 {
		delayedTaskCheckInterval = defaultDelayedTaskCheckInterval
	}
	// 状态转发
	forwarder := newForwarder(forwarderParams{
		logger:   logger,
		broker:   rdb,
		queues:   qnames,
		interval: delayedTaskCheckInterval,
	})
	// 订阅 取消正在执行的task
	subscriber := newSubscriber(subscriberParams{
		logger:       logger,
		broker:       rdb,
		cancelations: cancels,
	})
	// 消费并处理task
	processor := newProcessor(processorParams{
		logger:          logger,
		broker:          rdb,
		retryDelayFunc:  delayFunc,
		baseCtxFn:       baseCtxFn,
		isFailureFunc:   isFailureFunc,
		syncCh:          syncCh,
		cancelations:    cancels,
		concurrency:     n,
		queues:          queues,
		strictPriority:  cfg.StrictPriority,
		errHandler:      cfg.ErrorHandler,
		shutdownTimeout: shutdownTimeout,
		starting:        starting,
		finished:        finished,
	})
	// 回收租约过期的task
	recoverer := newRecoverer(recovererParams{
		logger:         logger,
		broker:         rdb,
		retryDelayFunc: delayFunc,
		isFailureFunc:  isFailureFunc,
		queues:         qnames,
		interval:       1 * time.Minute,
	})
	// broker心跳检查
	healthchecker := newHealthChecker(healthcheckerParams{
		logger:          logger,
		broker:          rdb,
		interval:        healthcheckInterval,
		healthcheckFunc: cfg.HealthCheckFunc,
	})
	// 清理已完成的task
	janitor := newJanitor(janitorParams{
		logger:   logger,
		broker:   rdb,
		queues:   qnames,
		interval: 8 * time.Second,
	})
	return &Server{
		logger:        logger,
		broker:        rdb,
		state:         srvState,
		forwarder:     forwarder,
		processor:     processor,
		syncer:        syncer,
		heartbeater:   heartbeater,
		subscriber:    subscriber,
		recoverer:     recoverer,
		healthchecker: healthchecker,
		janitor:       janitor,
	}
}

// A Handler processes tasks.
//
// ProcessTask should return nil if the processing of a task
// is successful.
//
// If ProcessTask returns a non-nil error or panics, the task
// will be retried after delay if retry-count is remaining,
// otherwise the task will be archived.
//
// One exception to this rule is when ProcessTask returns a SkipRetry error.
// If the returned error is SkipRetry or an error wraps SkipRetry, retry is
// skipped and the task will be immediately archived instead.
// A Handler处理任务。
// 如果任务处理成功，processstask应该返回nil。
// 如果ProcessTask返回一个非空的error或panic，如果retry-count还在，任务将在延迟后重试，否则任务将被归档。
// 这个规则的一个例外是当ProcessTask返回一个SkipRetry错误时。
// 如果返回的错误是SkipRetry或错误包装了SkipRetry，重试被跳过，该任务将立即被存档。
type Handler interface {
	ProcessTask(context.Context, *Task) error
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as a Handler. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
// HandlerFunc类型是一个适配器，允许将普通函数作为Handler使用。如果f是一个具有适当签名的函数，HandlerFunc(f)是一个调用f的Handler。
type HandlerFunc func(context.Context, *Task) error

// ProcessTask calls fn(ctx, task)
func (fn HandlerFunc) ProcessTask(ctx context.Context, task *Task) error {
	return fn(ctx, task)
}

// ErrServerClosed indicates that the operation is now illegal because of the server has been shutdown.
var ErrServerClosed = errors.New("asynq: Server closed")

// Run starts the task processing and blocks until
// an os signal to exit the program is received. Once it receives
// a signal, it gracefully shuts down all active workers and other
// goroutines to process the tasks.
//
// Run returns any error encountered at server startup time.
// If the server has already been shutdown, ErrServerClosed is returned.
// 开始任务处理并阻塞，直到收到一个退出程序的os信号。一旦它收到一个信号，它就会优雅地关闭所有活跃的worker和其他goroutines来处理这些任务。
// 返回在服务器启动时遇到的任何错误。
// 如果服务器已经关闭，则返回ErrServerClosed。
func (srv *Server) Run(handler Handler) error {
	if err := srv.Start(handler); err != nil {
		return err
	}
	srv.waitForSignals()
	srv.Shutdown()
	return nil
}

// Start starts the worker server. Once the server has started,
// it pulls tasks off queues and starts a worker goroutine for each task
// and then call Handler to process it.
// Tasks are processed concurrently by the workers up to the number of
// concurrency specified in Config.Concurrency.
//
// Start returns any error encountered at server startup time.
// If the server has already been shutdown, ErrServerClosed is returned.
// 启动worker服务器。一旦服务器启动，它就从队列中取出任务，并为每个任务启动一个worker goroutine，然后调用Handler来处理它。
// 任务由worker并发处理，达到Config.Concurrency中指定的并发数。
// Start返回服务器启动时遇到的任何错误。
// 如果服务器已经关闭，则返回ErrServerClosed。
func (srv *Server) Start(handler Handler) error {
	if handler == nil {
		return fmt.Errorf("asynq: server cannot run with nil handler")
	}
	srv.processor.handler = handler

	// 设置状态
	if err := srv.start(); err != nil {
		return err
	}
	srv.logger.Info("Starting processing")

	srv.heartbeater.start(&srv.wg)
	srv.healthchecker.start(&srv.wg)
	srv.subscriber.start(&srv.wg)
	srv.syncer.start(&srv.wg)
	srv.recoverer.start(&srv.wg)
	srv.forwarder.start(&srv.wg)
	srv.processor.start(&srv.wg)
	srv.janitor.start(&srv.wg)
	return nil
}

// Checks server state and returns an error if pre-condition is not met.
// Otherwise it sets the server state to active.
// 检查服务器状态，如果不满足先决条件，返回一个错误。
// 否则，它设置服务器状态为活动。
func (srv *Server) start() error {
	srv.state.mu.Lock()
	defer srv.state.mu.Unlock()
	switch srv.state.value {
	case srvStateActive:
		return fmt.Errorf("asynq: the server is already running")
	case srvStateStopped:
		return fmt.Errorf("asynq: the server is in the stopped state. Waiting for shutdown.")
	case srvStateClosed:
		return ErrServerClosed
	}
	srv.state.value = srvStateActive
	return nil
}

// Shutdown gracefully shuts down the server.
// It gracefully closes all active workers. The server will wait for
// active workers to finish processing tasks for duration specified in Config.ShutdownTimeout.
// If worker didn't finish processing a task during the timeout, the task will be pushed back to Redis.
// Shutdown优雅地关闭服务器。
// 它优雅地关闭所有active worker。服务器将在Config.ShutdownTimeout中指定的时间内等待活动的worker完成处理任务。
// 如果worker在超时时间内没有完成一个任务的处理，这个任务将被推回Redis。
func (srv *Server) Shutdown() {
	srv.state.mu.Lock()
	if srv.state.value == srvStateNew || srv.state.value == srvStateClosed {
		srv.state.mu.Unlock()
		// server is not running, do nothing and return.
		return
	}
	srv.state.value = srvStateClosed
	srv.state.mu.Unlock()

	srv.logger.Info("Starting graceful shutdown")
	// Note: The order of shutdown is important.
	// Sender goroutines should be terminated before the receiver goroutines.
	// processor -> syncer (via syncCh)
	// processor -> heartbeater (via starting, finished channels)
	//注:关机顺序很重要。
	//发送端goroutines应该在接收端goroutines之前终止。
	//processor -> syncer(通过syncCh)
	//processor -> heartbeater(通过启动，完成通道)
	srv.forwarder.shutdown()
	srv.processor.shutdown()
	srv.recoverer.shutdown()
	srv.syncer.shutdown()
	srv.subscriber.shutdown()
	srv.janitor.shutdown()
	srv.healthchecker.shutdown()
	srv.heartbeater.shutdown()
	srv.wg.Wait()

	srv.broker.Close()
	srv.logger.Info("Exiting")
}

// Stop signals the server to stop pulling new tasks off queues.
// Stop can be used before shutting down the server to ensure that all
// currently active tasks are processed before server shutdown.
//
// Stop does not shutdown the server, make sure to call Shutdown before exit.
// Stop表示服务器停止从队列中提取新的任务。
// 在关闭服务器之前可以使用Stop，以确保在服务器关闭之前所有当前活动的任务都被处理。
// Stop不关闭服务器，确保在退出前调用shutdown。
func (srv *Server) Stop() {
	srv.state.mu.Lock()
	if srv.state.value != srvStateActive {
		// Invalid calll to Stop, server can only go from Active state to Stopped state.
		// 无效的停止调用，服务器只能从Active状态到Stopped状态。
		srv.state.mu.Unlock()
		return
	}
	srv.state.value = srvStateStopped
	srv.state.mu.Unlock()

	srv.logger.Info("Stopping processor")
	srv.processor.stop()
	srv.logger.Info("Processor stopped")
}
