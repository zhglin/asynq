// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/rdb"
)

// A Client is responsible for scheduling tasks.
//
// A Client is used to register tasks that should be processed
// immediately or some time in the future.
//
// Clients are safe for concurrent use by multiple goroutines.
// 客户端负责安排任务。
// Client用于注册应该立即处理或将来某个时间处理的任务。
// 客户端可以被多个goroutines同时使用。
type Client struct {
	rdb *rdb.RDB
}

// NewClient returns a new Client instance given a redis connection option.
// 返回一个新的客户端实例给定一个redis连接选项。
func NewClient(r RedisConnOpt) *Client {
	// 创建redis链接，redis通用链接
	c, ok := r.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		panic(fmt.Sprintf("asynq: unsupported RedisConnOpt type %T", r))
	}
	return &Client{rdb: rdb.NewRDB(c)}
}

type OptionType int

const (
	MaxRetryOpt OptionType = iota
	QueueOpt
	TimeoutOpt
	DeadlineOpt
	UniqueOpt
	ProcessAtOpt
	ProcessInOpt
	TaskIDOpt
	RetentionOpt
)

// Option specifies the task processing behavior.
// 指定任务处理行为。
type Option interface {
	// String returns a string representation of the option.
	// 返回选项的字符串表示形式。
	String() string

	// Type describes the type of the option.
	// 描述选项的类型。
	Type() OptionType

	// Value returns a value used to create this option.
	// 返回用于创建此选项的值。
	Value() interface{}
}

// Internal option representations.
// 内部选项表示。
type (
	retryOption     int
	queueOption     string
	taskIDOption    string
	timeoutOption   time.Duration
	deadlineOption  time.Time
	uniqueOption    time.Duration
	processAtOption time.Time
	processInOption time.Duration
	retentionOption time.Duration
)

// MaxRetry returns an option to specify the max number of times
// the task will be retried.
//
// Negative retry count is treated as zero retry.
// MaxRetry返回一个选项，用于指定任务重试的最大次数。
// 重试次数为负被视为零重试。
func MaxRetry(n int) Option {
	if n < 0 {
		n = 0
	}
	return retryOption(n)
}

func (n retryOption) String() string     { return fmt.Sprintf("MaxRetry(%d)", int(n)) }
func (n retryOption) Type() OptionType   { return MaxRetryOpt }
func (n retryOption) Value() interface{} { return int(n) }

// Queue returns an option to specify the queue to enqueue the task into.
// Queue返回一个选项，用来指定任务进入的队列。
func Queue(qname string) Option {
	return queueOption(qname)
}

func (qname queueOption) String() string     { return fmt.Sprintf("Queue(%q)", string(qname)) }
func (qname queueOption) Type() OptionType   { return QueueOpt }
func (qname queueOption) Value() interface{} { return string(qname) }

// TaskID returns an option to specify the task ID.
// 返回指定任务ID的选项。
func TaskID(id string) Option {
	return taskIDOption(id)
}

func (id taskIDOption) String() string     { return fmt.Sprintf("TaskID(%q)", string(id)) }
func (id taskIDOption) Type() OptionType   { return TaskIDOpt }
func (id taskIDOption) Value() interface{} { return string(id) }

// Timeout returns an option to specify how long a task may run.
// If the timeout elapses before the Handler returns, then the task
// will be retried.
//
// Zero duration means no limit.
//
// If there's a conflicting Deadline option, whichever comes earliest
// will be used.
// Timeout返回一个指定任务可以运行多长时间的选项。
// 如果在Handler返回之前超时，那么任务将被重试。
// 零持续时间表示没有限制。
// 如果有一个冲突的Deadline选项，将使用最早的。
func Timeout(d time.Duration) Option {
	return timeoutOption(d)
}

func (d timeoutOption) String() string     { return fmt.Sprintf("Timeout(%v)", time.Duration(d)) }
func (d timeoutOption) Type() OptionType   { return TimeoutOpt }
func (d timeoutOption) Value() interface{} { return time.Duration(d) }

// Deadline returns an option to specify the deadline for the given task.
// If it reaches the deadline before the Handler returns, then the task
// will be retried.
//
// If there's a conflicting Timeout option, whichever comes earliest
// will be used.
// Deadline返回指定任务截止日期的选项。
// 如果它在Handler返回之前到达截止日期，那么任务将被重试。
// 如果有一个冲突的超时选项，将使用最早的。
func Deadline(t time.Time) Option {
	return deadlineOption(t)
}

func (t deadlineOption) String() string {
	return fmt.Sprintf("Deadline(%v)", time.Time(t).Format(time.UnixDate))
}
func (t deadlineOption) Type() OptionType   { return DeadlineOpt }
func (t deadlineOption) Value() interface{} { return time.Time(t) }

// Unique returns an option to enqueue a task only if the given task is unique.
// Task enqueued with this option is guaranteed to be unique within the given ttl.
// Once the task gets processed successfully or once the TTL has expired, another task with the same uniqueness may be enqueued.
// ErrDuplicateTask error is returned when enqueueing a duplicate task.
// TTL duration must be greater than or equal to 1 second.
//
// Uniqueness of a task is based on the following properties:
//     - Task Type
//     - Task Payload
//     - Queue Name
// Unique返回一个选项，当给定的任务是唯一的时，该选项才会进入队列。
// 这个选项保证在给定的ttl内是唯一的。
// 一旦任务被成功处理或者TTL过期，另一个具有相同唯一性的任务可能被加入队列。
// ErrDuplicateTask当一个重复的任务进入队列时返回错误。
// TTL持续时间必须大于或等于1秒。
// 任务的唯一性基于以下属性:
// -任务类型
// -任务负载
// -队列名
func Unique(ttl time.Duration) Option {
	return uniqueOption(ttl)
}

func (ttl uniqueOption) String() string     { return fmt.Sprintf("Unique(%v)", time.Duration(ttl)) }
func (ttl uniqueOption) Type() OptionType   { return UniqueOpt }
func (ttl uniqueOption) Value() interface{} { return time.Duration(ttl) }

// ProcessAt returns an option to specify when to process the given task.
//
// If there's a conflicting ProcessIn option, the last option passed to Enqueue overrides the others.
// ProcessAt返回一个选项来指定何时处理给定的任务。
// 如果有一个冲突的ProcessIn选项，传递给Enqueue的最后一个选项将覆盖其他选项。
func ProcessAt(t time.Time) Option {
	return processAtOption(t)
}

func (t processAtOption) String() string {
	return fmt.Sprintf("ProcessAt(%v)", time.Time(t).Format(time.UnixDate))
}
func (t processAtOption) Type() OptionType   { return ProcessAtOpt }
func (t processAtOption) Value() interface{} { return time.Time(t) }

// ProcessIn returns an option to specify when to process the given task relative to the current time.
//
// If there's a conflicting ProcessAt option, the last option passed to Enqueue overrides the others.
// ProcessIn返回一个选项，指定相对于当前时间何时处理给定的任务。
// 如果有一个冲突的ProcessAt选项，传递给Enqueue的最后一个选项将覆盖其他选项。
func ProcessIn(d time.Duration) Option {
	return processInOption(d)
}

func (d processInOption) String() string     { return fmt.Sprintf("ProcessIn(%v)", time.Duration(d)) }
func (d processInOption) Type() OptionType   { return ProcessInOpt }
func (d processInOption) Value() interface{} { return time.Duration(d) }

// Retention returns an option to specify the duration of retention period for the task.
// If this option is provided, the task will be stored as a completed task after successful processing.
// A completed task will be deleted after the specified duration elapses.
// Retention返回指定任务保留时长的选项。
// 如果提供了这个选项，则任务在处理成功后将被存储为一个已完成的任务。
// 已完成的任务将在指定的持续时间后被删除。
func Retention(d time.Duration) Option {
	return retentionOption(d)
}

func (ttl retentionOption) String() string     { return fmt.Sprintf("Retention(%v)", time.Duration(ttl)) }
func (ttl retentionOption) Type() OptionType   { return RetentionOpt }
func (ttl retentionOption) Value() interface{} { return time.Duration(ttl) }

// ErrDuplicateTask indicates that the given task could not be enqueued since it's a duplicate of another task.
//
// ErrDuplicateTask error only applies to tasks enqueued with a Unique option.
// ErrDuplicateTask表示给定的任务不能进入队列，因为它是另一个任务的副本。
// ErrDuplicateTask错误仅适用于带有Unique选项的任务队列。
var ErrDuplicateTask = errors.New("task already exists")

// ErrTaskIDConflict indicates that the given task could not be enqueued since its task ID already exists.
//
// ErrTaskIDConflict error only applies to tasks enqueued with a TaskID option.
// ErrTaskIDConflict表示给定的任务不能进入队列，因为任务ID已经存在。
// ErrTaskIDConflict error仅适用于带有TaskID选项的任务队列。
var ErrTaskIDConflict = errors.New("task ID conflicts with another task")

type option struct {
	retry     int           // 重试次数 默认 25
	queue     string        // 队列名称 默认 default
	taskID    string        // 任务id  默认 自动生成
	timeout   time.Duration // 超时时间 超过未完成 重试 默认 0
	deadline  time.Time     // 截止时间 默认 0
	uniqueTTL time.Duration // 唯一的时间
	processAt time.Time     // 处理任务时间 默认 当前时间
	retention time.Duration // 任务保留时长
}

// composeOptions merges user provided options into the default options
// and returns the composed option.
// It also validates the user provided options and returns an error if any of
// the user provided options fail the validations.
// composeOptions将用户提供的选项合并到默认选项中，并返回组合选项。
// 它还验证用户提供的选项，如果用户提供的任何选项验证失败，则返回一个错误。
// 如果有冲突的选项值，最后一个覆盖其他。
func composeOptions(opts ...Option) (option, error) {
	res := option{
		retry:     defaultMaxRetry,
		queue:     base.DefaultQueueName,
		taskID:    uuid.NewString(),
		timeout:   0, // do not set to deafultTimeout here
		deadline:  time.Time{},
		processAt: time.Now(),
	}
	for _, opt := range opts {
		switch opt := opt.(type) {
		case retryOption:
			res.retry = int(opt)
		case queueOption:
			qname := string(opt)
			if err := base.ValidateQueueName(qname); err != nil {
				return option{}, err
			}
			res.queue = qname
		case taskIDOption:
			id := string(opt)
			if err := validateTaskID(id); err != nil {
				return option{}, err
			}
			res.taskID = id
		case timeoutOption:
			res.timeout = time.Duration(opt)
		case deadlineOption:
			res.deadline = time.Time(opt)
		case uniqueOption:
			ttl := time.Duration(opt)
			if ttl < 1*time.Second {
				return option{}, errors.New("Unique TTL cannot be less than 1s")
			}
			res.uniqueTTL = ttl
		case processAtOption:
			res.processAt = time.Time(opt)
		case processInOption:
			res.processAt = time.Now().Add(time.Duration(opt))
		case retentionOption:
			res.retention = time.Duration(opt)
		default:
			// ignore unexpected option
		}
	}
	return res, nil
}

// validates user provided task ID string.
// 验证用户提供的任务ID字符串。不能全是空白符
func validateTaskID(id string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("task ID cannot be empty")
	}
	return nil
}

const (
	// Default max retry count used if nothing is specified.
	// 如果没有指定任何参数，则使用默认的最大重试计数。
	defaultMaxRetry = 25

	// Default timeout used if both timeout and deadline are not specified.
	// 如果timeout和deadline都没有指定，则使用默认的超时时间。
	defaultTimeout = 30 * time.Minute
)

// Value zero indicates no timeout and no deadline.
// 值为0表示没有超时时间和截止时间。
var (
	noTimeout  time.Duration = 0
	noDeadline time.Time     = time.Unix(0, 0)
)

// Close closes the connection with redis.
// Close关闭与redis的连接。
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Enqueue enqueues the given task to a queue.
//
// Enqueue returns TaskInfo and nil error if the task is enqueued successfully, otherwise returns a non-nil error.
//
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
// Any options provided to NewTask can be overridden by options passed to Enqueue.
// By deafult, max retry is set to 25 and timeout is set to 30 minutes.
//
// If no ProcessAt or ProcessIn options are provided, the task will be pending immediately.
//
// Enqueue uses context.Background internally; to specify the context, use EnqueueContext.
// 将给定的任务放入队列。
// 如果任务成功进入队列，Enqueue返回TaskInfo和nil错误，否则返回非nil错误。
// 参数opts指定任务处理的行为。
// 如果有冲突的选项值，最后一个覆盖其他。
// 提供给NewTask的任何选项都可以被传递给Enqueue的选项覆盖。
// 通过deafult，最大重试设置为25，超时时间设置为30分钟。
// 如果没有提供ProcessAt或ProcessIn选项，任务将立即挂起。
// Enqueue使用context.Background()上下文;要指定上下文，使用EnqueueContext。
func (c *Client) Enqueue(task *Task, opts ...Option) (*TaskInfo, error) {
	return c.EnqueueContext(context.Background(), task, opts...)
}

// EnqueueContext enqueues the given task to a queue.
//
// EnqueueContext returns TaskInfo and nil error if the task is enqueued successfully, otherwise returns a non-nil error.
//
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
// Any options provided to NewTask can be overridden by options passed to Enqueue.
// By deafult, max retry is set to 25 and timeout is set to 30 minutes.
//
// If no ProcessAt or ProcessIn options are provided, the task will be pending immediately.
//
// The first argument context applies to the enqueue operation. To specify task timeout and deadline, use Timeout and Deadline option instead.
// EnqueueContext将给定的任务排到队列中。
// EnqueueContext如果任务成功进入队列，则返回TaskInfo和nil错误，否则返回非nil错误。
// 参数opts指定任务处理的行为。
// 如果有冲突的选项值，最后一个覆盖其他。
// 提供给NewTask的任何选项都可以被传递给Enqueue的选项覆盖。
// 通过deafult，最大重试设置为25，超时时间设置为30分钟。
// 如果没有提供ProcessAt或ProcessIn选项，任务将立即挂起。
// 第一个参数context应用于enqueue操作。若要指定任务超时时间和截止时间，请使用“超时时间”和“截止时间”选项。
func (c *Client) EnqueueContext(ctx context.Context, task *Task, opts ...Option) (*TaskInfo, error) {
	if strings.TrimSpace(task.Type()) == "" {
		return nil, fmt.Errorf("task typename cannot be empty")
	}
	// merge task options with the options provided at enqueue time.
	// 将任务选项与排队时提供的选项合并。
	opts = append(task.opts, opts...)
	opt, err := composeOptions(opts...)
	if err != nil {
		return nil, err
	}
	// 设置截止时间
	deadline := noDeadline
	if !opt.deadline.IsZero() {
		deadline = opt.deadline
	}
	// 超时时间
	timeout := noTimeout
	if opt.timeout != 0 {
		timeout = opt.timeout
	}
	if deadline.Equal(noDeadline) && timeout == noTimeout {
		// If neither deadline nor timeout are set, use default timeout.
		// 如果没有设置截止时间和超时时间，则使用默认超时时间。
		timeout = defaultTimeout
	}
	// 如果是唯一任务，设置唯一键
	var uniqueKey string
	if opt.uniqueTTL > 0 {
		uniqueKey = base.UniqueKey(opt.queue, task.Type(), task.Payload())
	}
	// 转换内部对象
	msg := &base.TaskMessage{
		ID:        opt.taskID,
		Type:      task.Type(),
		Payload:   task.Payload(),
		Queue:     opt.queue,
		Retry:     opt.retry,
		Deadline:  deadline.Unix(),
		Timeout:   int64(timeout.Seconds()),
		UniqueKey: uniqueKey,
		Retention: int64(opt.retention.Seconds()),
	}
	now := time.Now()
	var state base.TaskState
	// 执行时间是当前时间或者之前 直接写入队列
	if opt.processAt.Before(now) || opt.processAt.Equal(now) {
		opt.processAt = now
		err = c.enqueue(ctx, msg, opt.uniqueTTL)
		state = base.TaskStatePending
	} else { // 执行时间在之后
		err = c.schedule(ctx, msg, opt.processAt, opt.uniqueTTL)
		state = base.TaskStateScheduled
	}
	switch {
	case errors.Is(err, errors.ErrDuplicateTask):
		return nil, fmt.Errorf("%w", ErrDuplicateTask)
	case errors.Is(err, errors.ErrTaskIdConflict):
		return nil, fmt.Errorf("%w", ErrTaskIDConflict)
	case err != nil:
		return nil, err
	}
	return newTaskInfo(msg, state, opt.processAt, nil), nil
}

// 写入队列
func (c *Client) enqueue(ctx context.Context, msg *base.TaskMessage, uniqueTTL time.Duration) error {
	if uniqueTTL > 0 {
		return c.rdb.EnqueueUnique(ctx, msg, uniqueTTL)
	}
	return c.rdb.Enqueue(ctx, msg)
}

// 调度
func (c *Client) schedule(ctx context.Context, msg *base.TaskMessage, t time.Time, uniqueTTL time.Duration) error {
	if uniqueTTL > 0 {
		ttl := t.Add(uniqueTTL).Sub(time.Now())
		return c.rdb.ScheduleUnique(ctx, msg, t, ttl)
	}
	return c.rdb.Schedule(ctx, msg, t)
}
