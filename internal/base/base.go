// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package base defines foundational types and constants used in asynq package.
package base

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/golang/protobuf/ptypes"
	"github.com/hibiken/asynq/internal/errors"
	pb "github.com/hibiken/asynq/internal/proto"
	"github.com/hibiken/asynq/internal/timeutil"
	"google.golang.org/protobuf/proto"
)

// Version of asynq library and CLI.
const Version = "0.22.1"

// DefaultQueueName is the queue name used if none are specified by user.
// DefaultQueueName是当用户没有指定队列时使用的队列名。
const DefaultQueueName = "default"

// DefaultQueue is the redis key for the default queue.
var DefaultQueue = PendingKey(DefaultQueueName)

// Global Redis keys.
const (
	AllServers    = "asynq:servers"    // ZSET
	AllWorkers    = "asynq:workers"    // ZSET
	AllSchedulers = "asynq:schedulers" // ZSET
	AllQueues     = "asynq:queues"     // SET
	CancelChannel = "asynq:cancel"     // PubSub channel
)

// TaskState denotes the state of a task.
type TaskState int

const (
	TaskStateActive    TaskState = iota + 1 // 任务当前正在由Handler处理
	TaskStatePending                        // 任务已准备好由Handler处理
	TaskStateScheduled                      // 任务计划在未来某个时间进行处理
	TaskStateRetry                          // 任务以前失败过，并计划在将来某个时间处理
	TaskStateArchived                       // 任务存档并存储以供检查之用
	TaskStateCompleted                      // 任务处理成功，并保留到保留TTL超时
)

func (s TaskState) String() string {
	switch s {
	case TaskStateActive:
		return "active"
	case TaskStatePending:
		return "pending"
	case TaskStateScheduled:
		return "scheduled"
	case TaskStateRetry:
		return "retry"
	case TaskStateArchived:
		return "archived"
	case TaskStateCompleted:
		return "completed"
	}
	panic(fmt.Sprintf("internal error: unknown task state %d", s))
}

func TaskStateFromString(s string) (TaskState, error) {
	switch s {
	case "active":
		return TaskStateActive, nil
	case "pending":
		return TaskStatePending, nil
	case "scheduled":
		return TaskStateScheduled, nil
	case "retry":
		return TaskStateRetry, nil
	case "archived":
		return TaskStateArchived, nil
	case "completed":
		return TaskStateCompleted, nil
	}
	return 0, errors.E(errors.FailedPrecondition, fmt.Sprintf("%q is not supported task state", s))
}

// ValidateQueueName validates a given qname to be used as a queue name.
// Returns nil if valid, otherwise returns non-nil error.
// ValidateQueueName验证给定的qname是否用作队列名。
// 有效时返回nil，否则返回非nil错误。
// 不能全是空白符
func ValidateQueueName(qname string) error {
	if len(strings.TrimSpace(qname)) == 0 {
		return fmt.Errorf("queue name must contain one or more characters")
	}
	return nil
}

// QueueKeyPrefix returns a prefix for all keys in the given queue.
// 返回给定队列中所有键的前缀。
func QueueKeyPrefix(qname string) string {
	return fmt.Sprintf("asynq:{%s}:", qname)
}

// TaskKeyPrefix returns a prefix for task key.
// 返回任务键的前缀。
func TaskKeyPrefix(qname string) string {
	return fmt.Sprintf("%st:", QueueKeyPrefix(qname))
}

// TaskKey returns a redis key for the given task message.
// 返回给定任务消息的redis键。
func TaskKey(qname, id string) string {
	return fmt.Sprintf("%s%s", TaskKeyPrefix(qname), id)
}

// PendingKey returns a redis key for the given queue name.
// 返回给定pending状态的队列名的redis键。
func PendingKey(qname string) string {
	return fmt.Sprintf("%spending", QueueKeyPrefix(qname))
}

// ActiveKey returns a redis key for the active tasks.
// active状态的队列名
func ActiveKey(qname string) string {
	return fmt.Sprintf("%sactive", QueueKeyPrefix(qname))
}

// ScheduledKey returns a redis key for the scheduled tasks.
func ScheduledKey(qname string) string {
	return fmt.Sprintf("%sscheduled", QueueKeyPrefix(qname))
}

// RetryKey returns a redis key for the retry tasks.
func RetryKey(qname string) string {
	return fmt.Sprintf("%sretry", QueueKeyPrefix(qname))
}

// ArchivedKey returns a redis key for the archived tasks.
func ArchivedKey(qname string) string {
	return fmt.Sprintf("%sarchived", QueueKeyPrefix(qname))
}

// LeaseKey returns a redis key for the lease.
func LeaseKey(qname string) string {
	return fmt.Sprintf("%slease", QueueKeyPrefix(qname))
}

func CompletedKey(qname string) string {
	return fmt.Sprintf("%scompleted", QueueKeyPrefix(qname))
}

// PausedKey returns a redis key to indicate that the given queue is paused.
// 被暂停的队列名。
func PausedKey(qname string) string {
	return fmt.Sprintf("%spaused", QueueKeyPrefix(qname))
}

// ProcessedTotalKey returns a redis key for total processed count for the given queue.
func ProcessedTotalKey(qname string) string {
	return fmt.Sprintf("%sprocessed", QueueKeyPrefix(qname))
}

// FailedTotalKey returns a redis key for total failure count for the given queue.
func FailedTotalKey(qname string) string {
	return fmt.Sprintf("%sfailed", QueueKeyPrefix(qname))
}

// ProcessedKey returns a redis key for processed count for the given day for the queue.
func ProcessedKey(qname string, t time.Time) string {
	return fmt.Sprintf("%sprocessed:%s", QueueKeyPrefix(qname), t.UTC().Format("2006-01-02"))
}

// FailedKey returns a redis key for failure count for the given day for the queue.
func FailedKey(qname string, t time.Time) string {
	return fmt.Sprintf("%sfailed:%s", QueueKeyPrefix(qname), t.UTC().Format("2006-01-02"))
}

// ServerInfoKey returns a redis key for process info.
func ServerInfoKey(hostname string, pid int, serverID string) string {
	return fmt.Sprintf("asynq:servers:{%s:%d:%s}", hostname, pid, serverID)
}

// WorkersKey returns a redis key for the workers given hostname, pid, and server ID.
func WorkersKey(hostname string, pid int, serverID string) string {
	return fmt.Sprintf("asynq:workers:{%s:%d:%s}", hostname, pid, serverID)
}

// SchedulerEntriesKey returns a redis key for the scheduler entries given scheduler ID.
func SchedulerEntriesKey(schedulerID string) string {
	return fmt.Sprintf("asynq:schedulers:{%s}", schedulerID)
}

// SchedulerHistoryKey returns a redis key for the scheduler's history for the given entry.
func SchedulerHistoryKey(entryID string) string {
	return fmt.Sprintf("asynq:scheduler_history:%s", entryID)
}

// UniqueKey returns a redis key with the given type, payload, and queue name.
// 返回一个具有给定类型、负载和队列名称的redis键。
func UniqueKey(qname, tasktype string, payload []byte) string {
	if payload == nil {
		return fmt.Sprintf("%sunique:%s:", QueueKeyPrefix(qname), tasktype)
	}
	checksum := md5.Sum(payload)
	return fmt.Sprintf("%sunique:%s:%s", QueueKeyPrefix(qname), tasktype, hex.EncodeToString(checksum[:]))
}

// TaskMessage is the internal representation of a task with additional metadata fields.
// Serialized data of this type gets written to redis.
// TaskMessage是一个带有额外元数据字段的任务的内部表示。
// 这种类型的序列化数据被写入redis。
type TaskMessage struct {
	// Type indicates the kind of the task to be performed.
	// 表示要执行的任务类型。
	Type string

	// Payload holds data needed to process the task.
	// 保存处理任务所需的数据。
	Payload []byte

	// ID is a unique identifier for each task.
	// 是每个任务的唯一标识符。
	ID string

	// Queue is a name this message should be enqueued to.
	// Queue是该消息应该入队到的名称。
	Queue string

	// Retry is the max number of retry for this task.
	// 此任务的最大重试次数。
	Retry int

	// Retried is the number of times we've retried this task so far.
	// 是我们到目前为止重试这个任务的次数。
	Retried int

	// ErrorMsg holds the error message from the last failure.
	// 保存上次失败的错误消息。
	ErrorMsg string

	// Time of last failure in Unix time,
	// the number of seconds elapsed since January 1, 1970 UTC.
	//
	// Use zero to indicate no last failure
	// Unix系统中最后一次失败的时间，自UTC时间1970年1月1日起经过的秒数。
	// 使用0表示没有上次失败
	LastFailedAt int64

	// Timeout specifies timeout in seconds.
	// If task processing doesn't complete within the timeout, the task will be retried
	// if retry count is remaining. Otherwise it will be moved to the archive.
	//
	// Use zero to indicate no timeout.
	// Timeout表示超时时间，单位为秒。
	// 如果任务处理没有在超时时间内完成，则在重试次数剩余的情况下，该任务将被重试。否则它将被移动到存档中。
	// 使用0表示不超时。
	Timeout int64

	// Deadline specifies the deadline for the task in Unix time,
	// the number of seconds elapsed since January 1, 1970 UTC.
	// If task processing doesn't complete before the deadline, the task will be retried
	// if retry count is remaining. Otherwise it will be moved to the archive.
	//
	// Use zero to indicate no deadline.
	// Deadline指定该任务的截止时间，单位是Unix时间，即UTC时间从1970年1月1日起经过的秒数。
	// 如果任务处理没有在截止日期之前完成，任务将会重试
	// 如果重试计数剩余。否则它将被移动到存档中。
	// 使用0表示没有截止日期。
	// 重试的截止日期，超过依然失败就不再进行重试
	Deadline int64

	// UniqueKey holds the redis key used for uniqueness lock for this task.
	//
	// Empty string indicates that no uniqueness lock was used.
	// UniqueKey持有这个任务的唯一锁的redis key。
	// 空字符串表示没有使用唯一性锁。
	UniqueKey string

	// Retention specifies the number of seconds the task should be retained after completion.
	// Retention指定任务完成后保留的秒数。
	Retention int64

	// CompletedAt is the time the task was processed successfully in Unix time,
	// the number of seconds elapsed since January 1, 1970 UTC.
	//
	// Use zero to indicate no value.
	// CompletedAt是Unix时间中任务被成功处理的时间，从UTC时间1970年1月1日起经过的秒数。
	// 使用0表示没有值。
	CompletedAt int64
}

// EncodeMessage marshals the given task message and returns an encoded bytes.
// proto消息编码
func EncodeMessage(msg *TaskMessage) ([]byte, error) {
	if msg == nil {
		return nil, fmt.Errorf("cannot encode nil message")
	}
	return proto.Marshal(&pb.TaskMessage{
		Type:         msg.Type,
		Payload:      msg.Payload,
		Id:           msg.ID,
		Queue:        msg.Queue,
		Retry:        int32(msg.Retry),
		Retried:      int32(msg.Retried),
		ErrorMsg:     msg.ErrorMsg,
		LastFailedAt: msg.LastFailedAt,
		Timeout:      msg.Timeout,
		Deadline:     msg.Deadline,
		UniqueKey:    msg.UniqueKey,
		Retention:    msg.Retention,
		CompletedAt:  msg.CompletedAt,
	})
}

// DecodeMessage unmarshals the given bytes and returns a decoded task message.
func DecodeMessage(data []byte) (*TaskMessage, error) {
	var pbmsg pb.TaskMessage
	if err := proto.Unmarshal(data, &pbmsg); err != nil {
		return nil, err
	}
	return &TaskMessage{
		Type:         pbmsg.GetType(),
		Payload:      pbmsg.GetPayload(),
		ID:           pbmsg.GetId(),
		Queue:        pbmsg.GetQueue(),
		Retry:        int(pbmsg.GetRetry()),
		Retried:      int(pbmsg.GetRetried()),
		ErrorMsg:     pbmsg.GetErrorMsg(),
		LastFailedAt: pbmsg.GetLastFailedAt(),
		Timeout:      pbmsg.GetTimeout(),
		Deadline:     pbmsg.GetDeadline(),
		UniqueKey:    pbmsg.GetUniqueKey(),
		Retention:    pbmsg.GetRetention(),
		CompletedAt:  pbmsg.GetCompletedAt(),
	}, nil
}

// TaskInfo describes a task message and its metadata.
type TaskInfo struct {
	Message       *TaskMessage
	State         TaskState
	NextProcessAt time.Time
	Result        []byte
}

// Z represents sorted set member.
type Z struct {
	Message *TaskMessage
	Score   int64
}

// ServerInfo holds information about a running server.
type ServerInfo struct {
	Host              string
	PID               int
	ServerID          string
	Concurrency       int
	Queues            map[string]int
	StrictPriority    bool
	Status            string
	Started           time.Time
	ActiveWorkerCount int
}

// EncodeServerInfo marshals the given ServerInfo and returns the encoded bytes.
func EncodeServerInfo(info *ServerInfo) ([]byte, error) {
	if info == nil {
		return nil, fmt.Errorf("cannot encode nil server info")
	}
	queues := make(map[string]int32)
	for q, p := range info.Queues {
		queues[q] = int32(p)
	}
	started, err := ptypes.TimestampProto(info.Started)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&pb.ServerInfo{
		Host:              info.Host,
		Pid:               int32(info.PID),
		ServerId:          info.ServerID,
		Concurrency:       int32(info.Concurrency),
		Queues:            queues,
		StrictPriority:    info.StrictPriority,
		Status:            info.Status,
		StartTime:         started,
		ActiveWorkerCount: int32(info.ActiveWorkerCount),
	})
}

// DecodeServerInfo decodes the given bytes into ServerInfo.
func DecodeServerInfo(b []byte) (*ServerInfo, error) {
	var pbmsg pb.ServerInfo
	if err := proto.Unmarshal(b, &pbmsg); err != nil {
		return nil, err
	}
	queues := make(map[string]int)
	for q, p := range pbmsg.GetQueues() {
		queues[q] = int(p)
	}
	startTime, err := ptypes.Timestamp(pbmsg.GetStartTime())
	if err != nil {
		return nil, err
	}
	return &ServerInfo{
		Host:              pbmsg.GetHost(),
		PID:               int(pbmsg.GetPid()),
		ServerID:          pbmsg.GetServerId(),
		Concurrency:       int(pbmsg.GetConcurrency()),
		Queues:            queues,
		StrictPriority:    pbmsg.GetStrictPriority(),
		Status:            pbmsg.GetStatus(),
		Started:           startTime,
		ActiveWorkerCount: int(pbmsg.GetActiveWorkerCount()),
	}, nil
}

// WorkerInfo holds information about a running worker.
type WorkerInfo struct {
	Host     string
	PID      int
	ServerID string
	ID       string
	Type     string
	Payload  []byte
	Queue    string
	Started  time.Time
	Deadline time.Time
}

// EncodeWorkerInfo marshals the given WorkerInfo and returns the encoded bytes.
func EncodeWorkerInfo(info *WorkerInfo) ([]byte, error) {
	if info == nil {
		return nil, fmt.Errorf("cannot encode nil worker info")
	}
	startTime, err := ptypes.TimestampProto(info.Started)
	if err != nil {
		return nil, err
	}
	deadline, err := ptypes.TimestampProto(info.Deadline)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&pb.WorkerInfo{
		Host:        info.Host,
		Pid:         int32(info.PID),
		ServerId:    info.ServerID,
		TaskId:      info.ID,
		TaskType:    info.Type,
		TaskPayload: info.Payload,
		Queue:       info.Queue,
		StartTime:   startTime,
		Deadline:    deadline,
	})
}

// DecodeWorkerInfo decodes the given bytes into WorkerInfo.
func DecodeWorkerInfo(b []byte) (*WorkerInfo, error) {
	var pbmsg pb.WorkerInfo
	if err := proto.Unmarshal(b, &pbmsg); err != nil {
		return nil, err
	}
	startTime, err := ptypes.Timestamp(pbmsg.GetStartTime())
	if err != nil {
		return nil, err
	}
	deadline, err := ptypes.Timestamp(pbmsg.GetDeadline())
	if err != nil {
		return nil, err
	}
	return &WorkerInfo{
		Host:     pbmsg.GetHost(),
		PID:      int(pbmsg.GetPid()),
		ServerID: pbmsg.GetServerId(),
		ID:       pbmsg.GetTaskId(),
		Type:     pbmsg.GetTaskType(),
		Payload:  pbmsg.GetTaskPayload(),
		Queue:    pbmsg.GetQueue(),
		Started:  startTime,
		Deadline: deadline,
	}, nil
}

// SchedulerEntry holds information about a periodic task registered with a scheduler.
type SchedulerEntry struct {
	// Identifier of this entry.
	ID string

	// Spec describes the schedule of this entry.
	Spec string

	// Type is the task type of the periodic task.
	Type string

	// Payload is the payload of the periodic task.
	Payload []byte

	// Opts is the options for the periodic task.
	Opts []string

	// Next shows the next time the task will be enqueued.
	Next time.Time

	// Prev shows the last time the task was enqueued.
	// Zero time if task was never enqueued.
	Prev time.Time
}

// EncodeSchedulerEntry marshals the given entry and returns an encoded bytes.
func EncodeSchedulerEntry(entry *SchedulerEntry) ([]byte, error) {
	if entry == nil {
		return nil, fmt.Errorf("cannot encode nil scheduler entry")
	}
	next, err := ptypes.TimestampProto(entry.Next)
	if err != nil {
		return nil, err
	}
	prev, err := ptypes.TimestampProto(entry.Prev)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&pb.SchedulerEntry{
		Id:              entry.ID,
		Spec:            entry.Spec,
		TaskType:        entry.Type,
		TaskPayload:     entry.Payload,
		EnqueueOptions:  entry.Opts,
		NextEnqueueTime: next,
		PrevEnqueueTime: prev,
	})
}

// DecodeSchedulerEntry unmarshals the given bytes and returns a decoded SchedulerEntry.
func DecodeSchedulerEntry(b []byte) (*SchedulerEntry, error) {
	var pbmsg pb.SchedulerEntry
	if err := proto.Unmarshal(b, &pbmsg); err != nil {
		return nil, err
	}
	next, err := ptypes.Timestamp(pbmsg.GetNextEnqueueTime())
	if err != nil {
		return nil, err
	}
	prev, err := ptypes.Timestamp(pbmsg.GetPrevEnqueueTime())
	if err != nil {
		return nil, err
	}
	return &SchedulerEntry{
		ID:      pbmsg.GetId(),
		Spec:    pbmsg.GetSpec(),
		Type:    pbmsg.GetTaskType(),
		Payload: pbmsg.GetTaskPayload(),
		Opts:    pbmsg.GetEnqueueOptions(),
		Next:    next,
		Prev:    prev,
	}, nil
}

// SchedulerEnqueueEvent holds information about an enqueue event by a scheduler.
type SchedulerEnqueueEvent struct {
	// ID of the task that was enqueued.
	TaskID string

	// Time the task was enqueued.
	EnqueuedAt time.Time
}

// EncodeSchedulerEnqueueEvent marshals the given event
// and returns an encoded bytes.
func EncodeSchedulerEnqueueEvent(event *SchedulerEnqueueEvent) ([]byte, error) {
	if event == nil {
		return nil, fmt.Errorf("cannot encode nil enqueue event")
	}
	enqueuedAt, err := ptypes.TimestampProto(event.EnqueuedAt)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&pb.SchedulerEnqueueEvent{
		TaskId:      event.TaskID,
		EnqueueTime: enqueuedAt,
	})
}

// DecodeSchedulerEnqueueEvent unmarshals the given bytes
// and returns a decoded SchedulerEnqueueEvent.
func DecodeSchedulerEnqueueEvent(b []byte) (*SchedulerEnqueueEvent, error) {
	var pbmsg pb.SchedulerEnqueueEvent
	if err := proto.Unmarshal(b, &pbmsg); err != nil {
		return nil, err
	}
	enqueuedAt, err := ptypes.Timestamp(pbmsg.GetEnqueueTime())
	if err != nil {
		return nil, err
	}
	return &SchedulerEnqueueEvent{
		TaskID:     pbmsg.GetTaskId(),
		EnqueuedAt: enqueuedAt,
	}, nil
}

// Cancelations is a collection that holds cancel functions for all active tasks.
//
// Cancelations are safe for concurrent use by multipel goroutines.
// cancations是一个集合，包含所有活动任务的cancel函数。
// 取消是安全的，同时使用multipel goroutines。
type Cancelations struct {
	mu          sync.Mutex
	cancelFuncs map[string]context.CancelFunc
}

// NewCancelations returns a Cancelations instance.
// 返回Cancelations实例
func NewCancelations() *Cancelations {
	return &Cancelations{
		cancelFuncs: make(map[string]context.CancelFunc),
	}
}

// Add adds a new cancel func to the collection.
// Add将一个新的cancel函数添加到集合中。
func (c *Cancelations) Add(id string, fn context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelFuncs[id] = fn
}

// Delete deletes a cancel func from the collection given an id.
// Delete从给定id的集合中删除一个cancel函数。
func (c *Cancelations) Delete(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cancelFuncs, id)
}

// Get returns a cancel func given an id.
// Get返回一个给定id的取消函数。
func (c *Cancelations) Get(id string) (fn context.CancelFunc, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn, ok = c.cancelFuncs[id]
	return fn, ok
}

// Lease is a time bound lease for worker to process task.
// It provides a communication channel between lessor and lessee about lease expiration.
// Lease是一个有时间限制的worker处理任务的租期。
// 它为出租人和承租人提供了一个关于租赁期满的沟通渠道。
type Lease struct {
	once sync.Once
	// 通知channel
	ch chan struct{}

	// 时钟
	Clock timeutil.Clock

	mu sync.Mutex
	// 租期时间
	expireAt time.Time // guarded by mu
}

// NewLease 构建并返回lease
func NewLease(expirationTime time.Time) *Lease {
	return &Lease{
		ch:       make(chan struct{}),
		expireAt: expirationTime,
		Clock:    timeutil.NewRealClock(),
	}
}

// Reset chanegs the lease to expire at the given time.
// It returns true if the lease is still valid and reset operation was successful, false if the lease had been expired.
func (l *Lease) Reset(expirationTime time.Time) bool {
	if !l.IsValid() {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.expireAt = expirationTime
	return true
}

// Sends a notification to lessee about expired lease
// Returns true if notification was sent, returns false if the lease is still valid and notification was not sent.
func (l *Lease) NotifyExpiration() bool {
	if l.IsValid() {
		return false
	}
	l.once.Do(l.closeCh)
	return true
}

func (l *Lease) closeCh() {
	close(l.ch)
}

// Done returns a communication channel from which the lessee can read to get notified when lessor notifies about lease expiration.
// Done返回一个通信通道，当出租人通知租约到期时，承租人可以从该通道读取到通知。
func (l *Lease) Done() <-chan struct{} {
	return l.ch
}

// Deadline returns the expiration time of the lease.
// 返回租约的到期时间。
func (l *Lease) Deadline() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.expireAt
}

// IsValid returns true if the lease's expieration time is in the future or equals to the current time,
// returns false otherwise.
// IsValid如果租期的过期时间在未来或等于当前时间则返回true，否则返回false。
func (l *Lease) IsValid() bool {
	now := l.Clock.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.expireAt.After(now) || l.expireAt.Equal(now)
}

// Broker is a message broker that supports operations to manage task queues.
//
// See rdb.RDB as a reference implementation.
// Broker是一个消息代理，支持管理任务队列的操作。
// 看到rdb.RDB作为参考实现。
type Broker interface {
	Ping() error
	Enqueue(ctx context.Context, msg *TaskMessage) error
	EnqueueUnique(ctx context.Context, msg *TaskMessage, ttl time.Duration) error
	Dequeue(qnames ...string) (*TaskMessage, time.Time, error)
	Done(ctx context.Context, msg *TaskMessage) error
	MarkAsComplete(ctx context.Context, msg *TaskMessage) error
	Requeue(ctx context.Context, msg *TaskMessage) error
	Schedule(ctx context.Context, msg *TaskMessage, processAt time.Time) error
	ScheduleUnique(ctx context.Context, msg *TaskMessage, processAt time.Time, ttl time.Duration) error
	Retry(ctx context.Context, msg *TaskMessage, processAt time.Time, errMsg string, isFailure bool) error
	Archive(ctx context.Context, msg *TaskMessage, errMsg string) error
	ForwardIfReady(qnames ...string) error
	DeleteExpiredCompletedTasks(qname string) error
	ListLeaseExpired(cutoff time.Time, qnames ...string) ([]*TaskMessage, error)
	ExtendLease(qname string, ids ...string) (time.Time, error)
	WriteServerState(info *ServerInfo, workers []*WorkerInfo, ttl time.Duration) error
	ClearServerState(host string, pid int, serverID string) error
	CancelationPubSub() (*redis.PubSub, error) // TODO: Need to decouple from redis to support other brokers
	PublishCancelation(id string) error
	WriteResult(qname, id string, data []byte) (n int, err error)
	Close() error
}
