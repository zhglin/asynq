// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq/internal/base"
)

// Task represents a unit of work to be performed.
// 表示要执行的工作单元。
type Task struct {
	// typename indicates the type of task to be performed.
	// 需要执行的任务类型名称。
	typename string

	// payload holds data needed to perform the task.
	// 保存执行任务所需的数据。
	payload []byte

	// opts holds options for the task.
	// 保存任务的选项。
	opts []Option

	// w is the ResultWriter for the task.
	// 执行的结果
	w *ResultWriter
}

func (t *Task) Type() string    { return t.typename }
func (t *Task) Payload() []byte { return t.payload }

// ResultWriter returns a pointer to the ResultWriter associated with the task.
//
// Nil pointer is returned if called on a newly created task (i.e. task created by calling NewTask).
// Only the tasks passed to Handler.ProcessTask have a valid ResultWriter pointer.
// ResultWriter返回一个指向与任务关联的ResultWriter的指针。
// 如果一个新创建的任务被调用，则返回Nil指针(即通过调用NewTask创建的任务)。
// 只有传递给Handler的任务。ProcessTask有一个有效的ResultWriter指针。
func (t *Task) ResultWriter() *ResultWriter { return t.w }

// NewTask returns a new Task given a type name and payload data.
// Options can be passed to configure task processing behavior.
// NewTask返回一个给定类型名和负载数据的新任务。
// 可以通过选项来配置任务处理行为。
func NewTask(typename string, payload []byte, opts ...Option) *Task {
	return &Task{
		typename: typename,
		payload:  payload,
		opts:     opts,
	}
}

// newTask creates a task with the given typename, payload and ResultWriter.
// 使用给定的类型名、有效负载和ResultWriter创建一个任务。
func newTask(typename string, payload []byte, w *ResultWriter) *Task {
	return &Task{
		typename: typename,
		payload:  payload,
		w:        w,
	}
}

// A TaskInfo describes a task and its metadata.
// TaskInfo描述任务及其元数据。
type TaskInfo struct {
	// ID is the identifier of the task.
	// ID是任务的标识符。
	ID string

	// Queue is the name of the queue in which the task belongs.
	// Queue是任务所属的队列名。
	Queue string

	// Type is the type name of the task.
	// Type是任务的类型名称。
	Type string

	// Payload is the payload data of the task.
	// 有效载荷是任务的有效载荷数据。
	Payload []byte

	// State indicates the task state.
	// “State”表示任务状态。
	State TaskState

	// MaxRetry is the maximum number of times the task can be retried.
	// MaxRetry是任务重试的最大次数。
	MaxRetry int

	// Retried is the number of times the task has retried so far.
	// retry是任务到目前为止重试的次数。
	Retried int

	// LastErr is the error message from the last failure.
	// LastErr是上次失败的错误消息。
	LastErr string

	// LastFailedAt is the time time of the last failure if any.
	// If the task has no failures, LastFailedAt is zero time (i.e. time.Time{}).
	// LastFailedAt是最后一次失败的时间。
	// 如果任务没有失败，LastFailedAt为0 time(即time. time{})。
	LastFailedAt time.Time

	// Timeout is the duration the task can be processed by Handler before being retried,
	// zero if not specified
	// Timeout是task在重试之前可以被Handler处理的时间，如果没有指定，则为0
	Timeout time.Duration

	// Deadline is the deadline for the task, zero value if not specified.
	// 截止日期是任务的截止日期，如果没有指定，则值为零。
	Deadline time.Time

	// NextProcessAt is the time the task is scheduled to be processed,
	// zero if not applicable.
	// NextProcessAt是任务计划被处理的时间，如果不适用则为零。
	NextProcessAt time.Time

	// IsOrphaned describes whether the task is left in active state with no worker processing it.
	// An orphaned task indicates that the worker has crashed or experienced network failures and was not able to
	// extend its lease on the task.
	//
	// This task will be recovered by running a server against the queue the task is in.
	// This field is only applicable to tasks with TaskStateActive.
	// IsOrphaned描述任务是否处于active状态，并且没有worker处理它。
	// 孤儿任务(orphan task)表示worker已经崩溃或经历了网络故障，无法延长任务的租期。
	// 该任务将通过对该任务所在队列运行服务器来恢复。
	// 该字段仅适用于TaskStateActive的任务。
	IsOrphaned bool

	// Retention is duration of the retention period after the task is successfully processed.
	// Retention指任务处理成功后的保留时长。
	Retention time.Duration

	// CompletedAt is the time when the task is processed successfully.
	// Zero value (i.e. time.Time{}) indicates no value.
	// CompletedAt任务被成功处理的时间。0值(即time.Time{})表示没有值。
	CompletedAt time.Time

	// Result holds the result data associated with the task.
	// Use ResultWriter to write result data from the Handler.
	// Result保存与任务关联的结果数据。
	// 使用ResultWriter从Handler写结果数据。
	Result []byte
}

// If t is non-zero, returns time converted from t as unix time in seconds.
// If t is zero, returns zero value of time.Time.
// 如果t非零，返回由t转换的unix时间，以秒为单位。
// 如果t为0，则返回time.Time的零值。
func fromUnixTimeOrZero(t int64) time.Time {
	if t == 0 {
		return time.Time{}
	}
	return time.Unix(t, 0)
}

// 添加任务后的返回信息
func newTaskInfo(msg *base.TaskMessage, state base.TaskState, nextProcessAt time.Time, result []byte) *TaskInfo {
	info := TaskInfo{
		ID:            msg.ID,
		Queue:         msg.Queue,
		Type:          msg.Type,
		Payload:       msg.Payload, // Do we need to make a copy?
		MaxRetry:      msg.Retry,
		Retried:       msg.Retried,
		LastErr:       msg.ErrorMsg,
		Timeout:       time.Duration(msg.Timeout) * time.Second,
		Deadline:      fromUnixTimeOrZero(msg.Deadline),
		Retention:     time.Duration(msg.Retention) * time.Second,
		NextProcessAt: nextProcessAt,
		LastFailedAt:  fromUnixTimeOrZero(msg.LastFailedAt),
		CompletedAt:   fromUnixTimeOrZero(msg.CompletedAt),
		Result:        result,
	}

	switch state {
	case base.TaskStateActive:
		info.State = TaskStateActive
	case base.TaskStatePending:
		info.State = TaskStatePending
	case base.TaskStateScheduled:
		info.State = TaskStateScheduled
	case base.TaskStateRetry:
		info.State = TaskStateRetry
	case base.TaskStateArchived:
		info.State = TaskStateArchived
	case base.TaskStateCompleted:
		info.State = TaskStateCompleted
	default:
		panic(fmt.Sprintf("internal error: unknown state: %d", state))
	}
	return &info
}

// TaskState denotes the state of a task.
// 指示任务的状态。
type TaskState int

const (
	// TaskStateActive Indicates that the task is currently being processed by Handler.
	// 指示该任务当前正在由Handler处理。
	TaskStateActive TaskState = iota + 1

	// TaskStatePending Indicates that the task is ready to be processed by Handler.
	// 指示任务已准备好由Handler处理
	TaskStatePending

	// TaskStateScheduled Indicates that the task is scheduled to be processed some time in the future.
	// 表示该任务计划在未来某个时间进行处理。
	TaskStateScheduled

	// TaskStateRetry Indicates that the task has previously failed and scheduled to be processed some time in the future.
	// 指示任务以前失败过，并计划在将来某个时间处理。
	TaskStateRetry

	// TaskStateArchived Indicates that the task is archived and stored for inspection purposes.
	// 指示将任务存档并存储以供检查之用。
	TaskStateArchived

	// TaskStateCompleted Indicates that the task is processed successfully and retained until the retention TTL expires.
	// 表示任务处理成功，并保留到保留TTL超时。
	TaskStateCompleted
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
	panic("asynq: unknown task state")
}

// RedisConnOpt is a discriminated union of types that represent Redis connection configuration option.
//
// RedisConnOpt represents a sum of following types:
//
//   - RedisClientOpt
//   - RedisFailoverClientOpt
//   - RedisClusterClientOpt
// RedisConnOpt是Redis连接配置选项类型的区分联合。
// RedisConnOpt表示以下类型的总和:
// ——RedisClientOpt
// ——RedisFailoverClientOpt
// ——RedisClusterClientOpt
type RedisConnOpt interface {
	// MakeRedisClient returns a new redis client instance.
	// Return value is intentionally opaque to hide the implementation detail of redis client.
	// MakeRedisClient返回一个新的redis客户端实例。返回值是故意不透明的，以隐藏redis客户端的实现细节。
	MakeRedisClient() interface{}
}

// RedisClientOpt is used to create a redis client that connects
// to a redis server directly.
// RedisClientOpt用于创建一个直接连接redis服务器的客户端。
type RedisClientOpt struct {
	// Network type to use, either tcp or unix.
	// Default is tcp.
	// 要使用的网络类型，tcp或unix。默认是tcp。
	Network string

	// Redis server address in "host:port" format.
	// Redis服务器地址为“host:port”格式。
	Addr string

	// Username to authenticate the current connection when Redis ACLs are used.
	// See: https://redis.io/commands/auth.
	// 使用Redis acl时验证当前连接的用户名。
	Username string

	// Password to authenticate the current connection.
	// See: https://redis.io/commands/auth.
	// 验证当前连接的密码。
	Password string

	// Redis DB to select after connecting to a server.
	// See: https://redis.io/commands/select.
	// 连接到服务器后要选择的Redis DB。
	DB int

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	// 拨号超时建立新连接。默认为5秒。
	DialTimeout time.Duration

	// Timeout for socket reads.
	// If timeout is reached, read commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is 3 seconds.
	// socket读取超时。
	// 如果超时，读命令将失败，并出现超时错误，而不是阻塞。
	// 取值为-1表示不超时，默认为0。
	// 默认为3秒。
	ReadTimeout time.Duration

	// Timeout for socket writes.
	// If timeout is reached, write commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is ReadTimout.
	// socket写入超时。
	// 如果超时，写命令将失败，并出现超时错误，而不是阻塞。
	// 取值为-1表示不超时，默认为0。
	// 默认为ReadTimout。
	WriteTimeout time.Duration

	// Maximum number of socket connections.
	// Default is 10 connections per every CPU as reported by runtime.NumCPU.
	// socket最大连接数。
	// 默认是每个CPU 10个连接，根据runtime.NumCPU报告。
	PoolSize int

	// TLS Config used to connect to a server.
	// TLS will be negotiated only if this field is set.
	// TLS配置用于连接服务器。
	// 只有设置了该字段，TLS才会协商。
	TLSConfig *tls.Config
}

func (opt RedisClientOpt) MakeRedisClient() interface{} {
	return redis.NewClient(&redis.Options{
		Network:      opt.Network,
		Addr:         opt.Addr,
		Username:     opt.Username,
		Password:     opt.Password,
		DB:           opt.DB,
		DialTimeout:  opt.DialTimeout,
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,
		PoolSize:     opt.PoolSize,
		TLSConfig:    opt.TLSConfig,
	})
}

// RedisFailoverClientOpt is used to creates a redis client that talks
// to redis sentinels for service discovery and has an automatic failover
// capability.
// RedisFailoverClientOpt用于创建一个redis客户端，与redis哨兵对话，以便发现服务，并具有自动故障转移能力。
type RedisFailoverClientOpt struct {
	// Redis master name that monitored by sentinels.
	// 被哨兵监视的Redis主名。
	MasterName string

	// Addresses of sentinels in "host:port" format.
	// Use at least three sentinels to avoid problems described in
	// https://redis.io/topics/sentinel.
	// 哨兵地址的格式为“host:port”。使用至少三个哨兵来避免中描述的问题
	SentinelAddrs []string

	// Redis sentinel password.
	// Redis哨兵密码。
	SentinelPassword string

	// Username to authenticate the current connection when Redis ACLs are used.
	// See: https://redis.io/commands/auth.
	// 使用Redis acl时，认证当前连接的用户名。
	Username string

	// Password to authenticate the current connection.
	// See: https://redis.io/commands/auth.
	// 验证当前连接的密码。
	Password string

	// Redis DB to select after connecting to a server.
	// See: https://redis.io/commands/select.
	// 连接到服务器后选择Redis DB
	DB int

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	// 拨号超时建立新连接。默认为5秒。
	DialTimeout time.Duration

	// Timeout for socket reads.
	// If timeout is reached, read commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is 3 seconds.
	// socket读取超时。
	// 如果超时，读命令将失败，并出现超时错误，而不是阻塞。
	// 取值为-1表示不超时，默认为0。
	// 默认为3秒。
	ReadTimeout time.Duration

	// Timeout for socket writes.
	// If timeout is reached, write commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is ReadTimeout
	// socket写入超时。
	// 如果超时，写命令将失败，并出现超时错误，而不是阻塞。
	// 取值为-1表示不超时，默认为0。
	// 默认为ReadTimout。
	WriteTimeout time.Duration

	// Maximum number of socket connections.
	// Default is 10 connections per every CPU as reported by runtime.NumCPU.
	// 默认是每个CPU 10个连接，根据runtime.NumCPU报告。
	PoolSize int

	// TLS Config used to connect to a server.
	// TLS will be negotiated only if this field is set.
	// TLS配置用于连接服务器。
	// 只有设置了该字段，TLS才会协商。
	TLSConfig *tls.Config
}

func (opt RedisFailoverClientOpt) MakeRedisClient() interface{} {
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:       opt.MasterName,
		SentinelAddrs:    opt.SentinelAddrs,
		SentinelPassword: opt.SentinelPassword,
		Username:         opt.Username,
		Password:         opt.Password,
		DB:               opt.DB,
		DialTimeout:      opt.DialTimeout,
		ReadTimeout:      opt.ReadTimeout,
		WriteTimeout:     opt.WriteTimeout,
		PoolSize:         opt.PoolSize,
		TLSConfig:        opt.TLSConfig,
	})
}

// RedisClusterClientOpt is used to creates a redis client that connects to
// redis cluster.
// RedisClusterClientOpt用于创建一个连接redis集群的redis客户端。
type RedisClusterClientOpt struct {
	// A seed list of host:port addresses of cluster nodes.
	// 集群节点的主机端口地址的种子列表。
	Addrs []string

	// The maximum number of retries before giving up.
	// Command is retried on network errors and MOVED/ASK redirects.
	// Default is 8 retries.
	// 放弃前的最大重试次数。
	// 命令在网络错误和MOVED/ASK重定向时重试。
	// 默认为8次。
	MaxRedirects int

	// Username to authenticate the current connection when Redis ACLs are used.
	// See: https://redis.io/commands/auth.
	// 使用Redis acl时验证当前连接的用户名。
	Username string

	// Password to authenticate the current connection.
	// See: https://redis.io/commands/auth.
	// 验证当前连接的密码。
	Password string

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	// 拨号超时建立新连接。
	// 默认为5秒。
	DialTimeout time.Duration

	// Timeout for socket reads.
	// If timeout is reached, read commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is 3 seconds.
	// socket读取超时。
	// 如果超时，读命令将失败，并出现超时错误，而不是阻塞。
	// 取值为-1表示不超时，默认为0。
	// 默认为3秒。
	ReadTimeout time.Duration

	// Timeout for socket writes.
	// If timeout is reached, write commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is ReadTimeout.
	// socket写入超时。
	// 如果超时，写命令将失败，并出现超时错误，而不是阻塞。
	// 取值为-1表示不超时，默认为0。
	// 默认为ReadTimout。
	WriteTimeout time.Duration

	// TLS Config used to connect to a server.
	// TLS will be negotiated only if this field is set.
	// TLS配置用于连接服务器。
	// 只有设置了该字段，TLS才会协商。
	TLSConfig *tls.Config
}

func (opt RedisClusterClientOpt) MakeRedisClient() interface{} {
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        opt.Addrs,
		MaxRedirects: opt.MaxRedirects,
		Username:     opt.Username,
		Password:     opt.Password,
		DialTimeout:  opt.DialTimeout,
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,
		TLSConfig:    opt.TLSConfig,
	})
}

// ParseRedisURI parses redis uri string and returns RedisConnOpt if uri is valid.
// It returns a non-nil error if uri cannot be parsed.
//
// Three URI schemes are supported, which are redis:, rediss:, redis-socket:, and redis-sentinel:.
// Supported formats are:
//     redis://[:password@]host[:port][/dbnumber]
//     rediss://[:password@]host[:port][/dbnumber]
//     redis-socket://[:password@]path[?db=dbnumber]
//     redis-sentinel://[:password@]host1[:port][,host2:[:port]][,hostN:[:port]][?master=masterName]
func ParseRedisURI(uri string) (RedisConnOpt, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("asynq: could not parse redis uri: %v", err)
	}
	switch u.Scheme {
	case "redis", "rediss":
		return parseRedisURI(u)
	case "redis-socket":
		return parseRedisSocketURI(u)
	case "redis-sentinel":
		return parseRedisSentinelURI(u)
	default:
		return nil, fmt.Errorf("asynq: unsupported uri scheme: %q", u.Scheme)
	}
}

func parseRedisURI(u *url.URL) (RedisConnOpt, error) {
	var db int
	var err error
	var redisConnOpt RedisClientOpt

	if len(u.Path) > 0 {
		xs := strings.Split(strings.Trim(u.Path, "/"), "/")
		db, err = strconv.Atoi(xs[0])
		if err != nil {
			return nil, fmt.Errorf("asynq: could not parse redis uri: database number should be the first segment of the path")
		}
	}
	var password string
	if v, ok := u.User.Password(); ok {
		password = v
	}

	if u.Scheme == "rediss" {
		h, _, err := net.SplitHostPort(u.Host)
		if err != nil {
			h = u.Host
		}
		redisConnOpt.TLSConfig = &tls.Config{ServerName: h}
	}

	redisConnOpt.Addr = u.Host
	redisConnOpt.Password = password
	redisConnOpt.DB = db

	return redisConnOpt, nil
}

func parseRedisSocketURI(u *url.URL) (RedisConnOpt, error) {
	const errPrefix = "asynq: could not parse redis socket uri"
	if len(u.Path) == 0 {
		return nil, fmt.Errorf("%s: path does not exist", errPrefix)
	}
	q := u.Query()
	var db int
	var err error
	if n := q.Get("db"); n != "" {
		db, err = strconv.Atoi(n)
		if err != nil {
			return nil, fmt.Errorf("%s: query param `db` should be a number", errPrefix)
		}
	}
	var password string
	if v, ok := u.User.Password(); ok {
		password = v
	}
	return RedisClientOpt{Network: "unix", Addr: u.Path, DB: db, Password: password}, nil
}

func parseRedisSentinelURI(u *url.URL) (RedisConnOpt, error) {
	addrs := strings.Split(u.Host, ",")
	master := u.Query().Get("master")
	var password string
	if v, ok := u.User.Password(); ok {
		password = v
	}
	return RedisFailoverClientOpt{MasterName: master, SentinelAddrs: addrs, Password: password}, nil
}

// ResultWriter is a client interface to write result data for a task.
// It writes the data to the redis instance the server is connected to.
// ResultWriter是写任务结果数据的客户端接口。
// 将数据写入服务器连接的redis实例。
type ResultWriter struct {
	id     string // task ID this writer is responsible for
	qname  string // queue name the task belongs to
	broker base.Broker
	ctx    context.Context // context associated with the task
}

// Write writes the given data as a result of the task the ResultWriter is associated with.
func (w *ResultWriter) Write(data []byte) (n int, err error) {
	select {
	case <-w.ctx.Done():
		return 0, fmt.Errorf("failed to result task result: %v", w.ctx.Err())
	default:
	}
	return w.broker.WriteResult(w.qname, w.id, data)
}

// TaskID returns the ID of the task the ResultWriter is associated with.
func (w *ResultWriter) TaskID() string {
	return w.id
}
