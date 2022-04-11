// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package rdb encapsulates the interactions with redis.
package rdb

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/timeutil"
	"github.com/spf13/cast"
)

const statsTTL = 90 * 24 * time.Hour // 90 days

// LeaseDuration is the duration used to initially create a lease and to extend it thereafter.
// 消息active的租赁时间
const LeaseDuration = 30 * time.Second

// RDB is a client interface to query and mutate task queues.
// RDB是一个用于查询和修改任务队列的客户端接口。
type RDB struct {
	client redis.UniversalClient // redis链接
	clock  timeutil.Clock        // 时钟
}

// NewRDB returns a new instance of RDB.
// 返回一个RDB的新实例。
func NewRDB(client redis.UniversalClient) *RDB {
	return &RDB{
		client: client,
		clock:  timeutil.NewRealClock(),
	}
}

// Close closes the connection with redis server.
// 关闭与redis server的连接。
func (r *RDB) Close() error {
	return r.client.Close()
}

// Client returns the reference to underlying redis client.
// 返回对底层redis客户端的引用。
func (r *RDB) Client() redis.UniversalClient {
	return r.client
}

// SetClock sets the clock used by RDB to the given clock.
//
// Use this function to set the clock to SimulatedClock in tests.
// 设置RDB使用的时钟为给定的时钟。
// 在测试中使用此函数将时钟设置为SimulatedClock。
func (r *RDB) SetClock(c timeutil.Clock) {
	r.clock = c
}

// Ping checks the connection with redis server.
// 检查与redis服务器的连接。
func (r *RDB) Ping() error {
	return r.client.Ping(context.Background()).Err()
}

// 执行脚本
func (r *RDB) runScript(ctx context.Context, op errors.Op, script *redis.Script, keys []string, args ...interface{}) error {
	if err := script.Run(ctx, r.client, keys, args...).Err(); err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("redis eval error: %v", err))
	}
	return nil
}

// Runs the given script with keys and args and retuns the script's return value as int64.
// 使用键和参数运行给定的脚本，并将脚本的返回值返回为int64。
func (r *RDB) runScriptWithErrorCode(ctx context.Context, op errors.Op, script *redis.Script, keys []string, args ...interface{}) (int64, error) {
	res, err := script.Run(ctx, r.client, keys, args...).Result()
	if err != nil {
		return 0, errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
	}
	n, ok := res.(int64)
	if !ok {
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from Lua script: %v", res))
	}
	return n, nil
}

// enqueueCmd enqueues a given task message.
//
// Input:
// KEYS[1] -> asynq:{<qname>}:t:<task_id> // task唯一标识
// KEYS[2] -> asynq:{<qname>}:pending	// 写入的队列名
// --
// ARGV[1] -> task message data	// task的消息内容
// ARGV[2] -> task ID			// 消息id
// ARGV[3] -> current unix time in nsec // 当前的纳秒时间
//
// Output:
// Returns 1 if successfully enqueued
// Returns 0 if task ID already exists
// EXISTS task唯一标识
// HSET task唯一标识 msg task内容 state 状态 pending_since 当前纳秒时间戳
// LPUSH 队列名 task唯一标识
var enqueueCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "pending",
           "pending_since", ARGV[3])
redis.call("LPUSH", KEYS[2], ARGV[2])
return 1
`)

// Enqueue adds the given task to the pending list of the queue.
// 将给定任务添加到队列的待处理队列中。
// pending队列
func (r *RDB) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "rdb.Enqueue"
	// 编码
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	// set添加queue名
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.PendingKey(msg.Queue),
	}
	argv := []interface{}{
		encoded,
		msg.ID,
		r.clock.Now().UnixNano(),
	}
	n, err := r.runScriptWithErrorCode(ctx, op, enqueueCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}

// enqueueUniqueCmd enqueues the task message if the task is unique.
//
// KEYS[1] -> unique key					// 唯一键
// KEYS[2] -> asynq:{<qname>}:t:<taskid>    // task标识
// KEYS[3] -> asynq:{<qname>}:pending       // 队列名
// --
// ARGV[1] -> task ID						// 消息id
// ARGV[2] -> uniqueness lock TTL           // 唯一锁定时长
// ARGV[3] -> task message data             // task信息
// ARGV[4] -> current unix time in nsec     // 当前的纳秒时间戳
//
// Output:
// Returns 1 if successfully enqueued
// Returns 0 if task ID conflicts with another task
// Returns -1 if task unique key already exists
// set 唯一键 消息id nx ex 过期时间
// EXISTS task标识
// HSET task标识 msg task内容 state pending pending_since 当前纳秒时间戳 unique_key 唯一键
// LPUSH 队列名 task标识
var enqueueUniqueCmd = redis.NewScript(`
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return -1 
end
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("HSET", KEYS[2],
           "msg", ARGV[3],
           "state", "pending",
           "pending_since", ARGV[4],
           "unique_key", KEYS[1])
redis.call("LPUSH", KEYS[3], ARGV[1])
return 1
`)

// EnqueueUnique inserts the given task if the task's uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
// 如果任务的惟一锁可以被获取，那么EnqueueUnique将插入给定的任务。
// 如果无法获取锁，返回ErrDuplicateTask。
// pending队列
func (r *RDB) EnqueueUnique(ctx context.Context, msg *base.TaskMessage, ttl time.Duration) error {
	var op errors.Op = "rdb.EnqueueUnique"
	// 编码
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Internal, "cannot encode task message: %v", err)
	}
	// set中写入队列名
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		msg.UniqueKey,
		base.TaskKey(msg.Queue, msg.ID),
		base.PendingKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		int(ttl.Seconds()),
		encoded,
		r.clock.Now().UnixNano(),
	}
	n, err := r.runScriptWithErrorCode(ctx, op, enqueueUniqueCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == -1 {
		return errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}

// Input:
// KEYS[1] -> asynq:{<qname>}:pending  //pending队列名
// KEYS[2] -> asynq:{<qname>}:paused   //paused队列名
// KEYS[3] -> asynq:{<qname>}:active   //active队列名
// KEYS[4] -> asynq:{<qname>}:lease    //lease队列名
// --
// ARGV[1] -> initial lease expiration Unix time // 租约时间长
// ARGV[2] -> task key prefix	                 // task前缀
//
// Output:
// Returns nil if no processable task is found in the given queue.
// Returns an encoded TaskMessage.
//
// Note: dequeueCmd checks whether a queue is paused first, before
// calling RPOPLPUSH to pop a task from the queue.
// EXISTS paused队列名   // 已暂停return nil
// RPOPLPUSH  pending队列名  active队列名  // pending队列最后一个元素写入active队列，获取不到返回 nil
// local key = ARGV[2] .. id // task前缀拼接taskId=task标识
// HSET key state active  // 设置task状态=active
// HDEL key pending_since // 删除pending_sine
// ZADD lease队列名 租约时间长 key
// HGET key msg // 获取task内容
var dequeueCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 0 then
	local id = redis.call("RPOPLPUSH", KEYS[1], KEYS[3])
	if id then
		local key = ARGV[2] .. id 
		redis.call("HSET", key, "state", "active")
		redis.call("HDEL", key, "pending_since")
		redis.call("ZADD", KEYS[4], ARGV[1], id)
		return redis.call("HGET", key, "msg")
	end
end
return nil`)

// Dequeue queries given queues in order and pops a task message
// off a queue if one exists and returns the message and its lease expiration time.
// Dequeue skips a queue if the queue is paused.
// If all queues are empty, ErrNoProcessableTask error is returned.
// 按顺序退出给定队列的查询，如果队列存在，弹出任务消息，并返回消息和它的租约过期时间。
// 如果队列被暂停，Dequeue会跳过队列。
// 如果所有队列都为空，则返回ErrNoProcessableTask错误。
// pending队列写入active队列
func (r *RDB) Dequeue(qnames ...string) (msg *base.TaskMessage, leaseExpirationTime time.Time, err error) {
	var op errors.Op = "rdb.Dequeue"
	for _, qname := range qnames {
		keys := []string{
			base.PendingKey(qname),
			base.PausedKey(qname),
			base.ActiveKey(qname),
			base.LeaseKey(qname),
		}
		leaseExpirationTime = r.clock.Now().Add(LeaseDuration)
		argv := []interface{}{
			leaseExpirationTime.Unix(),
			base.TaskKeyPrefix(qname),
		}
		// 获取task内容
		res, err := dequeueCmd.Run(context.Background(), r.client, keys, argv...).Result()
		if err == redis.Nil {
			continue
		} else if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
		}
		// 结果转成string
		encoded, err := cast.ToStringE(res)
		if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cast error: unexpected return value from Lua script: %v", res))
		}
		// 解码
		if msg, err = base.DecodeMessage([]byte(encoded)); err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cannot decode message: %v", err))
		}
		return msg, leaseExpirationTime, nil
	}
	// 获取不到数据
	return nil, time.Time{}, errors.E(op, errors.NotFound, errors.ErrNoProcessableTask)
}

// KEYS[1] -> asynq:{<qname>}:active
// KEYS[2] -> asynq:{<qname>}:lease
// KEYS[3] -> asynq:{<qname>}:t:<task_id>
// KEYS[4] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// KEYS[5] -> asynq:{<qname>}:processed
// -------
// ARGV[1] -> task ID
// ARGV[2] -> stats expiration timestamp
// ARGV[3] -> max int64 value
var doneCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("DEL", KEYS[3]) == 0 then
  return redis.error_reply("NOT FOUND")
end
local n = redis.call("INCR", KEYS[4])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[4], ARGV[2])
end
local total = redis.call("GET", KEYS[5])
if tonumber(total) == tonumber(ARGV[3]) then
	redis.call("SET", KEYS[5], 1)
else
	redis.call("INCR", KEYS[5])
end
return redis.status_reply("OK")
`)

// KEYS[1] -> asynq:{<qname>}:active					// active队列名
// KEYS[2] -> asynq:{<qname>}:lease						// lease队列名
// KEYS[3] -> asynq:{<qname>}:t:<task_id>				// task标识
// KEYS[4] -> asynq:{<qname>}:processed:<yyyy-mm-dd>	// 当天processed队列名
// KEYS[5] -> asynq:{<qname>}:processed					// 总的processed队列名
// KEYS[6] -> unique key								// task唯一键
// -------
// ARGV[1] -> task ID									// taskId
// ARGV[2] -> stats expiration timestamp				// 过期时间
// ARGV[3] -> max int64 value							// 标记
// LREM active队列名 0 taskId							// 从active队列中删掉
// ZREM lease队列名 taskId								// 从lease中删掉
// DEL task标识											// 删除task
// INCR 当天processed队列名								// 计数
// EXPIREAT 当天processed队列名							// 设置过期时间
// GET 总的processed队列名
// SET 总的processed队列名 1	|| INCR 总的processed队列名
// GET task唯一键
// DEL task唯一键
var doneUniqueCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("DEL", KEYS[3]) == 0 then
  return redis.error_reply("NOT FOUND")
end
local n = redis.call("INCR", KEYS[4])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[4], ARGV[2])
end
local total = redis.call("GET", KEYS[5])
if tonumber(total) == tonumber(ARGV[3]) then
	redis.call("SET", KEYS[5], 1)
else
	redis.call("INCR", KEYS[5])
end
if redis.call("GET", KEYS[6]) == ARGV[1] then
  redis.call("DEL", KEYS[6])
end
return redis.status_reply("OK")
`)

// Done removes the task from active queue and deletes the task.
// It removes a uniqueness lock acquired by the task, if any.
// 将任务从活动队列中移除并删除。
// 删除任务获取的惟一锁(如果有的话)。
// 删除task数据
func (r *RDB) Done(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "rdb.Done"
	now := r.clock.Now()
	expireAt := now.Add(statsTTL)
	keys := []string{
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.TaskKey(msg.Queue, msg.ID),
		base.ProcessedKey(msg.Queue, now),
		base.ProcessedTotalKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		expireAt.Unix(),
		int64(math.MaxInt64),
	}
	// Note: We cannot pass empty unique key when running this script in redis-cluster.
	if len(msg.UniqueKey) > 0 {
		keys = append(keys, msg.UniqueKey)
		return r.runScript(ctx, op, doneUniqueCmd, keys, argv...)
	}
	return r.runScript(ctx, op, doneCmd, keys, argv...)
}

// KEYS[1] -> asynq:{<qname>}:active
// KEYS[2] -> asynq:{<qname>}:lease
// KEYS[3] -> asynq:{<qname>}:completed
// KEYS[4] -> asynq:{<qname>}:t:<task_id>
// KEYS[5] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// KEYS[6] -> asynq:{<qname>}:processed
//
// ARGV[1] -> task ID
// ARGV[2] -> stats expiration timestamp
// ARGV[3] -> task exipration time in unix time
// ARGV[4] -> task message data
// ARGV[5] -> max int64 value
var markAsCompleteCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1]) ~= 1 then
  redis.redis.error_reply("INTERNAL")
end
redis.call("HSET", KEYS[4], "msg", ARGV[4], "state", "completed")
local n = redis.call("INCR", KEYS[5])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[5], ARGV[2])
end
local total = redis.call("GET", KEYS[6])
if tonumber(total) == tonumber(ARGV[5]) then
	redis.call("SET", KEYS[6], 1)
else
	redis.call("INCR", KEYS[6])
end
return redis.status_reply("OK")
`)

// KEYS[1] -> asynq:{<qname>}:active					// active队列名
// KEYS[2] -> asynq:{<qname>}:lease						// lease队列名
// KEYS[3] -> asynq:{<qname>}:completed					// completed队列名
// KEYS[4] -> asynq:{<qname>}:t:<task_id>				// task标识
// KEYS[5] -> asynq:{<qname>}:processed:<yyyy-mm-dd>	// 当天processed的队列名
// KEYS[6] -> asynq:{<qname>}:processed					// 总的processed队列名
// KEYS[7] -> asynq:{<qname>}:unique:{<checksum>}		// 唯一键
//
// ARGV[1] -> task ID									// taskId
// ARGV[2] -> stats expiration timestamp				// 过期时间
// ARGV[3] -> task exipration time in unix time			// 任务保留的时间
// ARGV[4] -> task message data							// 消息内容
// ARGV[5] -> max int64 value							// 标记
// LREM active队列名 0 taskId							// 从active中删除
// ZREM lease队列名 taskId								// 从lease中删除
// ZADD completed队列名 任务保留的时间 taskId				// 添加到completed有序集合
// HSET task标识 msg 消息内容 "state", "completed"		// 设置task信息
// INCR 当天processed的队列名
// EXPIREAT 当天processed的队列名 过期时间
// GET 总的processed队列名
// SET 总的processed队列名 1 || INCR 总的processed队列名
// GET 唯一键
// DEL 唯一键
var markAsCompleteUniqueCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1]) ~= 1 then
  redis.redis.error_reply("INTERNAL")
end
redis.call("HSET", KEYS[4], "msg", ARGV[4], "state", "completed")
local n = redis.call("INCR", KEYS[5])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[5], ARGV[2])
end
local total = redis.call("GET", KEYS[6])
if tonumber(total) == tonumber(ARGV[5]) then
	redis.call("SET", KEYS[6], 1)
else
	redis.call("INCR", KEYS[6])
end
if redis.call("GET", KEYS[7]) == ARGV[1] then
  redis.call("DEL", KEYS[7])
end
return redis.status_reply("OK")
`)

// MarkAsComplete removes the task from active queue to mark the task as completed.
// It removes a uniqueness lock acquired by the task, if any.
// task处理成功的记录
// MarkAsComplete将任务从活动队列中移除，标记为已完成。
// 删除任务获取的惟一锁(如果有的话)。
// completed队列
func (r *RDB) MarkAsComplete(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "rdb.MarkAsComplete"
	now := r.clock.Now()
	statsExpireAt := now.Add(statsTTL)
	msg.CompletedAt = now.Unix()
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	keys := []string{
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.CompletedKey(msg.Queue),
		base.TaskKey(msg.Queue, msg.ID),
		base.ProcessedKey(msg.Queue, now),
		base.ProcessedTotalKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		statsExpireAt.Unix(),
		now.Unix() + msg.Retention,
		encoded,
		int64(math.MaxInt64),
	}
	// Note: We cannot pass empty unique key when running this script in redis-cluster.
	// 注意:当在redis-cluster中运行这个脚本时，我们不能传递空的唯一键。
	if len(msg.UniqueKey) > 0 {
		keys = append(keys, msg.UniqueKey)
		return r.runScript(ctx, op, markAsCompleteUniqueCmd, keys, argv...)
	}
	return r.runScript(ctx, op, markAsCompleteCmd, keys, argv...)
}

// KEYS[1] -> asynq:{<qname>}:active		// active队列
// KEYS[2] -> asynq:{<qname>}:lease			// lease队列
// KEYS[3] -> asynq:{<qname>}:pending       // pending队列
// KEYS[4] -> asynq:{<qname>}:t:<task_id>   // task标识
// ARGV[1] -> task ID						// taskId
// Note: Use RPUSH to push to the head of the queue.
// LREM active队列 0 taskId  	// 从active队列删除taskId，失败返回not found
// ZREM lease队列  taskId        // 从lease队列删除taskId, 失败返回not found
// RPUSH pending队列 taskId      // 写入pending队列
// HSET task标识 state pending   // 设置task状态
var requeueCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("RPUSH", KEYS[3], ARGV[1])
redis.call("HSET", KEYS[4], "state", "pending")
return redis.status_reply("OK")`)

// Requeue moves the task from active queue to the specified queue.
// 将任务从active队列移动到pending队列。
// pending队列
func (r *RDB) Requeue(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "rdb.Requeue"
	keys := []string{
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.PendingKey(msg.Queue),
		base.TaskKey(msg.Queue, msg.ID),
	}
	return r.runScript(ctx, op, requeueCmd, keys, msg.ID)
}

// KEYS[1] -> asynq:{<qname>}:t:<task_id>	// task标识
// KEYS[2] -> asynq:{<qname>}:scheduled     // 队列名
// -------
// ARGV[1] -> task message data				// task内容
// ARGV[2] -> process_at time in Unix time  // 开始执行的时间戳
// ARGV[3] -> task ID 						// taskId
//
// Output:
// Returns 1 if successfully enqueued
// Returns 0 if task ID already exists
// EXISTS task标识
// HSET task标识 msg task内容 state scheduled
// ZADD 队列名 开始执行的时间戳 taskId
var scheduleCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "scheduled")
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
return 1
`)

// Schedule adds the task to the scheduled set to be processed in the future.
// 将任务添加到将来要处理的计划集。
// Scheduled队列
func (r *RDB) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	var op errors.Op = "rdb.Schedule"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.ScheduledKey(msg.Queue),
	}
	argv := []interface{}{
		encoded,
		processAt.Unix(),
		msg.ID,
	}
	n, err := r.runScriptWithErrorCode(ctx, op, scheduleCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}

// KEYS[1] -> unique key					// 锁定唯一键
// KEYS[2] -> asynq:{<qname>}:t:<task_id>   // task标识
// KEYS[3] -> asynq:{<qname>}:scheduled     // 队列名
// -------
// ARGV[1] -> task ID                       // taskId
// ARGV[2] -> uniqueness lock TTL           // 唯一锁定时长
// ARGV[3] -> score (process_at timestamp)  // 开始执行的时间
// ARGV[4] -> task message					// task内容
//
// Output:
// Returns 1 if successfully scheduled
// Returns 0 if task ID already exists
// Returns -1 if task unique key already exists
// SET 锁定唯一键 taskId NX EX 锁定时长
// EXISTS task标识
// HSET task标识 msg task内容 state scheduled unique_key 锁定唯一键
// ZADD 队列名 开始执行时间 taskId
var scheduleUniqueCmd = redis.NewScript(`
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return -1
end
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("HSET", KEYS[2],
           "msg", ARGV[4],
           "state", "scheduled",
           "unique_key", KEYS[1])
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1])
return 1
`)

// ScheduleUnique adds the task to the backlog queue to be processed in the future if the uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
// ScheduleUnique将任务添加到待处理的backlog队列中，如果可以获取唯一性锁。
// 如果无法获取锁，返回ErrDuplicateTask。
// Scheduled队列
func (r *RDB) ScheduleUnique(ctx context.Context, msg *base.TaskMessage, processAt time.Time, ttl time.Duration) error {
	var op errors.Op = "rdb.ScheduleUnique"
	// 编码
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode task message: %v", err))
	}
	// set记录队列名
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		msg.UniqueKey,
		base.TaskKey(msg.Queue, msg.ID),
		base.ScheduledKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		int(ttl.Seconds()),
		processAt.Unix(),
		encoded,
	}
	n, err := r.runScriptWithErrorCode(ctx, op, scheduleUniqueCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == -1 {
		return errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}

// KEYS[1] -> asynq:{<qname>}:t:<task_id> 				// task标识
// KEYS[2] -> asynq:{<qname>}:active					// active队列名
// KEYS[3] -> asynq:{<qname>}:lease						// lease队列名
// KEYS[4] -> asynq:{<qname>}:retry						// retry队列名
// KEYS[5] -> asynq:{<qname>}:processed:<yyyy-mm-dd>	// 当天的processed对列名
// KEYS[6] -> asynq:{<qname>}:failed:<yyyy-mm-dd>		// 当天的failed对列名
// KEYS[7] -> asynq:{<qname>}:processed					// processed总的队列名
// KEYS[8] -> asynq:{<qname>}:failed					// failed总的队列名
// -------
// ARGV[1] -> task ID									// taskId
// ARGV[2] -> updated base.TaskMessage value			// task内容
// ARGV[3] -> retry_at UNIX timestamp					// 重试时间
// ARGV[4] -> stats expiration timestamp				// 过期时间
// ARGV[5] -> is_failure (bool)							// 是否失败
// ARGV[6] -> max int64 value							// int64的最大值，标记
// LREM active队列名 0 taskId   		// 从active队列中删除taskId
// ZREM lease队列名 taskId      		// 从lease队列中删除taskId
// ZADD retry对列名 重试时间 taskId 	// 添加到retry有序集合中
// HSET task标识 msg task内容 state retry	// 记录task内容
// INCR 当天的processed对列名
// EXPIREAT 当天的processed对列名 过期时间
// INCR 当天的failed对列名
// EXPIREAT 当天的failed对列名 过期时间
// GET processed总的队列名 // 有值就incr
// SET processed总的队列名 1   || INCR processed总的队列名
// SET failed总的队列名 1      || INCR failed总的队列名
var retryCmd = redis.NewScript(`
if redis.call("LREM", KEYS[2], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", KEYS[4], ARGV[3], ARGV[1])
redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "retry")
if tonumber(ARGV[5]) == 1 then
	local n = redis.call("INCR", KEYS[5])
	if tonumber(n) == 1 then
		redis.call("EXPIREAT", KEYS[5], ARGV[4])
	end
	local m = redis.call("INCR", KEYS[6])
	if tonumber(m) == 1 then
		redis.call("EXPIREAT", KEYS[6], ARGV[4])
	end
    local total = redis.call("GET", KEYS[7])
    if tonumber(total) == tonumber(ARGV[6]) then
    	redis.call("SET", KEYS[7], 1)
    	redis.call("SET", KEYS[8], 1)
    else
    	redis.call("INCR", KEYS[7])
    	redis.call("INCR", KEYS[8])
    end
end
return redis.status_reply("OK")`)

// Retry moves the task from active to retry queue.
// It also annotates the message with the given error message and
// if isFailure is true increments the retried counter.
// Retry将任务从活动状态移到重试队列中。
// 它还用给定的错误消息注释消息，如果isFailure为true，则增加重试计数器。
// retry队列
func (r *RDB) Retry(ctx context.Context, msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	var op errors.Op = "rdb.Retry"
	now := r.clock.Now()
	// 设置task失败信息
	modified := *msg
	if isFailure {
		modified.Retried++
	}
	modified.ErrorMsg = errMsg
	modified.LastFailedAt = now.Unix()
	encoded, err := base.EncodeMessage(&modified)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}
	expireAt := now.Add(statsTTL)
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.RetryKey(msg.Queue),
		base.ProcessedKey(msg.Queue, now),
		base.FailedKey(msg.Queue, now),
		base.ProcessedTotalKey(msg.Queue),
		base.FailedTotalKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		encoded,
		processAt.Unix(),
		expireAt.Unix(),
		isFailure,
		int64(math.MaxInt64),
	}
	return r.runScript(ctx, op, retryCmd, keys, argv...)
}

const (
	// 存档中的最大任务数
	maxArchiveSize = 10000 // maximum number of tasks in archive
	// 永久删除归档任务的天数
	archivedExpirationInDays = 90 // number of days before an archived task gets deleted permanently
)

// KEYS[1] -> asynq:{<qname>}:t:<task_id>					// task标识
// KEYS[2] -> asynq:{<qname>}:active						// active队列名
// KEYS[3] -> asynq:{<qname>}:lease							// lease队列名
// KEYS[4] -> asynq:{<qname>}:archived						// archived队列名
// KEYS[5] -> asynq:{<qname>}:processed:<yyyy-mm-dd>		// 当天processed队列名
// KEYS[6] -> asynq:{<qname>}:failed:<yyyy-mm-dd>			// 当天failed队列名
// KEYS[7] -> asynq:{<qname>}:processed						// 总的processed队列名
// KEYS[8] -> asynq:{<qname>}:failed						// 总的failed队列名
// -------
// ARGV[1] -> task ID										// taskId
// ARGV[2] -> updated base.TaskMessage value				// task内容
// ARGV[3] -> died_at UNIX timestamp						// 当前时间
// ARGV[4] -> cutoff timestamp (e.g., 90 days ago)			// 移除条目的时间
// ARGV[5] -> max number of tasks in archive (e.g., 100)    // 保留的条目数量
// ARGV[6] -> stats expiration timestamp					// 过期时间
// ARGV[7] -> max int64 value								// int64的最大值 标记用
// LREM active队列名 0 taskId     							// 从active队列中删除taskId
// ZREM lease队列名 taskId									// 从lease队列中删除
// ZADD archived队列名 当前时间 taskId						// 添加到archived有序集合
// ZREMRANGEBYSCORE archived队列名 "-inf" 移除条目的时间		// 删除多久之前的数据
// ZREMRANGEBYRANK archived队列名 0 1000						// 保留1000条
// HSET task标识 msg task内容 state "archived"				// 设置task状态
// INCR 当天processed队列名									// 计数
// EXPIREAT 当天processed队列名 过期时间						// 设置过期时间
// INCR 当天failed队列名
// EXPIREAT 当天failed队列名 过期时间
var archiveCmd = redis.NewScript(`
if redis.call("LREM", KEYS[2], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", KEYS[4], ARGV[3], ARGV[1])
redis.call("ZREMRANGEBYSCORE", KEYS[4], "-inf", ARGV[4])
redis.call("ZREMRANGEBYRANK", KEYS[4], 0, -ARGV[5])
redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "archived")
local n = redis.call("INCR", KEYS[5])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[5], ARGV[6])
end
local m = redis.call("INCR", KEYS[6])
if tonumber(m) == 1 then
	redis.call("EXPIREAT", KEYS[6], ARGV[6])
end
local total = redis.call("GET", KEYS[7])
if tonumber(total) == tonumber(ARGV[7]) then
   	redis.call("SET", KEYS[7], 1)
   	redis.call("SET", KEYS[8], 1)
else
  	redis.call("INCR", KEYS[7])
   	redis.call("INCR", KEYS[8])
end
return redis.status_reply("OK")`)

// Archive sends the given task to archive, attaching the error message to the task.
// It also trims the archive by timestamp and set size.
// 处理失败的任务进行归档
// 将给定的任务发送给Archive，并将错误消息附加到任务中。
// 它还通过时间戳和设置大小来修剪存档。
// archive队列
func (r *RDB) Archive(ctx context.Context, msg *base.TaskMessage, errMsg string) error {
	var op errors.Op = "rdb.Archive"
	// 设置失败信息
	now := r.clock.Now()
	modified := *msg
	modified.ErrorMsg = errMsg
	modified.LastFailedAt = now.Unix()
	encoded, err := base.EncodeMessage(&modified)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}
	cutoff := now.AddDate(0, 0, -archivedExpirationInDays)
	expireAt := now.Add(statsTTL)
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.ArchivedKey(msg.Queue),
		base.ProcessedKey(msg.Queue, now),
		base.FailedKey(msg.Queue, now),
		base.ProcessedTotalKey(msg.Queue),
		base.FailedTotalKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		encoded,
		now.Unix(),
		cutoff.Unix(),
		maxArchiveSize,
		expireAt.Unix(),
		int64(math.MaxInt64),
	}
	return r.runScript(ctx, op, archiveCmd, keys, argv...)
}

// ForwardIfReady checks scheduled and retry sets of the given queues
// and move any tasks that are ready to be processed to the pending set.
// ForwardIfReady检查给定队列的scheduled和retry队列，并将任何准备好被处理的任务移动到pending集合。
func (r *RDB) ForwardIfReady(qnames ...string) error {
	var op errors.Op = "rdb.ForwardIfReady"
	for _, qname := range qnames {
		if err := r.forwardAll(qname); err != nil {
			return errors.E(op, errors.CanonicalCode(err), err)
		}
	}
	return nil
}

// KEYS[1] -> source queue (e.g. asynq:{<qname>:scheduled or asynq:{<qname>}:retry})	// scheduled||retry队列名
// KEYS[2] -> asynq:{<qname>}:pending													// pending队列名
// ARGV[1] -> current unix time in seconds												// 当前时间戳
// ARGV[2] -> task key prefix															// task前缀
// ARGV[3] -> current unix time in nsec													// 当前纳秒时间戳
// Note: Script moves tasks up to 100 at a time to keep the runtime of script short.
// ZRANGEBYSCORE scheduled||retry队列名 -inf 当前时间戳 "LIMIT", 0, 100                     // 返回距离当前时间最小的100个元素
// LPUSH pending队列名 taskid
// ZREM scheduled||retry队列名 taskId
// HSET task标识 state "pending" pending_since 当前纳秒数
// table.getn(ids) 迁移的长度
var forwardCmd = redis.NewScript(`
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 100)
for _, id in ipairs(ids) do
	redis.call("LPUSH", KEYS[2], id)
	redis.call("ZREM", KEYS[1], id)
	redis.call("HSET", ARGV[2] .. id,
               "state", "pending",
               "pending_since", ARGV[3])
end
return table.getn(ids)`)

// forward moves tasks with a score less than the current unix time
// from the src zset to the dst list. It returns the number of tasks moved.
// forward将分数小于当前Unix时间的任务从SRC zset移动到DST列表中。它返回已移动任务的数量。
func (r *RDB) forward(src, dst, taskKeyPrefix string) (int, error) {
	now := r.clock.Now()
	res, err := forwardCmd.Run(context.Background(), r.client,
		[]string{src, dst}, now.Unix(), taskKeyPrefix, now.UnixNano()).Result()
	if err != nil {
		return 0, errors.E(errors.Internal, fmt.Sprintf("redis eval error: %v", err))
	}
	n, err := cast.ToIntE(res)
	if err != nil {
		return 0, errors.E(errors.Internal, fmt.Sprintf("cast error: Lua script returned unexpected value: %v", res))
	}
	return n, nil
}

// forwardAll checks for tasks in scheduled/retry state that are ready to be run, and updates
// their state to "pending".
// forwardAll检查处于scheduled/retry状态的任务是否准备运行，并将其状态更新为“pending”。
func (r *RDB) forwardAll(qname string) (err error) {
	// 源队列
	sources := []string{base.ScheduledKey(qname), base.RetryKey(qname)}
	// 目标队列
	dst := base.PendingKey(qname)
	// task前缀
	taskKeyPrefix := base.TaskKeyPrefix(qname)
	for _, src := range sources {
		n := 1
		for n != 0 { // 每次100，直到n=0，把小于当前时间的task全都移到pending队列中
			n, err = r.forward(src, dst, taskKeyPrefix)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// KEYS[1] -> asynq:{<qname>}:completed								// completed队列名
// ARGV[1] -> current time in unix time								// 当前时间戳
// ARGV[2] -> task key prefix										// task前缀
// ARGV[3] -> batch size (i.e. maximum number of tasks to delete)   // 批量数量
//
// Returns the number of tasks deleted.
// ZRANGEBYSCORE completed队列名 "-inf" 当前时间戳  LIMIT 0 批量数量    // 获取截止到当前时间戳的taskId
// DEL task标识
// ZREM completed队列名 taskId
// 返回处理的数量
var deleteExpiredCompletedTasksCmd = redis.NewScript(`
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[3]))
for _, id in ipairs(ids) do
	redis.call("DEL", ARGV[2] .. id)
	redis.call("ZREM", KEYS[1], id)
end
return table.getn(ids)`)

// DeleteExpiredCompletedTasks checks for any expired tasks in the given queue's completed set,
// and delete all expired tasks.
// 检查给定队列的已完成集合中任何过期的任务，并删除所有过期的任务。
func (r *RDB) DeleteExpiredCompletedTasks(qname string) error {
	// Note: Do this operation in fix batches to prevent long running script.
	// 注意:该操作需要批量修复，避免脚本长时间运行。
	const batchSize = 100
	// 循环处理直到数量为0
	for {
		n, err := r.deleteExpiredCompletedTasks(qname, batchSize)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

// deleteExpiredCompletedTasks runs the lua script to delete expired deleted task with the specified
// batch size. It reports the number of tasks deleted.
// 运行lua脚本删除过期的已删除任务，并指定批处理大小。它报告删除的任务数。
func (r *RDB) deleteExpiredCompletedTasks(qname string, batchSize int) (int64, error) {
	var op errors.Op = "rdb.DeleteExpiredCompletedTasks"
	keys := []string{base.CompletedKey(qname)}
	argv := []interface{}{
		r.clock.Now().Unix(),
		base.TaskKeyPrefix(qname),
		batchSize,
	}
	res, err := deleteExpiredCompletedTasksCmd.Run(context.Background(), r.client, keys, argv...).Result()
	if err != nil {
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("redis eval error: %v", err))
	}
	n, ok := res.(int64)
	if !ok {
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from Lua script: %v", res))
	}
	return n, nil
}

// KEYS[1] -> asynq:{<qname>}:lease		// lease队列
// ARGV[1] -> cutoff in unix time		// 时间戳
// ARGV[2] -> task key prefix			// task前缀
// ZRANGEBYSCORE lease队列 -inf  时间戳  // 时间戳之前的所有task
// HGET task
// 返回所有消息
var listLeaseExpiredCmd = redis.NewScript(`
local res = {}
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
for _, id in ipairs(ids) do
	local key = ARGV[2] .. id
	table.insert(res, redis.call("HGET", key, "msg"))
end
return res
`)

// ListLeaseExpired returns a list of task messages with an expired lease from the given queues.
// 从给定队列返回具有过期租约的任务消息列表。
func (r *RDB) ListLeaseExpired(cutoff time.Time, qnames ...string) ([]*base.TaskMessage, error) {
	var op errors.Op = "rdb.ListLeaseExpired"
	var msgs []*base.TaskMessage
	for _, qname := range qnames {
		// 返回当前qname中所有租约过期的消息
		res, err := listLeaseExpiredCmd.Run(context.Background(), r.client,
			[]string{base.LeaseKey(qname)},
			cutoff.Unix(), base.TaskKeyPrefix(qname)).Result()
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("redis eval error: %v", err))
		}
		data, err := cast.ToStringSliceE(res)
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("cast error: Lua script returned unexpected value: %v", res))
		}
		// 对返回的task进行解码返回
		for _, s := range data {
			msg, err := base.DecodeMessage([]byte(s))
			if err != nil {
				return nil, errors.E(op, errors.Internal, fmt.Sprintf("cannot decode message: %v", err))
			}
			msgs = append(msgs, msg)
		}
	}
	return msgs, nil
}

// ExtendLease extends the lease for the given tasks by LeaseDuration (30s).
// It returns a new expiration time if the operation was successful.
// ExtendLease通过LeaseDuration (30s)扩展指定任务的租期。
// 如果操作成功，它返回一个新的过期时间。
func (r *RDB) ExtendLease(qname string, ids ...string) (expirationTime time.Time, err error) {
	expireAt := r.clock.Now().Add(LeaseDuration)
	var zs []*redis.Z
	for _, id := range ids {
		zs = append(zs, &redis.Z{Member: id, Score: float64(expireAt.Unix())})
	}
	// Use XX option to only update elements that already exist; Don't add new elements
	// 使用XX选项只更新已经存在的元素;不要添加新元素
	// TODO: Consider adding GT option to ensure we only "extend" the lease. Ceveat is that GT is supported from redis v6.2.0 or above.
	// 待办事项:考虑添加GT选项，以确保我们只“延长”租约。Ceveat的意思是GT是由redis v6.2.0或更高版本支持的。
	err = r.client.ZAddXX(context.Background(), base.LeaseKey(qname), zs...).Err()
	if err != nil {
		return time.Time{}, err
	}
	return expireAt, nil
}

// KEYS[1]  -> asynq:servers:{<host:pid:sid>}			// server标识
// KEYS[2]  -> asynq:workers:{<host:pid:sid>}			// workers标识
// ARGV[1]  -> TTL in seconds							// 过期时间
// ARGV[2]  -> server info								// server信息
// ARGV[3:] -> alternate key-value pair of (worker id, worker data)	 // task信息
// Note: Add key to ZSET with expiration time as score.
// ref: https://github.com/antirez/redis/issues/135#issuecomment-2361996
// SETEX server标识 过期时间 server信息
// DEl workers标识
// HSET workers标识 taskId  task
// EXPIRE workers标识 过期时间
var writeServerStateCmd = redis.NewScript(`
redis.call("SETEX", KEYS[1], ARGV[1], ARGV[2])
redis.call("DEL", KEYS[2])
for i = 3, table.getn(ARGV)-1, 2 do
	redis.call("HSET", KEYS[2], ARGV[i], ARGV[i+1])
end
redis.call("EXPIRE", KEYS[2], ARGV[1])
return redis.status_reply("OK")`)

// WriteServerState writes server state data to redis with expiration set to the value ttl.
// WriteServerState将服务器状态数据写入redis，过期时间设置为ttl值。
func (r *RDB) WriteServerState(info *base.ServerInfo, workers []*base.WorkerInfo, ttl time.Duration) error {
	var op errors.Op = "rdb.WriteServerState"
	ctx := context.Background()
	// 编码serverInfo
	bytes, err := base.EncodeServerInfo(info)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode server info: %v", err))
	}
	// 过期时间
	exp := r.clock.Now().Add(ttl).UTC()
	// lua脚本参数
	args := []interface{}{ttl.Seconds(), bytes} // args to the lua script
	for _, w := range workers {
		bytes, err := base.EncodeWorkerInfo(w)
		if err != nil {
			continue // skip bad data
		}
		args = append(args, w.ID, bytes)
	}
	skey := base.ServerInfoKey(info.Host, info.PID, info.ServerID)
	wkey := base.WorkersKey(info.Host, info.PID, info.ServerID)
	// 单独执行是因为集群模式下key不在同一个slot，lua脚本会执行异常
	// redis添加server
	if err := r.client.ZAdd(ctx, base.AllServers, &redis.Z{Score: float64(exp.Unix()), Member: skey}).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	// redis添加workers
	if err := r.client.ZAdd(ctx, base.AllWorkers, &redis.Z{Score: float64(exp.Unix()), Member: wkey}).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zadd", Err: err})
	}
	return r.runScript(ctx, op, writeServerStateCmd, []string{skey, wkey}, args...)
}

// KEYS[1] -> asynq:servers:{<host:pid:sid>}	// 当前servers标识
// KEYS[2] -> asynq:workers:{<host:pid:sid>}	// 当前workers标识
// DEL  当前servers标识
// DLE  当前workers标识
var clearServerStateCmd = redis.NewScript(`
redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])
return redis.status_reply("OK")`)

// ClearServerState deletes server state data from redis.
// 从redis删除服务器状态数据。
func (r *RDB) ClearServerState(host string, pid int, serverID string) error {
	var op errors.Op = "rdb.ClearServerState"
	ctx := context.Background()
	skey := base.ServerInfoKey(host, pid, serverID)
	wkey := base.WorkersKey(host, pid, serverID)
	// 从所有servers有序集合中删除当前server
	if err := r.client.ZRem(ctx, base.AllServers, skey).Err(); err != nil {
		return errors.E(op, errors.Internal, &errors.RedisCommandError{Command: "zrem", Err: err})
	}
	// 从所有workers中删除当前worker
	if err := r.client.ZRem(ctx, base.AllWorkers, wkey).Err(); err != nil {
		return errors.E(op, errors.Internal, &errors.RedisCommandError{Command: "zrem", Err: err})
	}
	return r.runScript(ctx, op, clearServerStateCmd, []string{skey, wkey})
}

// KEYS[1]  -> asynq:schedulers:{<schedulerID>}		// 当前scheduler标识
// ARGV[1]  -> TTL in seconds						// 过期时间
// ARGV[2:] -> schedler entries						// 当前scheduler的cron
// DEL 当前scheduler标识
// LPUSH 当前scheduler标识 当前scheduler的cron
// EXPIRE 当前scheduler标识 过期时间
var writeSchedulerEntriesCmd = redis.NewScript(`
redis.call("DEL", KEYS[1])
for i = 2, #ARGV do
	redis.call("LPUSH", KEYS[1], ARGV[i])
end
redis.call("EXPIRE", KEYS[1], ARGV[1])
return redis.status_reply("OK")`)

// WriteSchedulerEntries writes scheduler entries data to redis with expiration set to the value ttl.
// 写入调度程序cron数据到redis，过期时间设置为ttl值。
func (r *RDB) WriteSchedulerEntries(schedulerID string, entries []*base.SchedulerEntry, ttl time.Duration) error {
	var op errors.Op = "rdb.WriteSchedulerEntries"
	ctx := context.Background()
	args := []interface{}{ttl.Seconds()}
	for _, e := range entries {
		bytes, err := base.EncodeSchedulerEntry(e)
		if err != nil {
			continue // skip bad data
		}
		args = append(args, bytes)
	}
	exp := r.clock.Now().Add(ttl).UTC()
	key := base.SchedulerEntriesKey(schedulerID)
	// scheduler添加到AllSchedulers
	err := r.client.ZAdd(ctx, base.AllSchedulers, &redis.Z{Score: float64(exp.Unix()), Member: key}).Err()
	if err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zadd", Err: err})
	}
	return r.runScript(ctx, op, writeSchedulerEntriesCmd, []string{key}, args...)
}

// ClearSchedulerEntries deletes scheduler entries data from redis.
// 从redis删除调度器条目数据。
func (r *RDB) ClearSchedulerEntries(scheduelrID string) error {
	var op errors.Op = "rdb.ClearSchedulerEntries"
	ctx := context.Background()
	key := base.SchedulerEntriesKey(scheduelrID)
	// 从AllSchedulers中删除schedulr
	if err := r.client.ZRem(ctx, base.AllSchedulers, key).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zrem", Err: err})
	}
	// 删除scheduelrID对应的队列
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "del", Err: err})
	}
	return nil
}

// CancelationPubSub returns a pubsub for cancelation messages.
// 订阅CancelChannel队列，获取需要取消的taskId
func (r *RDB) CancelationPubSub() (*redis.PubSub, error) {
	var op errors.Op = "rdb.CancelationPubSub"
	ctx := context.Background()
	pubsub := r.client.Subscribe(ctx, base.CancelChannel)
	_, err := pubsub.Receive(ctx)
	if err != nil {
		return nil, errors.E(op, errors.Unknown, fmt.Sprintf("redis pubsub receive error: %v", err))
	}
	return pubsub, nil
}

// PublishCancelation publish cancelation message to all subscribers.
// The message is the ID for the task to be canceled.
// 向所有订阅者发布取消消息。消息是要取消的任务的ID。
func (r *RDB) PublishCancelation(id string) error {
	var op errors.Op = "rdb.PublishCancelation"
	ctx := context.Background()
	if err := r.client.Publish(ctx, base.CancelChannel, id).Err(); err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("redis pubsub publish error: %v", err))
	}
	return nil
}

// KEYS[1] -> asynq:scheduler_history:<entryID>			// key
// ARGV[1] -> enqueued_at timestamp						// 时间戳
// ARGV[2] -> serialized SchedulerEnqueueEvent data		// data
// ARGV[3] -> max number of events to be persisted		// 保存的条目数
// ZREMRANGEBYRANK key 0 -1000
// ZADD key 时间戳 data
var recordSchedulerEnqueueEventCmd = redis.NewScript(`
redis.call("ZREMRANGEBYRANK", KEYS[1], 0, -ARGV[3])
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[2])
return redis.status_reply("OK")`)

// Maximum number of enqueue events to store per entry.
// 每个条目存储的最大排队事件数。
const maxEvents = 1000

// RecordSchedulerEnqueueEvent records the time when the given task was enqueued.
// 记录给定任务进入队列的时间。
func (r *RDB) RecordSchedulerEnqueueEvent(entryID string, event *base.SchedulerEnqueueEvent) error {
	var op errors.Op = "rdb.RecordSchedulerEnqueueEvent"
	ctx := context.Background()
	data, err := base.EncodeSchedulerEnqueueEvent(event)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode scheduler enqueue event: %v", err))
	}
	keys := []string{
		base.SchedulerHistoryKey(entryID),
	}
	argv := []interface{}{
		event.EnqueuedAt.Unix(),
		data,
		maxEvents,
	}
	return r.runScript(ctx, op, recordSchedulerEnqueueEventCmd, keys, argv...)
}

// ClearSchedulerHistory deletes the enqueue event history for the given scheduler entry.
// 删除给定调度程序条目的入队事件历史记录。
func (r *RDB) ClearSchedulerHistory(entryID string) error {
	var op errors.Op = "rdb.ClearSchedulerHistory"
	ctx := context.Background()
	key := base.SchedulerHistoryKey(entryID)
	// 直接del
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "del", Err: err})
	}
	return nil
}

// WriteResult writes the given result data for the specified task.
func (r *RDB) WriteResult(qname, taskID string, data []byte) (int, error) {
	var op errors.Op = "rdb.WriteResult"
	ctx := context.Background()
	taskKey := base.TaskKey(qname, taskID)
	if err := r.client.HSet(ctx, taskKey, "result", data).Err(); err != nil {
		return 0, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hset", Err: err})
	}
	return len(data), nil
}
