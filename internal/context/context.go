// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package context

import (
	"context"
	"time"

	"github.com/hibiken/asynq/internal/base"
)

// A taskMetadata holds task scoped data to put in context.
// taskMetadata保存要放在上下文中的任务范围数据。
type taskMetadata struct {
	id         string
	maxRetry   int
	retryCount int
	qname      string
}

// ctxKey type is unexported to prevent collisions with context keys defined in
// other packages.
// ctxKey类型不被导出，以防止与其他包中定义的上下文键发生冲突。
type ctxKey int

// metadataCtxKey is the context key for the task metadata.
// Its value of zero is arbitrary.
// metadataCtxKey是任务元数据的上下文键。
// 0的值是任意的。
const metadataCtxKey ctxKey = 0

// New returns a context and cancel function for a given task message.
// 返回给定任务消息的上下文和取消函数。
func New(base context.Context, msg *base.TaskMessage, deadline time.Time) (context.Context, context.CancelFunc) {
	metadata := taskMetadata{
		id:         msg.ID,
		maxRetry:   msg.Retry,
		retryCount: msg.Retried,
		qname:      msg.Queue,
	}
	// 记录task信息
	ctx := context.WithValue(base, metadataCtxKey, metadata)
	// 设置中止时间
	return context.WithDeadline(ctx, deadline)
}

// GetTaskID extracts a task ID from a context, if any.
//
// ID of a task is guaranteed to be unique.
// ID of a task doesn't change if the task is being retried.
// GetTaskID从上下文中提取任务ID(如果有的话)。
// 任务ID保证唯一。
// 任务正在重试时，任务ID不改变。
func GetTaskID(ctx context.Context) (id string, ok bool) {
	metadata, ok := ctx.Value(metadataCtxKey).(taskMetadata)
	if !ok {
		return "", false
	}
	return metadata.id, true
}

// GetRetryCount extracts retry count from a context, if any.
//
// Return value n indicates the number of times associated task has been
// retried so far.
// GetRetryCount从上下文中提取重试计数，如果有的话。
// 返回值n表示关联任务到目前为止重试的次数。
func GetRetryCount(ctx context.Context) (n int, ok bool) {
	metadata, ok := ctx.Value(metadataCtxKey).(taskMetadata)
	if !ok {
		return 0, false
	}
	return metadata.retryCount, true
}

// GetMaxRetry extracts maximum retry from a context, if any.
//
// Return value n indicates the maximum number of times the assoicated task
// can be retried if ProcessTask returns a non-nil error.
// GetMaxRetry从上下文中提取最大重试次数，如果有的话。
// 返回值n表示当ProcessTask返回非nil错误时，关联的任务可以重试的最大次数。
func GetMaxRetry(ctx context.Context) (n int, ok bool) {
	metadata, ok := ctx.Value(metadataCtxKey).(taskMetadata)
	if !ok {
		return 0, false
	}
	return metadata.maxRetry, true
}

// GetQueueName extracts queue name from a context, if any.
//
// Return value qname indicates which queue the task was pulled from.
// GetQueueName从上下文(如果有的话)中提取队列名称。
// 返回值qname表示任务是从哪个队列中取出的。
func GetQueueName(ctx context.Context) (qname string, ok bool) {
	metadata, ok := ctx.Value(metadataCtxKey).(taskMetadata)
	if !ok {
		return "", false
	}
	return metadata.qname, true
}
