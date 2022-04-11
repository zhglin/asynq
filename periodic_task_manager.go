// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"crypto/sha256"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// PeriodicTaskManager manages scheduling of periodic tasks.
// It syncs scheduler's entries by calling the config provider periodically.
// PeriodicTaskManager管理定时任务调度。
// 它通过定期调用配置提供程序来同步调度程序的条目。
// 管理scheduler
type PeriodicTaskManager struct {
	s            *Scheduler
	p            PeriodicTaskConfigProvider // task信息配置，一次返回多个
	syncInterval time.Duration
	done         chan (struct{})
	wg           sync.WaitGroup
	m            map[string]string // map[hash]entryID	// 已添加的task
}

type PeriodicTaskManagerOpts struct {
	// Required: must be non nil
	PeriodicTaskConfigProvider PeriodicTaskConfigProvider

	// Required: must be non nil
	RedisConnOpt RedisConnOpt

	// Optional: scheduler options
	// 选项
	*SchedulerOpts

	// Optional: default is 3m
	SyncInterval time.Duration
}

const defaultSyncInterval = 3 * time.Minute

// NewPeriodicTaskManager returns a new PeriodicTaskManager instance.
// The given opts should specify the RedisConnOp and PeriodicTaskConfigProvider at minimum.
// NewPeriodicTaskManager返回一个新的PeriodicTaskManager实例。
// 给出的选项应该至少指定redisconp和PeriodicTaskConfigProvider。
func NewPeriodicTaskManager(opts PeriodicTaskManagerOpts) (*PeriodicTaskManager, error) {
	if opts.PeriodicTaskConfigProvider == nil {
		return nil, fmt.Errorf("PeriodicTaskConfigProvider cannot be nil")
	}
	if opts.RedisConnOpt == nil {
		return nil, fmt.Errorf("RedisConnOpt cannot be nil")
	}
	// 构建scheduler
	scheduler := NewScheduler(opts.RedisConnOpt, opts.SchedulerOpts)
	syncInterval := opts.SyncInterval
	if syncInterval == 0 {
		syncInterval = defaultSyncInterval
	}
	return &PeriodicTaskManager{
		s:            scheduler,
		p:            opts.PeriodicTaskConfigProvider,
		syncInterval: syncInterval,
		done:         make(chan struct{}),
		m:            make(map[string]string),
	}, nil
}

// PeriodicTaskConfigProvider provides configs for periodic tasks.
// GetConfigs will be called by a PeriodicTaskManager periodically to
// sync the scheduler's entries with the configs returned by the provider.
// PeriodicTaskConfigProvider提供定时任务配置。
// GetConfigs将被PeriodicTaskManager定期调用，以将调度器的条目与提供程序返回的configs同步。
type PeriodicTaskConfigProvider interface {
	GetConfigs() ([]*PeriodicTaskConfig, error)
}

// PeriodicTaskConfig specifies the details of a periodic task.
// 定时任务的详细信息。
type PeriodicTaskConfig struct {
	Cronspec string   // required: must be non empty string	cron表达式
	Task     *Task    // required: must be non nil	task
	Opts     []Option // optional: can be nil	task选项
}

// 对task任务生成hash字符串
func (c *PeriodicTaskConfig) hash() string {
	h := sha256.New()
	io.WriteString(h, c.Cronspec)
	io.WriteString(h, c.Task.Type())
	h.Write(c.Task.Payload())
	opts := stringifyOptions(c.Opts)
	sort.Strings(opts)
	for _, opt := range opts {
		io.WriteString(h, opt)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// 校验task相关配置
func validatePeriodicTaskConfig(c *PeriodicTaskConfig) error {
	if c == nil {
		return fmt.Errorf("PeriodicTaskConfig cannot be nil")
	}
	if c.Task == nil {
		return fmt.Errorf("PeriodicTaskConfig.Task cannot be nil")
	}
	if c.Cronspec == "" {
		return fmt.Errorf("PeriodicTaskConfig.Cronspec cannot be empty")
	}
	return nil
}

// Start starts a scheduler and background goroutine to sync the scheduler with the configs
// returned by the provider.
//
// Start returns any error encountered at start up time.
// 启动一个调度程序和后台goroutine，以同步调度程序与配置程序返回。
// Start返回启动时遇到的任何错误。
func (mgr *PeriodicTaskManager) Start() error {
	if mgr.s == nil || mgr.p == nil {
		panic("asynq: cannot start uninitialized PeriodicTaskManager; use NewPeriodicTaskManager to initialize")
	}
	// 添加task
	if err := mgr.initialSync(); err != nil {
		return fmt.Errorf("asynq: %v", err)
	}
	// 启动scheduler
	if err := mgr.s.Start(); err != nil {
		return fmt.Errorf("asynq: %v", err)
	}
	mgr.wg.Add(1)
	go func() {
		defer mgr.wg.Done()
		ticker := time.NewTicker(mgr.syncInterval)
		for {
			select {
			case <-mgr.done:
				mgr.s.logger.Debugf("Stopping syncer goroutine")
				ticker.Stop()
				return
			case <-ticker.C:
				mgr.sync() // 动态调整task
			}
		}
	}()
	return nil
}

// Shutdown gracefully shuts down the manager.
// It notifies a background syncer goroutine to stop and stops scheduler.
func (mgr *PeriodicTaskManager) Shutdown() {
	close(mgr.done)
	mgr.wg.Wait()
	mgr.s.Shutdown()
}

// Run starts the manager and blocks until an os signal to exit the program is received.
// Once it receives a signal, it gracefully shuts down the manager.
// 运行启动管理器并阻塞，直到收到一个退出程序的os信号。
// 一旦它收到一个信号，它优雅地关闭管理器。
func (mgr *PeriodicTaskManager) Run() error {
	if err := mgr.Start(); err != nil {
		return err
	}
	mgr.s.waitForSignals()
	mgr.Shutdown()
	mgr.s.logger.Debugf("PeriodicTaskManager exiting")
	return nil
}

// 初始化task
func (mgr *PeriodicTaskManager) initialSync() error {
	// 获取task配置
	configs, err := mgr.p.GetConfigs()
	if err != nil {
		return fmt.Errorf("initial call to GetConfigs failed: %v", err)
	}
	// 校验task
	for _, c := range configs {
		if err := validatePeriodicTaskConfig(c); err != nil {
			return fmt.Errorf("initial call to GetConfigs contained an invalid config: %v", err)
		}
	}
	mgr.add(configs)
	return nil
}

// scheduler中添加task
func (mgr *PeriodicTaskManager) add(configs []*PeriodicTaskConfig) {
	for _, c := range configs {
		entryID, err := mgr.s.Register(c.Cronspec, c.Task, c.Opts...)
		if err != nil {
			mgr.s.logger.Errorf("Failed to register periodic task: cronspec=%q task=%q",
				c.Cronspec, c.Task.Type())
			continue
		}
		mgr.m[c.hash()] = entryID
		mgr.s.logger.Infof("Successfully registered periodic task: cronspec=%q task=%q, entryID=%s",
			c.Cronspec, c.Task.Type(), entryID)
	}
}

// scheduler中删除task
func (mgr *PeriodicTaskManager) remove(removed map[string]string) {
	for hash, entryID := range removed {
		if err := mgr.s.Unregister(entryID); err != nil {
			mgr.s.logger.Errorf("Failed to unregister periodic task: %v", err)
			continue
		}
		delete(mgr.m, hash)
		mgr.s.logger.Infof("Successfully unregistered periodic task: entryID=%s", entryID)
	}
}

func (mgr *PeriodicTaskManager) sync() {
	configs, err := mgr.p.GetConfigs()
	if err != nil {
		mgr.s.logger.Errorf("Failed to get periodic task configs: %v", err)
		return
	}
	for _, c := range configs {
		if err := validatePeriodicTaskConfig(c); err != nil {
			mgr.s.logger.Errorf("Failed to sync: GetConfigs returned an invalid config: %v", err)
			return
		}
	}
	// Diff and only register/unregister the newly added/removed entries.
	removed := mgr.diffRemoved(configs)
	added := mgr.diffAdded(configs)
	mgr.remove(removed)
	mgr.add(added)
}

// diffRemoved diffs the incoming configs with the registered config and returns
// a map containing hash and entryID of each config that was removed.
// diffRemoved将传入的配置与注册的配置进行区分，并返回一个包含哈希值和每个被删除的配置的entryID的映射。
func (mgr *PeriodicTaskManager) diffRemoved(configs []*PeriodicTaskConfig) map[string]string {
	newConfigs := make(map[string]string)
	for _, c := range configs {
		newConfigs[c.hash()] = "" // empty value since we don't have entryID yet
	}
	removed := make(map[string]string)
	for k, v := range mgr.m {
		// test whether existing config is present in the incoming configs
		if _, found := newConfigs[k]; !found {
			removed[k] = v
		}
	}
	return removed
}

// diffAdded diffs the incoming configs with the registered configs and returns
// a list of configs that were added.
// 将传入的配置与已注册的配置进行区分，并返回已添加的配置的列表。
func (mgr *PeriodicTaskManager) diffAdded(configs []*PeriodicTaskConfig) []*PeriodicTaskConfig {
	var added []*PeriodicTaskConfig
	for _, c := range configs {
		if _, found := mgr.m[c.hash()]; !found {
			added = append(added, c)
		}
	}
	return added
}
