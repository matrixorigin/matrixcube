// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/hbstream"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedulers"
	"github.com/matrixorigin/matrixcube/components/prophet/statistics"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"go.uber.org/zap"
)

var (
	errSchedulerExists = errors.New("scheduler already exists")
)

const (
	runSchedulerCheckInterval = 3 * time.Second
	collectFactor             = 0.8
	collectTimeout            = 5 * time.Minute
	maxScheduleRetries        = 10
	maxLoadConfigRetries      = 10

	patrolScanResourceLimit = 128 // It takes about 14 minutes to iterate 1 million resources.
)

// coordinator is used to manage all schedulers and checkers to decide if the resource needs to be scheduled.
type coordinator struct {
	sync.RWMutex

	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
	cluster           *RaftCluster
	checkers          *schedule.CheckerController
	resourceScatterer *schedule.ResourceScatterer
	resourceSplitter  *schedule.ResourceSplitter
	schedulers        map[string]*scheduleController
	opController      *schedule.OperatorController
	hbStreams         *hbstream.HeartbeatStreams
	pluginInterface   *schedule.PluginInterface
}

// newCoordinator creates a new coordinator.
func newCoordinator(ctx context.Context, cluster *RaftCluster, hbStreams *hbstream.HeartbeatStreams) *coordinator {
	ctx, cancel := context.WithCancel(ctx)
	opController := schedule.NewOperatorController(ctx, cluster, hbStreams)
	return &coordinator{
		ctx:               ctx,
		cancel:            cancel,
		cluster:           cluster,
		checkers:          schedule.NewCheckerController(ctx, cluster, cluster.ruleManager, opController),
		resourceScatterer: schedule.NewResourceScatterer(ctx, cluster),
		resourceSplitter:  schedule.NewResourceSplitter(cluster, schedule.NewSplitResourcesHandler(cluster, opController)),
		schedulers:        make(map[string]*scheduleController),
		opController:      opController,
		hbStreams:         hbStreams,
		pluginInterface:   schedule.NewPluginInterface(cluster.GetLogger()),
	}
}

// patrolResources is used to scan resources.
// The checkers will check these resources to decide if they need to do some operations.
func (c *coordinator) patrolResources() {
	defer func() {
		if err := recover(); err != nil {
			c.cluster.logger.Error("fail to patrol resources",
				zap.Any("error", err))
		}
	}()

	defer c.wg.Done()
	timer := time.NewTimer(c.cluster.GetOpts().GetPatrolResourceInterval())
	defer timer.Stop()

	c.cluster.logger.Info("coordinator starts patrol resources")
	keys := make(map[uint64][]byte)
	for _, g := range c.cluster.GetReplicationConfig().Groups {
		keys[g] = nil
	}

	start := time.Now()
	for {
		select {
		case <-timer.C:
			timer.Reset(c.cluster.GetOpts().GetPatrolResourceInterval())
		case <-c.ctx.Done():
			c.cluster.logger.Info("patrol resources has been stopped")
			return
		}

		// Check suspect resources first.
		c.checkSuspectResources()
		// Check suspect key ranges
		c.checkSuspectKeyRanges()
		// Check resources in the waiting list
		c.checkWaitingResources()

		for _, group := range c.cluster.GetReplicationConfig().Groups {
			c.doScan(group, keys)
			if len(keys[group]) == 0 {
				patrolCheckResourcesGauge.Set(time.Since(start).Seconds())
				start = time.Now()
			}
		}
	}
}

func (c *coordinator) doScan(group uint64, keys map[uint64][]byte) {
	key := keys[group]
	resources := c.cluster.ScanResources(group, key, nil, patrolScanResourceLimit)
	if len(resources) == 0 {
		// Resets the scan key.
		keys[group] = nil
		return
	}

	for _, res := range resources {
		// Skips the resource if there is already a pending operator.
		if c.opController.GetOperator(res.Meta.ID()) != nil {
			continue
		}

		c.cluster.logger.Info("on check resource",
			zap.Uint64("resource", res.Meta.ID()))
		ops := c.checkers.CheckResource(res)

		keys[group] = res.GetEndKey()
		if len(ops) == 0 {
			continue
		}

		if !c.opController.ExceedContainerLimit(ops...) {
			c.opController.AddWaitingOperator(ops...)
			c.checkers.RemoveWaitingResource(res.Meta.ID())
			c.cluster.RemoveSuspectResource(res.Meta.ID())
		} else {
			c.checkers.AddWaitingResource(res)
		}
	}
	// Updates the label level isolation statistics.
	c.cluster.updateResourcesLabelLevelStats(resources)
}

func (c *coordinator) checkSuspectResources() {
	for _, id := range c.cluster.GetSuspectResources() {
		res := c.cluster.GetResource(id)
		if res == nil {
			// the resource could be recent split, continue to wait.
			continue
		}
		if c.opController.GetOperator(id) != nil {
			c.cluster.RemoveSuspectResource(id)
			continue
		}
		ops := c.checkers.CheckResource(res)
		if len(ops) == 0 {
			continue
		}

		if !c.opController.ExceedContainerLimit(ops...) {
			c.opController.AddWaitingOperator(ops...)
			c.cluster.RemoveSuspectResource(res.Meta.ID())
		}
	}
}

// checkSuspectKeyRanges would pop one suspect key range group
// The resources of new version key range and old version key range would be placed into
// the suspect resources map
func (c *coordinator) checkSuspectKeyRanges() {
	group, keyRange, success := c.cluster.PopOneSuspectKeyRange()
	if !success {
		return
	}
	limit := 1024
	resources := c.cluster.ScanResources(group, keyRange[0], keyRange[1], limit)
	if len(resources) == 0 {
		return
	}
	resourceIDList := make([]uint64, 0, len(resources))
	for _, res := range resources {
		resourceIDList = append(resourceIDList, res.Meta.ID())
	}

	// if the last resource's end key is smaller the keyRange[1] which means there existed the remaining resources between
	// keyRange[0] and keyRange[1] after scan resources, so we put the end key and keyRange[1] into Suspect KeyRanges
	lastRes := resources[len(resources)-1]
	if lastRes.GetEndKey() != nil && bytes.Compare(lastRes.GetEndKey(), keyRange[1]) < 0 {
		c.cluster.AddSuspectKeyRange(group, lastRes.GetEndKey(), keyRange[1])
	}
	c.cluster.AddSuspectResources(resourceIDList...)
}

func (c *coordinator) checkWaitingResources() {
	items := c.checkers.GetWaitingResources()
	resourceWaitingListGauge.Set(float64(len(items)))
	for _, item := range items {
		id := item.Key
		res := c.cluster.GetResource(id)
		if res == nil {
			// the resource could be recent split, continue to wait.
			continue
		}
		if c.opController.GetOperator(id) != nil {
			c.checkers.RemoveWaitingResource(id)
			continue
		}
		ops := c.checkers.CheckResource(res)
		if len(ops) == 0 {
			continue
		}

		if !c.opController.ExceedContainerLimit(ops...) {
			c.opController.AddWaitingOperator(ops...)
			c.checkers.RemoveWaitingResource(res.Meta.ID())
		}
	}
}

// drivePushOperator is used to push the unfinished operator to the executor.
func (c *coordinator) drivePushOperator() {
	defer func() {
		if err := recover(); err != nil {
			c.cluster.logger.Error("fail to drivePushOperator",
				zap.Any("error", err))
		}
	}()

	defer c.wg.Done()
	c.cluster.logger.Info("coordinator begins to actively drive push operator")
	ticker := time.NewTicker(schedule.PushOperatorTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			c.cluster.logger.Info("drive push operator has been stopped")
			return
		case <-ticker.C:
			c.opController.PushOperators()
		}
	}
}

func (c *coordinator) run() {
	ticker := time.NewTicker(runSchedulerCheckInterval)
	defer ticker.Stop()
	c.cluster.logger.Info("coordinator starts to collect cluster information")
	for {
		if c.shouldRun() {
			c.cluster.logger.Info("coordinator has finished cluster information preparation")
			break
		}
		select {
		case <-ticker.C:
		case <-c.ctx.Done():
			c.cluster.logger.Info("coordinator stops running")
			return
		}
	}
	c.cluster.logger.Info("coordinator starts to run schedulers")
	var (
		scheduleNames []string
		configs       []string
		err           error
	)
	for i := 0; i < maxLoadConfigRetries; i++ {
		scheduleNames, configs, err = c.cluster.storage.LoadAllScheduleConfig()
		select {
		case <-c.ctx.Done():
			c.cluster.logger.Info("coordinator stops running")
			return
		default:
		}
		if err == nil {
			break
		}
		c.cluster.logger.Error("fail to cannot load schedulers' config",
			zap.Int("reties", i),
			zap.Error(err))
	}
	if err != nil {
		c.cluster.logger.Fatal("fail to load schedulers' config",
			zap.Error(err))
	}

	scheduleCfg := c.cluster.opt.GetScheduleConfig().Clone()
	// The new way to create scheduler with the independent configuration.
	for i, name := range scheduleNames {
		data := configs[i]
		typ := schedule.FindSchedulerTypeByName(name)
		var cfg config.SchedulerConfig
		for _, c := range scheduleCfg.Schedulers {
			if c.Type == typ {
				cfg = c
				break
			}
		}
		if len(cfg.Type) == 0 {
			c.cluster.logger.Error("the scheduler not found",
				zap.String("name", name))
			continue
		}
		if cfg.Disable {
			c.cluster.logger.Info("skip create scheduler with independent configuration",
				zap.String("name", name),
				zap.String("type", cfg.Type),
				zap.Any("args", cfg.Args))
			continue
		}
		s, err := schedule.CreateScheduler(cfg.Type, c.opController, c.cluster.storage, schedule.ConfigJSONDecoder([]byte(data)))
		if err != nil {
			c.cluster.logger.Error("fail to create scheduler with independent configuration",
				zap.String("name", name),
				zap.String("type", cfg.Type),
				zap.Any("args", cfg.Args),
				zap.Error(err))
			continue
		}
		c.cluster.logger.Info("create scheduler with independent configuration",
			zap.String("name", s.GetName()))
		if err = c.addScheduler(s); err != nil {
			c.cluster.logger.Error("fail to add scheduler with independent configuration",
				zap.String("name", s.GetName()),
				zap.Any("args", cfg.Args),
				zap.Error(err))
		}
	}

	// The old way to create the scheduler.
	k := 0
	for _, schedulerCfg := range scheduleCfg.Schedulers {
		if schedulerCfg.Disable {
			scheduleCfg.Schedulers[k] = schedulerCfg
			k++
			c.cluster.logger.Info("skip create scheduler",
				zap.String("type", schedulerCfg.Type),
				zap.Any("args", schedulerCfg.Args))
			continue
		}

		s, err := schedule.CreateScheduler(schedulerCfg.Type, c.opController, c.cluster.storage, schedule.ConfigSliceDecoder(schedulerCfg.Type, schedulerCfg.Args))
		if err != nil {
			c.cluster.logger.Error("fail to create scheduler",
				zap.String("type", schedulerCfg.Type),
				zap.Any("args", schedulerCfg.Args),
				zap.Error(err))
			continue
		}

		c.cluster.logger.Info("create scheduler",
			zap.String("name", s.GetName()),
			zap.Any("args", schedulerCfg.Args))
		if err = c.addScheduler(s, schedulerCfg.Args...); err != nil && err != errSchedulerExists {
			c.cluster.logger.Error("fail to add scheduler",
				zap.String("name", s.GetName()),
				zap.Any("args", schedulerCfg.Args),
				zap.Error(err))
		} else {
			// Only records the valid scheduler config.
			scheduleCfg.Schedulers[k] = schedulerCfg
			k++
		}
	}

	// Removes the invalid scheduler config and persist.
	scheduleCfg.Schedulers = scheduleCfg.Schedulers[:k]
	c.cluster.opt.SetScheduleConfig(scheduleCfg)
	if err := c.cluster.opt.Persist(c.cluster.storage); err != nil {
		c.cluster.logger.Error("fail to persist schedule config",
			zap.Error(err))
	}

	c.wg.Add(2)
	// Starts to patrol resources.
	go c.patrolResources()
	go c.drivePushOperator()
}

func (c *coordinator) stop() {
	c.cancel()
}

// Hack to retrieve info from scheduler.
// TODO: remove it.
type hasHotStatus interface {
	GetHotReadStatus() *statistics.ContainerHotPeersInfos
	GetHotWriteStatus() *statistics.ContainerHotPeersInfos
	GetWritePendingInfluence() map[uint64]schedulers.Influence
	GetReadPendingInfluence() map[uint64]schedulers.Influence
}

func (c *coordinator) getHotWriteResources() *statistics.ContainerHotPeersInfos {
	c.RLock()
	defer c.RUnlock()
	s, ok := c.schedulers[schedulers.HotResourceName]
	if !ok {
		return nil
	}
	if h, ok := s.Scheduler.(hasHotStatus); ok {
		return h.GetHotWriteStatus()
	}
	return nil
}

func (c *coordinator) getHotReadResources() *statistics.ContainerHotPeersInfos {
	c.RLock()
	defer c.RUnlock()
	s, ok := c.schedulers[schedulers.HotResourceName]
	if !ok {
		return nil
	}
	if h, ok := s.Scheduler.(hasHotStatus); ok {
		return h.GetHotReadStatus()
	}
	return nil
}

func (c *coordinator) getSchedulers() []string {
	c.RLock()
	defer c.RUnlock()
	names := make([]string, 0, len(c.schedulers))
	for name := range c.schedulers {
		names = append(names, name)
	}
	return names
}

func (c *coordinator) getSchedulerHandlers() map[string]http.Handler {
	c.RLock()
	defer c.RUnlock()
	handlers := make(map[string]http.Handler, len(c.schedulers))
	for name, scheduler := range c.schedulers {
		handlers[name] = scheduler.Scheduler
	}
	return handlers
}

func (c *coordinator) collectSchedulerMetrics() {
	c.RLock()
	defer c.RUnlock()
	for _, s := range c.schedulers {
		var allowScheduler float64
		// If the scheduler is not allowed to schedule, it will disappear in Grafana panel.
		// See issue #1341.
		if !s.IsPaused() {
			allowScheduler = 1
		}
		schedulerStatusGauge.WithLabelValues(s.GetName(), "allow").Set(allowScheduler)
	}
}

func (c *coordinator) resetSchedulerMetrics() {
	schedulerStatusGauge.Reset()
}

func (c *coordinator) collectHotSpotMetrics() {
	c.RLock()
	// Collects hot write resource metrics.
	s, ok := c.schedulers[schedulers.HotResourceName]
	if !ok {
		c.RUnlock()
		return
	}
	c.RUnlock()
	containers := c.cluster.GetContainers()
	status := s.Scheduler.(hasHotStatus).GetHotWriteStatus()
	pendings := s.Scheduler.(hasHotStatus).GetWritePendingInfluence()
	for _, s := range containers {
		containerAddress := s.Meta.Addr()
		containerID := s.Meta.ID()
		containerLabel := fmt.Sprintf("%d", containerID)
		stat, ok := status.AsPeer[containerID]
		if ok {
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "total_written_bytes_as_peer").Set(stat.TotalBytesRate)
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "total_written_keys_as_peer").Set(stat.TotalKeysRate)
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "hot_write_resource_as_peer").Set(float64(stat.Count))
		} else {
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "total_written_bytes_as_peer").Set(0)
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "hot_write_resource_as_peer").Set(0)
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "total_written_keys_as_peer").Set(0)
		}

		stat, ok = status.AsLeader[containerID]
		if ok {
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "total_written_bytes_as_leader").Set(stat.TotalBytesRate)
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "total_written_keys_as_leader").Set(stat.TotalKeysRate)
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "hot_write_resource_as_leader").Set(float64(stat.Count))
		} else {
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "total_written_bytes_as_leader").Set(0)
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "total_written_keys_as_leader").Set(0)
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "hot_write_resource_as_leader").Set(0)
		}

		infl := pendings[containerID]
		// TODO: add to tidb-ansible after merging pending influence into operator influence.
		hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "write_pending_influence_byte_rate").Set(infl.ByteRate)
		hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "write_pending_influence_key_rate").Set(infl.KeyRate)
		hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "write_pending_influence_count").Set(infl.Count)
	}

	// Collects hot read resource metrics.
	status = s.Scheduler.(hasHotStatus).GetHotReadStatus()
	pendings = s.Scheduler.(hasHotStatus).GetReadPendingInfluence()
	for _, s := range containers {
		containerAddress := s.Meta.Addr()
		containerID := s.Meta.ID()
		containerLabel := fmt.Sprintf("%d", containerID)
		stat, ok := status.AsLeader[containerID]
		if ok {
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "total_read_bytes_as_leader").Set(stat.TotalBytesRate)
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "total_read_keys_as_leader").Set(stat.TotalKeysRate)
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "hot_read_resource_as_leader").Set(float64(stat.Count))
		} else {
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "total_read_bytes_as_leader").Set(0)
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "total_read_keys_as_leader").Set(0)
			hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "hot_read_resource_as_leader").Set(0)
		}

		infl := pendings[containerID]
		hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "read_pending_influence_byte_rate").Set(infl.ByteRate)
		hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "read_pending_influence_key_rate").Set(infl.KeyRate)
		hotSpotStatusGauge.WithLabelValues(containerAddress, containerLabel, "read_pending_influence_count").Set(infl.Count)
	}
}

func (c *coordinator) resetHotSpotMetrics() {
	hotSpotStatusGauge.Reset()
}

func (c *coordinator) shouldRun() bool {
	return c.cluster.isPrepared()
}

func (c *coordinator) addScheduler(scheduler schedule.Scheduler, args ...string) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.schedulers[scheduler.GetName()]; ok {
		return errSchedulerExists
	}

	s := newScheduleController(c, scheduler)
	if err := s.Prepare(c.cluster); err != nil {
		return err
	}

	c.wg.Add(1)
	go c.runScheduler(s)
	c.schedulers[s.GetName()] = s
	c.cluster.opt.AddSchedulerCfg(s.GetType(), args)
	return nil
}

func (c *coordinator) removeScheduler(name string) error {
	c.Lock()
	defer c.Unlock()
	if c.cluster == nil {
		return errors.New("cluster not bootstrapped")
	}
	s, ok := c.schedulers[name]
	if !ok {
		return fmt.Errorf("scheduler %s not found", name)
	}

	s.Stop()
	schedulerStatusGauge.WithLabelValues(name, "allow").Set(0)
	delete(c.schedulers, name)

	var err error
	opt := c.cluster.opt

	if err = c.removeOptScheduler(opt, name); err != nil {
		c.cluster.logger.Error("fail to remove scheduler",
			zap.String("name", name),
			zap.Error(err))
		return err
	}

	if err = opt.Persist(c.cluster.storage); err != nil {
		c.cluster.logger.Error("fail to persist scheduler config",
			zap.Error(err))
		return err
	}

	if err = c.cluster.storage.RemoveScheduleConfig(name); err != nil {
		c.cluster.logger.Error("fail to remove the scheduler config",
			zap.Error(err))
		return err
	}

	return nil
}

func (c *coordinator) removeOptScheduler(o *config.PersistOptions, name string) error {
	v := o.GetScheduleConfig().Clone()
	for i, schedulerCfg := range v.Schedulers {
		// To create a temporary scheduler is just used to get scheduler's name
		decoder := schedule.ConfigSliceDecoder(schedulerCfg.Type, schedulerCfg.Args)
		tmp, err := schedule.CreateScheduler(schedulerCfg.Type, schedule.NewOperatorController(c.ctx, c.cluster, nil), storage.NewTestStorage(), decoder)
		if err != nil {
			return err
		}
		if tmp.GetName() == name {
			if config.IsDefaultScheduler(tmp.GetType()) {
				schedulerCfg.Disable = true
				v.Schedulers[i] = schedulerCfg
			} else {
				v.Schedulers = append(v.Schedulers[:i], v.Schedulers[i+1:]...)
			}
			o.SetScheduleConfig(v)
			return nil
		}
	}
	return nil
}

func (c *coordinator) pauseOrResumeScheduler(name string, t int64) error {
	c.Lock()
	defer c.Unlock()
	if c.cluster == nil {
		return errors.New("cluster not bootstrapped")
	}
	var s []*scheduleController
	if name != "all" {
		sc, ok := c.schedulers[name]
		if !ok {
			return fmt.Errorf("scheduler %s not found", name)
		}
		s = append(s, sc)
	} else {
		for _, sc := range c.schedulers {
			s = append(s, sc)
		}
	}
	var err error
	for _, sc := range s {
		var delayUntil int64
		if t > 0 {
			delayUntil = time.Now().Unix() + t
		}
		atomic.StoreInt64(&sc.delayUntil, delayUntil)
	}
	return err
}

func (c *coordinator) isSchedulerPaused(name string) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return false, errors.New("cluster not bootstrapped")
	}
	s, ok := c.schedulers[name]
	if !ok {
		return false, fmt.Errorf("scheduler %s not found", name)
	}
	return s.IsPaused(), nil
}

func (c *coordinator) isSchedulerDisabled(name string) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return false, errors.New("cluster not bootstrapped")
	}
	s, ok := c.schedulers[name]
	if !ok {
		return false, fmt.Errorf("scheduler %s not found", name)
	}
	t := s.GetType()
	scheduleConfig := c.cluster.GetOpts().GetScheduleConfig()
	for _, s := range scheduleConfig.Schedulers {
		if t == s.Type {
			return s.Disable, nil
		}
	}
	return false, nil
}

func (c *coordinator) runScheduler(s *scheduleController) {
	defer func() {
		if err := recover(); err != nil {
			c.cluster.logger.Error("runScheduler failed", zap.Any("error", err))
		}
	}()

	defer c.wg.Done()
	defer s.Cleanup(c.cluster)

	timer := time.NewTimer(s.GetInterval())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			timer.Reset(s.GetInterval())
			if !s.AllowSchedule() {
				continue
			}
			if op := s.Schedule(); op != nil {
				added := c.opController.AddWaitingOperator(op...)
				c.cluster.logger.Debug("operators added",
					zap.String("name", s.GetName()),
					zap.Int("added", added),
					zap.Int("total", len(op)))
			}

		case <-s.Ctx().Done():
			c.cluster.logger.Info("scheduler has been stopped",
				zap.String("name", s.GetName()))
			return
		}
	}
}

// scheduleController is used to manage a scheduler to schedule.
type scheduleController struct {
	schedule.Scheduler
	cluster      *RaftCluster
	opController *schedule.OperatorController
	nextInterval time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	delayUntil   int64
}

// newScheduleController creates a new scheduleController.
func newScheduleController(c *coordinator, s schedule.Scheduler) *scheduleController {
	ctx, cancel := context.WithCancel(c.ctx)
	return &scheduleController{
		Scheduler:    s,
		cluster:      c.cluster,
		opController: c.opController,
		nextInterval: s.GetMinInterval(),
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (s *scheduleController) Ctx() context.Context {
	return s.ctx
}

func (s *scheduleController) Stop() {
	s.cancel()
}

func (s *scheduleController) Schedule() []*operator.Operator {
	for i := 0; i < maxScheduleRetries; i++ {
		// If we have schedule, reset interval to the minimal interval.
		if op := s.Scheduler.Schedule(s.cluster); op != nil {
			s.nextInterval = s.Scheduler.GetMinInterval()
			return op
		}
	}
	s.nextInterval = s.Scheduler.GetNextInterval(s.nextInterval)
	return nil
}

// GetInterval returns the interval of scheduling for a scheduler.
func (s *scheduleController) GetInterval() time.Duration {
	return s.nextInterval
}

// AllowSchedule returns if a scheduler is allowed to schedule.
func (s *scheduleController) AllowSchedule() bool {
	return s.Scheduler.IsScheduleAllowed(s.cluster) && !s.IsPaused()
}

// isPaused returns if a scheduler is paused.
func (s *scheduleController) IsPaused() bool {
	delayUntil := atomic.LoadInt64(&s.delayUntil)
	return time.Now().Unix() < delayUntil
}
