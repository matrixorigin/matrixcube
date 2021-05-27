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
	"github.com/matrixorigin/matrixcube/components/prophet/util"
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
		pluginInterface:   schedule.NewPluginInterface(),
	}
}

// patrolResources is used to scan resources.
// The checkers will check these resources to decide if they need to do some operations.
func (c *coordinator) patrolResources() {
	defer func() {
		if err := recover(); err != nil {
			util.GetLogger().Errorf("patrolResources failed with %+v", err)
		}
	}()

	defer c.wg.Done()
	timer := time.NewTimer(c.cluster.GetOpts().GetPatrolResourceInterval())
	defer timer.Stop()

	util.GetLogger().Info("coordinator starts patrol resources")
	start := time.Now()
	var key []byte
	for {
		select {
		case <-timer.C:
			timer.Reset(c.cluster.GetOpts().GetPatrolResourceInterval())
		case <-c.ctx.Done():
			util.GetLogger().Info("patrol resources has been stopped")
			return
		}

		// Check suspect resources first.
		c.checkSuspectResources()
		// Check suspect key ranges
		c.checkSuspectKeyRanges()
		// Check resources in the waiting list
		c.checkWaitingResources()

		resources := c.cluster.ScanResources(key, nil, patrolScanResourceLimit)
		if len(resources) == 0 {
			// Resets the scan key.
			key = nil
			continue
		}

		for _, res := range resources {
			// Skips the resource if there is already a pending operator.
			if c.opController.GetOperator(res.Meta.ID()) != nil {
				continue
			}

			ops := c.checkers.CheckResource(res)

			key = res.GetEndKey()
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
		if len(key) == 0 {
			patrolCheckResourcesGauge.Set(time.Since(start).Seconds())
			start = time.Now()
		}
	}
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
	keyRange, success := c.cluster.PopOneSuspectKeyRange()
	if !success {
		return
	}
	limit := 1024
	resources := c.cluster.ScanResources(keyRange[0], keyRange[1], limit)
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
		c.cluster.AddSuspectKeyRange(lastRes.GetEndKey(), keyRange[1])
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
			util.GetLogger().Errorf("drivePushOperator failed with %+v", err)
		}
	}()

	defer c.wg.Done()
	util.GetLogger().Info("coordinator begins to actively drive push operator")
	ticker := time.NewTicker(schedule.PushOperatorTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			util.GetLogger().Info("drive push operator has been stopped")
			return
		case <-ticker.C:
			c.opController.PushOperators()
		}
	}
}

func (c *coordinator) run() {
	ticker := time.NewTicker(runSchedulerCheckInterval)
	defer ticker.Stop()
	util.GetLogger().Info("coordinator starts to collect cluster information")
	for {
		if c.shouldRun() {
			util.GetLogger().Info("coordinator has finished cluster information preparation")
			break
		}
		select {
		case <-ticker.C:
		case <-c.ctx.Done():
			util.GetLogger().Info("coordinator stops running")
			return
		}
	}
	util.GetLogger().Info("coordinator starts to run schedulers")
	var (
		scheduleNames []string
		configs       []string
		err           error
	)
	for i := 0; i < maxLoadConfigRetries; i++ {
		scheduleNames, configs, err = c.cluster.storage.LoadAllScheduleConfig()
		select {
		case <-c.ctx.Done():
			util.GetLogger().Info("coordinator stops running")
			return
		default:
		}
		if err == nil {
			break
		}
		util.GetLogger().Errorf("cannot load schedulers' config, retry times %d, error %+v",
			i,
			err)
	}
	if err != nil {
		util.GetLogger().Fatalf("cannot load schedulers' config, error %+v",
			err)
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
			util.GetLogger().Errorf("the scheduler %s type not found",
				name)
			continue
		}
		if cfg.Disable {
			util.GetLogger().Infof("skip create scheduler %s type %s with independent configuration, args %+v",
				name,
				cfg.Type,
				cfg.Args)
			continue
		}
		s, err := schedule.CreateScheduler(cfg.Type, c.opController, c.cluster.storage, schedule.ConfigJSONDecoder([]byte(data)))
		if err != nil {
			util.GetLogger().Errorf("can not create scheduler %s with independent configuration, args %+v, error %+v",
				name,
				cfg.Args,
				err)
			continue
		}
		util.GetLogger().Infof("create scheduler %s with independent configuration",
			s.GetName())
		if err = c.addScheduler(s); err != nil {
			util.GetLogger().Error("can not add scheduler %s with independent configuration, args %+v, error %+v",
				s.GetName(),
				cfg.Args,
				err)
		}
	}

	// The old way to create the scheduler.
	k := 0
	for _, schedulerCfg := range scheduleCfg.Schedulers {
		if schedulerCfg.Disable {
			scheduleCfg.Schedulers[k] = schedulerCfg
			k++
			util.GetLogger().Infof("skip create scheduler, type %s, args %+v",
				schedulerCfg.Type,
				schedulerCfg.Args)
			continue
		}

		s, err := schedule.CreateScheduler(schedulerCfg.Type, c.opController, c.cluster.storage, schedule.ConfigSliceDecoder(schedulerCfg.Type, schedulerCfg.Args))
		if err != nil {
			util.GetLogger().Errorf("create scheduler type %s, args %+v failed with %+v",
				schedulerCfg.Type,
				schedulerCfg.Args,
				err)
			continue
		}

		util.GetLogger().Infof("create scheduler %s, args %+v",
			s.GetName(),
			schedulerCfg.Args)
		if err = c.addScheduler(s, schedulerCfg.Args...); err != nil && err != errSchedulerExists {
			util.GetLogger().Errorf("can not add scheduler %s, args %+v",
				s.GetName(),
				schedulerCfg.Args,
				err)
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
		util.GetLogger().Errorf("cannot persist schedule config, error %+v",
			err)
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
		util.GetLogger().Errorf("can not remove scheduler %s, failed with %+v",
			name,
			err)
		return err
	}

	if err = opt.Persist(c.cluster.storage); err != nil {
		util.GetLogger().Errorf("persist scheduler config failed with %+v",
			err)
		return err
	}

	if err = c.cluster.storage.RemoveScheduleConfig(name); err != nil {
		util.GetLogger().Errorf("remove the scheduler config failed with %+v",
			err)
		return err
	}

	return nil
}

func (c *coordinator) removeOptScheduler(o *config.PersistOptions, name string) error {
	v := o.GetScheduleConfig().Clone()
	for i, schedulerCfg := range v.Schedulers {
		// To create a temporary scheduler is just used to get scheduler's name
		decoder := schedule.ConfigSliceDecoder(schedulerCfg.Type, schedulerCfg.Args)
		tmp, err := schedule.CreateScheduler(schedulerCfg.Type, schedule.NewOperatorController(c.ctx, nil, nil), storage.NewTestStorage(), decoder)
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
			util.GetLogger().Errorf("runScheduler failed with %+v", err)
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
				util.GetLogger().Debugf("scheduler %s add %d operators, total %d",
					s.GetName(),
					added,
					len(op))
			}

		case <-s.Ctx().Done():
			util.GetLogger().Infof("scheduler %s has been stopped",
				s.GetName())
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
