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
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/event"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/checker"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/hbstream"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/statistics"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util/cache"
	"github.com/matrixorigin/matrixcube/components/prophet/util/keyutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	batch = int64(256)
)

var backgroundJobInterval = 10 * time.Second

const (
	clientTimeout            = 3 * time.Second
	defaultChangedEventLimit = 10000
	// persistLimitRetryTimes is used to reduce the probability of the persistent error
	// since the once the store is add or remove, we shouldn't return an error even if the store limit is failed to persist.
	persistLimitRetryTimes = 5
	persistLimitWaitTime   = 100 * time.Millisecond
)

var (
	errResourceDestroyed = errors.New("resource destroyed")
)

// Server is the interface for cluster.
type Server interface {
	GetConfig() *config.Config
	GetPersistOptions() *config.PersistOptions
	GetStorage() storage.Storage
	GetHBStreams() *hbstream.HeartbeatStreams
	GetRaftCluster() *RaftCluster
	GetBasicCluster() *core.BasicCluster
}

// RaftCluster is used for cluster config management.
// Raft cluster key format:
// cluster 1 -> /1/raft, value is metapb.Cluster
// cluster 2 -> /2/raft
// For cluster 1
// container 1 -> /1/raft/s/1, value is metadata.Container
// resource 1 -> /1/raft/r/1, value is metadata.Resource
type RaftCluster struct {
	sync.RWMutex
	ctx context.Context

	running bool

	clusterID   uint64
	clusterRoot string

	// cached cluster info
	core    *core.BasicCluster
	opt     *config.PersistOptions
	storage storage.Storage
	limiter *ContainerLimiter

	prepareChecker  *prepareChecker
	changedEvents   chan rpcpb.EventNotify
	createResourceC chan struct{}

	labelLevelStats *statistics.LabelStatistics
	resourceStats   *statistics.ResourceStatistics
	hotStat         *statistics.HotStat

	coordinator      *coordinator
	suspectResources *cache.TTLUint64 // suspectResources are resources that may need fix
	suspectKeyRanges *cache.TTLString // suspect key-range resources that may need fix

	wg   sync.WaitGroup
	quit chan struct{}

	ruleManager                 *placement.RuleManager
	etcdClient                  *clientv3.Client
	adapter                     metadata.Adapter
	resourceStateChangedHandler func(res metadata.Resource, from metapb.ResourceState, to metapb.ResourceState)

	logger *zap.Logger
}

// NewRaftCluster create a new cluster.
func NewRaftCluster(ctx context.Context,
	root string,
	clusterID uint64,
	etcdClient *clientv3.Client,
	adapter metadata.Adapter,
	resourceStateChangedHandler func(res metadata.Resource, from metapb.ResourceState, to metapb.ResourceState),
	logger *zap.Logger) *RaftCluster {
	return &RaftCluster{
		ctx:                         ctx,
		running:                     false,
		clusterID:                   clusterID,
		clusterRoot:                 root,
		etcdClient:                  etcdClient,
		adapter:                     adapter,
		resourceStateChangedHandler: resourceStateChangedHandler,
		logger:                      log.Adjust(logger).Named("raft-cluster"),
	}
}

// GetReplicationConfig get the replication config.
func (c *RaftCluster) GetReplicationConfig() *config.ReplicationConfig {
	cfg := &config.ReplicationConfig{}
	*cfg = *c.opt.GetReplicationConfig()
	return cfg
}

// InitCluster initializes the raft cluster.
func (c *RaftCluster) InitCluster(opt *config.PersistOptions, storage storage.Storage, basicCluster *core.BasicCluster) {
	basicCluster.Reset()
	c.core = basicCluster
	c.opt = opt
	c.storage = storage
	c.labelLevelStats = statistics.NewLabelStatistics()
	c.hotStat = statistics.NewHotStat()
	c.prepareChecker = newPrepareChecker()
	c.suspectResources = cache.NewIDTTL(c.ctx, time.Minute, 3*time.Minute)
	c.suspectKeyRanges = cache.NewStringTTL(c.ctx, time.Minute, 3*time.Minute)

	c.changedEvents = make(chan rpcpb.EventNotify, defaultChangedEventLimit)
	c.createResourceC = make(chan struct{}, 1)
}

// Start starts a cluster.
func (c *RaftCluster) Start(s Server) error {
	c.Lock()
	defer c.Unlock()

	if c.running {
		c.logger.Warn("raft cluster has already been started")
		return nil
	}

	c.InitCluster(s.GetPersistOptions(), s.GetStorage(), s.GetBasicCluster())
	cluster, err := c.LoadClusterInfo()
	if err != nil {
		return err
	}
	if cluster == nil {
		return nil
	}

	c.ruleManager = placement.NewRuleManager(c.storage, c, c.GetLogger())
	if c.opt.IsPlacementRulesEnabled() {
		err = c.ruleManager.Initialize(c.opt.GetMaxReplicas(), c.opt.GetLocationLabels())
		if err != nil {
			return err
		}
	}

	c.coordinator = newCoordinator(c.ctx, cluster, s.GetHBStreams())
	c.resourceStats = statistics.NewResourceStatistics(c.opt, c.ruleManager)
	c.limiter = NewContainerLimiter(s.GetPersistOptions(), c.logger)
	c.quit = make(chan struct{})

	c.wg.Add(2)
	go c.runCoordinator()
	go c.runBackgroundJobs(backgroundJobInterval)
	c.running = true

	return nil
}

// LoadClusterInfo loads cluster related info.
func (c *RaftCluster) LoadClusterInfo() (*RaftCluster, error) {
	start := time.Now()
	if err := c.storage.LoadContainers(batch, func(meta metadata.Container, leaderWeight, resourceWeight float64) {
		c.core.PutContainer(core.NewCachedContainer(meta,
			core.SetLeaderWeight(leaderWeight),
			core.SetResourceWeight(resourceWeight)))
	}); err != nil {
		return nil, err
	}
	c.logger.Info("containers loaded",
		zap.Int("count", c.GetContainerCount()),
		zap.Duration("cost", time.Since(start)))

	// used to load resource from kv storage to cache storage.
	start = time.Now()
	if err := c.storage.LoadResources(batch, func(meta metadata.Resource) {
		c.core.CheckAndPutResource(core.NewCachedResource(meta, nil))
	}); err != nil {
		return nil, err
	}
	c.logger.Info("resources loaded",
		zap.Int("count", c.GetResourceCount()),
		zap.Duration("cost", time.Since(start)))

	for _, container := range c.GetContainers() {
		c.hotStat.GetOrCreateRollingContainerStats(container.Meta.ID())
	}

	// load resource group rules
	start = time.Now()
	c.storage.LoadScheduleGroupRules(batch, func(rule metapb.ScheduleGroupRule) {
		c.core.AddScheduleGroupRule(rule)
	})
	c.logger.Info("resource group rules loaded",
		zap.Int("count", c.core.GetResourceGroupRuleCount()),
		zap.Duration("cost", time.Since(start)))
	return c, nil
}

func (c *RaftCluster) runBackgroundJobs(interval time.Duration) {
	defer func() {
		if err := recover(); err != nil {
			c.logger.Error("runBackgroundJobs crashed", zap.Any("error", err))
		}
	}()
	defer c.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.quit:
			c.logger.Info("metrics are reset")
			c.resetMetrics()
			c.Lock()
			close(c.createResourceC)
			close(c.changedEvents)
			c.Unlock()
			c.logger.Info("background jobs has been stopped")
			return
		case <-ticker.C:
			c.checkContainers()
			c.collectMetrics()
			c.coordinator.opController.PruneHistory()
			c.doNotifyCreateResources()
		case <-c.createResourceC:
			c.doNotifyCreateResources()
		}
	}
}

func (c *RaftCluster) runCoordinator() {
	defer func() {
		if err := recover(); err != nil {
			c.logger.Error("runBackgroundJobs crashed", zap.Any("error", err))
		}
	}()

	defer c.wg.Done()
	defer func() {
		c.coordinator.wg.Wait()
		c.logger.Info("coordinator has been stopped")
	}()
	c.coordinator.run()
	<-c.coordinator.ctx.Done()
	c.logger.Info("coordinator is stopping")
}

// Stop stops the cluster.
func (c *RaftCluster) Stop() {
	c.Lock()

	if !c.running {
		c.Unlock()
		return
	}

	c.running = false
	close(c.quit)
	c.coordinator.stop()
	c.Unlock()
	c.wg.Wait()
}

// IsRunning return if the cluster is running.
func (c *RaftCluster) IsRunning() bool {
	c.RLock()
	defer c.RUnlock()
	return c.running
}

//GetScheduleGroupKeys returns group keys
func (c *RaftCluster) GetScheduleGroupKeys() []string {
	c.RLock()
	defer c.RUnlock()
	return c.core.GetScheduleGroupKeys()
}

// GetOperatorController returns the operator controller.
func (c *RaftCluster) GetOperatorController() *schedule.OperatorController {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.opController
}

// GetResourceScatter returns the resource scatter.
func (c *RaftCluster) GetResourceScatter() *schedule.ResourceScatterer {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.resourceScatterer
}

// GetResourceSplitter returns the resource splitter
func (c *RaftCluster) GetResourceSplitter() *schedule.ResourceSplitter {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.resourceSplitter
}

// GetHeartbeatStreams returns the heartbeat streams.
func (c *RaftCluster) GetHeartbeatStreams() *hbstream.HeartbeatStreams {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.hbStreams
}

// GetStorage returns the storage.
func (c *RaftCluster) GetStorage() storage.Storage {
	c.RLock()
	defer c.RUnlock()
	return c.storage
}

// SetStorage set the storage for test purpose.
func (c *RaftCluster) SetStorage(s storage.Storage) {
	c.Lock()
	defer c.Unlock()
	c.storage = s
}

// GetOpts returns cluster's configuration.
func (c *RaftCluster) GetOpts() *config.PersistOptions {
	return c.opt
}

// AddSuspectResources adds resources to suspect list.
func (c *RaftCluster) AddSuspectResources(resourceIDs ...uint64) {
	c.Lock()
	defer c.Unlock()
	for _, resourceID := range resourceIDs {
		c.suspectResources.Put(resourceID, nil)
	}
}

// GetSuspectResources gets all suspect resources.
func (c *RaftCluster) GetSuspectResources() []uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.suspectResources.GetAllID()
}

// RemoveSuspectResource removes resource from suspect list.
func (c *RaftCluster) RemoveSuspectResource(id uint64) {
	c.Lock()
	defer c.Unlock()
	c.suspectResources.Remove(id)
}

// AddSuspectKeyRange adds the key range with the its ruleID as the key
// The instance of each keyRange is like following format:
// [2][]byte: start key/end key
func (c *RaftCluster) AddSuspectKeyRange(group uint64, start, end []byte) {
	c.Lock()
	defer c.Unlock()
	c.suspectKeyRanges.Put(keyutil.BuildKeyRangeKey(group, start, end), [2][]byte{start, end})
}

// PopOneSuspectKeyRange gets one suspect keyRange group.
// it would return value and true if pop success, or return empty [][2][]byte and false
// if suspectKeyRanges couldn't pop keyRange group.
func (c *RaftCluster) PopOneSuspectKeyRange() (uint64, [2][]byte, bool) {
	c.Lock()
	defer c.Unlock()
	key, value, success := c.suspectKeyRanges.Pop()
	if !success {
		return 0, [2][]byte{}, false
	}
	v, ok := value.([2][]byte)
	if !ok {
		return 0, [2][]byte{}, false
	}
	return keyutil.GetGroupFromRangeKey(key), v, true
}

// ClearSuspectKeyRanges clears the suspect keyRanges, only for unit test
func (c *RaftCluster) ClearSuspectKeyRanges() {
	c.Lock()
	defer c.Unlock()
	c.suspectKeyRanges.Clear()
}

// HandleContainerHeartbeat updates the container status.
func (c *RaftCluster) HandleContainerHeartbeat(stats *metapb.ContainerStats) error {
	c.Lock()
	defer c.Unlock()

	containerID := stats.GetContainerID()
	container := c.GetContainer(containerID)
	if container == nil {
		return fmt.Errorf("container %v not found", containerID)
	}
	newContainer := container.Clone(core.SetContainerStats(stats), core.SetLastHeartbeatTS(time.Now()))
	if newContainer.IsLowSpace(c.opt.GetLowSpaceRatio(), c.core.GetScheduleGroupKeys()) {
		c.logger.Warn("container does not have enough disk space, capacity %d, available %d",
			zap.Uint64("container", newContainer.Meta.ID()),
			zap.Uint64("capacity", newContainer.GetCapacity()),
			zap.Uint64("available", newContainer.GetAvailable()))
	}
	if newContainer.NeedPersist() && c.storage != nil {
		if err := c.storage.PutContainer(newContainer.Meta); err != nil {
			c.logger.Error("fail to persist container",
				zap.Uint64("contianer", newContainer.Meta.ID()),
				zap.Error(err))
		} else {
			newContainer = newContainer.Clone(core.SetLastPersistTime(time.Now()))
		}

		c.changedEvents <- event.NewContainerEvent(newContainer.Meta)
	}
	if container := c.core.GetContainer(newContainer.Meta.ID()); container != nil {
		c.hotStat.UpdateContainerHeartbeatMetrics(container)
	}
	c.core.PutContainer(newContainer)
	c.changedEvents <- event.NewContainerStatsEvent(newContainer.GetContainerStats())
	c.hotStat.Observe(newContainer.Meta.ID(), newContainer.GetContainerStats())
	c.hotStat.UpdateTotalLoad(c.core.GetContainers())
	c.hotStat.FilterUnhealthyContainer(c)

	// c.limiter is nil before "start" is called
	if c.limiter != nil && c.opt.GetContainerLimitMode() == "auto" {
		c.limiter.Collect(newContainer.GetContainerStats())
	}

	return nil
}

// processResourceHeartbeat updates the resource information.
func (c *RaftCluster) processResourceHeartbeat(res *core.CachedResource) error {
	c.RLock()
	origin, err := c.core.PreCheckPutResource(res)
	if err != nil {
		c.RUnlock()
		return err
	}

	writeItems := c.CheckWriteStatus(res)
	readItems := c.CheckReadStatus(res)
	c.RUnlock()

	// Cube support remove running resources asynchronously, it will add remove job into embed etcd, and
	// each node execute these job on local to remove resource. So we need check whether the resource removed
	// or not.
	var checkMaybeDestroyed metadata.Resource
	if origin != nil {
		checkMaybeDestroyed = origin.Meta
	}
	if checkMaybeDestroyed == nil {
		checkMaybeDestroyed, err = c.storage.GetResource(res.Meta.ID())
		if err != nil {
			return err
		}
	}
	if checkMaybeDestroyed != nil && checkMaybeDestroyed.State() == metapb.ResourceState_Destroyed {
		return errResourceDestroyed
	}

	// Save to storage if meta is updated.
	// Save to cache if meta or leader is updated, or contains any down/pending peer.
	// Mark isNew if the resource in cache does not have leader.
	var saveKV, saveCache, isNew bool
	if origin == nil {
		c.logger.Debug("insert new resource",
			zap.Uint64("reosurce", res.Meta.ID()))
		saveKV, saveCache, isNew = true, true, true
	} else {
		r := res.Meta.Epoch()
		o := origin.Meta.Epoch()
		if r.GetVersion() > o.GetVersion() {
			c.logger.Info("resource version changed",
				zap.Uint64("reosurce", res.Meta.ID()),
				zap.Uint64("from", o.GetVersion()),
				zap.Uint64("to", r.GetVersion()))
			saveKV, saveCache = true, true
		}
		if r.GetConfVer() > o.GetConfVer() {
			c.logger.Info("resource ConfVer changed",
				zap.Uint64("reosurce", res.Meta.ID()),
				zap.Uint64("from", o.GetConfVer()),
				zap.Uint64("to", r.GetConfVer()))
			saveKV, saveCache = true, true
		}
		if res.GetLeader().GetID() != origin.GetLeader().GetID() {
			if origin.GetLeader().GetID() == 0 {
				isNew = true
			} else {
				c.logger.Info("resource leader changed",
					zap.Uint64("reosurce", res.Meta.ID()),
					zap.Uint64("from", origin.GetLeader().GetContainerID()),
					zap.Uint64("to", res.GetLeader().GetContainerID()))
			}
			saveCache = true
		}
		if !core.SortedPeersStatsEqual(res.GetDownPeers(), origin.GetDownPeers()) {
			saveCache = true
		}
		if !core.SortedPeersEqual(res.GetPendingPeers(), origin.GetPendingPeers()) {
			saveCache = true
		}
		if len(res.Meta.Peers()) != len(origin.Meta.Peers()) {
			saveKV, saveCache = true, true
		}
		if res.Meta.State() != origin.Meta.State() {
			saveKV, saveCache = true, true
		}

		if res.GetApproximateSize() != origin.GetApproximateSize() ||
			res.GetApproximateKeys() != origin.GetApproximateKeys() {
			saveCache = true
		}

		if res.GetBytesWritten() != origin.GetBytesWritten() ||
			res.GetBytesRead() != origin.GetBytesRead() ||
			res.GetKeysWritten() != origin.GetKeysWritten() ||
			res.GetKeysRead() != origin.GetKeysRead() {
			saveCache = true
		}

	}

	if len(writeItems) == 0 && len(readItems) == 0 && !saveKV && !saveCache && !isNew {
		return nil
	}

	c.Lock()
	inCreating := c.core.IsWaittingCreateResource(res.Meta.ID())
	if isNew && inCreating {
		if c.resourceStateChangedHandler != nil {
			c.resourceStateChangedHandler(res.Meta, metapb.ResourceState_Creating,
				metapb.ResourceState_Running)
		}
	}

	if saveCache {
		// To prevent a concurrent heartbeat of another resource from overriding the up-to-date resource info by a stale one,
		// check its validation again here.
		//
		// However it can't solve the race condition of concurrent heartbeats from the same resource.
		if _, err := c.core.PreCheckPutResource(res); err != nil {
			c.Unlock()
			return err
		}

		overlaps := c.core.PutResource(res)
		if c.storage != nil {
			for _, item := range overlaps {
				if err := c.storage.RemoveResource(item.Meta); err != nil {
					c.logger.Error("fail to delete resource from storage",
						zap.Uint64("resource", item.Meta.ID()),
						zap.Error(err))
				}
			}
		}
		for _, item := range overlaps {
			if c.resourceStats != nil {
				c.resourceStats.ClearDefunctResource(item.Meta.ID())
			}
			c.labelLevelStats.ClearDefunctResource(item.Meta.ID())
		}

		// Update related containers.
		containerMap := make(map[uint64]struct{})
		for _, p := range res.Meta.Peers() {
			containerMap[p.GetContainerID()] = struct{}{}
		}
		if origin != nil {
			for _, p := range origin.Meta.Peers() {
				containerMap[p.GetContainerID()] = struct{}{}
			}
		}
		for key := range containerMap {
			c.updateContainerStatusLocked(res.GetGroupKey(), key)
		}
		resourceEventCounter.WithLabelValues("update_cache").Inc()
	}

	if isNew {
		c.prepareChecker.collect(res)
	}

	if c.resourceStats != nil {
		c.resourceStats.Observe(res, c.takeResourceContainersLocked(res))
	}

	for _, writeItem := range writeItems {
		c.hotStat.Update(writeItem)
	}
	for _, readItem := range readItems {
		c.hotStat.Update(readItem)
	}
	c.Unlock()

	// If there are concurrent heartbeats from the same resource, the last write will win even if
	// writes to storage in the critical area. So don't use mutex to protect it.
	if saveKV && c.storage != nil {
		if !inCreating && res.Meta.State() == metapb.ResourceState_Creating {
			res = res.Clone(core.WithState(metapb.ResourceState_Running))
		}

		if err := c.storage.PutResource(res.Meta); err != nil {
			// Not successfully saved to storage is not fatal, it only leads to longer warm-up
			// after restart. Here we only log the error then go on updating cache.
			c.logger.Error("fail to save resource to storage",
				zap.Uint64("resource", res.Meta.ID()),
				zap.Error(err))
		}
		resourceEventCounter.WithLabelValues("update_kv").Inc()
	}
	c.RLock()
	if saveKV || saveCache || isNew {
		if c.changedEvents != nil {
			c.changedEvents <- event.NewResourceEvent(res.Meta, res.GetLeader().GetID(), false, false)
		}
	}
	if saveCache {
		if c.changedEvents != nil {
			c.changedEvents <- event.NewResourceStatsEvent(res.GetStat())
		}
	}
	c.RUnlock()

	return nil
}

func (c *RaftCluster) updateContainerStatusLocked(groupKey string, id uint64) {
	leaderCount := c.core.GetContainerLeaderCount(groupKey, id)
	resourceCount := c.core.GetContainerResourceCount(groupKey, id)
	pendingPeerCount := c.core.GetContainerPendingPeerCount(groupKey, id)
	leaderResourceSize := c.core.GetContainerLeaderResourceSize(groupKey, id)
	resourceSize := c.core.GetContainerResourceSize(groupKey, id)
	c.core.UpdateContainerStatus(groupKey, id, leaderCount, resourceCount, pendingPeerCount, leaderResourceSize, resourceSize)
}

// GetResourceByKey gets CachedResource by resource key from cluster.
func (c *RaftCluster) GetResourceByKey(group uint64, resourceKey []byte) *core.CachedResource {
	return c.core.SearchResource(group, resourceKey)
}

// GetPrevResourceByKey gets previous resource and leader peer by the resource key from cluster.
func (c *RaftCluster) GetPrevResourceByKey(group uint64, resourceKey []byte) *core.CachedResource {
	return c.core.SearchPrevResource(group, resourceKey)
}

// ScanResources scans resource with start key, until the resource contains endKey, or
// total number greater than limit.
func (c *RaftCluster) ScanResources(group uint64, startKey, endKey []byte, limit int) []*core.CachedResource {
	return c.core.ScanRange(group, startKey, endKey, limit)
}

// GetResource searches for a resource by ID.
func (c *RaftCluster) GetResource(resourceID uint64) *core.CachedResource {
	return c.core.GetResource(resourceID)
}

// GetMetaResources gets resources from cluster.
func (c *RaftCluster) GetMetaResources() []metadata.Resource {
	return c.core.GetMetaResources()
}

// GetResources returns all resources' information in detail.
func (c *RaftCluster) GetResources() []*core.CachedResource {
	return c.core.GetResources()
}

// GetResourceCount returns total count of resources
func (c *RaftCluster) GetResourceCount() int {
	return c.core.GetResourceCount()
}

// GetContainerResources returns all resources' information with a given containerID.
func (c *RaftCluster) GetContainerResources(groupKey string, containerID uint64) []*core.CachedResource {
	return c.core.GetContainerResources(groupKey, containerID)
}

// RandLeaderResource returns a random resource that has leader on the container.
func (c *RaftCluster) RandLeaderResource(groupKey string, containerID uint64, ranges []core.KeyRange, opts ...core.ResourceOption) *core.CachedResource {
	return c.core.RandLeaderResource(groupKey, containerID, ranges, opts...)
}

// RandFollowerResource returns a random resource that has a follower on the container.
func (c *RaftCluster) RandFollowerResource(groupKey string, containerID uint64, ranges []core.KeyRange, opts ...core.ResourceOption) *core.CachedResource {
	return c.core.RandFollowerResource(groupKey, containerID, ranges, opts...)
}

// RandPendingResource returns a random resource that has a pending peer on the container.
func (c *RaftCluster) RandPendingResource(groupKey string, containerID uint64, ranges []core.KeyRange, opts ...core.ResourceOption) *core.CachedResource {
	return c.core.RandPendingResource(groupKey, containerID, ranges, opts...)
}

// RandLearnerResource returns a random resource that has a learner peer on the container.
func (c *RaftCluster) RandLearnerResource(groupKey string, containerID uint64, ranges []core.KeyRange, opts ...core.ResourceOption) *core.CachedResource {
	return c.core.RandLearnerResource(groupKey, containerID, ranges, opts...)
}

// RandHotResourceFromContainer randomly picks a hot resource in specified container.
func (c *RaftCluster) RandHotResourceFromContainer(container uint64, kind statistics.FlowKind) *core.CachedResource {
	c.RLock()
	defer c.RUnlock()
	r := c.hotStat.RandHotResourceFromContainer(container, kind, c.opt.GetHotResourceCacheHitsThreshold())
	if r == nil {
		return nil
	}
	return c.GetResource(r.ResourceID)
}

// GetLeaderContainer returns all containers that contains the resource's leader peer.
func (c *RaftCluster) GetLeaderContainer(res *core.CachedResource) *core.CachedContainer {
	return c.core.GetLeaderContainer(res)
}

// GetFollowerContainers returns all containers that contains the resource's follower peer.
func (c *RaftCluster) GetFollowerContainers(res *core.CachedResource) []*core.CachedContainer {
	return c.core.GetFollowerContainers(res)
}

// GetResourceContainers returns all containers that contains the resource's peer.
func (c *RaftCluster) GetResourceContainers(res *core.CachedResource) []*core.CachedContainer {
	return c.core.GetResourceContainers(res)
}

// GetContainerCount returns the count of containers.
func (c *RaftCluster) GetContainerCount() int {
	return c.core.GetContainerCount()
}

// GetContainerResourceCount returns the number of resources for a given container.
func (c *RaftCluster) GetContainerResourceCount(groupKey string, containerID uint64) int {
	return c.core.GetContainerResourceCount(groupKey, containerID)
}

// GetAverageResourceSize returns the average resource approximate size.
func (c *RaftCluster) GetAverageResourceSize() int64 {
	return c.core.GetAverageResourceSize()
}

// GetResourceStats returns resource statistics from cluster.
func (c *RaftCluster) GetResourceStats(group uint64, startKey, endKey []byte) *statistics.ResourceStats {
	c.RLock()
	defer c.RUnlock()
	return statistics.GetResourceStats(c.core.ScanRange(group, startKey, endKey, -1))
}

// GetContainersStats returns containers' statistics from cluster.
// And it will be unnecessary to filter unhealthy container, because it has been solved in process heartbeat
func (c *RaftCluster) GetContainersStats() *statistics.ContainersStats {
	c.RLock()
	defer c.RUnlock()
	return c.hotStat.ContainersStats
}

// DropCacheResource removes a resource from the cache.
func (c *RaftCluster) DropCacheResource(id uint64) {
	c.RLock()
	defer c.RUnlock()
	if res := c.GetResource(id); res != nil {
		c.core.RemoveResource(res)
	}
}

// GetCacheCluster gets the cached cluster.
func (c *RaftCluster) GetCacheCluster() *core.BasicCluster {
	c.RLock()
	defer c.RUnlock()
	return c.core
}

// GetMetaContainers gets containers from cluster.
func (c *RaftCluster) GetMetaContainers() []metadata.Container {
	return c.core.GetMetaContainers()
}

// GetContainers returns all containers in the cluster.
func (c *RaftCluster) GetContainers() []*core.CachedContainer {
	return c.core.GetContainers()
}

// GetContainer gets container from cluster.
func (c *RaftCluster) GetContainer(containerID uint64) *core.CachedContainer {
	return c.core.GetContainer(containerID)
}

// IsResourceHot checks if a resource is in hot state.
func (c *RaftCluster) IsResourceHot(res *core.CachedResource) bool {
	c.RLock()
	defer c.RUnlock()
	return c.hotStat.IsResourceHot(res, c.opt.GetHotResourceCacheHitsThreshold())
}

// GetAdjacentResources returns resources' information that are adjacent with the specific resource ID.
func (c *RaftCluster) GetAdjacentResources(res *core.CachedResource) (*core.CachedResource, *core.CachedResource) {
	return c.core.GetAdjacentResources(res)
}

// UpdateContainerLabels updates a container's location labels
// If 'force' is true, then update the container's labels forcibly.
func (c *RaftCluster) UpdateContainerLabels(containerID uint64, labels []metapb.Pair, force bool) error {
	container := c.GetContainer(containerID)
	if container == nil {
		return fmt.Errorf("invalid container ID %d, not found", containerID)
	}
	newContainer := container.Meta.Clone()
	newContainer.SetLabels(labels)
	// PutContainer will perform label merge.
	return c.putContainerImpl(newContainer, force)
}

// PutContainer puts a container.
func (c *RaftCluster) PutContainer(container metadata.Container) error {
	if err := c.putContainerImpl(container, false); err != nil {
		return err
	}
	c.AddContainerLimit(container)
	return nil
}

// putContainerImpl puts a container.
// If 'force' is true, then overwrite the container's labels.
func (c *RaftCluster) putContainerImpl(container metadata.Container, force bool) error {
	c.Lock()
	defer c.Unlock()

	if container.ID() == 0 {
		return fmt.Errorf("invalid put container %v", container)
	}

	// container address can not be the same as other containers.
	for _, s := range c.GetContainers() {
		// It's OK to start a new store on the same address if the old store has been removed or physically destroyed.
		if s.IsTombstone() || s.IsPhysicallyDestroyed() {

			continue
		}
		if s.Meta.ID() != container.ID() && s.Meta.Addr() == container.Addr() {
			return fmt.Errorf("duplicated container address: %v, already registered by %v", container, s.Meta)
		}
	}

	s := c.GetContainer(container.ID())
	if s == nil {
		// Add a new container.
		s = core.NewCachedContainer(container)
	} else {
		// Use the given labels to update the container.
		labels := container.Labels()
		if !force {
			// If 'force' isn't set, the given labels will merge into those labels which already existed in the container.
			labels = s.MergeLabels(labels)
		}
		// Update an existed container.
		v, githash := container.Version()
		s = s.Clone(
			core.SetContainerAddress(container.Addr(), container.ShardAddr()),
			core.SetContainerVersion(githash, v),
			core.SetContainerLabels(labels),
			core.SetContainerStartTime(container.StartTimestamp()),
			core.SetContainerDeployPath(container.DeployPath()),
		)
	}
	if err := c.checkContainerLabels(s); err != nil {
		return err
	}
	return c.putContainerLocked(s)
}

func (c *RaftCluster) checkContainerLabels(s *core.CachedContainer) error {
	if c.opt.IsPlacementRulesEnabled() {
		return nil
	}
	keysSet := make(map[string]struct{})
	for _, k := range c.opt.GetLocationLabels() {
		keysSet[k] = struct{}{}
		if v := s.GetLabelValue(k); len(v) == 0 {
			c.logger.Warn("container label configuration is incorrect",
				zap.Uint64("container", s.Meta.ID()),
				zap.String("label", k))
			if c.opt.GetStrictlyMatchLabel() {
				return fmt.Errorf("label configuration is incorrect, need to specify the key: %s ", k)
			}
		}
	}
	for _, label := range s.Meta.Labels() {
		key := label.GetKey()
		if _, ok := keysSet[key]; !ok {
			c.logger.Warn("container not found the key match with the label",
				zap.Uint64("container", s.Meta.ID()),
				zap.String("label", key))
			if c.opt.GetStrictlyMatchLabel() {
				return fmt.Errorf("key matching the label was not found in the Prophet, container label key: %s ", key)
			}
		}
	}
	return nil
}

// RemoveContainer marks a container as offline in cluster.
// State transition: Up -> Offline.
func (c *RaftCluster) RemoveContainer(containerID uint64, physicallyDestroyed bool) error {
	c.Lock()
	defer c.Unlock()

	container := c.GetContainer(containerID)
	if container == nil {
		return fmt.Errorf("container %d not found", containerID)
	}

	// Remove an offline container should be OK, nothing to do.
	if container.IsOffline() && container.IsPhysicallyDestroyed() == physicallyDestroyed {
		return nil
	}

	if container.IsTombstone() {
		return fmt.Errorf("container %d is tombstone", containerID)
	}

	if container.IsPhysicallyDestroyed() {
		return fmt.Errorf("container %d is physically destroyed", containerID)
	}

	newContainer := container.Clone(core.OfflineContainer(physicallyDestroyed))
	c.logger.Warn("container has been offline",
		zap.Uint64("container", newContainer.Meta.ID()),
		zap.String("container-address", newContainer.Meta.Addr()),
		zap.Bool("physically-destroyed", physicallyDestroyed))
	err := c.putContainerLocked(newContainer)
	if err == nil {
		// TODO: if the persist operation encounters error, the "Unlimited" will be rollback.
		// And considering the store state has changed, RemoveStore is actually successful.
		c.SetContainerLimit(containerID, limit.RemovePeer, limit.Unlimited)
	}
	return err
}

// buryContainer marks a store as tombstone in cluster.
// The store should be empty before calling this func
// State transition: Offline -> Tombstone.
func (c *RaftCluster) buryContainer(containerID uint64) error {
	c.Lock()
	defer c.Unlock()

	container := c.GetContainer(containerID)
	if container == nil {
		return fmt.Errorf("container %d not found", containerID)
	}

	// Bury a tombstone container should be OK, nothing to do.
	if container.IsTombstone() {
		return nil
	}

	if container.IsUp() {
		return fmt.Errorf("container %d is UP", containerID)
	}

	newContainer := container.Clone(core.TombstoneContainer())
	c.logger.Warn("container has been tombstone",
		zap.Uint64("container", newContainer.Meta.ID()),
		zap.String("container-address", newContainer.Meta.Addr()),
		zap.Bool("physically-destroyed", newContainer.IsPhysicallyDestroyed()))
	err := c.putContainerLocked(newContainer)
	if err == nil {
		c.RemoveContainerLimit(containerID)
	}
	return err
}

// PauseLeaderTransfer prevents the container from been selected as source or
// target container of TransferLeader.
func (c *RaftCluster) PauseLeaderTransfer(containerID uint64) error {
	return c.core.PauseLeaderTransfer(containerID)
}

// ResumeLeaderTransfer cleans a container's pause state. The container can be selected
// as source or target of TransferLeader again.
func (c *RaftCluster) ResumeLeaderTransfer(containerID uint64) {
	c.core.ResumeLeaderTransfer(containerID)
}

// AttachAvailableFunc attaches an available function to a specific container.
func (c *RaftCluster) AttachAvailableFunc(containerID uint64, limitType limit.Type, f func() bool) {
	c.core.AttachAvailableFunc(containerID, limitType, f)
}

// UpContainer up a store from offline
func (c *RaftCluster) UpContainer(containerID uint64) error {
	c.Lock()
	defer c.Unlock()
	container := c.GetContainer(containerID)
	if container == nil {
		return fmt.Errorf("container %d not found", containerID)
	}

	if container.IsTombstone() {
		return fmt.Errorf("container %d is tombstone", containerID)
	}

	if container.IsPhysicallyDestroyed() {
		return fmt.Errorf("container %d is physically destroyed", containerID)
	}

	if container.IsUp() {
		return nil
	}

	newContainer := container.Clone(core.UpContainer())
	c.logger.Warn("container has been up",
		zap.Uint64("container", newContainer.Meta.ID()),
		zap.String("container-address", newContainer.Meta.Addr()))
	return c.putContainerLocked(newContainer)
}

// SetContainerWeight sets up a container's leader/resource balance weight.
func (c *RaftCluster) SetContainerWeight(containerID uint64, leaderWeight, resourceWeight float64) error {
	c.Lock()
	defer c.Unlock()

	container := c.GetContainer(containerID)
	if container == nil {
		return fmt.Errorf("container %d not found", containerID)
	}

	if err := c.storage.PutContainerWeight(containerID, leaderWeight, resourceWeight); err != nil {
		return err
	}

	newContainer := container.Clone(
		core.SetLeaderWeight(leaderWeight),
		core.SetResourceWeight(resourceWeight),
	)

	return c.putContainerLocked(newContainer)
}

func (c *RaftCluster) putContainerLocked(container *core.CachedContainer) error {
	if c.storage != nil {
		if err := c.storage.PutContainer(container.Meta); err != nil {
			return err
		}
	}
	c.core.PutContainer(container)
	c.hotStat.GetOrCreateRollingContainerStats(container.Meta.ID())
	return nil
}

func (c *RaftCluster) checkContainers() {
	var offlineContainers []metadata.Container
	var upContainerCount int
	containers := c.GetContainers()
	groupKeys := c.core.GetScheduleGroupKeys()
	for _, container := range containers {
		// the container has already been tombstone
		if container.IsTombstone() {
			continue
		}

		if container.IsUp() {
			if !container.IsLowSpace(c.opt.GetLowSpaceRatio(), groupKeys) {
				upContainerCount++
			}
			continue
		}

		offlineContainer := container.Meta
		resourceCount := 0
		for _, group := range groupKeys {
			resourceCount += c.core.GetContainerResourceCount(group, offlineContainer.ID())
		}

		// If the container is empty, it can be buried.
		if resourceCount == 0 {
			if err := c.buryContainer(offlineContainer.ID()); err != nil {
				c.logger.Error("fail to bury container",
					zap.Uint64("container", offlineContainer.ID()),
					zap.String("container-address", offlineContainer.Addr()),
					zap.Error(err))
			}
		} else {
			offlineContainers = append(offlineContainers, offlineContainer)
		}
	}

	if len(offlineContainers) == 0 {
		return
	}

	// When placement rules feature is enabled. It is hard to determine required replica count precisely.
	if !c.opt.IsPlacementRulesEnabled() && upContainerCount < c.opt.GetMaxReplicas() {
		for _, container := range offlineContainers {
			c.logger.Warn("container may not turn into Tombstone, there are no extra up container has enough space to accommodate the extra replica",
				zap.Uint64("container", container.ID()),
				zap.String("container-address", container.Addr()))
		}
	}
}

// RemoveTombStoneRecords removes the tombStone Records.
func (c *RaftCluster) RemoveTombStoneRecords() error {
	c.Lock()
	defer c.Unlock()
	for _, groupKey := range c.core.GetScheduleGroupKeys() {
		for _, container := range c.GetContainers() {
			if container.IsTombstone() {
				if container.GetResourceCount(groupKey) > 0 {
					c.logger.Warn("skip removing tombstone container",
						zap.Uint64("container", container.Meta.ID()),
						zap.String("container-address", container.Meta.Addr()))
					continue
				}

				// the container has already been tombstone
				err := c.deleteContainerLocked(container)
				if err != nil {
					c.logger.Error("fail to delete container",
						zap.Uint64("container", container.Meta.ID()),
						zap.String("container-address", container.Meta.Addr()))
					return err
				}
				c.RemoveContainerLimit(container.Meta.ID())

				c.logger.Info("container deleted",
					zap.Uint64("container", container.Meta.ID()),
					zap.String("container-address", container.Meta.Addr()))
			}
		}
	}
	return nil
}

func (c *RaftCluster) deleteContainerLocked(container *core.CachedContainer) error {
	if c.storage != nil {
		if err := c.storage.RemoveContainer(container.Meta); err != nil {
			return err
		}
	}
	c.core.DeleteContainer(container)
	c.hotStat.RemoveRollingContainerStats(container.Meta.ID())
	return nil
}

func (c *RaftCluster) collectMetrics() {
	statsMap := statistics.NewContainerStatisticsMap(c.opt)
	containers := c.GetContainers()
	for _, s := range containers {
		statsMap.Observe(s, c.hotStat.ContainersStats)
	}
	statsMap.Collect()

	c.coordinator.collectSchedulerMetrics()
	c.coordinator.collectHotSpotMetrics()
	c.coordinator.opController.CollectContainerLimitMetrics()
	c.collectClusterMetrics()
}

func (c *RaftCluster) resetMetrics() {
	statsMap := statistics.NewContainerStatisticsMap(c.opt)
	statsMap.Reset()

	c.coordinator.resetSchedulerMetrics()
	c.coordinator.resetHotSpotMetrics()
	c.resetClusterMetrics()
}

func (c *RaftCluster) collectClusterMetrics() {
	c.RLock()
	defer c.RUnlock()
	if c.resourceStats == nil {
		return
	}
	c.resourceStats.Collect()
	c.labelLevelStats.Collect()
	// collect hot cache metrics
	c.hotStat.CollectMetrics()
}

func (c *RaftCluster) resetClusterMetrics() {
	c.RLock()
	defer c.RUnlock()
	if c.resourceStats == nil {
		return
	}
	c.resourceStats.Reset()
	c.labelLevelStats.Reset()
	// reset hot cache metrics
	c.hotStat.ResetMetrics()
}

// GetResourceStatsByType gets the status of the resource by types.
func (c *RaftCluster) GetResourceStatsByType(typ statistics.ResourceStatisticType) []*core.CachedResource {
	c.RLock()
	defer c.RUnlock()
	if c.resourceStats == nil {
		return nil
	}
	return c.resourceStats.GetResourceStatsByType(typ)
}

// GetOfflineResourceStatsByType gets the status of the offline resource by types.
func (c *RaftCluster) GetOfflineResourceStatsByType(typ statistics.ResourceStatisticType) []*core.CachedResource {
	c.RLock()
	defer c.RUnlock()
	if c.resourceStats == nil {
		return nil
	}
	return c.resourceStats.GetOfflineResourceStatsByType(typ)
}

func (c *RaftCluster) updateResourcesLabelLevelStats(resources []*core.CachedResource) {
	c.Lock()
	defer c.Unlock()
	for _, res := range resources {
		c.labelLevelStats.Observe(res, c.takeResourceContainersLocked(res), c.opt.GetLocationLabels())
	}
}

func (c *RaftCluster) takeResourceContainersLocked(res *core.CachedResource) []*core.CachedContainer {
	containers := make([]*core.CachedContainer, 0, len(res.Meta.Peers()))
	for _, p := range res.Meta.Peers() {
		if container := c.core.TakeContainer(p.ContainerID); container != nil {
			containers = append(containers, container)
		}
	}
	return containers
}

// AllocID allocs ID.
func (c *RaftCluster) AllocID() (uint64, error) {
	return c.storage.KV().AllocID()
}

// ChangedEventNotifier changedEventNotifier
func (c *RaftCluster) ChangedEventNotifier() <-chan rpcpb.EventNotify {
	return c.changedEvents
}

// GetMergeChecker returns merge checker.
func (c *RaftCluster) GetMergeChecker() *checker.MergeChecker {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.checkers.GetMergeChecker()
}

// isPrepared if the cluster information is collected
func (c *RaftCluster) isPrepared() bool {
	c.RLock()
	defer c.RUnlock()
	return c.prepareChecker.check(c)
}

// GetContainersLoads returns load stats of all containers.
func (c *RaftCluster) GetContainersLoads() map[uint64][]float64 {
	c.RLock()
	defer c.RUnlock()
	return c.hotStat.GetContainersLoads()
}

// ResourceReadStats returns hot resource's read stats.
// The result only includes peers that are hot enough.
func (c *RaftCluster) ResourceReadStats() map[uint64][]*statistics.HotPeerStat {
	// ResourceStats is a thread-safe method
	return c.hotStat.ResourceStats(statistics.ReadFlow, c.GetOpts().GetHotResourceCacheHitsThreshold())
}

// ResourceWriteStats returns hot resource's write stats.
// The result only includes peers that are hot enough.
func (c *RaftCluster) ResourceWriteStats() map[uint64][]*statistics.HotPeerStat {
	// ResourceStats is a thread-safe method
	return c.hotStat.ResourceStats(statistics.WriteFlow, c.GetOpts().GetHotResourceCacheHitsThreshold())
}

// CheckWriteStatus checks the write status, returns whether need update statistics and item.
func (c *RaftCluster) CheckWriteStatus(res *core.CachedResource) []*statistics.HotPeerStat {
	return c.hotStat.CheckWrite(res)
}

// CheckReadStatus checks the read status, returns whether need update statistics and item.
func (c *RaftCluster) CheckReadStatus(res *core.CachedResource) []*statistics.HotPeerStat {
	return c.hotStat.CheckRead(res)
}

// GetRuleManager returns the rule manager reference.
func (c *RaftCluster) GetRuleManager() *placement.RuleManager {
	c.RLock()
	defer c.RUnlock()
	return c.ruleManager
}

// FitResource tries to fit the resource with placement rules.
func (c *RaftCluster) FitResource(res *core.CachedResource) *placement.ResourceFit {
	return c.GetRuleManager().FitResource(c, res)
}

type prepareChecker struct {
	reactiveResources map[uint64]int
	start             time.Time
	sum               int
	isPrepared        bool
}

func newPrepareChecker() *prepareChecker {
	return &prepareChecker{
		start:             time.Now(),
		reactiveResources: make(map[uint64]int),
	}
}

// Before starting up the scheduler, we need to take the proportion of the resources on each container into consideration.
func (checker *prepareChecker) check(c *RaftCluster) bool {
	if checker.isPrepared || time.Since(checker.start) > collectTimeout {
		return true
	}
	// The number of active resources should be more than total resource of all containers * collectFactor
	if float64(c.core.GetResourceCount())*collectFactor > float64(checker.sum) {
		return false
	}
	for _, container := range c.GetContainers() {
		if !container.IsUp() {
			continue
		}
		containerID := container.Meta.ID()
		n := 0
		for _, group := range c.GetScheduleGroupKeys() {
			n += c.core.GetContainerResourceCount(group, containerID)
		}

		// For each container, the number of active resources should be more than total resource of the container * collectFactor
		if float64(n)*collectFactor > float64(checker.reactiveResources[containerID]) {
			return false
		}
	}
	checker.isPrepared = true
	return true
}

func (checker *prepareChecker) collect(res *core.CachedResource) {
	for _, p := range res.Meta.Peers() {
		checker.reactiveResources[p.GetContainerID()]++
	}
	checker.sum++
}

// GetHotWriteResources gets hot write resources' info.
func (c *RaftCluster) GetHotWriteResources() *statistics.ContainerHotPeersInfos {
	c.RLock()
	co := c.coordinator
	c.RUnlock()
	return co.getHotWriteResources()
}

// GetHotReadResources gets hot read resources' info.
func (c *RaftCluster) GetHotReadResources() *statistics.ContainerHotPeersInfos {
	c.RLock()
	co := c.coordinator
	c.RUnlock()
	return co.getHotReadResources()
}

// GetSchedulers gets all schedulers.
func (c *RaftCluster) GetSchedulers() []string {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.getSchedulers()
}

// GetSchedulerHandlers gets all scheduler handlers.
func (c *RaftCluster) GetSchedulerHandlers() map[string]http.Handler {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.getSchedulerHandlers()
}

// AddScheduler adds a scheduler.
func (c *RaftCluster) AddScheduler(scheduler schedule.Scheduler, args ...string) error {
	c.Lock()
	defer c.Unlock()
	return c.coordinator.addScheduler(scheduler, args...)
}

// RemoveScheduler removes a scheduler.
func (c *RaftCluster) RemoveScheduler(name string) error {
	c.Lock()
	defer c.Unlock()
	return c.coordinator.removeScheduler(name)
}

// PauseOrResumeScheduler pauses or resumes a scheduler.
func (c *RaftCluster) PauseOrResumeScheduler(name string, t int64) error {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.pauseOrResumeScheduler(name, t)
}

// IsSchedulerPaused checks if a scheduler is paused.
func (c *RaftCluster) IsSchedulerPaused(name string) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.isSchedulerPaused(name)
}

// IsSchedulerDisabled checks if a scheduler is disabled.
func (c *RaftCluster) IsSchedulerDisabled(name string) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.isSchedulerDisabled(name)
}

// GetContainerLimiter returns the dynamic adjusting limiter
func (c *RaftCluster) GetContainerLimiter() *ContainerLimiter {
	return c.limiter
}

// GetContainerLimitByType returns the container limit for a given container ID and type.
func (c *RaftCluster) GetContainerLimitByType(containerID uint64, typ limit.Type) float64 {
	return c.opt.GetContainerLimitByType(containerID, typ)
}

// GetAllContainersLimit returns all container limit
func (c *RaftCluster) GetAllContainersLimit() map[uint64]config.ContainerLimitConfig {
	return c.opt.GetAllContainersLimit()
}

// AddContainerLimit add a container limit for a given container ID.
func (c *RaftCluster) AddContainerLimit(container metadata.Container) {
	containerID := container.ID()
	cfg := c.opt.GetScheduleConfig().Clone()
	if _, ok := cfg.ContainerLimit[containerID]; ok {
		return
	}

	sc := config.ContainerLimitConfig{
		AddPeer:    config.DefaultContainerLimit.GetDefaultContainerLimit(limit.AddPeer),
		RemovePeer: config.DefaultContainerLimit.GetDefaultContainerLimit(limit.RemovePeer),
	}

	cfg.ContainerLimit[containerID] = sc
	c.opt.SetScheduleConfig(cfg)

	var err error
	for i := 0; i < persistLimitRetryTimes; i++ {
		if err = c.opt.Persist(c.storage); err == nil {
			c.logger.Info("container limit added",
				zap.Uint64("container", containerID))
			return
		}
		time.Sleep(persistLimitWaitTime)
	}
	c.logger.Error("fail to persist container limit",
		zap.Uint64("container", containerID),
		zap.Error(err))
}

// RemoveContainerLimit remove a container limit for a given container ID.
func (c *RaftCluster) RemoveContainerLimit(containerID uint64) {
	cfg := c.opt.GetScheduleConfig().Clone()
	for _, limitType := range limit.TypeNameValue {
		c.AttachAvailableFunc(containerID, limitType, nil)
	}
	delete(cfg.ContainerLimit, containerID)
	c.opt.SetScheduleConfig(cfg)

	var err error
	for i := 0; i < persistLimitRetryTimes; i++ {
		if err = c.opt.Persist(c.storage); err == nil {
			c.logger.Info("container limit removed",
				zap.Uint64("container", containerID))
			return
		}
		time.Sleep(persistLimitWaitTime)
	}
	c.logger.Error("fail to persist container limit",
		zap.Uint64("container", containerID),
		zap.Error(err))
}

// SetContainerLimit sets a container limit for a given type and rate.
func (c *RaftCluster) SetContainerLimit(containerID uint64, typ limit.Type, ratePerMin float64) error {
	old := c.opt.GetScheduleConfig().Clone()
	c.opt.SetContainerLimit(containerID, typ, ratePerMin)
	if err := c.opt.Persist(c.storage); err != nil {
		// roll back the store limit
		c.opt.SetScheduleConfig(old)
		c.logger.Error("fail to persist container limit",
			zap.Uint64("container", containerID),
			zap.Error(err))
		return err
	}

	c.logger.Error("container limit changed",
		zap.Uint64("container", containerID),
		zap.String("type", typ.String()),
		zap.Float64("new-value", ratePerMin))
	return nil
}

// SetAllContainersLimit sets all container limit for a given type and rate.
func (c *RaftCluster) SetAllContainersLimit(typ limit.Type, ratePerMin float64) error {
	old := c.opt.GetScheduleConfig().Clone()
	oldAdd := config.DefaultContainerLimit.GetDefaultContainerLimit(limit.AddPeer)
	oldRemove := config.DefaultContainerLimit.GetDefaultContainerLimit(limit.RemovePeer)
	c.opt.SetAllContainersLimit(typ, ratePerMin)
	if err := c.opt.Persist(c.storage); err != nil {
		// roll back the store limit
		c.opt.SetScheduleConfig(old)
		config.DefaultContainerLimit.SetDefaultContainerLimit(limit.AddPeer, oldAdd)
		config.DefaultContainerLimit.SetDefaultContainerLimit(limit.RemovePeer, oldRemove)
		c.logger.Error("fail to persist containers limit",
			zap.Error(err))
		return err
	}
	c.logger.Info("all containers limit changed")
	return nil
}

// GetClusterVersion returns the current cluster version.
func (c *RaftCluster) GetClusterVersion() string {
	return c.opt.GetClusterVersion().String()
}

// GetLogger returns zap logger
func (c *RaftCluster) GetLogger() *zap.Logger {
	return c.logger
}

// DisableJointConsensus do nothing
func (c *RaftCluster) DisableJointConsensus() {

}

// JointConsensusEnabled always returns true
func (c *RaftCluster) JointConsensusEnabled() bool {
	return true
}

// GetResourceFactory resource factory
func (c *RaftCluster) GetResourceFactory() func() metadata.Resource {
	return c.adapter.NewResource
}
