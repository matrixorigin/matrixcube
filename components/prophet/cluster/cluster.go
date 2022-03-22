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
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/checker"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/hbstream"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/statistics"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util/cache"
	"github.com/matrixorigin/matrixcube/components/prophet/util/keyutil"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
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
	// since the once the store is added or removed, we shouldn't return an error even if the store limit is failed to persist.
	persistLimitRetryTimes = 5
	persistLimitWaitTime   = 100 * time.Millisecond
)

var (
	errShardDestroyed = errors.New("resource destroyed")
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
// container 1 -> /1/raft/s/1, value is *metapb.Store
// resource 1 -> /1/raft/r/1, value is *metapb.Shard
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
	limiter *StoreLimiter

	prepareChecker *prepareChecker
	changedEvents  chan rpcpb.EventNotify
	createShardC   chan struct{}

	labelLevelStats *statistics.LabelStatistics
	resourceStats   *statistics.ShardStatistics
	hotStat         *statistics.HotStat

	coordinator      *coordinator
	suspectShards    *cache.TTLUint64 // suspectShards are resources that may need fix
	suspectKeyRanges *cache.TTLString // suspect key-range resources that may need fix

	wg   sync.WaitGroup
	quit chan struct{}

	ruleManager                 *placement.RuleManager
	etcdClient                  *clientv3.Client
	resourceStateChangedHandler func(res *metapb.Shard, from metapb.ShardState, to metapb.ShardState)

	logger *zap.Logger
}

// NewRaftCluster create a new cluster.
func NewRaftCluster(
	ctx context.Context,
	root string,
	clusterID uint64,
	etcdClient *clientv3.Client,
	resourceStateChangedHandler func(res *metapb.Shard, from metapb.ShardState, to metapb.ShardState),
	logger *zap.Logger,
) *RaftCluster {
	return &RaftCluster{
		ctx:                         ctx,
		running:                     false,
		clusterID:                   clusterID,
		clusterRoot:                 root,
		etcdClient:                  etcdClient,
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
	c.suspectShards = cache.NewIDTTL(c.ctx, time.Minute, 3*time.Minute)
	c.suspectKeyRanges = cache.NewStringTTL(c.ctx, time.Minute, 3*time.Minute)

	c.changedEvents = make(chan rpcpb.EventNotify, defaultChangedEventLimit)
	c.createShardC = make(chan struct{}, 1)
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
	c.resourceStats = statistics.NewShardStatistics(c.opt, c.ruleManager)
	c.limiter = NewStoreLimiter(s.GetPersistOptions(), c.logger)
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
	if err := c.storage.LoadStores(batch, func(meta metapb.Store, leaderWeight, resourceWeight float64) {
		c.core.PutStore(core.NewCachedStore(meta,
			core.SetLeaderWeight(leaderWeight),
			core.SetShardWeight(resourceWeight)))
	}); err != nil {
		return nil, err
	}
	c.logger.Info("containers loaded",
		zap.Int("count", c.GetStoreCount()),
		zap.Duration("cost", time.Since(start)))

	// used to load resource from kv storage to cache storage.
	start = time.Now()
	if err := c.storage.LoadShards(batch, func(meta metapb.Shard) {
		c.core.CheckAndPutShard(core.NewCachedShard(meta, nil))
	}); err != nil {
		return nil, err
	}
	c.logger.Info("resources loaded",
		zap.Int("count", c.GetShardCount()),
		zap.Duration("cost", time.Since(start)))

	for _, container := range c.GetStores() {
		c.hotStat.GetOrCreateRollingStoreStats(container.Meta.GetID())
	}

	// load resource group rules
	start = time.Now()
	c.storage.LoadScheduleGroupRules(batch, func(rule metapb.ScheduleGroupRule) {
		c.core.AddScheduleGroupRule(rule)
	})
	c.logger.Info("resource group rules loaded",
		zap.Int("count", c.core.GetShardGroupRuleCount()),
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
			close(c.createShardC)
			close(c.changedEvents)
			c.createShardC = nil
			c.changedEvents = nil
			c.Unlock()
			c.logger.Info("background jobs has been stopped")
			return
		case <-ticker.C:
			c.checkStores()
			c.collectMetrics()
			c.coordinator.opController.PruneHistory()
			c.doNotifyCreateShards()
		case <-c.createShardC:
			c.doNotifyCreateShards()
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
	if c.core != nil {
		c.core.Reset()
	}
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

func (c *RaftCluster) GetScheduleGroupKeysWithPrefix(prefix string) []string {
	c.RLock()
	defer c.RUnlock()
	return c.core.GetScheduleGroupKeysWithPrefix(prefix)
}

// GetOperatorController returns the operator controller.
func (c *RaftCluster) GetOperatorController() *schedule.OperatorController {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.opController
}

// GetShardScatter returns the resource scatter.
func (c *RaftCluster) GetShardScatter() *schedule.ShardScatterer {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.resourceScatterer
}

// GetShardSplitter returns the resource splitter
func (c *RaftCluster) GetShardSplitter() *schedule.ShardSplitter {
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

// AddSuspectShards adds resources to suspect list.
func (c *RaftCluster) AddSuspectShards(resourceIDs ...uint64) {
	c.Lock()
	defer c.Unlock()
	for _, resourceID := range resourceIDs {
		c.suspectShards.Put(resourceID, nil)
	}
}

// GetSuspectShards gets all suspect resources.
func (c *RaftCluster) GetSuspectShards() []uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.suspectShards.GetAllID()
}

// RemoveSuspectShard removes resource from suspect list.
func (c *RaftCluster) RemoveSuspectShard(id uint64) {
	c.Lock()
	defer c.Unlock()
	c.suspectShards.Remove(id)
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

// HandleStoreHeartbeat updates the container status.
func (c *RaftCluster) HandleStoreHeartbeat(stats *metapb.StoreStats) error {
	c.Lock()
	defer c.Unlock()

	containerID := stats.GetStoreID()
	container := c.GetStore(containerID)
	if container == nil {
		return fmt.Errorf("container %v not found", containerID)
	}
	newStore := container.Clone(core.SetStoreStats(stats), core.SetLastHeartbeatTS(time.Now()))
	if newStore.IsLowSpace(c.opt.GetLowSpaceRatio()) {
		c.logger.Warn("container does not have enough disk space, capacity %d, available %d",
			zap.Uint64("container", newStore.Meta.GetID()),
			zap.Uint64("capacity", newStore.GetCapacity()),
			zap.Uint64("available", newStore.GetAvailable()))
	}
	if newStore.NeedPersist() && c.storage != nil {
		if err := c.storage.PutStore(newStore.Meta); err != nil {
			c.logger.Error("fail to persist container",
				zap.Uint64("contianer", newStore.Meta.GetID()),
				zap.Error(err))
		} else {
			newStore = newStore.Clone(core.SetLastPersistTime(time.Now()))
		}

		c.addNotifyLocked(event.NewStoreEvent(newStore.Meta))
	}
	if container := c.core.GetStore(newStore.Meta.GetID()); container != nil {
		c.hotStat.UpdateStoreHeartbeatMetrics(container)
	}

	c.core.PutStore(newStore)
	c.addNotifyLocked(event.NewStoreStatsEvent(newStore.GetStoreStats()))

	c.hotStat.Observe(newStore.Meta.GetID(), newStore.GetStoreStats())
	c.hotStat.UpdateTotalLoad(c.core.GetStores())
	c.hotStat.FilterUnhealthyStore(c)

	// c.limiter is nil before "start" is called
	if c.limiter != nil && c.opt.GetStoreLimitMode() == "auto" {
		c.limiter.Collect(newStore.GetStoreStats())
	}

	return nil
}

// processShardHeartbeat updates the resource information.
func (c *RaftCluster) processShardHeartbeat(res *core.CachedShard) error {
	c.RLock()
	origin, err := c.core.PreCheckPutShard(res)
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
	var checkMaybeDestroyed *metapb.Shard
	if origin != nil {
		checkMaybeDestroyed = &origin.Meta
	} else {
		checkMaybeDestroyed, err = c.storage.GetShard(res.Meta.GetID())
		if err != nil {
			return err
		}
	}
	if checkMaybeDestroyed.GetState() == metapb.ShardState_Destroyed {
		return errShardDestroyed
	}

	// Save to storage if meta is updated.
	// Save to cache if meta or leader is updated, or contains any down/pending peer.
	// Mark isNew if the resource in cache does not have leader.
	var saveKV, saveCache, isNew bool
	if origin == nil {
		c.logger.Debug("insert new resource",
			zap.Uint64("resource", res.Meta.GetID()))
		saveKV, saveCache, isNew = true, true, true
	} else {
		r := res.Meta.GetEpoch()
		o := origin.Meta.GetEpoch()
		if r.GetGeneration() > o.GetGeneration() {
			c.logger.Info("resource version changed",
				zap.Uint64("resource", res.Meta.GetID()),
				zap.Uint64("from", o.GetGeneration()),
				zap.Uint64("to", r.GetGeneration()))
			saveKV, saveCache = true, true
		}
		if r.GetConfigVer() > o.GetConfigVer() {
			c.logger.Info("resource ConfVer changed",
				zap.Uint64("resource", res.Meta.GetID()),
				log.ReplicasField("peers", res.Meta.GetReplicas()),
				zap.Uint64("from", o.GetConfigVer()),
				zap.Uint64("to", r.GetConfigVer()))
			saveKV, saveCache = true, true
		}
		if res.GetLeader().GetID() != origin.GetLeader().GetID() {
			if origin.GetLeader().GetID() == 0 {
				isNew = true
			} else {
				c.logger.Info("resource leader changed",
					zap.Uint64("resource", res.Meta.GetID()),
					zap.Uint64("from", origin.GetLeader().GetStoreID()),
					zap.Uint64("to", res.GetLeader().GetStoreID()))
			}
			saveCache = true
		}
		if !core.SortedPeersStatsEqual(res.GetDownPeers(), origin.GetDownPeers()) {
			saveCache = true
		}
		if !core.SortedPeersEqual(res.GetPendingPeers(), origin.GetPendingPeers()) {
			saveCache = true
		}
		if len(res.Meta.GetReplicas()) != len(origin.Meta.GetReplicas()) {
			saveKV, saveCache = true, true
		}
		if res.Meta.GetState() != origin.Meta.GetState() {
			saveKV, saveCache = true, true
		}
		if res.GetGroupKey() != origin.GetGroupKey() {
			saveCache = true
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
	inCreating := c.core.IsWaitingCreateShard(res.Meta.GetID())
	if isNew && inCreating {
		if c.resourceStateChangedHandler != nil {
			c.resourceStateChangedHandler(&res.Meta, metapb.ShardState_Creating,
				metapb.ShardState_Running)
		}
	}

	if saveCache {
		// To prevent a concurrent heartbeat of another resource from overriding the up-to-date resource info by a stale one,
		// check its validation again here.
		//
		// However it can't solve the race condition of concurrent heartbeats from the same resource.
		if _, err := c.core.PreCheckPutShard(res); err != nil {
			c.Unlock()
			return err
		}

		overlaps := c.core.PutShard(res)
		if c.storage != nil {
			for _, item := range overlaps {
				if err := c.storage.RemoveShard(item.Meta); err != nil {
					c.logger.Error("fail to delete resource from storage",
						zap.Uint64("resource", item.Meta.GetID()),
						zap.Error(err))
				}
			}
		}
		for _, item := range overlaps {
			if c.resourceStats != nil {
				c.resourceStats.ClearDefunctShard(item.Meta.GetID())
			}
			c.labelLevelStats.ClearDefunctShard(item.Meta.GetID())
		}

		// Update related containers.
		containerMap := make(map[uint64]struct{})
		for _, p := range res.Meta.GetReplicas() {
			containerMap[p.GetStoreID()] = struct{}{}
		}
		if origin != nil {
			for _, p := range origin.Meta.GetReplicas() {
				containerMap[p.GetStoreID()] = struct{}{}
			}
		}
		for key := range containerMap {
			c.updateStoreStatusLocked(res.GetGroupKey(), key)
			if origin != nil && origin.GetGroupKey() != res.GetGroupKey() {
				c.logger.Debug("update container status",
					zap.Uint64("resource", res.Meta.GetID()),
					zap.Uint64("size", uint64(res.GetApproximateSize())),
					log.HexField("origin-group-key", []byte(origin.GetGroupKey())),
					log.HexField("current-group-key", []byte(res.GetGroupKey())))
				c.updateStoreStatusLocked(origin.GetGroupKey(), key)
			}
		}
		resourceEventCounter.WithLabelValues("update_cache").Inc()
	}

	if isNew {
		c.prepareChecker.collect(res)
	}

	if c.resourceStats != nil {
		c.resourceStats.Observe(res, c.takeShardStoresLocked(res))
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
		if !inCreating && res.Meta.GetState() == metapb.ShardState_Creating {
			res = res.Clone(core.WithState(metapb.ShardState_Running))
		}

		if err := c.storage.PutShard(res.Meta); err != nil {
			// Not successfully saved to storage is not fatal, it only leads to longer warm-up
			// after restart. Here we only log the error then go on updating cache.
			c.logger.Error("fail to save resource to storage",
				zap.Uint64("resource", res.Meta.GetID()),
				zap.Error(err))
		}
		resourceEventCounter.WithLabelValues("update_kv").Inc()
	}
	c.RLock()
	if saveKV || saveCache || isNew {
		if res.GetLeader().GetID() != 0 {
			from := uint64(0)
			if origin != nil {
				from = origin.GetLeader().GetStoreID()
			}
			c.logger.Debug("notify resource leader changed",
				zap.Uint64("resource", res.Meta.GetID()),
				zap.Uint64("from", from),
				zap.Uint64("to", res.GetLeader().GetStoreID()))
		}
		c.addNotifyLocked(event.NewShardEvent(res.Meta, res.GetLeader().GetID(), false, false))
	}
	if saveCache {
		c.addNotifyLocked(event.NewShardStatsEvent(res.GetStat()))
	}
	c.RUnlock()

	return nil
}

func (c *RaftCluster) updateStoreStatusLocked(groupKey string, containerID uint64) {
	leaderCount := c.core.GetStoreLeaderCount(groupKey, containerID)
	resourceCount := c.core.GetStoreShardCount(groupKey, containerID)
	pendingPeerCount := c.core.GetStorePendingPeerCount(groupKey, containerID)
	leaderShardSize := c.core.GetStoreLeaderShardSize(groupKey, containerID)
	resourceSize := c.core.GetStoreShardSize(groupKey, containerID)
	c.core.UpdateStoreStatus(groupKey, containerID, leaderCount, resourceCount, pendingPeerCount, leaderShardSize, resourceSize)
}

// GetShardByKey gets CachedShard by resource key from cluster.
func (c *RaftCluster) GetShardByKey(group uint64, resourceKey []byte) *core.CachedShard {
	return c.core.SearchShard(group, resourceKey)
}

// ScanShards scans resource with start key, until the resource contains endKey, or
// total number greater than limit.
func (c *RaftCluster) ScanShards(group uint64, startKey, endKey []byte, limit int) []*core.CachedShard {
	return c.core.ScanRange(group, startKey, endKey, limit)
}

// GetDestroyingShards returns all resources in destroying state
func (c *RaftCluster) GetDestroyingShards() []*core.CachedShard {
	return c.core.GetDestroyingShards()
}

// GetShard searches for a resource by ID.
func (c *RaftCluster) GetShard(resourceID uint64) *core.CachedShard {
	return c.core.GetShard(resourceID)
}

// GetMetaShards gets resources from cluster.
func (c *RaftCluster) GetMetaShards() []metapb.Shard {
	return c.core.GetMetaShards()
}

// GetShards returns all resources' information in detail.
func (c *RaftCluster) GetShards() []*core.CachedShard {
	return c.core.GetShards()
}

// GetShardCount returns total count of resources
func (c *RaftCluster) GetShardCount() int {
	return c.core.GetShardCount()
}

// GetStoreShards returns all resources' information with a given containerID.
func (c *RaftCluster) GetStoreShards(groupKey string, containerID uint64) []*core.CachedShard {
	return c.core.GetStoreShards(groupKey, containerID)
}

// RandLeaderShard returns a random resource that has leader on the container.
func (c *RaftCluster) RandLeaderShard(groupKey string, containerID uint64, ranges []core.KeyRange, opts ...core.ShardOption) *core.CachedShard {
	return c.core.RandLeaderShard(groupKey, containerID, ranges, opts...)
}

// RandFollowerShard returns a random resource that has a follower on the container.
func (c *RaftCluster) RandFollowerShard(groupKey string, containerID uint64, ranges []core.KeyRange, opts ...core.ShardOption) *core.CachedShard {
	return c.core.RandFollowerShard(groupKey, containerID, ranges, opts...)
}

// RandPendingShard returns a random resource that has a pending peer on the container.
func (c *RaftCluster) RandPendingShard(groupKey string, containerID uint64, ranges []core.KeyRange, opts ...core.ShardOption) *core.CachedShard {
	return c.core.RandPendingShard(groupKey, containerID, ranges, opts...)
}

// RandLearnerShard returns a random resource that has a learner peer on the container.
func (c *RaftCluster) RandLearnerShard(groupKey string, containerID uint64, ranges []core.KeyRange, opts ...core.ShardOption) *core.CachedShard {
	return c.core.RandLearnerShard(groupKey, containerID, ranges, opts...)
}

// RandHotShardFromStore randomly picks a hot resource in specified container.
func (c *RaftCluster) RandHotShardFromStore(container uint64, kind statistics.FlowKind) *core.CachedShard {
	c.RLock()
	defer c.RUnlock()
	r := c.hotStat.RandHotShardFromStore(container, kind, c.opt.GetHotShardCacheHitsThreshold())
	if r == nil {
		return nil
	}
	return c.GetShard(r.ShardID)
}

// GetLeaderStore returns all containers that contains the resource's leader peer.
func (c *RaftCluster) GetLeaderStore(res *core.CachedShard) *core.CachedStore {
	return c.core.GetLeaderStore(res)
}

// GetFollowerStores returns all containers that contains the resource's follower peer.
func (c *RaftCluster) GetFollowerStores(res *core.CachedShard) []*core.CachedStore {
	return c.core.GetFollowerStores(res)
}

// GetShardStores returns all containers that contains the resource's peer.
func (c *RaftCluster) GetShardStores(res *core.CachedShard) []*core.CachedStore {
	return c.core.GetShardStores(res)
}

// GetStoreCount returns the count of containers.
func (c *RaftCluster) GetStoreCount() int {
	return c.core.GetStoreCount()
}

// GetStoreShardCount returns the number of resources for a given container.
func (c *RaftCluster) GetStoreShardCount(groupKey string, containerID uint64) int {
	return c.core.GetStoreShardCount(groupKey, containerID)
}

// GetAverageShardSize returns the average resource approximate size.
func (c *RaftCluster) GetAverageShardSize() int64 {
	return c.core.GetAverageShardSize()
}

// GetShardStats returns resource statistics from cluster.
func (c *RaftCluster) GetShardStats(group uint64, startKey, endKey []byte) *statistics.ShardStats {
	c.RLock()
	defer c.RUnlock()
	return statistics.GetShardStats(c.core.ScanRange(group, startKey, endKey, -1))
}

// GetStoresStats returns containers' statistics from cluster.
// And it will be unnecessary to filter unhealthy container, because it has been solved in process heartbeat
func (c *RaftCluster) GetStoresStats() *statistics.StoresStats {
	c.RLock()
	defer c.RUnlock()
	return c.hotStat.StoresStats
}

// DropCacheShard removes a resource from the cache.
func (c *RaftCluster) DropCacheShard(id uint64) {
	c.RLock()
	defer c.RUnlock()
	if res := c.GetShard(id); res != nil {
		c.core.RemoveShard(res)
	}
}

// GetCacheCluster gets the cached cluster.
func (c *RaftCluster) GetCacheCluster() *core.BasicCluster {
	c.RLock()
	defer c.RUnlock()
	return c.core
}

// GetMetaStores gets containers from cluster.
func (c *RaftCluster) GetMetaStores() []metapb.Store {
	return c.core.GetMetaStores()
}

// GetStores returns all containers in the cluster.
func (c *RaftCluster) GetStores() []*core.CachedStore {
	return c.core.GetStores()
}

// GetStore gets container from cluster.
func (c *RaftCluster) GetStore(containerID uint64) *core.CachedStore {
	return c.core.GetStore(containerID)
}

// IsShardHot checks if a resource is in hot state.
func (c *RaftCluster) IsShardHot(res *core.CachedShard) bool {
	c.RLock()
	defer c.RUnlock()
	return c.hotStat.IsShardHot(res, c.opt.GetHotShardCacheHitsThreshold())
}

// GetAdjacentShards returns resources' information that are adjacent with the specific resource ID.
func (c *RaftCluster) GetAdjacentShards(res *core.CachedShard) (*core.CachedShard, *core.CachedShard) {
	return c.core.GetAdjacentShards(res)
}

// UpdateStoreLabels updates a container's location labels
// If 'force' is true, then update the container's labels forcibly.
func (c *RaftCluster) UpdateStoreLabels(containerID uint64, labels []metapb.Label, force bool) error {
	container := c.GetStore(containerID)
	if container == nil {
		return fmt.Errorf("invalid container ID %d, not found", containerID)
	}
	newStore := container.Meta
	newStore.SetLabels(labels)
	// PutStore will perform label merge.
	return c.putStoreImpl(newStore, force)
}

// PutStore puts a container.
func (c *RaftCluster) PutStore(container metapb.Store) error {
	if err := c.putStoreImpl(container, false); err != nil {
		return err
	}
	c.AddStoreLimit(container)
	return nil
}

// putStoreImpl puts a container.
// If 'force' is true, then overwrite the container's labels.
func (c *RaftCluster) putStoreImpl(container metapb.Store, force bool) error {
	c.Lock()
	defer c.Unlock()

	if container.GetID() == 0 {
		return fmt.Errorf("invalid put container %v", container)
	}

	// container address can not be the same as other containers.
	for _, s := range c.GetStores() {
		// It's OK to start a new store on the same address if the old store has been removed or physically destroyed.
		if s.IsTombstone() || s.IsPhysicallyDestroyed() {

			continue
		}
		if s.Meta.GetID() != container.GetID() && s.Meta.GetClientAddress() == container.GetClientAddress() {
			return fmt.Errorf("duplicated container address: %v, already registered by %v", container, s.Meta)
		}
	}

	s := c.GetStore(container.GetID())
	if s == nil {
		// Add a new container.
		s = core.NewCachedStore(container)
	} else {
		// Use the given labels to update the container.
		labels := container.GetLabels()
		if !force {
			// If 'force' isn't set, the given labels will merge into those labels which already existed in the container.
			labels = s.MergeLabels(labels)
		}
		// Update an existed container.
		v, githash := container.GetVersionAndGitHash()
		s = s.Clone(
			core.SetStoreAddress(container.GetClientAddress(), container.GetRaftAddress()),
			core.SetStoreVersion(githash, v),
			core.SetStoreLabels(labels),
			core.SetStoreStartTime(container.GetStartTime()),
			core.SetStoreDeployPath(container.GetDeployPath()),
		)
	}
	if err := c.checkStoreLabels(s); err != nil {
		return err
	}
	return c.putStoreLocked(s)
}

func (c *RaftCluster) checkStoreLabels(s *core.CachedStore) error {
	if c.opt.IsPlacementRulesEnabled() {
		return nil
	}
	keysSet := make(map[string]struct{})
	for _, k := range c.opt.GetLocationLabels() {
		keysSet[k] = struct{}{}
		if v := s.GetLabelValue(k); len(v) == 0 {
			c.logger.Warn("container label configuration is incorrect",
				zap.Uint64("container", s.Meta.GetID()),
				zap.String("label", k))
			if c.opt.GetStrictlyMatchLabel() {
				return fmt.Errorf("label configuration is incorrect, need to specify the key: %s ", k)
			}
		}
	}
	for _, label := range s.Meta.GetLabels() {
		key := label.GetKey()
		if _, ok := keysSet[key]; !ok {
			c.logger.Warn("container not found the key match with the label",
				zap.Uint64("container", s.Meta.GetID()),
				zap.String("label", key))
			if c.opt.GetStrictlyMatchLabel() {
				return fmt.Errorf("key matching the label was not found in the Prophet, container label key: %s ", key)
			}
		}
	}
	return nil
}

// RemoveStore marks a container as offline in cluster.
// State transition: Up -> Offline.
func (c *RaftCluster) RemoveStore(containerID uint64, physicallyDestroyed bool) error {
	c.Lock()
	defer c.Unlock()

	container := c.GetStore(containerID)
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

	newStore := container.Clone(core.OfflineStore(physicallyDestroyed))
	c.logger.Warn("container has been offline",
		zap.Uint64("container", newStore.Meta.GetID()),
		zap.String("container-address", newStore.Meta.GetClientAddress()),
		zap.Bool("physically-destroyed", physicallyDestroyed))
	err := c.putStoreLocked(newStore)
	if err == nil {
		// TODO: if the persist operation encounters error, the "Unlimited" will be rollback.
		// And considering the store state has changed, RemoveStore is actually successful.
		c.SetStoreLimit(containerID, limit.RemovePeer, limit.Unlimited)
	}
	return err
}

// buryStore marks a store as tombstone in cluster.
// The store should be empty before calling this func
// State transition: Offline -> Tombstone.
func (c *RaftCluster) buryStore(containerID uint64) error {
	c.Lock()
	defer c.Unlock()

	container := c.GetStore(containerID)
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

	newStore := container.Clone(core.TombstoneStore())
	c.logger.Warn("container has been tombstone",
		zap.Uint64("container", newStore.Meta.GetID()),
		zap.String("container-address", newStore.Meta.GetClientAddress()),
		zap.Bool("physically-destroyed", newStore.IsPhysicallyDestroyed()))
	err := c.putStoreLocked(newStore)
	if err == nil {
		c.RemoveStoreLimit(containerID)
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

// UpStore up a store from offline
func (c *RaftCluster) UpStore(containerID uint64) error {
	c.Lock()
	defer c.Unlock()
	container := c.GetStore(containerID)
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

	newStore := container.Clone(core.UpStore())
	c.logger.Warn("container has been up",
		zap.Uint64("container", newStore.Meta.GetID()),
		zap.String("container-address", newStore.Meta.GetClientAddress()))
	return c.putStoreLocked(newStore)
}

// SetStoreWeight sets up a container's leader/resource balance weight.
func (c *RaftCluster) SetStoreWeight(containerID uint64, leaderWeight, resourceWeight float64) error {
	c.Lock()
	defer c.Unlock()

	container := c.GetStore(containerID)
	if container == nil {
		return fmt.Errorf("container %d not found", containerID)
	}

	if err := c.storage.PutStoreWeight(containerID, leaderWeight, resourceWeight); err != nil {
		return err
	}

	newStore := container.Clone(
		core.SetLeaderWeight(leaderWeight),
		core.SetShardWeight(resourceWeight),
	)

	return c.putStoreLocked(newStore)
}

func (c *RaftCluster) putStoreLocked(container *core.CachedStore) error {
	if c.storage != nil {
		if err := c.storage.PutStore(container.Meta); err != nil {
			return err
		}
	}
	c.core.PutStore(container)
	c.hotStat.GetOrCreateRollingStoreStats(container.Meta.GetID())
	return nil
}

func (c *RaftCluster) checkStores() {
	var offlineStores []metapb.Store
	var upStoreCount int
	containers := c.GetStores()
	groupKeys := c.core.GetScheduleGroupKeys()
	for _, container := range containers {
		// the container has already been tombstone
		if container.IsTombstone() {
			continue
		}

		if container.IsUp() {
			if !container.IsLowSpace(c.opt.GetLowSpaceRatio()) {
				upStoreCount++
			}
			continue
		}

		offlineStore := container.Meta
		resourceCount := 0
		for _, group := range groupKeys {
			resourceCount += c.core.GetStoreShardCount(group, offlineStore.GetID())
		}

		// If the container is empty, it can be buried.
		if resourceCount == 0 {
			if err := c.buryStore(offlineStore.GetID()); err != nil {
				c.logger.Error("fail to bury container",
					zap.Uint64("container", offlineStore.GetID()),
					zap.String("container-address", offlineStore.GetClientAddress()),
					zap.Error(err))
			}
		} else {
			offlineStores = append(offlineStores, offlineStore)
		}
	}

	if len(offlineStores) == 0 {
		return
	}

	// When placement rules feature is enabled. It is hard to determine required replica count precisely.
	if !c.opt.IsPlacementRulesEnabled() && upStoreCount < c.opt.GetMaxReplicas() {
		for _, container := range offlineStores {
			c.logger.Warn("container may not turn into Tombstone, there are no extra up container has enough space to accommodate the extra replica",
				zap.Uint64("container", container.GetID()),
				zap.String("container-address", container.GetClientAddress()))
		}
	}
}

// RemoveTombStoneRecords removes the tombStone Records.
func (c *RaftCluster) RemoveTombStoneRecords() error {
	c.Lock()
	defer c.Unlock()
	for _, groupKey := range c.core.GetScheduleGroupKeys() {
		for _, container := range c.GetStores() {
			if container.IsTombstone() {
				if container.GetShardCount(groupKey) > 0 {
					c.logger.Warn("skip removing tombstone container",
						zap.Uint64("container", container.Meta.GetID()),
						zap.String("container-address", container.Meta.GetClientAddress()))
					continue
				}

				// the container has already been tombstone
				err := c.deleteStoreLocked(container)
				if err != nil {
					c.logger.Error("fail to delete container",
						zap.Uint64("container", container.Meta.GetID()),
						zap.String("container-address", container.Meta.GetClientAddress()))
					return err
				}
				c.RemoveStoreLimit(container.Meta.GetID())

				c.logger.Info("container deleted",
					zap.Uint64("container", container.Meta.GetID()),
					zap.String("container-address", container.Meta.GetClientAddress()))
			}
		}
	}
	return nil
}

func (c *RaftCluster) deleteStoreLocked(container *core.CachedStore) error {
	if c.storage != nil {
		if err := c.storage.RemoveStore(container.Meta); err != nil {
			return err
		}
	}
	c.core.DeleteStore(container)
	c.hotStat.RemoveRollingStoreStats(container.Meta.GetID())
	return nil
}

func (c *RaftCluster) collectMetrics() {
	statsMap := statistics.NewStoreStatisticsMap(c.opt)
	containers := c.GetStores()
	for _, s := range containers {
		statsMap.Observe(s, c.hotStat.StoresStats)
	}
	statsMap.Collect()

	c.coordinator.collectSchedulerMetrics()
	c.coordinator.collectHotSpotMetrics()
	c.coordinator.opController.CollectStoreLimitMetrics()
	c.collectClusterMetrics()
}

func (c *RaftCluster) resetMetrics() {
	statsMap := statistics.NewStoreStatisticsMap(c.opt)
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

// GetShardStatsByType gets the status of the resource by types.
func (c *RaftCluster) GetShardStatsByType(typ statistics.ShardStatisticType) []*core.CachedShard {
	c.RLock()
	defer c.RUnlock()
	if c.resourceStats == nil {
		return nil
	}
	return c.resourceStats.GetShardStatsByType(typ)
}

// GetOfflineShardStatsByType gets the status of the offline resource by types.
func (c *RaftCluster) GetOfflineShardStatsByType(typ statistics.ShardStatisticType) []*core.CachedShard {
	c.RLock()
	defer c.RUnlock()
	if c.resourceStats == nil {
		return nil
	}
	return c.resourceStats.GetOfflineShardStatsByType(typ)
}

func (c *RaftCluster) updateShardsLabelLevelStats(resources []*core.CachedShard) {
	c.Lock()
	defer c.Unlock()
	for _, res := range resources {
		c.labelLevelStats.Observe(res, c.takeShardStoresLocked(res), c.opt.GetLocationLabels())
	}
}

func (c *RaftCluster) takeShardStoresLocked(res *core.CachedShard) []*core.CachedStore {
	containers := make([]*core.CachedStore, 0, len(res.Meta.GetReplicas()))
	for _, p := range res.Meta.GetReplicas() {
		if container := c.core.GetStore(p.StoreID); container != nil {
			containers = append(containers, container)
		}
	}
	return containers
}

// AllocID allocs ID.
func (c *RaftCluster) AllocID() (uint64, error) {
	return c.storage.AllocID()
}

// ChangedEventNotifier changedEventNotifier
func (c *RaftCluster) ChangedEventNotifier() <-chan rpcpb.EventNotify {
	c.RLock()
	defer c.RUnlock()
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
	return c.prepareChecker.checkLocked(c)
}

// GetStoresLoads returns load stats of all containers.
func (c *RaftCluster) GetStoresLoads() map[uint64][]float64 {
	c.RLock()
	defer c.RUnlock()
	return c.hotStat.GetStoresLoads()
}

// ShardReadStats returns hot resource's read stats.
// The result only includes peers that are hot enough.
func (c *RaftCluster) ShardReadStats() map[uint64][]*statistics.HotPeerStat {
	// ShardStats is a thread-safe method
	return c.hotStat.ShardStats(statistics.ReadFlow, c.GetOpts().GetHotShardCacheHitsThreshold())
}

// ShardWriteStats returns hot resource's write stats.
// The result only includes peers that are hot enough.
func (c *RaftCluster) ShardWriteStats() map[uint64][]*statistics.HotPeerStat {
	// ShardStats is a thread-safe method
	return c.hotStat.ShardStats(statistics.WriteFlow, c.GetOpts().GetHotShardCacheHitsThreshold())
}

// CheckWriteStatus checks the write status, returns whether need update statistics and item.
func (c *RaftCluster) CheckWriteStatus(res *core.CachedShard) []*statistics.HotPeerStat {
	return c.hotStat.CheckWrite(res)
}

// CheckReadStatus checks the read status, returns whether need update statistics and item.
func (c *RaftCluster) CheckReadStatus(res *core.CachedShard) []*statistics.HotPeerStat {
	return c.hotStat.CheckRead(res)
}

// GetRuleManager returns the rule manager reference.
func (c *RaftCluster) GetRuleManager() *placement.RuleManager {
	c.RLock()
	defer c.RUnlock()
	return c.ruleManager
}

// FitShard tries to fit the resource with placement rules.
func (c *RaftCluster) FitShard(res *core.CachedShard) *placement.ShardFit {
	return c.GetRuleManager().FitShard(c, res)
}

type prepareChecker struct {
	reactiveShards map[uint64]int
	start          time.Time
	sum            int
	isPrepared     bool
}

func newPrepareChecker() *prepareChecker {
	return &prepareChecker{
		start:          time.Now(),
		reactiveShards: make(map[uint64]int),
	}
}

// Before starting up the scheduler, we need to take the proportion of the resources on each container into consideration.
func (checker *prepareChecker) checkLocked(c *RaftCluster) bool {
	if checker.isPrepared || time.Since(checker.start) > collectTimeout {
		return true
	}
	// The number of active resources should be more than total resource of all containers * collectFactor
	if float64(c.core.GetShardCount())*collectFactor > float64(checker.sum) {
		return false
	}
	for _, container := range c.core.GetStores() {
		if !container.IsUp() {
			continue
		}
		containerID := container.Meta.GetID()
		n := 0
		for _, group := range c.core.GetScheduleGroupKeys() {
			n += c.core.GetStoreShardCount(group, containerID)
		}

		// For each container, the number of active resources should be more than total resource of the container * collectFactor
		if float64(n)*collectFactor > float64(checker.reactiveShards[containerID]) {
			return false
		}
	}
	checker.isPrepared = true
	return true
}

func (checker *prepareChecker) collect(res *core.CachedShard) {
	for _, p := range res.Meta.GetReplicas() {
		checker.reactiveShards[p.GetStoreID()]++
	}
	checker.sum++
}

// GetHotWriteShards gets hot write resources' info.
func (c *RaftCluster) GetHotWriteShards() *statistics.StoreHotPeersInfos {
	c.RLock()
	co := c.coordinator
	c.RUnlock()
	return co.getHotWriteShards()
}

// GetHotReadShards gets hot read resources' info.
func (c *RaftCluster) GetHotReadShards() *statistics.StoreHotPeersInfos {
	c.RLock()
	co := c.coordinator
	c.RUnlock()
	return co.getHotReadShards()
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

// GetStoreLimiter returns the dynamic adjusting limiter
func (c *RaftCluster) GetStoreLimiter() *StoreLimiter {
	return c.limiter
}

// GetStoreLimitByType returns the container limit for a given container ID and type.
func (c *RaftCluster) GetStoreLimitByType(containerID uint64, typ limit.Type) float64 {
	return c.opt.GetStoreLimitByType(containerID, typ)
}

// GetAllStoresLimit returns all container limit
func (c *RaftCluster) GetAllStoresLimit() map[uint64]config.StoreLimitConfig {
	return c.opt.GetAllStoresLimit()
}

// AddStoreLimit add a container limit for a given container ID.
func (c *RaftCluster) AddStoreLimit(container metapb.Store) {
	containerID := container.GetID()
	cfg := c.opt.GetScheduleConfig().Clone()
	if _, ok := cfg.StoreLimit[containerID]; ok {
		return
	}

	sc := config.StoreLimitConfig{
		AddPeer:    config.DefaultStoreLimit.GetDefaultStoreLimit(limit.AddPeer),
		RemovePeer: config.DefaultStoreLimit.GetDefaultStoreLimit(limit.RemovePeer),
	}

	cfg.StoreLimit[containerID] = sc
	c.opt.SetScheduleConfig(cfg)

	var err error
	for i := 0; i < persistLimitRetryTimes; i++ {
		if err = c.opt.Persist(c.storage); err == nil {
			c.logger.Info("container limit added",
				zap.Any("limit", cfg.StoreLimit[containerID]),
				zap.Uint64("container", containerID))
			return
		}
		time.Sleep(persistLimitWaitTime)
	}
	c.logger.Error("fail to persist container limit",
		zap.Uint64("container", containerID),
		zap.Error(err))
}

// RemoveStoreLimit remove a container limit for a given container ID.
func (c *RaftCluster) RemoveStoreLimit(containerID uint64) {
	cfg := c.opt.GetScheduleConfig().Clone()
	for _, limitType := range limit.TypeNameValue {
		c.AttachAvailableFunc(containerID, limitType, nil)
	}
	delete(cfg.StoreLimit, containerID)
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

// SetStoreLimit sets a container limit for a given type and rate.
func (c *RaftCluster) SetStoreLimit(containerID uint64, typ limit.Type, ratePerMin float64) error {
	old := c.opt.GetScheduleConfig().Clone()
	c.opt.SetStoreLimit(containerID, typ, ratePerMin)
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

// SetAllStoresLimit sets all container limit for a given type and rate.
func (c *RaftCluster) SetAllStoresLimit(typ limit.Type, ratePerMin float64) error {
	old := c.opt.GetScheduleConfig().Clone()
	oldAdd := config.DefaultStoreLimit.GetDefaultStoreLimit(limit.AddPeer)
	oldRemove := config.DefaultStoreLimit.GetDefaultStoreLimit(limit.RemovePeer)
	c.opt.SetAllStoresLimit(typ, ratePerMin)
	if err := c.opt.Persist(c.storage); err != nil {
		// roll back the store limit
		c.opt.SetScheduleConfig(old)
		config.DefaultStoreLimit.SetDefaultStoreLimit(limit.AddPeer, oldAdd)
		config.DefaultStoreLimit.SetDefaultStoreLimit(limit.RemovePeer, oldRemove)
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

func (c *RaftCluster) addNotifyLocked(event rpcpb.EventNotify) {
	if c.changedEvents != nil {
		c.changedEvents <- event
	}
}
