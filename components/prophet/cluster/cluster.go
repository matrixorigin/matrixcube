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
	errShardDestroyed = errors.New("shard destroyed")
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
// store 1 -> /1/raft/s/1, value is *metapb.Store
// shard 1 -> /1/raft/r/1, value is *metapb.Shard
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
	shardStats      *statistics.ShardStatistics

	coordinator      *coordinator
	suspectShards    *cache.TTLUint64 // suspectShards are shards that may need fix
	suspectKeyRanges *cache.TTLString // suspect key-range shards that may need fix

	wg   sync.WaitGroup
	quit chan struct{}

	ruleManager              *placement.RuleManager
	etcdClient               *clientv3.Client
	shardStateChangedHandler func(res *metapb.Shard, from metapb.ShardState, to metapb.ShardState)

	logger *zap.Logger
}

// NewRaftCluster create a new cluster.
func NewRaftCluster(
	ctx context.Context,
	root string,
	clusterID uint64,
	etcdClient *clientv3.Client,
	shardStateChangedHandler func(res *metapb.Shard, from metapb.ShardState, to metapb.ShardState),
	logger *zap.Logger,
) *RaftCluster {
	return &RaftCluster{
		ctx:                      ctx,
		running:                  false,
		clusterID:                clusterID,
		clusterRoot:              root,
		etcdClient:               etcdClient,
		shardStateChangedHandler: shardStateChangedHandler,
		logger:                   log.Adjust(logger).Named("raft-cluster"),
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
	c.shardStats = statistics.NewShardStatistics(c.opt, c.ruleManager)
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
	if err := c.storage.LoadStores(batch, func(meta metapb.Store, leaderWeight, shardWeight float64) {
		c.core.PutStore(core.NewCachedStore(meta,
			core.SetLeaderWeight(leaderWeight),
			core.SetShardWeight(shardWeight)))
	}); err != nil {
		return nil, err
	}
	c.logger.Info("stores loaded",
		zap.Int("count", c.GetStoreCount()),
		zap.Duration("cost", time.Since(start)))

	// used to load shard from kv storage to cache storage.
	start = time.Now()
	if err := c.storage.LoadShards(batch, func(meta metapb.Shard) {
		c.core.CheckAndPutShard(core.NewCachedShard(meta, nil))
	}); err != nil {
		return nil, err
	}
	c.logger.Info("shards loaded",
		zap.Int("count", c.GetShardCount()),
		zap.Duration("cost", time.Since(start)))

	// load shard group rules
	start = time.Now()
	c.storage.LoadScheduleGroupRules(batch, func(rule metapb.ScheduleGroupRule) {
		c.core.AddScheduleGroupRule(rule)
	})
	c.logger.Info("shard group rules loaded",
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

// GetShardScatter returns the shard scatter.
func (c *RaftCluster) GetShardScatter() *schedule.ShardScatterer {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.shardScatterer
}

// GetShardSplitter returns the shard splitter
func (c *RaftCluster) GetShardSplitter() *schedule.ShardSplitter {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.shardSplitter
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

// AddSuspectShards adds shards to suspect list.
func (c *RaftCluster) AddSuspectShards(shardIDs ...uint64) {
	c.Lock()
	defer c.Unlock()
	for _, shardID := range shardIDs {
		c.suspectShards.Put(shardID, nil)
	}
}

// GetSuspectShards gets all suspect shards.
func (c *RaftCluster) GetSuspectShards() []uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.suspectShards.GetAllID()
}

// RemoveSuspectShard removes shard from suspect list.
func (c *RaftCluster) RemoveSuspectShard(id uint64) {
	c.Lock()
	defer c.Unlock()
	c.suspectShards.Remove(id)
}

// AddSuspectKeyRange adds the key range with its ruleID as the key
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

// HandleStoreHeartbeat updates the store status.
func (c *RaftCluster) HandleStoreHeartbeat(stats *metapb.StoreStats) error {
	c.Lock()
	defer c.Unlock()

	storeID := stats.GetStoreID()
	store := c.GetStore(storeID)
	if store == nil {
		return fmt.Errorf("store %v not found", storeID)
	}
	newStore := store.Clone(core.SetStoreStats(stats), core.SetLastHeartbeatTS(time.Now()))
	if newStore.IsLowSpace(c.opt.GetLowSpaceRatio()) {
		c.logger.Warn("store does not have enough disk space, capacity %d, available %d",
			zap.Uint64("store", newStore.Meta.GetID()),
			zap.Uint64("capacity", newStore.GetCapacity()),
			zap.Uint64("available", newStore.GetAvailable()))
	}
	if newStore.NeedPersist() && c.storage != nil {
		if err := c.storage.PutStore(newStore.Meta); err != nil {
			c.logger.Error("fail to persist store",
				zap.Uint64("store", newStore.Meta.GetID()),
				zap.Error(err))
		} else {
			newStore = newStore.Clone(core.SetLastPersistTime(time.Now()))
		}

		c.addNotifyLocked(event.NewStoreEvent(newStore.Meta))
	}

	c.core.PutStore(newStore)
	c.addNotifyLocked(event.NewStoreStatsEvent(newStore.GetStoreStats()))

	// c.limiter is nil before "start" is called
	if c.limiter != nil && c.opt.GetStoreLimitMode() == "auto" {
		c.limiter.Collect(newStore.GetStoreStats())
	}

	return nil
}

// processShardHeartbeat updates the shard information.
func (c *RaftCluster) processShardHeartbeat(res *core.CachedShard) error {
	c.RLock()
	origin, err := c.core.PreCheckPutShard(res)
	if err != nil {
		c.RUnlock()
		return err
	}
	c.RUnlock()

	// Cube support remove running shards asynchronously, it will add remove job into embed etcd, and
	// each node execute these job on local to remove shard. So we need check whether the shard removed
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
	// Mark isNew if the shard in cache does not have leader.
	var saveKV, saveCache, isNew bool
	if origin == nil {
		c.logger.Debug("insert new shard",
			zap.Uint64("shard", res.Meta.GetID()))
		saveKV, saveCache, isNew = true, true, true
	} else {
		r := res.Meta.GetEpoch()
		o := origin.Meta.GetEpoch()
		if r.GetGeneration() > o.GetGeneration() {
			c.logger.Info("shard version changed",
				zap.Uint64("shard", res.Meta.GetID()),
				zap.Uint64("from", o.GetGeneration()),
				zap.Uint64("to", r.GetGeneration()))
			saveKV, saveCache = true, true
		}
		if r.GetConfigVer() > o.GetConfigVer() {
			c.logger.Info("shard ConfVer changed",
				zap.Uint64("shard", res.Meta.GetID()),
				log.ReplicasField("peers", res.Meta.GetReplicas()),
				zap.Uint64("from", o.GetConfigVer()),
				zap.Uint64("to", r.GetConfigVer()))
			saveKV, saveCache = true, true
		}
		if res.GetLeader().GetID() != origin.GetLeader().GetID() {
			if origin.GetLeader().GetID() == 0 {
				isNew = true
			} else {
				c.logger.Info("shard leader changed",
					zap.Uint64("shard", res.Meta.GetID()),
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

	if !saveKV && !saveCache && !isNew {
		return nil
	}

	c.Lock()
	inCreating := c.core.IsWaitingCreateShard(res.Meta.GetID())
	if isNew && inCreating {
		if c.shardStateChangedHandler != nil {
			c.shardStateChangedHandler(&res.Meta, metapb.ShardState_Creating,
				metapb.ShardState_Running)
		}
	}

	if saveCache {
		// To prevent a concurrent heartbeat of another shard from overriding the up-to-date shard info by a stale one,
		// check its validation again here.
		//
		// However, it can't solve the race condition of concurrent heartbeats from the same shard.
		if _, err := c.core.PreCheckPutShard(res); err != nil {
			c.Unlock()
			return err
		}

		overlaps := c.core.PutShard(res)
		if c.storage != nil {
			for _, item := range overlaps {
				if err := c.storage.RemoveShard(item.Meta); err != nil {
					c.logger.Error("fail to delete shard from storage",
						zap.Uint64("shard", item.Meta.GetID()),
						zap.Error(err))
				}
			}
		}
		for _, item := range overlaps {
			if c.shardStats != nil {
				c.shardStats.ClearDefunctShard(item.Meta.GetID())
			}
			c.labelLevelStats.ClearDefunctShard(item.Meta.GetID())
		}

		// Update related stores.
		storeMap := make(map[uint64]struct{})
		for _, p := range res.Meta.GetReplicas() {
			storeMap[p.GetStoreID()] = struct{}{}
		}
		if origin != nil {
			for _, p := range origin.Meta.GetReplicas() {
				storeMap[p.GetStoreID()] = struct{}{}
			}
		}
		for key := range storeMap {
			c.updateStoreStatusLocked(res.GetGroupKey(), key)
			if origin != nil && origin.GetGroupKey() != res.GetGroupKey() {
				c.logger.Debug("update store status",
					zap.Uint64("shard", res.Meta.GetID()),
					zap.Uint64("size", uint64(res.GetApproximateSize())),
					log.HexField("origin-group-key", []byte(origin.GetGroupKey())),
					log.HexField("current-group-key", []byte(res.GetGroupKey())))
				c.updateStoreStatusLocked(origin.GetGroupKey(), key)
			}
		}
		shardEventCounter.WithLabelValues("update_cache").Inc()
	}

	if isNew {
		c.prepareChecker.collect(res)
	}

	if c.shardStats != nil {
		c.shardStats.Observe(res, c.takeShardStoresLocked(res))
	}

	c.Unlock()

	// If there are concurrent heartbeats from the same shard, the last write will win even if
	// writes to storage in the critical area. So don't use mutex to protect it.
	if saveKV && c.storage != nil {
		if !inCreating && res.Meta.GetState() == metapb.ShardState_Creating {
			res = res.Clone(core.WithState(metapb.ShardState_Running))
		}

		if err := c.storage.PutShard(res.Meta); err != nil {
			// Not successfully saved to storage is not fatal, it only leads to longer warm-up
			// after restart. Here we only log the error then go on updating cache.
			c.logger.Error("fail to save shard to storage",
				zap.Uint64("shard", res.Meta.GetID()),
				zap.Error(err))
		}
		shardEventCounter.WithLabelValues("update_kv").Inc()
	}
	c.RLock()
	if saveKV || saveCache || isNew {
		if res.GetLeader().GetID() != 0 {
			from := uint64(0)
			if origin != nil {
				from = origin.GetLeader().GetStoreID()
			}
			c.logger.Debug("notify shard leader changed",
				zap.Uint64("shard", res.Meta.GetID()),
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

func (c *RaftCluster) updateStoreStatusLocked(groupKey string, storeID uint64) {
	leaderCount := c.core.GetStoreLeaderCount(groupKey, storeID)
	shardCount := c.core.GetStoreShardCount(groupKey, storeID)
	pendingPeerCount := c.core.GetStorePendingPeerCount(groupKey, storeID)
	leaderShardSize := c.core.GetStoreLeaderShardSize(groupKey, storeID)
	shardSize := c.core.GetStoreShardSize(groupKey, storeID)
	c.core.UpdateStoreStatus(groupKey, storeID, leaderCount, shardCount, pendingPeerCount, leaderShardSize, shardSize)
}

// GetShardByKey gets CachedShard by shard key from cluster.
func (c *RaftCluster) GetShardByKey(group uint64, shardKey []byte) *core.CachedShard {
	return c.core.SearchShard(group, shardKey)
}

// ScanShards scans shard with start key, until the shard contains endKey, or
// total number greater than limit.
func (c *RaftCluster) ScanShards(group uint64, startKey, endKey []byte, limit int) []*core.CachedShard {
	return c.core.ScanRange(group, startKey, endKey, limit)
}

// GetDestroyingShards returns all shards in destroying state
func (c *RaftCluster) GetDestroyingShards() []*core.CachedShard {
	return c.core.GetDestroyingShards()
}

// GetShard searches for a shard by ID.
func (c *RaftCluster) GetShard(shardID uint64) *core.CachedShard {
	return c.core.GetShard(shardID)
}

// GetMetaShards gets shards from cluster.
func (c *RaftCluster) GetMetaShards() []metapb.Shard {
	return c.core.GetMetaShards()
}

// GetShards returns all shards' information in detail.
func (c *RaftCluster) GetShards() []*core.CachedShard {
	return c.core.GetShards()
}

// GetShardCount returns total count of shards
func (c *RaftCluster) GetShardCount() int {
	return c.core.GetShardCount()
}

// GetStoreShards returns all shards' information with a given storeID.
func (c *RaftCluster) GetStoreShards(groupKey string, storeID uint64) []*core.CachedShard {
	return c.core.GetStoreShards(groupKey, storeID)
}

// RandLeaderShard returns a random shard that has leader on the store.
func (c *RaftCluster) RandLeaderShard(groupKey string, storeID uint64, ranges []core.KeyRange, opts ...core.ShardOption) *core.CachedShard {
	return c.core.RandLeaderShard(groupKey, storeID, ranges, opts...)
}

// RandFollowerShard returns a random shard that has a follower on the store.
func (c *RaftCluster) RandFollowerShard(groupKey string, storeID uint64, ranges []core.KeyRange, opts ...core.ShardOption) *core.CachedShard {
	return c.core.RandFollowerShard(groupKey, storeID, ranges, opts...)
}

// RandPendingShard returns a random shard that has a pending peer on the store.
func (c *RaftCluster) RandPendingShard(groupKey string, storeID uint64, ranges []core.KeyRange, opts ...core.ShardOption) *core.CachedShard {
	return c.core.RandPendingShard(groupKey, storeID, ranges, opts...)
}

// RandLearnerShard returns a random shard that has a learner peer on the store.
func (c *RaftCluster) RandLearnerShard(groupKey string, storeID uint64, ranges []core.KeyRange, opts ...core.ShardOption) *core.CachedShard {
	return c.core.RandLearnerShard(groupKey, storeID, ranges, opts...)
}

// GetLeaderStore returns all stores that contain the shard's leader peer.
func (c *RaftCluster) GetLeaderStore(res *core.CachedShard) *core.CachedStore {
	return c.core.GetLeaderStore(res)
}

// GetFollowerStores returns all stores that contain the shard's follower peer.
func (c *RaftCluster) GetFollowerStores(res *core.CachedShard) []*core.CachedStore {
	return c.core.GetFollowerStores(res)
}

// GetShardStores returns all stores that contain the shard's peer.
func (c *RaftCluster) GetShardStores(res *core.CachedShard) []*core.CachedStore {
	return c.core.GetShardStores(res)
}

// GetStoreCount returns the count of stores.
func (c *RaftCluster) GetStoreCount() int {
	return c.core.GetStoreCount()
}

// GetStoreShardCount returns the number of shards for a given store.
func (c *RaftCluster) GetStoreShardCount(groupKey string, storeID uint64) int {
	return c.core.GetStoreShardCount(groupKey, storeID)
}

// GetAverageShardSize returns the average shard approximate size.
func (c *RaftCluster) GetAverageShardSize() int64 {
	return c.core.GetAverageShardSize()
}

// GetShardStats returns shard statistics from cluster.
func (c *RaftCluster) GetShardStats(group uint64, startKey, endKey []byte) *statistics.ShardStats {
	c.RLock()
	defer c.RUnlock()
	return statistics.GetShardStats(c.core.ScanRange(group, startKey, endKey, -1))
}

// DropCacheShard removes a shard from the cache.
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

// GetMetaStores gets stores from cluster.
func (c *RaftCluster) GetMetaStores() []metapb.Store {
	return c.core.GetMetaStores()
}

// GetStores returns all stores in the cluster.
func (c *RaftCluster) GetStores() []*core.CachedStore {
	return c.core.GetStores()
}

// GetStore gets store from cluster.
func (c *RaftCluster) GetStore(storeID uint64) *core.CachedStore {
	return c.core.GetStore(storeID)
}

// GetAdjacentShards returns shards' information that are adjacent with the specific shard ID.
func (c *RaftCluster) GetAdjacentShards(res *core.CachedShard) (*core.CachedShard, *core.CachedShard) {
	return c.core.GetAdjacentShards(res)
}

// UpdateStoreLabels updates a store's location labels
// If 'force' is true, then update the store's labels forcibly.
func (c *RaftCluster) UpdateStoreLabels(storeID uint64, labels []metapb.Label, force bool) error {
	store := c.GetStore(storeID)
	if store == nil {
		return fmt.Errorf("invalid store ID %d, not found", storeID)
	}
	newStore := store.Meta
	newStore.SetLabels(labels)
	// PutStore will perform label merge.
	return c.putStoreImpl(newStore, force)
}

// PutStore puts a store.
func (c *RaftCluster) PutStore(store metapb.Store) error {
	if err := c.putStoreImpl(store, false); err != nil {
		return err
	}
	c.AddStoreLimit(store)
	return nil
}

// putStoreImpl puts a store.
// If 'force' is true, then overwrite the store's labels.
func (c *RaftCluster) putStoreImpl(store metapb.Store, force bool) error {
	c.Lock()
	defer c.Unlock()

	if store.GetID() == 0 {
		return fmt.Errorf("invalid put store %v", store)
	}

	// store address can not be the same as other stores.
	for _, s := range c.GetStores() {
		// It's OK to start a new store on the same address if the old store has been removed or physically destroyed.
		if s.IsTombstone() || s.IsPhysicallyDestroyed() {

			continue
		}
		if s.Meta.GetID() != store.GetID() && s.Meta.GetClientAddress() == store.GetClientAddress() {
			return fmt.Errorf("duplicated store address: %v, already registered by %v", store, s.Meta)
		}
	}

	s := c.GetStore(store.GetID())
	if s == nil {
		// Add a new store.
		s = core.NewCachedStore(store)
	} else {
		// Use the given labels to update the store.
		labels := store.GetLabels()
		if !force {
			// If 'force' isn't set, the given labels will merge into those labels which already existed in the store.
			labels = s.MergeLabels(labels)
		}
		// Update an existed store.
		v, githash := store.GetVersionAndGitHash()
		s = s.Clone(
			core.SetStoreAddress(store.GetClientAddress(), store.GetRaftAddress()),
			core.SetStoreVersion(githash, v),
			core.SetStoreLabels(labels),
			core.SetStoreStartTime(store.GetStartTime()),
			core.SetStoreDeployPath(store.GetDeployPath()),
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
			c.logger.Warn("store label configuration is incorrect",
				zap.Uint64("store", s.Meta.GetID()),
				zap.String("label", k))
			if c.opt.GetStrictlyMatchLabel() {
				return fmt.Errorf("label configuration is incorrect, need to specify the key: %s ", k)
			}
		}
	}
	for _, label := range s.Meta.GetLabels() {
		key := label.GetKey()
		if _, ok := keysSet[key]; !ok {
			c.logger.Warn("store not found the key match with the label",
				zap.Uint64("store", s.Meta.GetID()),
				zap.String("label", key))
			if c.opt.GetStrictlyMatchLabel() {
				return fmt.Errorf("key matching the label was not found in the Prophet, store label key: %s ", key)
			}
		}
	}
	return nil
}

// RemoveStore marks a store as offline in cluster.
// State transition: Up -> Offline.
func (c *RaftCluster) RemoveStore(storeID uint64, physicallyDestroyed bool) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return fmt.Errorf("store %d not found", storeID)
	}

	// Remove an offline store should be OK, nothing to do.
	if store.IsOffline() && store.IsPhysicallyDestroyed() == physicallyDestroyed {
		return nil
	}

	if store.IsTombstone() {
		return fmt.Errorf("store %d is tombstone", storeID)
	}

	if store.IsPhysicallyDestroyed() {
		return fmt.Errorf("store %d is physically destroyed", storeID)
	}

	newStore := store.Clone(core.OfflineStore(physicallyDestroyed))
	c.logger.Warn("store has been offline",
		zap.Uint64("store", newStore.Meta.GetID()),
		zap.String("store-address", newStore.Meta.GetClientAddress()),
		zap.Bool("physically-destroyed", physicallyDestroyed))
	err := c.putStoreLocked(newStore)
	if err == nil {
		// TODO: if the persist operation encounters error, the "Unlimited" will be rollback.
		// And considering the store state has changed, RemoveStore is actually successful.
		c.SetStoreLimit(storeID, limit.RemovePeer, limit.Unlimited)
	}
	return err
}

// buryStore marks a store as tombstone in cluster.
// The store should be empty before calling this func
// State transition: Offline -> Tombstone.
func (c *RaftCluster) buryStore(storeID uint64) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return fmt.Errorf("store %d not found", storeID)
	}

	// Bury a tombstone store should be OK, nothing to do.
	if store.IsTombstone() {
		return nil
	}

	if store.IsUp() {
		return fmt.Errorf("store %d is UP", storeID)
	}

	newStore := store.Clone(core.TombstoneStore())
	c.logger.Warn("store has been tombstone",
		zap.Uint64("store", newStore.Meta.GetID()),
		zap.String("store-address", newStore.Meta.GetClientAddress()),
		zap.Bool("physically-destroyed", newStore.IsPhysicallyDestroyed()))
	err := c.putStoreLocked(newStore)
	if err == nil {
		c.RemoveStoreLimit(storeID)
	}
	return err
}

// PauseLeaderTransfer prevents the store from been selected as source or
// target store of TransferLeader.
func (c *RaftCluster) PauseLeaderTransfer(storeID uint64) error {
	return c.core.PauseLeaderTransfer(storeID)
}

// ResumeLeaderTransfer cleans a store's pause state. The store can be selected
// as source or target of TransferLeader again.
func (c *RaftCluster) ResumeLeaderTransfer(storeID uint64) {
	c.core.ResumeLeaderTransfer(storeID)
}

// AttachAvailableFunc attaches an available function to a specific store.
func (c *RaftCluster) AttachAvailableFunc(storeID uint64, limitType limit.Type, f func() bool) {
	c.core.AttachAvailableFunc(storeID, limitType, f)
}

// UpStore up a store from offline
func (c *RaftCluster) UpStore(storeID uint64) error {
	c.Lock()
	defer c.Unlock()
	store := c.GetStore(storeID)
	if store == nil {
		return fmt.Errorf("store %d not found", storeID)
	}

	if store.IsTombstone() {
		return fmt.Errorf("store %d is tombstone", storeID)
	}

	if store.IsPhysicallyDestroyed() {
		return fmt.Errorf("store %d is physically destroyed", storeID)
	}

	if store.IsUp() {
		return nil
	}

	newStore := store.Clone(core.UpStore())
	c.logger.Warn("store has been up",
		zap.Uint64("store", newStore.Meta.GetID()),
		zap.String("store-address", newStore.Meta.GetClientAddress()))
	return c.putStoreLocked(newStore)
}

// SetStoreWeight sets up a store's leader/shard balance weight.
func (c *RaftCluster) SetStoreWeight(storeID uint64, leaderWeight, shardWeight float64) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return fmt.Errorf("store %d not found", storeID)
	}

	if err := c.storage.PutStoreWeight(storeID, leaderWeight, shardWeight); err != nil {
		return err
	}

	newStore := store.Clone(
		core.SetLeaderWeight(leaderWeight),
		core.SetShardWeight(shardWeight),
	)

	return c.putStoreLocked(newStore)
}

func (c *RaftCluster) putStoreLocked(store *core.CachedStore) error {
	if c.storage != nil {
		if err := c.storage.PutStore(store.Meta); err != nil {
			return err
		}
	}
	c.core.PutStore(store)
	return nil
}

func (c *RaftCluster) checkStores() {
	var offlineStores []metapb.Store
	var upStoreCount int
	stores := c.GetStores()
	groupKeys := c.core.GetScheduleGroupKeys()
	for _, store := range stores {
		// the store has already been tombstone
		if store.IsTombstone() {
			continue
		}

		if store.IsUp() {
			if !store.IsLowSpace(c.opt.GetLowSpaceRatio()) {
				upStoreCount++
			}
			continue
		}

		offlineStore := store.Meta
		shardCount := 0
		for _, group := range groupKeys {
			shardCount += c.core.GetStoreShardCount(group, offlineStore.GetID())
		}

		// If the store is empty, it can be buried.
		if shardCount == 0 {
			if err := c.buryStore(offlineStore.GetID()); err != nil {
				c.logger.Error("fail to bury store",
					zap.Uint64("store", offlineStore.GetID()),
					zap.String("store-address", offlineStore.GetClientAddress()),
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
		for _, store := range offlineStores {
			c.logger.Warn("store may not turn into Tombstone, there are no extra up store has enough space to accommodate the extra replica",
				zap.Uint64("store", store.GetID()),
				zap.String("store-address", store.GetClientAddress()))
		}
	}
}

// RemoveTombStoneRecords removes the tombStone Records.
func (c *RaftCluster) RemoveTombStoneRecords() error {
	c.Lock()
	defer c.Unlock()
	for _, groupKey := range c.core.GetScheduleGroupKeys() {
		for _, store := range c.GetStores() {
			if store.IsTombstone() {
				if store.GetShardCount(groupKey) > 0 {
					c.logger.Warn("skip removing tombstone store",
						zap.Uint64("store", store.Meta.GetID()),
						zap.String("store-address", store.Meta.GetClientAddress()))
					continue
				}

				// the store has already been tombstone
				err := c.deleteStoreLocked(store)
				if err != nil {
					c.logger.Error("fail to delete store",
						zap.Uint64("store", store.Meta.GetID()),
						zap.String("store-address", store.Meta.GetClientAddress()))
					return err
				}
				c.RemoveStoreLimit(store.Meta.GetID())

				c.logger.Info("store deleted",
					zap.Uint64("store", store.Meta.GetID()),
					zap.String("store-address", store.Meta.GetClientAddress()))
			}
		}
	}
	return nil
}

func (c *RaftCluster) deleteStoreLocked(store *core.CachedStore) error {
	if c.storage != nil {
		if err := c.storage.RemoveStore(store.Meta); err != nil {
			return err
		}
	}
	c.core.DeleteStore(store)
	return nil
}

func (c *RaftCluster) collectMetrics() {
	c.coordinator.collectSchedulerMetrics()
	c.coordinator.opController.CollectStoreLimitMetrics()
	c.collectClusterMetrics()
}

func (c *RaftCluster) resetMetrics() {
	statsMap := statistics.NewStoreStatisticsMap(c.opt)
	statsMap.Reset()

	c.coordinator.resetSchedulerMetrics()
	c.resetClusterMetrics()
}

func (c *RaftCluster) collectClusterMetrics() {
	c.RLock()
	defer c.RUnlock()
	if c.shardStats == nil {
		return
	}
	c.shardStats.Collect()
	c.labelLevelStats.Collect()
}

func (c *RaftCluster) resetClusterMetrics() {
	c.RLock()
	defer c.RUnlock()
	if c.shardStats == nil {
		return
	}
	c.shardStats.Reset()
	c.labelLevelStats.Reset()
}

// GetShardStatsByType gets the status of the shard by types.
func (c *RaftCluster) GetShardStatsByType(typ statistics.ShardStatisticType) []*core.CachedShard {
	c.RLock()
	defer c.RUnlock()
	if c.shardStats == nil {
		return nil
	}
	return c.shardStats.GetShardStatsByType(typ)
}

// GetOfflineShardStatsByType gets the status of the offline shard by types.
func (c *RaftCluster) GetOfflineShardStatsByType(typ statistics.ShardStatisticType) []*core.CachedShard {
	c.RLock()
	defer c.RUnlock()
	if c.shardStats == nil {
		return nil
	}
	return c.shardStats.GetOfflineShardStatsByType(typ)
}

func (c *RaftCluster) updateShardsLabelLevelStats(shards []*core.CachedShard) {
	c.Lock()
	defer c.Unlock()
	for _, res := range shards {
		c.labelLevelStats.Observe(res, c.takeShardStoresLocked(res), c.opt.GetLocationLabels())
	}
}

func (c *RaftCluster) takeShardStoresLocked(res *core.CachedShard) []*core.CachedStore {
	stores := make([]*core.CachedStore, 0, len(res.Meta.GetReplicas()))
	for _, p := range res.Meta.GetReplicas() {
		if store := c.core.GetStore(p.StoreID); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
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

// GetRuleManager returns the rule manager reference.
func (c *RaftCluster) GetRuleManager() *placement.RuleManager {
	c.RLock()
	defer c.RUnlock()
	return c.ruleManager
}

// FitShard tries to fit the shard with placement rules.
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

// Before starting up the scheduler, we need to take the proportion of the shards on each store into consideration.
func (checker *prepareChecker) checkLocked(c *RaftCluster) bool {
	if checker.isPrepared || time.Since(checker.start) > collectTimeout {
		return true
	}
	// The number of active shards should be more than total shard of all stores * collectFactor
	if float64(c.core.GetShardCount())*collectFactor > float64(checker.sum) {
		return false
	}
	for _, store := range c.core.GetStores() {
		if !store.IsUp() {
			continue
		}
		storeID := store.Meta.GetID()
		n := 0
		for _, group := range c.core.GetScheduleGroupKeys() {
			n += c.core.GetStoreShardCount(group, storeID)
		}

		// For each store, the number of active shards should be more than total shard of the store * collectFactor
		if float64(n)*collectFactor > float64(checker.reactiveShards[storeID]) {
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

// GetStoreLimitByType returns the store limit for a given store ID and type.
func (c *RaftCluster) GetStoreLimitByType(storeID uint64, typ limit.Type) float64 {
	return c.opt.GetStoreLimitByType(storeID, typ)
}

// GetAllStoresLimit returns all store limit
func (c *RaftCluster) GetAllStoresLimit() map[uint64]config.StoreLimitConfig {
	return c.opt.GetAllStoresLimit()
}

// AddStoreLimit add a store limit for a given store ID.
func (c *RaftCluster) AddStoreLimit(store metapb.Store) {
	storeID := store.GetID()
	cfg := c.opt.GetScheduleConfig().Clone()
	if _, ok := cfg.StoreLimit[storeID]; ok {
		return
	}

	sc := config.StoreLimitConfig{
		AddPeer:    config.DefaultStoreLimit.GetDefaultStoreLimit(limit.AddPeer),
		RemovePeer: config.DefaultStoreLimit.GetDefaultStoreLimit(limit.RemovePeer),
	}

	cfg.StoreLimit[storeID] = sc
	c.opt.SetScheduleConfig(cfg)

	var err error
	for i := 0; i < persistLimitRetryTimes; i++ {
		if err = c.opt.Persist(c.storage); err == nil {
			c.logger.Info("store limit added",
				zap.Any("limit", cfg.StoreLimit[storeID]),
				zap.Uint64("store", storeID))
			return
		}
		time.Sleep(persistLimitWaitTime)
	}
	c.logger.Error("fail to persist store limit",
		zap.Uint64("store", storeID),
		zap.Error(err))
}

// RemoveStoreLimit remove a store limit for a given store ID.
func (c *RaftCluster) RemoveStoreLimit(storeID uint64) {
	cfg := c.opt.GetScheduleConfig().Clone()
	for _, limitType := range limit.TypeNameValue {
		c.AttachAvailableFunc(storeID, limitType, nil)
	}
	delete(cfg.StoreLimit, storeID)
	c.opt.SetScheduleConfig(cfg)

	var err error
	for i := 0; i < persistLimitRetryTimes; i++ {
		if err = c.opt.Persist(c.storage); err == nil {
			c.logger.Info("store limit removed",
				zap.Uint64("store", storeID))
			return
		}
		time.Sleep(persistLimitWaitTime)
	}
	c.logger.Error("fail to persist store limit",
		zap.Uint64("store", storeID),
		zap.Error(err))
}

// SetStoreLimit sets a store limit for a given type and rate.
func (c *RaftCluster) SetStoreLimit(storeID uint64, typ limit.Type, ratePerMin float64) error {
	old := c.opt.GetScheduleConfig().Clone()
	c.opt.SetStoreLimit(storeID, typ, ratePerMin)
	if err := c.opt.Persist(c.storage); err != nil {
		// roll back the store limit
		c.opt.SetScheduleConfig(old)
		c.logger.Error("fail to persist store limit",
			zap.Uint64("store", storeID),
			zap.Error(err))
		return err
	}

	c.logger.Error("store limit changed",
		zap.Uint64("store", storeID),
		zap.String("type", typ.String()),
		zap.Float64("new-value", ratePerMin))
	return nil
}

// SetAllStoresLimit sets all store limit for a given type and rate.
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
		c.logger.Error("fail to persist stores limit",
			zap.Error(err))
		return err
	}
	c.logger.Info("all stores limit changed")
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
