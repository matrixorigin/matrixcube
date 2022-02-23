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

package mockcluster

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/statistics"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"go.uber.org/zap"
)

const (
	defaultStoreCapacity = 100 * (1 << 30) // 100GiB
	defaultShardSize     = 96 * (1 << 20)  // 96MiB
	mb                   = (1 << 20)       // 1MiB
)

// Cluster is used to mock clusterInfo for test use.
type Cluster struct {
	*core.BasicCluster
	*placement.RuleManager
	*statistics.HotStat
	*config.PersistOptions
	storage       storage.Storage
	ID            uint64
	suspectShards map[uint64]struct{}

	supportJointConsensus bool
}

// NewCluster creates a new Cluster
func NewCluster(opts *config.PersistOptions) *Cluster {
	clus := &Cluster{
		storage:               storage.NewTestStorage(),
		BasicCluster:          core.NewBasicCluster(nil),
		HotStat:               statistics.NewHotStat(),
		PersistOptions:        opts,
		suspectShards:         map[uint64]struct{}{},
		supportJointConsensus: true,
	}
	if clus.PersistOptions.GetReplicationConfig().EnablePlacementRules {
		clus.initRuleManager()
	}
	clus.BasicCluster.ScheduleGroupKeys[""] = struct{}{}
	return clus
}

// DisableJointConsensus mock
func (mc *Cluster) DisableJointConsensus() {
	mc.supportJointConsensus = false
}

// GetLogger returns zap logger
func (mc *Cluster) GetLogger() *zap.Logger {
	return log.Adjust(nil)
}

// JointConsensusEnabled mock
func (mc *Cluster) JointConsensusEnabled() bool {
	return mc.supportJointConsensus
}

// GetOpts returns the cluster configuration.
func (mc *Cluster) GetOpts() *config.PersistOptions {
	return mc.PersistOptions
}

// AllocID allocs a new unique ID.
func (mc *Cluster) AllocID() (uint64, error) {
	return mc.storage.KV().AllocID()
}

// ScanShards scans resource with start key, until number greater than limit.
func (mc *Cluster) ScanShards(group uint64, startKey, endKey []byte, limit int) []*core.CachedShard {
	return mc.Shards.ScanRange(group, startKey, endKey, limit)
}

// LoadShard puts resource info without leader
func (mc *Cluster) LoadShard(resID uint64, followerIds ...uint64) {
	//  resources load from etcd will have no leader
	r := mc.newMockCachedShard(resID, 0, followerIds...).Clone(core.WithLeader(nil))
	mc.PutShard(r)
}

// GetStoresLoads gets stores load statistics.
func (mc *Cluster) GetStoresLoads() map[uint64][]float64 {
	return mc.HotStat.GetStoresLoads()
}

// GetStoreShardCount gets resource count with a given container.
func (mc *Cluster) GetStoreShardCount(groupKey string, containerID uint64) int {
	return mc.Shards.GetStoreShardCount(groupKey, containerID)
}

// GetStore gets a container with a given container ID.
func (mc *Cluster) GetStore(containerID uint64) *core.CachedStore {
	return mc.Stores.GetStore(containerID)
}

// IsShardHot checks if the resource is hot.
func (mc *Cluster) IsShardHot(res *core.CachedShard) bool {
	return mc.HotCache.IsShardHot(res, mc.GetHotShardCacheHitsThreshold())
}

// ShardReadStats returns hot Shard's read stats.
// The result only includes peers that are hot enough.
func (mc *Cluster) ShardReadStats() map[uint64][]*statistics.HotPeerStat {
	return mc.HotCache.ShardStats(statistics.ReadFlow, mc.GetHotShardCacheHitsThreshold())
}

// ShardWriteStats returns hot Shard's write stats.
// The result only includes peers that are hot enough.
func (mc *Cluster) ShardWriteStats() map[uint64][]*statistics.HotPeerStat {
	return mc.HotCache.ShardStats(statistics.WriteFlow, mc.GetHotShardCacheHitsThreshold())
}

// RandHotShardFromStore random picks a hot resource in specify container.
func (mc *Cluster) RandHotShardFromStore(containerID uint64, kind statistics.FlowKind) *core.CachedShard {
	r := mc.HotCache.RandHotShardFromStore(containerID, kind, mc.GetHotShardCacheHitsThreshold())
	if r == nil {
		return nil
	}
	return mc.GetShard(r.ShardID)
}

// AllocPeer allocs a new peer on a container.
func (mc *Cluster) AllocPeer(containerID uint64) (metapb.Replica, error) {
	peerID, err := mc.AllocID()
	if err != nil {
		return metapb.Replica{}, err
	}

	return metapb.Replica{
		ID:      peerID,
		StoreID: containerID,
	}, nil
}

func (mc *Cluster) initRuleManager() {
	if mc.RuleManager == nil {
		mc.RuleManager = placement.NewRuleManager(mc.storage, mc, nil)
		mc.RuleManager.Initialize(int(mc.GetReplicationConfig().MaxReplicas), mc.GetReplicationConfig().LocationLabels)
	}
}

// FitShard fits a resource to the rules it matches.
func (mc *Cluster) FitShard(res *core.CachedShard) *placement.ShardFit {
	return mc.RuleManager.FitShard(mc.BasicCluster, res)
}

// GetRuleManager returns the ruleManager of the cluster.
func (mc *Cluster) GetRuleManager() *placement.RuleManager {
	return mc.RuleManager
}

// SetStoreUP sets container state to be up.
func (mc *Cluster) SetStoreUP(containerID uint64) {
	container := mc.GetStore(containerID)
	newStore := container.Clone(
		core.UpStore(),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutStore(newStore)
}

// SetStoreDisconnect changes a container's state to disconnected.
func (mc *Cluster) SetStoreDisconnect(containerID uint64) {
	container := mc.GetStore(containerID)
	newStore := container.Clone(
		core.UpStore(),
		core.SetLastHeartbeatTS(time.Now().Add(-time.Second*30)),
	)
	mc.PutStore(newStore)
}

// SetStoreDown sets container down.
func (mc *Cluster) SetStoreDown(containerID uint64) {
	container := mc.GetStore(containerID)
	newStore := container.Clone(
		core.UpStore(),
		core.SetLastHeartbeatTS(time.Time{}),
	)
	mc.PutStore(newStore)
}

// SetStoreOffline sets container state to be offline.
func (mc *Cluster) SetStoreOffline(containerID uint64) {
	container := mc.GetStore(containerID)
	newStore := container.Clone(core.OfflineStore(false))
	mc.PutStore(newStore)
}

// SetStoreBusy sets container busy.
func (mc *Cluster) SetStoreBusy(containerID uint64, busy bool) {
	container := mc.GetStore(containerID)
	newStats := proto.Clone(container.GetStoreStats()).(*metapb.StoreStats)
	newStats.IsBusy = busy
	newStore := container.Clone(
		core.SetStoreStats(newStats),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutStore(newStore)
}

// AddLeaderStore adds container with specified count of leader.
func (mc *Cluster) AddLeaderStore(containerID uint64, leaderCount int, leaderSizes ...int64) {
	stats := &metapb.StoreStats{}
	stats.Capacity = defaultStoreCapacity
	stats.UsedSize = uint64(leaderCount) * defaultShardSize
	stats.Available = stats.Capacity - uint64(leaderCount)*defaultShardSize
	var leaderSize int64
	if len(leaderSizes) != 0 {
		leaderSize = leaderSizes[0]
	} else {
		leaderSize = int64(leaderCount) * defaultShardSize / mb
	}

	container := core.NewCachedStore(
		&metapb.Store{ID: containerID},
		core.SetStoreStats(stats),
		core.SetLeaderCount("", leaderCount),
		core.SetLeaderSize("", leaderSize),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.SetStoreLimit(containerID, limit.AddPeer, 60)
	mc.SetStoreLimit(containerID, limit.RemovePeer, 60)
	mc.PutStore(container)
}

// AddShardStore adds container with specified count of resource.
func (mc *Cluster) AddShardStore(containerID uint64, resourceCount int) {
	stats := &metapb.StoreStats{}
	stats.Capacity = defaultStoreCapacity
	stats.UsedSize = uint64(resourceCount) * defaultShardSize
	stats.Available = stats.Capacity - uint64(resourceCount)*defaultShardSize
	container := core.NewCachedStore(
		&metapb.Store{
			ID:     containerID,
			Labels: []metapb.Pair{{Key: "ID", Value: fmt.Sprintf("%v", containerID)}},
		},
		core.SetStoreStats(stats),
		core.SetShardCount("", resourceCount),
		core.SetShardSize("", int64(resourceCount)*defaultShardSize/mb),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.SetStoreLimit(containerID, limit.AddPeer, 60)
	mc.SetStoreLimit(containerID, limit.RemovePeer, 60)
	mc.PutStore(container)
}

// AddShardStoreWithLeader adds container with specified count of resource and leader.
func (mc *Cluster) AddShardStoreWithLeader(containerID uint64, resourceCount int, leaderCounts ...int) {
	leaderCount := resourceCount
	if len(leaderCounts) != 0 {
		leaderCount = leaderCounts[0]
	}
	mc.AddShardStore(containerID, resourceCount)
	for i := 0; i < leaderCount; i++ {
		id, _ := mc.AllocID()
		mc.AddLeaderShard(id, containerID)
	}
}

// AddLabelsStore adds container with specified count of resource and labels.
func (mc *Cluster) AddLabelsStore(containerID uint64, resourceCount int, labels map[string]string) {
	newLabels := make([]metapb.Pair, 0, len(labels))
	for k, v := range labels {
		newLabels = append(newLabels, metapb.Pair{Key: k, Value: v})
	}
	stats := &metapb.StoreStats{}
	stats.Capacity = defaultStoreCapacity
	stats.Available = stats.Capacity - uint64(resourceCount)*defaultShardSize
	stats.UsedSize = uint64(resourceCount) * defaultShardSize
	container := core.NewCachedStore(
		&metapb.Store{
			ID:     containerID,
			Labels: newLabels,
		},
		core.SetStoreStats(stats),
		core.SetShardCount("", resourceCount),
		core.SetShardSize("", int64(resourceCount)*defaultShardSize/mb),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.SetStoreLimit(containerID, limit.AddPeer, 60)
	mc.SetStoreLimit(containerID, limit.RemovePeer, 60)
	mc.PutStore(container)
}

// AddLeaderShard adds resource with specified leader and followers.
func (mc *Cluster) AddLeaderShard(resID uint64, leaderStoreID uint64, followerStoreIDs ...uint64) *core.CachedShard {
	origin := mc.newMockCachedShard(resID, leaderStoreID, followerStoreIDs...)
	res := origin.Clone(core.SetApproximateSize(defaultShardSize/mb), core.SetApproximateKeys(10))
	mc.PutShard(res)
	return res
}

// AddShardWithLearner adds resource with specified leader, followers and learners.
func (mc *Cluster) AddShardWithLearner(resID uint64, leaderStoreID uint64, followerStoreIDs, learnerStoreIDs []uint64) *core.CachedShard {
	origin := mc.MockCachedShard(resID, leaderStoreID, followerStoreIDs, learnerStoreIDs, metapb.ShardEpoch{})
	res := origin.Clone(core.SetApproximateSize(defaultShardSize/mb), core.SetApproximateKeys(10))
	mc.PutShard(res)
	return res
}

// AddLeaderShardWithRange adds resource with specified leader, followers and key range.
func (mc *Cluster) AddLeaderShardWithRange(resID uint64, startKey string, endKey string, leaderID uint64, followerIds ...uint64) {
	o := mc.newMockCachedShard(resID, leaderID, followerIds...)
	r := o.Clone(
		core.WithStartKey([]byte(startKey)),
		core.WithEndKey([]byte(endKey)),
	)
	mc.PutShard(r)
}

// AddLeaderShardWithReadInfo adds resource with specified leader, followers and read info.
func (mc *Cluster) AddLeaderShardWithReadInfo(
	resID uint64, leaderID uint64,
	readBytes, readKeys uint64,
	reportInterval uint64,
	followerIds []uint64, filledNums ...int) []*statistics.HotPeerStat {
	r := mc.newMockCachedShard(resID, leaderID, followerIds...)
	r = r.Clone(core.SetReadBytes(readBytes))
	r = r.Clone(core.SetReadKeys(readKeys))
	r = r.Clone(core.SetReportInterval(reportInterval))
	filledNum := mc.HotCache.GetFilledPeriod(statistics.ReadFlow)
	if len(filledNums) > 0 {
		filledNum = filledNums[0]
	}

	var items []*statistics.HotPeerStat
	for i := 0; i < filledNum; i++ {
		items = mc.HotCache.CheckRead(r)
		for _, item := range items {
			mc.HotCache.Update(item)
		}
	}
	mc.PutShard(r)
	return items
}

// AddLeaderShardWithWriteInfo adds resource with specified leader, followers and write info.
func (mc *Cluster) AddLeaderShardWithWriteInfo(
	resID uint64, leaderID uint64,
	writtenBytes, writtenKeys uint64,
	reportInterval uint64,
	followerIds []uint64, filledNums ...int) []*statistics.HotPeerStat {
	r := mc.newMockCachedShard(resID, leaderID, followerIds...)
	r = r.Clone(core.SetWrittenBytes(writtenBytes))
	r = r.Clone(core.SetWrittenKeys(writtenKeys))
	r = r.Clone(core.SetReportInterval(reportInterval))

	filledNum := mc.HotCache.GetFilledPeriod(statistics.WriteFlow)
	if len(filledNums) > 0 {
		filledNum = filledNums[0]
	}

	var items []*statistics.HotPeerStat
	for i := 0; i < filledNum; i++ {
		items = mc.HotCache.CheckWrite(r)
		for _, item := range items {
			mc.HotCache.Update(item)
		}
	}
	mc.PutShard(r)
	return items
}

// UpdateStoreLeaderWeight updates container leader weight.
func (mc *Cluster) UpdateStoreLeaderWeight(containerID uint64, weight float64) {
	container := mc.GetStore(containerID)
	newStore := container.Clone(core.SetLeaderWeight(weight))
	mc.PutStore(newStore)
}

// UpdateStoreShardWeight updates container resource weight.
func (mc *Cluster) UpdateStoreShardWeight(containerID uint64, weight float64) {
	container := mc.GetStore(containerID)
	newStore := container.Clone(core.SetShardWeight(weight))
	mc.PutStore(newStore)
}

// UpdateStoreLeaderSize updates container leader size.
func (mc *Cluster) UpdateStoreLeaderSize(containerID uint64, size int64) {
	container := mc.GetStore(containerID)
	newStats := proto.Clone(container.GetStoreStats()).(*metapb.StoreStats)
	newStats.Available = newStats.Capacity - uint64(container.GetLeaderSize(""))
	newStore := container.Clone(
		core.SetStoreStats(newStats),
		core.SetLeaderSize("", size),
	)
	mc.PutStore(newStore)
}

// UpdateStoreShardSize updates container resource size.
func (mc *Cluster) UpdateStoreShardSize(containerID uint64, size int64) {
	container := mc.GetStore(containerID)
	newStats := proto.Clone(container.GetStoreStats()).(*metapb.StoreStats)
	newStats.Available = newStats.Capacity - uint64(container.GetShardSize(""))
	newStore := container.Clone(
		core.SetStoreStats(newStats),
		core.SetShardSize("", size),
	)
	mc.PutStore(newStore)
}

// UpdateLeaderCount updates container leader count.
func (mc *Cluster) UpdateLeaderCount(containerID uint64, leaderCount int) {
	container := mc.GetStore(containerID)
	newStore := container.Clone(
		core.SetLeaderCount("", leaderCount),
		core.SetLeaderSize("", int64(leaderCount)*defaultShardSize/mb),
	)
	mc.PutStore(newStore)
}

// UpdateShardCount updates container resource count.
func (mc *Cluster) UpdateShardCount(containerID uint64, resourceCount int) {
	container := mc.GetStore(containerID)
	newStore := container.Clone(
		core.SetShardCount("", resourceCount),
		core.SetShardSize("", int64(resourceCount)*defaultShardSize/mb),
	)
	mc.PutStore(newStore)
}

// UpdateSnapshotCount updates container snapshot count.
func (mc *Cluster) UpdateSnapshotCount(containerID uint64, snapshotCount int) {
	container := mc.GetStore(containerID)
	newStats := proto.Clone(container.GetStoreStats()).(*metapb.StoreStats)
	newStats.ApplyingSnapCount = uint64(snapshotCount)
	newStore := container.Clone(core.SetStoreStats(newStats))
	mc.PutStore(newStore)
}

// UpdatePendingPeerCount updates container pending peer count.
func (mc *Cluster) UpdatePendingPeerCount(containerID uint64, pendingPeerCount int) {
	container := mc.GetStore(containerID)
	newStore := container.Clone(core.SetPendingPeerCount("", pendingPeerCount))
	mc.PutStore(newStore)
}

// UpdateStorageRatio updates container storage ratio count.
func (mc *Cluster) UpdateStorageRatio(containerID uint64, usedRatio, availableRatio float64) {
	container := mc.GetStore(containerID)
	newStats := proto.Clone(container.GetStoreStats()).(*metapb.StoreStats)
	newStats.Capacity = defaultStoreCapacity
	newStats.UsedSize = uint64(float64(newStats.Capacity) * usedRatio)
	newStats.Available = uint64(float64(newStats.Capacity) * availableRatio)
	newStore := container.Clone(core.SetStoreStats(newStats))
	mc.PutStore(newStore)
}

// UpdateStorageWrittenStats updates container written bytes.
func (mc *Cluster) UpdateStorageWrittenStats(containerID, bytesWritten, keysWritten uint64) {
	container := mc.GetStore(containerID)
	newStats := proto.Clone(container.GetStoreStats()).(*metapb.StoreStats)
	newStats.WrittenBytes = bytesWritten
	newStats.WrittenKeys = keysWritten
	now := time.Now().Second()
	interval := &metapb.TimeInterval{Start: uint64(now - statistics.StoreHeartBeatReportInterval), End: uint64(now)}
	newStats.Interval = interval
	newStore := container.Clone(core.SetStoreStats(newStats))
	mc.Set(containerID, newStats)
	mc.PutStore(newStore)
}

// UpdateStorageReadStats updates container written bytes.
func (mc *Cluster) UpdateStorageReadStats(containerID, bytesWritten, keysWritten uint64) {
	container := mc.GetStore(containerID)
	newStats := proto.Clone(container.GetStoreStats()).(*metapb.StoreStats)
	newStats.ReadBytes = bytesWritten
	newStats.ReadKeys = keysWritten
	now := time.Now().Second()
	interval := &metapb.TimeInterval{Start: uint64(now - statistics.StoreHeartBeatReportInterval), End: uint64(now)}
	newStats.Interval = interval
	newStore := container.Clone(core.SetStoreStats(newStats))
	mc.Set(containerID, newStats)
	mc.PutStore(newStore)
}

// UpdateStorageWrittenBytes updates container written bytes.
func (mc *Cluster) UpdateStorageWrittenBytes(containerID uint64, bytesWritten uint64) {
	container := mc.GetStore(containerID)
	newStats := proto.Clone(container.GetStoreStats()).(*metapb.StoreStats)
	newStats.WrittenBytes = bytesWritten
	newStats.WrittenKeys = bytesWritten / 100
	now := time.Now().Second()
	interval := &metapb.TimeInterval{Start: uint64(now - statistics.StoreHeartBeatReportInterval), End: uint64(now)}
	newStats.Interval = interval
	newStore := container.Clone(core.SetStoreStats(newStats))
	mc.Set(containerID, newStats)
	mc.PutStore(newStore)
}

// UpdateStorageReadBytes updates container read bytes.
func (mc *Cluster) UpdateStorageReadBytes(containerID uint64, bytesRead uint64) {
	container := mc.GetStore(containerID)
	newStats := proto.Clone(container.GetStoreStats()).(*metapb.StoreStats)
	newStats.ReadBytes = bytesRead
	newStats.ReadKeys = bytesRead / 100
	now := time.Now().Second()
	interval := &metapb.TimeInterval{Start: uint64(now - statistics.StoreHeartBeatReportInterval), End: uint64(now)}
	newStats.Interval = interval
	newStore := container.Clone(core.SetStoreStats(newStats))
	mc.Set(containerID, newStats)
	mc.PutStore(newStore)
}

// UpdateStorageWrittenKeys updates container written keys.
func (mc *Cluster) UpdateStorageWrittenKeys(containerID uint64, keysWritten uint64) {
	container := mc.GetStore(containerID)
	newStats := proto.Clone(container.GetStoreStats()).(*metapb.StoreStats)
	newStats.WrittenKeys = keysWritten
	newStats.WrittenBytes = keysWritten * 100
	now := time.Now().Second()
	interval := &metapb.TimeInterval{Start: uint64(now - statistics.StoreHeartBeatReportInterval), End: uint64(now)}
	newStats.Interval = interval
	newStore := container.Clone(core.SetStoreStats(newStats))
	mc.Set(containerID, newStats)
	mc.PutStore(newStore)
}

// UpdateStorageReadKeys updates container read bytes.
func (mc *Cluster) UpdateStorageReadKeys(containerID uint64, keysRead uint64) {
	container := mc.GetStore(containerID)
	newStats := proto.Clone(container.GetStoreStats()).(*metapb.StoreStats)
	newStats.ReadKeys = keysRead
	newStats.ReadBytes = keysRead * 100
	now := time.Now().Second()
	interval := &metapb.TimeInterval{Start: uint64(now - statistics.StoreHeartBeatReportInterval), End: uint64(now)}
	newStats.Interval = interval
	newStore := container.Clone(core.SetStoreStats(newStats))
	mc.Set(containerID, newStats)
	mc.PutStore(newStore)
}

// UpdateStoreStatus updates container status.
func (mc *Cluster) UpdateStoreStatus(id uint64) {
	leaderCount := mc.Shards.GetStoreLeaderCount("", id)
	resourceCount := mc.Shards.GetStoreShardCount("", id)
	pendingPeerCount := mc.Shards.GetStorePendingPeerCount("", id)
	leaderSize := mc.Shards.GetStoreLeaderShardSize("", id)
	resourceSize := mc.Shards.GetStoreShardSize("", id)
	container := mc.Stores.GetStore(id)
	stats := &metapb.StoreStats{}
	stats.Capacity = defaultStoreCapacity
	stats.Available = stats.Capacity - uint64(container.GetShardSize("")*mb)
	stats.UsedSize = uint64(container.GetShardSize("") * mb)
	newStore := container.Clone(
		core.SetStoreStats(stats),
		core.SetLeaderCount("", leaderCount),
		core.SetShardCount("", resourceCount),
		core.SetPendingPeerCount("", pendingPeerCount),
		core.SetLeaderSize("", leaderSize),
		core.SetShardSize("", resourceSize),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutStore(newStore)
}

func (mc *Cluster) newMockCachedShard(resID uint64, leaderStoreID uint64, followerStoreIDs ...uint64) *core.CachedShard {
	return mc.MockCachedShard(resID, leaderStoreID, followerStoreIDs, []uint64{}, metapb.ShardEpoch{})
}

// CheckLabelProperty checks label property.
func (mc *Cluster) CheckLabelProperty(typ string, labels []metapb.Pair) bool {
	for _, cfg := range mc.GetLabelPropertyConfig()[typ] {
		for _, l := range labels {
			if l.Key == cfg.Key && l.Value == cfg.Value {
				return true
			}
		}
	}
	return false
}

// PutShardStores mocks method.
func (mc *Cluster) PutShardStores(id uint64, containerIDs ...uint64) {
	meta := &metapb.Shard{
		ID:    id,
		Start: []byte(strconv.FormatUint(id, 10)),
		End:   []byte(strconv.FormatUint(id+1, 10)),
	}
	for _, id := range containerIDs {
		meta.SetReplicas(append(meta.GetReplicas(), metapb.Replica{StoreID: id}))
	}
	mc.PutShard(core.NewCachedShard(meta, &metapb.Replica{StoreID: containerIDs[0]}))
}

// PutStoreWithLabels mocks method.
func (mc *Cluster) PutStoreWithLabels(id uint64, labelPairs ...string) {
	labels := make(map[string]string)
	for i := 0; i < len(labelPairs); i += 2 {
		labels[labelPairs[i]] = labelPairs[i+1]
	}
	mc.AddLabelsStore(id, 0, labels)
}

// RemoveScheduler mocks method.
func (mc *Cluster) RemoveScheduler(name string) error {
	return nil
}

// MockCachedShard returns a mock resource
// If leaderStoreID is zero, the resources would have no leader
func (mc *Cluster) MockCachedShard(resID uint64, leaderStoreID uint64,
	followerStoreIDs, learnerStoreIDs []uint64, epoch metapb.ShardEpoch) *core.CachedShard {

	res := &metapb.Shard{
		ID:    resID,
		Start: []byte(fmt.Sprintf("%20d", resID)),
		End:   []byte(fmt.Sprintf("%20d", resID+1)),
		Epoch: epoch,
	}

	var leader *metapb.Replica
	if leaderStoreID != 0 {
		peer, _ := mc.AllocPeer(leaderStoreID)
		leader = &peer
		res.SetReplicas(append(res.GetReplicas(), peer))
	}
	for _, containerID := range followerStoreIDs {
		peer, _ := mc.AllocPeer(containerID)
		res.SetReplicas(append(res.GetReplicas(), peer))
	}
	for _, containerID := range learnerStoreIDs {
		peer, _ := mc.AllocPeer(containerID)
		peer.Role = metapb.ReplicaRole_Learner
		res.SetReplicas(append(res.GetReplicas(), peer))
	}
	return core.NewCachedShard(res, leader)
}

// SetStoreLabel set the labels to the target container
func (mc *Cluster) SetStoreLabel(containerID uint64, labels map[string]string) {
	container := mc.GetStore(containerID)
	newLabels := make([]metapb.Pair, 0, len(labels))
	for k, v := range labels {
		newLabels = append(newLabels, metapb.Pair{Key: k, Value: v})
	}
	newStore := container.Clone(core.SetStoreLabels(newLabels))
	mc.PutStore(newStore)
}

// AddSuspectShards mock method
func (mc *Cluster) AddSuspectShards(ids ...uint64) {
	for _, id := range ids {
		mc.suspectShards[id] = struct{}{}
	}
}

// CheckShardUnderSuspect only used for unit test
func (mc *Cluster) CheckShardUnderSuspect(id uint64) bool {
	_, ok := mc.suspectShards[id]
	return ok
}

// ResetSuspectShards only used for unit test
func (mc *Cluster) ResetSuspectShards() {
	mc.suspectShards = map[uint64]struct{}{}
}

// GetShardByKey get resource by key
func (mc *Cluster) GetShardByKey(group uint64, resKey []byte) *core.CachedShard {
	return mc.SearchShard(group, resKey)
}

// SetStoreLastHeartbeatInterval set the last heartbeat to the target container
func (mc *Cluster) SetStoreLastHeartbeatInterval(containerID uint64, interval time.Duration) {
	container := mc.GetStore(containerID)
	newStore := container.Clone(core.SetLastHeartbeatTS(time.Now().Add(-interval)))
	mc.PutStore(newStore)
}
