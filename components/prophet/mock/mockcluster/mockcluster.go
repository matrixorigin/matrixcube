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
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/statistics"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"go.uber.org/zap"
)

const (
	defaultContainerCapacity = 100 * (1 << 30) // 100GiB
	defaultResourceSize      = 96 * (1 << 20)  // 96MiB
	mb                       = (1 << 20)       // 1MiB
)

// Cluster is used to mock clusterInfo for test use.
type Cluster struct {
	*core.BasicCluster
	*placement.RuleManager
	*statistics.HotStat
	*config.PersistOptions
	storage          storage.Storage
	ID               uint64
	suspectResources map[uint64]struct{}

	supportJointConsensus bool
}

// NewCluster creates a new Cluster
func NewCluster(opts *config.PersistOptions) *Cluster {
	clus := &Cluster{
		storage:               storage.NewTestStorage(),
		BasicCluster:          core.NewBasicCluster(func() metadata.Resource { return &metadata.TestResource{} }, nil),
		HotStat:               statistics.NewHotStat(),
		PersistOptions:        opts,
		suspectResources:      map[uint64]struct{}{},
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

// GetResourceFactory returns a metdata resource create factory
func (mc *Cluster) GetResourceFactory() func() metadata.Resource {
	return func() metadata.Resource {
		return &metadata.TestResource{}
	}
}

// GetOpts returns the cluster configuration.
func (mc *Cluster) GetOpts() *config.PersistOptions {
	return mc.PersistOptions
}

// AllocID allocs a new unique ID.
func (mc *Cluster) AllocID() (uint64, error) {
	return mc.storage.KV().AllocID()
}

// ScanResources scans resource with start key, until number greater than limit.
func (mc *Cluster) ScanResources(group uint64, startKey, endKey []byte, limit int) []*core.CachedResource {
	return mc.Resources.ScanRange(group, startKey, endKey, limit)
}

// LoadResource puts resource info without leader
func (mc *Cluster) LoadResource(resID uint64, followerIds ...uint64) {
	//  resources load from etcd will have no leader
	r := mc.newMockCachedResource(resID, 0, followerIds...).Clone(core.WithLeader(nil))
	mc.PutResource(r)
}

// GetContainersLoads gets stores load statistics.
func (mc *Cluster) GetContainersLoads() map[uint64][]float64 {
	return mc.HotStat.GetContainersLoads()
}

// GetContainerResourceCount gets resource count with a given container.
func (mc *Cluster) GetContainerResourceCount(groupKey string, containerID uint64) int {
	return mc.Resources.GetContainerResourceCount(groupKey, containerID)
}

// GetContainer gets a container with a given container ID.
func (mc *Cluster) GetContainer(containerID uint64) *core.CachedContainer {
	return mc.Containers.GetContainer(containerID)
}

// IsResourceHot checks if the resource is hot.
func (mc *Cluster) IsResourceHot(res *core.CachedResource) bool {
	return mc.HotCache.IsResourceHot(res, mc.GetHotResourceCacheHitsThreshold())
}

// ResourceReadStats returns hot Resource's read stats.
// The result only includes peers that are hot enough.
func (mc *Cluster) ResourceReadStats() map[uint64][]*statistics.HotPeerStat {
	return mc.HotCache.ResourceStats(statistics.ReadFlow, mc.GetHotResourceCacheHitsThreshold())
}

// ResourceWriteStats returns hot Resource's write stats.
// The result only includes peers that are hot enough.
func (mc *Cluster) ResourceWriteStats() map[uint64][]*statistics.HotPeerStat {
	return mc.HotCache.ResourceStats(statistics.WriteFlow, mc.GetHotResourceCacheHitsThreshold())
}

// RandHotResourceFromContainer random picks a hot resource in specify container.
func (mc *Cluster) RandHotResourceFromContainer(containerID uint64, kind statistics.FlowKind) *core.CachedResource {
	r := mc.HotCache.RandHotResourceFromContainer(containerID, kind, mc.GetHotResourceCacheHitsThreshold())
	if r == nil {
		return nil
	}
	return mc.GetResource(r.ResourceID)
}

// AllocPeer allocs a new peer on a container.
func (mc *Cluster) AllocPeer(containerID uint64) (metapb.Replica, error) {
	peerID, err := mc.AllocID()
	if err != nil {
		return metapb.Replica{}, err
	}

	return metapb.Replica{
		ID:          peerID,
		ContainerID: containerID,
	}, nil
}

func (mc *Cluster) initRuleManager() {
	if mc.RuleManager == nil {
		mc.RuleManager = placement.NewRuleManager(mc.storage, mc, nil)
		mc.RuleManager.Initialize(int(mc.GetReplicationConfig().MaxReplicas), mc.GetReplicationConfig().LocationLabels)
	}
}

// FitResource fits a resource to the rules it matches.
func (mc *Cluster) FitResource(res *core.CachedResource) *placement.ResourceFit {
	return mc.RuleManager.FitResource(mc.BasicCluster, res)
}

// GetRuleManager returns the ruleManager of the cluster.
func (mc *Cluster) GetRuleManager() *placement.RuleManager {
	return mc.RuleManager
}

// SetContainerUP sets container state to be up.
func (mc *Cluster) SetContainerUP(containerID uint64) {
	container := mc.GetContainer(containerID)
	newContainer := container.Clone(
		core.UpContainer(),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutContainer(newContainer)
}

// SetContainerDisconnect changes a container's state to disconnected.
func (mc *Cluster) SetContainerDisconnect(containerID uint64) {
	container := mc.GetContainer(containerID)
	newContainer := container.Clone(
		core.UpContainer(),
		core.SetLastHeartbeatTS(time.Now().Add(-time.Second*30)),
	)
	mc.PutContainer(newContainer)
}

// SetContainerDown sets container down.
func (mc *Cluster) SetContainerDown(containerID uint64) {
	container := mc.GetContainer(containerID)
	newContainer := container.Clone(
		core.UpContainer(),
		core.SetLastHeartbeatTS(time.Time{}),
	)
	mc.PutContainer(newContainer)
}

// SetContainerOffline sets container state to be offline.
func (mc *Cluster) SetContainerOffline(containerID uint64) {
	container := mc.GetContainer(containerID)
	newContainer := container.Clone(core.OfflineContainer(false))
	mc.PutContainer(newContainer)
}

// SetContainerBusy sets container busy.
func (mc *Cluster) SetContainerBusy(containerID uint64, busy bool) {
	container := mc.GetContainer(containerID)
	newStats := proto.Clone(container.GetContainerStats()).(*metapb.ContainerStats)
	newStats.IsBusy = busy
	newContainer := container.Clone(
		core.SetContainerStats(newStats),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutContainer(newContainer)
}

// AddLeaderContainer adds container with specified count of leader.
func (mc *Cluster) AddLeaderContainer(containerID uint64, leaderCount int, leaderSizes ...int64) {
	stats := &metapb.ContainerStats{}
	stats.Capacity = defaultContainerCapacity
	stats.UsedSize = uint64(leaderCount) * defaultResourceSize
	stats.Available = stats.Capacity - uint64(leaderCount)*defaultResourceSize
	var leaderSize int64
	if len(leaderSizes) != 0 {
		leaderSize = leaderSizes[0]
	} else {
		leaderSize = int64(leaderCount) * defaultResourceSize / mb
	}

	container := core.NewCachedContainer(
		metadata.NewTestContainer(containerID),
		core.SetContainerStats(stats),
		core.SetLeaderCount("", leaderCount),
		core.SetLeaderSize("", leaderSize),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.SetContainerLimit(containerID, limit.AddPeer, 60)
	mc.SetContainerLimit(containerID, limit.RemovePeer, 60)
	mc.PutContainer(container)
}

// AddResourceContainer adds container with specified count of resource.
func (mc *Cluster) AddResourceContainer(containerID uint64, resourceCount int) {
	stats := &metapb.ContainerStats{}
	stats.Capacity = defaultContainerCapacity
	stats.UsedSize = uint64(resourceCount) * defaultResourceSize
	stats.Available = stats.Capacity - uint64(resourceCount)*defaultResourceSize
	container := core.NewCachedContainer(
		&metadata.TestContainer{CID: containerID, CLabels: []metapb.Pair{
			{
				Key:   "ID",
				Value: fmt.Sprintf("%v", containerID),
			},
		}},
		core.SetContainerStats(stats),
		core.SetResourceCount("", resourceCount),
		core.SetResourceSize("", int64(resourceCount)*defaultResourceSize/mb),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.SetContainerLimit(containerID, limit.AddPeer, 60)
	mc.SetContainerLimit(containerID, limit.RemovePeer, 60)
	mc.PutContainer(container)
}

// AddResourceContainerWithLeader adds container with specified count of resource and leader.
func (mc *Cluster) AddResourceContainerWithLeader(containerID uint64, resourceCount int, leaderCounts ...int) {
	leaderCount := resourceCount
	if len(leaderCounts) != 0 {
		leaderCount = leaderCounts[0]
	}
	mc.AddResourceContainer(containerID, resourceCount)
	for i := 0; i < leaderCount; i++ {
		id, _ := mc.AllocID()
		mc.AddLeaderResource(id, containerID)
	}
}

// AddLabelsContainer adds container with specified count of resource and labels.
func (mc *Cluster) AddLabelsContainer(containerID uint64, resourceCount int, labels map[string]string) {
	newLabels := make([]metapb.Pair, 0, len(labels))
	for k, v := range labels {
		newLabels = append(newLabels, metapb.Pair{Key: k, Value: v})
	}
	stats := &metapb.ContainerStats{}
	stats.Capacity = defaultContainerCapacity
	stats.Available = stats.Capacity - uint64(resourceCount)*defaultResourceSize
	stats.UsedSize = uint64(resourceCount) * defaultResourceSize
	container := core.NewCachedContainer(
		&metadata.TestContainer{
			CID:     containerID,
			CLabels: newLabels,
		},
		core.SetContainerStats(stats),
		core.SetResourceCount("", resourceCount),
		core.SetResourceSize("", int64(resourceCount)*defaultResourceSize/mb),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.SetContainerLimit(containerID, limit.AddPeer, 60)
	mc.SetContainerLimit(containerID, limit.RemovePeer, 60)
	mc.PutContainer(container)
}

// AddLeaderResource adds resource with specified leader and followers.
func (mc *Cluster) AddLeaderResource(resID uint64, leaderContainerID uint64, followerContainerIDs ...uint64) *core.CachedResource {
	origin := mc.newMockCachedResource(resID, leaderContainerID, followerContainerIDs...)
	res := origin.Clone(core.SetApproximateSize(defaultResourceSize/mb), core.SetApproximateKeys(10))
	mc.PutResource(res)
	return res
}

// AddResourceWithLearner adds resource with specified leader, followers and learners.
func (mc *Cluster) AddResourceWithLearner(resID uint64, leaderContainerID uint64, followerContainerIDs, learnerContainerIDs []uint64) *core.CachedResource {
	origin := mc.MockCachedResource(resID, leaderContainerID, followerContainerIDs, learnerContainerIDs, metapb.ResourceEpoch{})
	res := origin.Clone(core.SetApproximateSize(defaultResourceSize/mb), core.SetApproximateKeys(10))
	mc.PutResource(res)
	return res
}

// AddLeaderResourceWithRange adds resource with specified leader, followers and key range.
func (mc *Cluster) AddLeaderResourceWithRange(resID uint64, startKey string, endKey string, leaderID uint64, followerIds ...uint64) {
	o := mc.newMockCachedResource(resID, leaderID, followerIds...)
	r := o.Clone(
		core.WithStartKey([]byte(startKey)),
		core.WithEndKey([]byte(endKey)),
	)
	mc.PutResource(r)
}

// AddLeaderResourceWithReadInfo adds resource with specified leader, followers and read info.
func (mc *Cluster) AddLeaderResourceWithReadInfo(
	resID uint64, leaderID uint64,
	readBytes, readKeys uint64,
	reportInterval uint64,
	followerIds []uint64, filledNums ...int) []*statistics.HotPeerStat {
	r := mc.newMockCachedResource(resID, leaderID, followerIds...)
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
	mc.PutResource(r)
	return items
}

// AddLeaderResourceWithWriteInfo adds resource with specified leader, followers and write info.
func (mc *Cluster) AddLeaderResourceWithWriteInfo(
	resID uint64, leaderID uint64,
	writtenBytes, writtenKeys uint64,
	reportInterval uint64,
	followerIds []uint64, filledNums ...int) []*statistics.HotPeerStat {
	r := mc.newMockCachedResource(resID, leaderID, followerIds...)
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
	mc.PutResource(r)
	return items
}

// UpdateContainerLeaderWeight updates container leader weight.
func (mc *Cluster) UpdateContainerLeaderWeight(containerID uint64, weight float64) {
	container := mc.GetContainer(containerID)
	newContainer := container.Clone(core.SetLeaderWeight(weight))
	mc.PutContainer(newContainer)
}

// UpdateContainerResourceWeight updates container resource weight.
func (mc *Cluster) UpdateContainerResourceWeight(containerID uint64, weight float64) {
	container := mc.GetContainer(containerID)
	newContainer := container.Clone(core.SetResourceWeight(weight))
	mc.PutContainer(newContainer)
}

// UpdateContainerLeaderSize updates container leader size.
func (mc *Cluster) UpdateContainerLeaderSize(containerID uint64, size int64) {
	container := mc.GetContainer(containerID)
	newStats := proto.Clone(container.GetContainerStats()).(*metapb.ContainerStats)
	newStats.Available = newStats.Capacity - uint64(container.GetLeaderSize(""))
	newContainer := container.Clone(
		core.SetContainerStats(newStats),
		core.SetLeaderSize("", size),
	)
	mc.PutContainer(newContainer)
}

// UpdateContainerResourceSize updates container resource size.
func (mc *Cluster) UpdateContainerResourceSize(containerID uint64, size int64) {
	container := mc.GetContainer(containerID)
	newStats := proto.Clone(container.GetContainerStats()).(*metapb.ContainerStats)
	newStats.Available = newStats.Capacity - uint64(container.GetResourceSize(""))
	newContainer := container.Clone(
		core.SetContainerStats(newStats),
		core.SetResourceSize("", size),
	)
	mc.PutContainer(newContainer)
}

// UpdateLeaderCount updates container leader count.
func (mc *Cluster) UpdateLeaderCount(containerID uint64, leaderCount int) {
	container := mc.GetContainer(containerID)
	newContainer := container.Clone(
		core.SetLeaderCount("", leaderCount),
		core.SetLeaderSize("", int64(leaderCount)*defaultResourceSize/mb),
	)
	mc.PutContainer(newContainer)
}

// UpdateResourceCount updates container resource count.
func (mc *Cluster) UpdateResourceCount(containerID uint64, resourceCount int) {
	container := mc.GetContainer(containerID)
	newContainer := container.Clone(
		core.SetResourceCount("", resourceCount),
		core.SetResourceSize("", int64(resourceCount)*defaultResourceSize/mb),
	)
	mc.PutContainer(newContainer)
}

// UpdateSnapshotCount updates container snapshot count.
func (mc *Cluster) UpdateSnapshotCount(containerID uint64, snapshotCount int) {
	container := mc.GetContainer(containerID)
	newStats := proto.Clone(container.GetContainerStats()).(*metapb.ContainerStats)
	newStats.ApplyingSnapCount = uint64(snapshotCount)
	newContainer := container.Clone(core.SetContainerStats(newStats))
	mc.PutContainer(newContainer)
}

// UpdatePendingPeerCount updates container pending peer count.
func (mc *Cluster) UpdatePendingPeerCount(containerID uint64, pendingPeerCount int) {
	container := mc.GetContainer(containerID)
	newContainer := container.Clone(core.SetPendingPeerCount("", pendingPeerCount))
	mc.PutContainer(newContainer)
}

// UpdateStorageRatio updates container storage ratio count.
func (mc *Cluster) UpdateStorageRatio(containerID uint64, usedRatio, availableRatio float64) {
	container := mc.GetContainer(containerID)
	newStats := proto.Clone(container.GetContainerStats()).(*metapb.ContainerStats)
	newStats.Capacity = defaultContainerCapacity
	newStats.UsedSize = uint64(float64(newStats.Capacity) * usedRatio)
	newStats.Available = uint64(float64(newStats.Capacity) * availableRatio)
	newContainer := container.Clone(core.SetContainerStats(newStats))
	mc.PutContainer(newContainer)
}

// UpdateStorageWrittenStats updates container written bytes.
func (mc *Cluster) UpdateStorageWrittenStats(containerID, bytesWritten, keysWritten uint64) {
	container := mc.GetContainer(containerID)
	newStats := proto.Clone(container.GetContainerStats()).(*metapb.ContainerStats)
	newStats.WrittenBytes = bytesWritten
	newStats.WrittenKeys = keysWritten
	now := time.Now().Second()
	interval := &metapb.TimeInterval{Start: uint64(now - statistics.ContainerHeartBeatReportInterval), End: uint64(now)}
	newStats.Interval = interval
	newContainer := container.Clone(core.SetContainerStats(newStats))
	mc.Set(containerID, newStats)
	mc.PutContainer(newContainer)
}

// UpdateStorageReadStats updates container written bytes.
func (mc *Cluster) UpdateStorageReadStats(containerID, bytesWritten, keysWritten uint64) {
	container := mc.GetContainer(containerID)
	newStats := proto.Clone(container.GetContainerStats()).(*metapb.ContainerStats)
	newStats.ReadBytes = bytesWritten
	newStats.ReadKeys = keysWritten
	now := time.Now().Second()
	interval := &metapb.TimeInterval{Start: uint64(now - statistics.ContainerHeartBeatReportInterval), End: uint64(now)}
	newStats.Interval = interval
	newContainer := container.Clone(core.SetContainerStats(newStats))
	mc.Set(containerID, newStats)
	mc.PutContainer(newContainer)
}

// UpdateStorageWrittenBytes updates container written bytes.
func (mc *Cluster) UpdateStorageWrittenBytes(containerID uint64, bytesWritten uint64) {
	container := mc.GetContainer(containerID)
	newStats := proto.Clone(container.GetContainerStats()).(*metapb.ContainerStats)
	newStats.WrittenBytes = bytesWritten
	newStats.WrittenKeys = bytesWritten / 100
	now := time.Now().Second()
	interval := &metapb.TimeInterval{Start: uint64(now - statistics.ContainerHeartBeatReportInterval), End: uint64(now)}
	newStats.Interval = interval
	newContainer := container.Clone(core.SetContainerStats(newStats))
	mc.Set(containerID, newStats)
	mc.PutContainer(newContainer)
}

// UpdateStorageReadBytes updates container read bytes.
func (mc *Cluster) UpdateStorageReadBytes(containerID uint64, bytesRead uint64) {
	container := mc.GetContainer(containerID)
	newStats := proto.Clone(container.GetContainerStats()).(*metapb.ContainerStats)
	newStats.ReadBytes = bytesRead
	newStats.ReadKeys = bytesRead / 100
	now := time.Now().Second()
	interval := &metapb.TimeInterval{Start: uint64(now - statistics.ContainerHeartBeatReportInterval), End: uint64(now)}
	newStats.Interval = interval
	newContainer := container.Clone(core.SetContainerStats(newStats))
	mc.Set(containerID, newStats)
	mc.PutContainer(newContainer)
}

// UpdateStorageWrittenKeys updates container written keys.
func (mc *Cluster) UpdateStorageWrittenKeys(containerID uint64, keysWritten uint64) {
	container := mc.GetContainer(containerID)
	newStats := proto.Clone(container.GetContainerStats()).(*metapb.ContainerStats)
	newStats.WrittenKeys = keysWritten
	newStats.WrittenBytes = keysWritten * 100
	now := time.Now().Second()
	interval := &metapb.TimeInterval{Start: uint64(now - statistics.ContainerHeartBeatReportInterval), End: uint64(now)}
	newStats.Interval = interval
	newContainer := container.Clone(core.SetContainerStats(newStats))
	mc.Set(containerID, newStats)
	mc.PutContainer(newContainer)
}

// UpdateStorageReadKeys updates container read bytes.
func (mc *Cluster) UpdateStorageReadKeys(containerID uint64, keysRead uint64) {
	container := mc.GetContainer(containerID)
	newStats := proto.Clone(container.GetContainerStats()).(*metapb.ContainerStats)
	newStats.ReadKeys = keysRead
	newStats.ReadBytes = keysRead * 100
	now := time.Now().Second()
	interval := &metapb.TimeInterval{Start: uint64(now - statistics.ContainerHeartBeatReportInterval), End: uint64(now)}
	newStats.Interval = interval
	newContainer := container.Clone(core.SetContainerStats(newStats))
	mc.Set(containerID, newStats)
	mc.PutContainer(newContainer)
}

// UpdateContainerStatus updates container status.
func (mc *Cluster) UpdateContainerStatus(id uint64) {
	leaderCount := mc.Resources.GetContainerLeaderCount("", id)
	resourceCount := mc.Resources.GetContainerResourceCount("", id)
	pendingPeerCount := mc.Resources.GetContainerPendingPeerCount("", id)
	leaderSize := mc.Resources.GetContainerLeaderResourceSize("", id)
	resourceSize := mc.Resources.GetContainerResourceSize("", id)
	container := mc.Containers.GetContainer(id)
	stats := &metapb.ContainerStats{}
	stats.Capacity = defaultContainerCapacity
	stats.Available = stats.Capacity - uint64(container.GetResourceSize("")*mb)
	stats.UsedSize = uint64(container.GetResourceSize("") * mb)
	newContainer := container.Clone(
		core.SetContainerStats(stats),
		core.SetLeaderCount("", leaderCount),
		core.SetResourceCount("", resourceCount),
		core.SetPendingPeerCount("", pendingPeerCount),
		core.SetLeaderSize("", leaderSize),
		core.SetResourceSize("", resourceSize),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutContainer(newContainer)
}

func (mc *Cluster) newMockCachedResource(resID uint64, leaderContainerID uint64, followerContainerIDs ...uint64) *core.CachedResource {
	return mc.MockCachedResource(resID, leaderContainerID, followerContainerIDs, []uint64{}, metapb.ResourceEpoch{})
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

// PutResourceContainers mocks method.
func (mc *Cluster) PutResourceContainers(id uint64, containerIDs ...uint64) {
	meta := &metadata.TestResource{
		ResID: id,
		Start: []byte(strconv.FormatUint(id, 10)),
		End:   []byte(strconv.FormatUint(id+1, 10)),
	}
	for _, id := range containerIDs {
		meta.ResPeers = append(meta.ResPeers, metapb.Replica{ContainerID: id})
	}
	mc.PutResource(core.NewCachedResource(meta, &metapb.Replica{ContainerID: containerIDs[0]}))
}

// PutContainerWithLabels mocks method.
func (mc *Cluster) PutContainerWithLabels(id uint64, labelPairs ...string) {
	labels := make(map[string]string)
	for i := 0; i < len(labelPairs); i += 2 {
		labels[labelPairs[i]] = labelPairs[i+1]
	}
	mc.AddLabelsContainer(id, 0, labels)
}

// RemoveScheduler mocks method.
func (mc *Cluster) RemoveScheduler(name string) error {
	return nil
}

// MockCachedResource returns a mock resource
// If leaderContainerID is zero, the resources would have no leader
func (mc *Cluster) MockCachedResource(resID uint64, leaderContainerID uint64,
	followerContainerIDs, learnerContainerIDs []uint64, epoch metapb.ResourceEpoch) *core.CachedResource {

	res := &metadata.TestResource{
		ResID:    resID,
		Start:    []byte(fmt.Sprintf("%20d", resID)),
		End:      []byte(fmt.Sprintf("%20d", resID+1)),
		ResEpoch: epoch,
	}
	var leader *metapb.Replica
	if leaderContainerID != 0 {
		peer, _ := mc.AllocPeer(leaderContainerID)
		leader = &peer
		res.ResPeers = append(res.ResPeers, peer)
	}
	for _, containerID := range followerContainerIDs {
		peer, _ := mc.AllocPeer(containerID)
		res.ResPeers = append(res.ResPeers, peer)
	}
	for _, containerID := range learnerContainerIDs {
		peer, _ := mc.AllocPeer(containerID)
		peer.Role = metapb.ReplicaRole_Learner
		res.ResPeers = append(res.ResPeers, peer)
	}
	return core.NewCachedResource(res, leader)
}

// SetContainerLabel set the labels to the target container
func (mc *Cluster) SetContainerLabel(containerID uint64, labels map[string]string) {
	container := mc.GetContainer(containerID)
	newLabels := make([]metapb.Pair, 0, len(labels))
	for k, v := range labels {
		newLabels = append(newLabels, metapb.Pair{Key: k, Value: v})
	}
	newContainer := container.Clone(core.SetContainerLabels(newLabels))
	mc.PutContainer(newContainer)
}

// AddSuspectResources mock method
func (mc *Cluster) AddSuspectResources(ids ...uint64) {
	for _, id := range ids {
		mc.suspectResources[id] = struct{}{}
	}
}

// CheckResourceUnderSuspect only used for unit test
func (mc *Cluster) CheckResourceUnderSuspect(id uint64) bool {
	_, ok := mc.suspectResources[id]
	return ok
}

// ResetSuspectResources only used for unit test
func (mc *Cluster) ResetSuspectResources() {
	mc.suspectResources = map[uint64]struct{}{}
}

// GetResourceByKey get resource by key
func (mc *Cluster) GetResourceByKey(group uint64, resKey []byte) *core.CachedResource {
	return mc.SearchResource(group, resKey)
}

// SetContainerLastHeartbeatInterval set the last heartbeat to the target container
func (mc *Cluster) SetContainerLastHeartbeatInterval(containerID uint64, interval time.Duration) {
	container := mc.GetContainer(containerID)
	newContainer := container.Clone(core.SetLastHeartbeatTS(time.Now().Add(-interval)))
	mc.PutContainer(newContainer)
}
