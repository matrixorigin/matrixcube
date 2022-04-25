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

package statistics

import (
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

const (
	unknown   = "unknown"
	labelType = "label"
)

type storeStatistics struct {
	opt             *config.PersistOptions
	Up              int
	Disconnect      int
	Unhealthy       int
	Down            int
	Offline         int
	Tombstone       int
	LowSpace        int
	StorageSize     uint64
	StorageCapacity uint64
	ShardCount      int
	LeaderCount     int
	LabelCounter    map[string]int
}

func newStoreStatistics(opt *config.PersistOptions) *storeStatistics {
	return &storeStatistics{
		opt:          opt,
		LabelCounter: make(map[string]int),
	}
}

func (s *storeStatistics) Observe(store *core.CachedStore, stats *StoresStats) {
	for _, k := range s.opt.GetLocationLabels() {
		v := store.GetLabelValue(k)
		if v == "" {
			v = unknown
		}
		key := fmt.Sprintf("%s:%s", k, v)
		// exclude tombstone
		if store.GetState() != metapb.StoreState_StoreTombstone {
			s.LabelCounter[key]++
		}
	}
	storeAddress := store.Meta.GetClientAddress()
	id := strconv.FormatUint(store.Meta.GetID(), 10)
	// Store state.
	switch store.GetState() {
	case metapb.StoreState_Up:
		if store.DownTime() >= s.opt.GetMaxStoreDownTime() {
			s.Down++
		} else if store.IsUnhealthy() {
			s.Unhealthy++
		} else if store.IsDisconnected() {
			s.Disconnect++
		} else {
			s.Up++
		}
	case metapb.StoreState_Down:
		s.Offline++
	case metapb.StoreState_StoreTombstone:
		s.Tombstone++
		s.resetStoreStatistics(storeAddress, id)
		return
	}
	if store.IsLowSpace(s.opt.GetLowSpaceRatio()) {
		s.LowSpace++
	}

	// Store stats.
	s.StorageSize += store.StorageSize()
	s.StorageCapacity += store.GetCapacity()

	var resSize, resCount, leaderSize, leaderCount float64
	s.ShardCount += store.GetTotalShardCount()
	s.LeaderCount += store.GetTotalLeaderCount()
	resSize += float64(store.GetTotalShardSize())
	resCount += float64(store.GetTotalShardCount())
	leaderSize += float64(store.GetTotalLeaderSize())
	leaderCount += float64(store.GetTotalLeaderCount())

	storeStatusGauge.WithLabelValues(storeAddress, id, "container_available").Set(float64(store.GetAvailable()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "container_used").Set(float64(store.GetUsedSize()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "container_capacity").Set(float64(store.GetCapacity()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "container_available_avg").Set(float64(store.GetAvgAvailable()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "container_available_deviation").Set(float64(store.GetAvailableDeviation()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "resource_size").Set(resSize)
	storeStatusGauge.WithLabelValues(storeAddress, id, "resource_count").Set(resCount)
	storeStatusGauge.WithLabelValues(storeAddress, id, "leader_size").Set(leaderSize)
	storeStatusGauge.WithLabelValues(storeAddress, id, "leader_count").Set(leaderCount)

	// Store flows.
	containerFlowStats := stats.GetRollingStoreStats(store.Meta.GetID())
	if containerFlowStats == nil {
		return
	}
	storeStatusGauge.WithLabelValues(storeAddress, id, "container_write_rate_bytes").Set(containerFlowStats.GetLoad(StoreWriteBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "container_read_rate_bytes").Set(containerFlowStats.GetLoad(StoreReadBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "container_write_rate_keys").Set(containerFlowStats.GetLoad(StoreWriteKeys))
	storeStatusGauge.WithLabelValues(storeAddress, id, "container_read_rate_keys").Set(containerFlowStats.GetLoad(StoreReadKeys))
	storeStatusGauge.WithLabelValues(storeAddress, id, "container_cpu_usage").Set(containerFlowStats.GetLoad(StoreCPUUsage))
	storeStatusGauge.WithLabelValues(storeAddress, id, "container_disk_read_rate").Set(containerFlowStats.GetLoad(StoreDiskReadRate))
	storeStatusGauge.WithLabelValues(storeAddress, id, "container_disk_write_rate").Set(containerFlowStats.GetLoad(StoreDiskWriteRate))

	storeStatusGauge.WithLabelValues(storeAddress, id, "container_write_rate_bytes_instant").Set(containerFlowStats.GetInstantLoad(StoreWriteBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "container_read_rate_bytes_instant").Set(containerFlowStats.GetInstantLoad(StoreReadBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "container_write_rate_keys_instant").Set(containerFlowStats.GetInstantLoad(StoreWriteKeys))
	storeStatusGauge.WithLabelValues(storeAddress, id, "container_read_rate_keys_instant").Set(containerFlowStats.GetInstantLoad(StoreReadKeys))
}

func (s *storeStatistics) Collect() {
	metrics := make(map[string]float64)
	metrics["container_up_count"] = float64(s.Up)
	metrics["container_disconnected_count"] = float64(s.Disconnect)
	metrics["container_down_count"] = float64(s.Down)
	metrics["container_unhealth_count"] = float64(s.Unhealthy)
	metrics["container_offline_count"] = float64(s.Offline)
	metrics["container_tombstone_count"] = float64(s.Tombstone)
	metrics["container_low_space_count"] = float64(s.LowSpace)
	metrics["resource_count"] = float64(s.ShardCount)
	metrics["leader_count"] = float64(s.LeaderCount)
	metrics["storage_size"] = float64(s.StorageSize)
	metrics["storage_capacity"] = float64(s.StorageCapacity)

	for typ, value := range metrics {
		clusterStatusGauge.WithLabelValues(typ).Set(value)
	}

	// Current scheduling configurations of the cluster
	configs := make(map[string]float64)
	configs["leader-schedule-limit"] = float64(s.opt.GetLeaderScheduleLimit())
	configs["resource-schedule-limit"] = float64(s.opt.GetShardScheduleLimit())
	configs["merge-schedule-limit"] = float64(s.opt.GetMergeScheduleLimit())
	configs["replica-schedule-limit"] = float64(s.opt.GetReplicaScheduleLimit())
	configs["max-replicas"] = float64(s.opt.GetMaxReplicas())
	configs["high-space-ratio"] = s.opt.GetHighSpaceRatio()
	configs["low-space-ratio"] = s.opt.GetLowSpaceRatio()
	configs["tolerant-size-ratio"] = s.opt.GetTolerantSizeRatio()
	configs["hot-resource-schedule-limit"] = float64(s.opt.GetHotShardScheduleLimit())
	configs["hot-resource-cache-hits-threshold"] = float64(s.opt.GetHotShardCacheHitsThreshold())
	configs["max-pending-peer-count"] = float64(s.opt.GetMaxPendingPeerCount())
	configs["max-snapshot-count"] = float64(s.opt.GetMaxSnapshotCount())
	configs["max-merge-resource-size"] = float64(s.opt.GetMaxMergeShardSize())
	configs["max-merge-resource-keys"] = float64(s.opt.GetMaxMergeShardKeys())

	var enableMakeUpReplica, enableRemoveDownReplica, enableRemoveExtraReplica, enableReplaceOfflineReplica float64
	if s.opt.IsMakeUpReplicaEnabled() {
		enableMakeUpReplica = 1
	}
	if s.opt.IsRemoveDownReplicaEnabled() {
		enableRemoveDownReplica = 1
	}
	if s.opt.IsRemoveExtraReplicaEnabled() {
		enableRemoveExtraReplica = 1
	}
	if s.opt.IsReplaceOfflineReplicaEnabled() {
		enableReplaceOfflineReplica = 1
	}

	configs["enable-makeup-replica"] = enableMakeUpReplica
	configs["enable-remove-down-replica"] = enableRemoveDownReplica
	configs["enable-remove-extra-replica"] = enableRemoveExtraReplica
	configs["enable-replace-offline-replica"] = enableReplaceOfflineReplica

	for typ, value := range configs {
		configStatusGauge.WithLabelValues(typ).Set(value)
	}

	for name, value := range s.LabelCounter {
		placementStatusGauge.WithLabelValues(labelType, name).Set(float64(value))
	}
}

func (s *storeStatistics) resetStoreStatistics(containerAddress string, id string) {
	metrics := []string{
		"resource_score",
		"leader_score",
		"resource_size",
		"resource_count",
		"leader_size",
		"leader_count",
		"container_available",
		"container_used",
		"container_capacity",
		"container_write_rate_bytes",
		"container_read_rate_bytes",
		"container_write_rate_keys",
		"container_read_rate_keys",
	}
	for _, m := range metrics {
		storeStatusGauge.DeleteLabelValues(containerAddress, id, m)
	}
}

type containerStatisticsMap struct {
	opt   *config.PersistOptions
	stats *storeStatistics
}

// NewStoreStatisticsMap create a container statistics map
func NewStoreStatisticsMap(opt *config.PersistOptions) *containerStatisticsMap {
	return &containerStatisticsMap{
		opt:   opt,
		stats: newStoreStatistics(opt),
	}
}

func (m *containerStatisticsMap) Observe(container *core.CachedStore, stats *StoresStats) {
	m.stats.Observe(container, stats)
}

func (m *containerStatisticsMap) Collect() {
	m.stats.Collect()
}

func (m *containerStatisticsMap) Reset() {
	storeStatusGauge.Reset()
	clusterStatusGauge.Reset()
	placementStatusGauge.Reset()
}
