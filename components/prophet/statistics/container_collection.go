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

type containerStatistics struct {
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

func newStoreStatistics(opt *config.PersistOptions) *containerStatistics {
	return &containerStatistics{
		opt:          opt,
		LabelCounter: make(map[string]int),
	}
}

func (s *containerStatistics) Observe(container *core.CachedStore, stats *StoresStats) {
	for _, k := range s.opt.GetLocationLabels() {
		v := container.GetLabelValue(k)
		if v == "" {
			v = unknown
		}
		key := fmt.Sprintf("%s:%s", k, v)
		// exclude tombstone
		if container.GetState() != metapb.StoreState_StoreTombstone {
			s.LabelCounter[key]++
		}
	}
	containerAddress := container.Meta.Addr()
	id := strconv.FormatUint(container.Meta.ID(), 10)
	// Store state.
	switch container.GetState() {
	case metapb.StoreState_UP:
		if container.DownTime() >= s.opt.GetMaxStoreDownTime() {
			s.Down++
		} else if container.IsUnhealthy() {
			s.Unhealthy++
		} else if container.IsDisconnected() {
			s.Disconnect++
		} else {
			s.Up++
		}
	case metapb.StoreState_Offline:
		s.Offline++
	case metapb.StoreState_StoreTombstone:
		s.Tombstone++
		s.resetStoreStatistics(containerAddress, id)
		return
	}
	if container.IsLowSpace(s.opt.GetLowSpaceRatio()) {
		s.LowSpace++
	}

	// Store stats.
	s.StorageSize += container.StorageSize()
	s.StorageCapacity += container.GetCapacity()

	var resSize, resCount, leaderSize, leaderCount float64
	s.ShardCount += container.GetTotalShardCount()
	s.LeaderCount += container.GetTotalLeaderCount()
	resSize += float64(container.GetTotalShardSize())
	resCount += float64(container.GetTotalShardCount())
	leaderSize += float64(container.GetTotalLeaderSize())
	leaderCount += float64(container.GetTotalLeaderCount())

	containerStatusGauge.WithLabelValues(containerAddress, id, "container_available").Set(float64(container.GetAvailable()))
	containerStatusGauge.WithLabelValues(containerAddress, id, "container_used").Set(float64(container.GetUsedSize()))
	containerStatusGauge.WithLabelValues(containerAddress, id, "container_capacity").Set(float64(container.GetCapacity()))
	containerStatusGauge.WithLabelValues(containerAddress, id, "container_available_avg").Set(float64(container.GetAvgAvailable()))
	containerStatusGauge.WithLabelValues(containerAddress, id, "container_available_deviation").Set(float64(container.GetAvailableDeviation()))
	containerStatusGauge.WithLabelValues(containerAddress, id, "resource_size").Set(resSize)
	containerStatusGauge.WithLabelValues(containerAddress, id, "resource_count").Set(resCount)
	containerStatusGauge.WithLabelValues(containerAddress, id, "leader_size").Set(leaderSize)
	containerStatusGauge.WithLabelValues(containerAddress, id, "leader_count").Set(leaderCount)

	// Store flows.
	containerFlowStats := stats.GetRollingStoreStats(container.Meta.ID())
	if containerFlowStats == nil {
		return
	}
	containerStatusGauge.WithLabelValues(containerAddress, id, "container_write_rate_bytes").Set(containerFlowStats.GetLoad(StoreWriteBytes))
	containerStatusGauge.WithLabelValues(containerAddress, id, "container_read_rate_bytes").Set(containerFlowStats.GetLoad(StoreReadBytes))
	containerStatusGauge.WithLabelValues(containerAddress, id, "container_write_rate_keys").Set(containerFlowStats.GetLoad(StoreWriteKeys))
	containerStatusGauge.WithLabelValues(containerAddress, id, "container_read_rate_keys").Set(containerFlowStats.GetLoad(StoreReadKeys))
	containerStatusGauge.WithLabelValues(containerAddress, id, "container_cpu_usage").Set(containerFlowStats.GetLoad(StoreCPUUsage))
	containerStatusGauge.WithLabelValues(containerAddress, id, "container_disk_read_rate").Set(containerFlowStats.GetLoad(StoreDiskReadRate))
	containerStatusGauge.WithLabelValues(containerAddress, id, "container_disk_write_rate").Set(containerFlowStats.GetLoad(StoreDiskWriteRate))

	containerStatusGauge.WithLabelValues(containerAddress, id, "container_write_rate_bytes_instant").Set(containerFlowStats.GetInstantLoad(StoreWriteBytes))
	containerStatusGauge.WithLabelValues(containerAddress, id, "container_read_rate_bytes_instant").Set(containerFlowStats.GetInstantLoad(StoreReadBytes))
	containerStatusGauge.WithLabelValues(containerAddress, id, "container_write_rate_keys_instant").Set(containerFlowStats.GetInstantLoad(StoreWriteKeys))
	containerStatusGauge.WithLabelValues(containerAddress, id, "container_read_rate_keys_instant").Set(containerFlowStats.GetInstantLoad(StoreReadKeys))
}

func (s *containerStatistics) Collect() {
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

func (s *containerStatistics) resetStoreStatistics(containerAddress string, id string) {
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
		containerStatusGauge.DeleteLabelValues(containerAddress, id, m)
	}
}

type containerStatisticsMap struct {
	opt   *config.PersistOptions
	stats *containerStatistics
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
	containerStatusGauge.Reset()
	clusterStatusGauge.Reset()
	placementStatusGauge.Reset()
}
