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
	"sync"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/util/movingaverage"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

// StoresStats is a cache hold hot resources.
type StoresStats struct {
	sync.RWMutex
	rollingStoresStats map[uint64]*RollingStoreStats
	totalLoads         []float64
}

// NewStoresStats creates a new hot spot cache.
func NewStoresStats() *StoresStats {
	return &StoresStats{
		rollingStoresStats: make(map[uint64]*RollingStoreStats),
		totalLoads:         make([]float64, StoreStatCount),
	}
}

// RemoveRollingStoreStats removes RollingStoreStats with a given container ID.
func (s *StoresStats) RemoveRollingStoreStats(StoreID uint64) {
	s.Lock()
	defer s.Unlock()
	delete(s.rollingStoresStats, StoreID)
}

// GetRollingStoreStats gets RollingStoreStats with a given container ID.
func (s *StoresStats) GetRollingStoreStats(containerID uint64) *RollingStoreStats {
	s.RLock()
	defer s.RUnlock()
	return s.rollingStoresStats[containerID]
}

// GetOrCreateRollingStoreStats gets or creates RollingStoreStats with a given container ID.
func (s *StoresStats) GetOrCreateRollingStoreStats(containerID uint64) *RollingStoreStats {
	s.Lock()
	defer s.Unlock()
	ret, ok := s.rollingStoresStats[containerID]
	if !ok {
		ret = newRollingStoreStats()
		s.rollingStoresStats[containerID] = ret
	}
	return ret
}

// Observe records the current container status with a given container.
func (s *StoresStats) Observe(containerID uint64, stats *metapb.StoreStats) {
	container := s.GetOrCreateRollingStoreStats(containerID)
	container.Observe(stats)
}

// Set sets the container statistics (for test).
func (s *StoresStats) Set(containerID uint64, stats *metapb.StoreStats) {
	container := s.GetOrCreateRollingStoreStats(containerID)
	container.Set(stats)
}

// UpdateTotalLoad updates the total loads of all stores.
func (s *StoresStats) UpdateTotalLoad(containers []*core.CachedStore) {
	s.Lock()
	defer s.Unlock()
	for i := range s.totalLoads {
		s.totalLoads[i] = 0
	}
	for _, container := range containers {
		if !container.IsUp() {
			continue
		}
		stats, ok := s.rollingStoresStats[container.Meta.ID()]
		if !ok {
			continue
		}
		for i := range s.totalLoads {
			s.totalLoads[i] += stats.GetLoad(StoreStatKind(i))
		}
	}
}

// GetStoresLoads returns all stores loads.
func (s *StoresStats) GetStoresLoads() map[uint64][]float64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64][]float64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		for i := StoreStatKind(0); i < StoreStatCount; i++ {
			res[storeID] = append(res[storeID], stats.GetLoad(i))
		}
	}
	return res
}

func (s *StoresStats) containerIsUnhealthy(cluster core.StoreSetInformer, containerID uint64) bool {
	container := cluster.GetStore(containerID)
	return container.IsTombstone() || container.IsUnhealthy() || container.IsPhysicallyDestroyed()
}

// FilterUnhealthyStore filter unhealthy container
func (s *StoresStats) FilterUnhealthyStore(cluster core.StoreSetInformer) {
	s.Lock()
	defer s.Unlock()
	for containerID := range s.rollingStoresStats {
		if s.containerIsUnhealthy(cluster, containerID) {
			delete(s.rollingStoresStats, containerID)
		}
	}
}

// UpdateStoreHeartbeatMetrics is used to update container heartbeat interval metrics
func (s *StoresStats) UpdateStoreHeartbeatMetrics(container *core.CachedStore) {
	containerHeartbeatIntervalHist.Observe(time.Since(container.GetLastHeartbeatTS()).Seconds())
}

// RollingStoreStats are multiple sets of recent historical records with specified windows size.
type RollingStoreStats struct {
	sync.RWMutex
	timeMedians map[StoreStatKind]*movingaverage.TimeMedian
	movingAvgs  map[StoreStatKind]movingaverage.MovingAvg
}

const (
	containerStatsRollingWindows = 3
	// DefaultAotSize is default size of average over time.
	DefaultAotSize = 2
	// DefaultWriteMfSize is default size of write median filter
	DefaultWriteMfSize = 5
	// DefaultReadMfSize is default size of read median filter
	DefaultReadMfSize = 3
)

// newRollingStoreStats creates a RollingStoreStats.
func newRollingStoreStats() *RollingStoreStats {
	timeMedians := make(map[StoreStatKind]*movingaverage.TimeMedian)
	interval := StoreHeartBeatReportInterval * time.Second
	timeMedians[StoreReadBytes] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultReadMfSize, interval)
	timeMedians[StoreReadKeys] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultReadMfSize, interval)
	timeMedians[StoreWriteBytes] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultWriteMfSize, interval)
	timeMedians[StoreWriteKeys] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultWriteMfSize, interval)

	movingAvgs := make(map[StoreStatKind]movingaverage.MovingAvg)
	movingAvgs[StoreCPUUsage] = movingaverage.NewMedianFilter(containerStatsRollingWindows)
	movingAvgs[StoreDiskReadRate] = movingaverage.NewMedianFilter(containerStatsRollingWindows)
	movingAvgs[StoreDiskWriteRate] = movingaverage.NewMedianFilter(containerStatsRollingWindows)

	return &RollingStoreStats{
		timeMedians: timeMedians,
		movingAvgs:  movingAvgs,
	}
}
func collect(records []metapb.RecordPair) float64 {
	var total uint64
	for _, record := range records {
		total += record.GetValue()
	}
	return float64(total)
}

// Observe records current statistics.
func (r *RollingStoreStats) Observe(stats *metapb.StoreStats) {
	statInterval := stats.GetInterval()
	interval := statInterval.GetEnd() - statInterval.GetStart()

	r.Lock()
	defer r.Unlock()
	r.timeMedians[StoreWriteBytes].Add(float64(stats.WrittenBytes), time.Duration(interval)*time.Second)
	r.timeMedians[StoreWriteKeys].Add(float64(stats.WrittenKeys), time.Duration(interval)*time.Second)
	r.timeMedians[StoreReadBytes].Add(float64(stats.ReadBytes), time.Duration(interval)*time.Second)
	r.timeMedians[StoreReadKeys].Add(float64(stats.ReadKeys), time.Duration(interval)*time.Second)

	// Updates the cpu usages and disk rw rates of Store.
	r.movingAvgs[StoreCPUUsage].Add(collect(stats.GetCpuUsages()))
	r.movingAvgs[StoreDiskReadRate].Add(collect(stats.GetReadIORates()))
	r.movingAvgs[StoreDiskWriteRate].Add(collect(stats.GetWriteIORates()))

}

// Set sets the statistics (for test).
func (r *RollingStoreStats) Set(stats *metapb.StoreStats) {
	statInterval := stats.GetInterval()
	interval := statInterval.GetEnd() - statInterval.GetStart()
	if interval == 0 {
		return
	}
	r.Lock()
	defer r.Unlock()
	r.timeMedians[StoreWriteBytes].Set(float64(stats.WrittenBytes) / float64(interval))
	r.timeMedians[StoreReadBytes].Set(float64(stats.ReadBytes) / float64(interval))
	r.timeMedians[StoreWriteKeys].Set(float64(stats.WrittenKeys) / float64(interval))
	r.timeMedians[StoreReadKeys].Set(float64(stats.WrittenKeys) / float64(interval))
	r.movingAvgs[StoreCPUUsage].Set(collect(stats.GetCpuUsages()))
	r.movingAvgs[StoreDiskReadRate].Set(collect(stats.GetReadIORates()))
	r.movingAvgs[StoreDiskWriteRate].Set(collect(stats.GetWriteIORates()))

}

// GetLoad returns store's load.
func (r *RollingStoreStats) GetLoad(k StoreStatKind) float64 {
	r.RLock()
	defer r.RUnlock()
	switch k {
	case StoreReadBytes:
		return r.timeMedians[StoreReadBytes].Get()
	case StoreReadKeys:
		return r.timeMedians[StoreReadKeys].Get()
	case StoreWriteBytes:
		return r.timeMedians[StoreWriteBytes].Get()
	case StoreWriteKeys:
		return r.timeMedians[StoreWriteKeys].Get()
	case StoreCPUUsage:
		return r.movingAvgs[StoreCPUUsage].Get()
	case StoreDiskReadRate:
		return r.movingAvgs[StoreDiskReadRate].Get()
	case StoreDiskWriteRate:
		return r.movingAvgs[StoreDiskWriteRate].Get()
	}
	return 0
}

// GetInstantLoad returns Store's instant load.
// MovingAvgs do not support GetInstantaneous() so they return average values.
func (r *RollingStoreStats) GetInstantLoad(k StoreStatKind) float64 {
	r.RLock()
	defer r.RUnlock()
	switch k {
	case StoreReadBytes:
		return r.timeMedians[StoreReadBytes].GetInstantaneous()
	case StoreReadKeys:
		return r.timeMedians[StoreReadKeys].GetInstantaneous()
	case StoreWriteBytes:
		return r.timeMedians[StoreWriteBytes].GetInstantaneous()
	case StoreWriteKeys:
		return r.timeMedians[StoreWriteKeys].GetInstantaneous()
	case StoreCPUUsage:
		return r.movingAvgs[StoreCPUUsage].Get()
	case StoreDiskReadRate:
		return r.movingAvgs[StoreDiskReadRate].Get()
	case StoreDiskWriteRate:
		return r.movingAvgs[StoreDiskWriteRate].Get()
	}
	return 0
}
