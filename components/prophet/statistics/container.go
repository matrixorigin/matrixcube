package statistics

import (
	"sync"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/movingaverage"
)

// ContainersStats is a cache hold hot resources.
type ContainersStats struct {
	sync.RWMutex
	rollingContainersStats map[uint64]*RollingContainerStats
	totalLoads             []float64
}

// NewContainersStats creates a new hot spot cache.
func NewContainersStats() *ContainersStats {
	return &ContainersStats{
		rollingContainersStats: make(map[uint64]*RollingContainerStats),
		totalLoads:             make([]float64, ContainerStatCount),
	}
}

// RemoveRollingContainerStats removes RollingContainerStats with a given container ID.
func (s *ContainersStats) RemoveRollingContainerStats(ContainerID uint64) {
	s.Lock()
	defer s.Unlock()
	delete(s.rollingContainersStats, ContainerID)
}

// GetRollingContainerStats gets RollingContainerStats with a given container ID.
func (s *ContainersStats) GetRollingContainerStats(containerID uint64) *RollingContainerStats {
	s.RLock()
	defer s.RUnlock()
	return s.rollingContainersStats[containerID]
}

// GetOrCreateRollingContainerStats gets or creates RollingContainerStats with a given container ID.
func (s *ContainersStats) GetOrCreateRollingContainerStats(containerID uint64) *RollingContainerStats {
	s.Lock()
	defer s.Unlock()
	ret, ok := s.rollingContainersStats[containerID]
	if !ok {
		ret = newRollingContainerStats()
		s.rollingContainersStats[containerID] = ret
	}
	return ret
}

// Observe records the current container status with a given container.
func (s *ContainersStats) Observe(containerID uint64, stats *metapb.ContainerStats) {
	container := s.GetOrCreateRollingContainerStats(containerID)
	container.Observe(stats)
}

// Set sets the container statistics (for test).
func (s *ContainersStats) Set(containerID uint64, stats *metapb.ContainerStats) {
	container := s.GetOrCreateRollingContainerStats(containerID)
	container.Set(stats)
}

// UpdateTotalLoad updates the total loads of all stores.
func (s *ContainersStats) UpdateTotalLoad(containers []*core.CachedContainer) {
	s.Lock()
	defer s.Unlock()
	for i := range s.totalLoads {
		s.totalLoads[i] = 0
	}
	for _, container := range containers {
		if !container.IsUp() {
			continue
		}
		stats, ok := s.rollingContainersStats[container.Meta.ID()]
		if !ok {
			continue
		}
		for i := range s.totalLoads {
			s.totalLoads[i] += stats.GetLoad(ContainerStatKind(i))
		}
	}
}

// GetContainersLoads returns all stores loads.
func (s *ContainersStats) GetContainersLoads() map[uint64][]float64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64][]float64, len(s.rollingContainersStats))
	for storeID, stats := range s.rollingContainersStats {
		for i := ContainerStatKind(0); i < ContainerStatCount; i++ {
			res[storeID] = append(res[storeID], stats.GetLoad(i))
		}
	}
	return res
}

func (s *ContainersStats) containerIsUnhealthy(cluster core.ContainerSetInformer, containerID uint64) bool {
	container := cluster.GetContainer(containerID)
	return container.IsTombstone() || container.IsUnhealthy() || container.IsPhysicallyDestroyed()
}

// FilterUnhealthyContainer filter unhealthy container
func (s *ContainersStats) FilterUnhealthyContainer(cluster core.ContainerSetInformer) {
	s.Lock()
	defer s.Unlock()
	for containerID := range s.rollingContainersStats {
		if s.containerIsUnhealthy(cluster, containerID) {
			delete(s.rollingContainersStats, containerID)
		}
	}
}

// UpdateContainerHeartbeatMetrics is used to update container heartbeat interval metrics
func (s *ContainersStats) UpdateContainerHeartbeatMetrics(container *core.CachedContainer) {
	containerHeartbeatIntervalHist.Observe(time.Since(container.GetLastHeartbeatTS()).Seconds())
}

// RollingContainerStats are multiple sets of recent historical records with specified windows size.
type RollingContainerStats struct {
	sync.RWMutex
	timeMedians map[ContainerStatKind]*movingaverage.TimeMedian
	movingAvgs  map[ContainerStatKind]movingaverage.MovingAvg
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

// newRollingContainerStats creates a RollingContainerStats.
func newRollingContainerStats() *RollingContainerStats {
	timeMedians := make(map[ContainerStatKind]*movingaverage.TimeMedian)
	interval := ContainerHeartBeatReportInterval * time.Second
	timeMedians[ContainerReadBytes] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultReadMfSize, interval)
	timeMedians[ContainerReadKeys] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultReadMfSize, interval)
	timeMedians[ContainerWriteBytes] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultWriteMfSize, interval)
	timeMedians[ContainerWriteKeys] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultWriteMfSize, interval)

	movingAvgs := make(map[ContainerStatKind]movingaverage.MovingAvg)
	movingAvgs[ContainerCPUUsage] = movingaverage.NewMedianFilter(containerStatsRollingWindows)
	movingAvgs[ContainerDiskReadRate] = movingaverage.NewMedianFilter(containerStatsRollingWindows)
	movingAvgs[ContainerDiskWriteRate] = movingaverage.NewMedianFilter(containerStatsRollingWindows)

	return &RollingContainerStats{
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
func (r *RollingContainerStats) Observe(stats *metapb.ContainerStats) {
	statInterval := stats.GetInterval()
	interval := statInterval.GetEnd() - statInterval.GetStart()
	util.GetLogger().Debugf("update container stats, %+v", stats)

	r.Lock()
	defer r.Unlock()
	r.timeMedians[ContainerWriteBytes].Add(float64(stats.WrittenBytes), time.Duration(interval)*time.Second)
	r.timeMedians[ContainerWriteKeys].Add(float64(stats.WrittenKeys), time.Duration(interval)*time.Second)
	r.timeMedians[ContainerReadBytes].Add(float64(stats.ReadBytes), time.Duration(interval)*time.Second)
	r.timeMedians[ContainerReadKeys].Add(float64(stats.ReadKeys), time.Duration(interval)*time.Second)

	// Updates the cpu usages and disk rw rates of Container.
	r.movingAvgs[ContainerCPUUsage].Add(collect(stats.GetCpuUsages()))
	r.movingAvgs[ContainerDiskReadRate].Add(collect(stats.GetReadIORates()))
	r.movingAvgs[ContainerDiskWriteRate].Add(collect(stats.GetWriteIORates()))

}

// Set sets the statistics (for test).
func (r *RollingContainerStats) Set(stats *metapb.ContainerStats) {
	statInterval := stats.GetInterval()
	interval := statInterval.GetEnd() - statInterval.GetStart()
	if interval == 0 {
		return
	}
	r.Lock()
	defer r.Unlock()
	r.timeMedians[ContainerWriteBytes].Set(float64(stats.WrittenBytes) / float64(interval))
	r.timeMedians[ContainerReadBytes].Set(float64(stats.ReadBytes) / float64(interval))
	r.timeMedians[ContainerWriteKeys].Set(float64(stats.WrittenKeys) / float64(interval))
	r.timeMedians[ContainerReadKeys].Set(float64(stats.WrittenKeys) / float64(interval))
	r.movingAvgs[ContainerCPUUsage].Set(collect(stats.GetCpuUsages()))
	r.movingAvgs[ContainerDiskReadRate].Set(collect(stats.GetReadIORates()))
	r.movingAvgs[ContainerDiskWriteRate].Set(collect(stats.GetWriteIORates()))

}

// GetLoad returns store's load.
func (r *RollingContainerStats) GetLoad(k ContainerStatKind) float64 {
	r.RLock()
	defer r.RUnlock()
	switch k {
	case ContainerReadBytes:
		return r.timeMedians[ContainerReadBytes].Get()
	case ContainerReadKeys:
		return r.timeMedians[ContainerReadKeys].Get()
	case ContainerWriteBytes:
		return r.timeMedians[ContainerWriteBytes].Get()
	case ContainerWriteKeys:
		return r.timeMedians[ContainerWriteKeys].Get()
	case ContainerCPUUsage:
		return r.movingAvgs[ContainerCPUUsage].Get()
	case ContainerDiskReadRate:
		return r.movingAvgs[ContainerDiskReadRate].Get()
	case ContainerDiskWriteRate:
		return r.movingAvgs[ContainerDiskWriteRate].Get()
	}
	return 0
}

// GetInstantLoad returns Container's instant load.
// MovingAvgs do not support GetInstantaneous() so they return average values.
func (r *RollingContainerStats) GetInstantLoad(k ContainerStatKind) float64 {
	r.RLock()
	defer r.RUnlock()
	switch k {
	case ContainerReadBytes:
		return r.timeMedians[ContainerReadBytes].GetInstantaneous()
	case ContainerReadKeys:
		return r.timeMedians[ContainerReadKeys].GetInstantaneous()
	case ContainerWriteBytes:
		return r.timeMedians[ContainerWriteBytes].GetInstantaneous()
	case ContainerWriteKeys:
		return r.timeMedians[ContainerWriteKeys].GetInstantaneous()
	case ContainerCPUUsage:
		return r.movingAvgs[ContainerCPUUsage].Get()
	case ContainerDiskReadRate:
		return r.movingAvgs[ContainerDiskReadRate].Get()
	case ContainerDiskWriteRate:
		return r.movingAvgs[ContainerDiskWriteRate].Get()
	}
	return 0
}
