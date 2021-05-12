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
	bytesReadRate          float64
	bytesWriteRate         float64
	keysReadRate           float64
	keysWriteRate          float64
}

// NewContainersStats creates a new hot spot cache.
func NewContainersStats() *ContainersStats {
	return &ContainersStats{
		rollingContainersStats: make(map[uint64]*RollingContainerStats),
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

// UpdateTotalBytesRate updates the total bytes write rate and read rate.
func (s *ContainersStats) UpdateTotalBytesRate(f func() []*core.CachedContainer) {
	var totalBytesWriteRate float64
	var totalBytesReadRate float64
	var writeRate, readRate float64
	ss := f()
	s.RLock()
	defer s.RUnlock()
	for _, container := range ss {
		if container.IsUp() {
			stats, ok := s.rollingContainersStats[container.Meta.ID()]
			if !ok {
				continue
			}
			writeRate, readRate = stats.GetBytesRate()
			totalBytesWriteRate += writeRate
			totalBytesReadRate += readRate
		}
	}
	s.bytesWriteRate = totalBytesWriteRate
	s.bytesReadRate = totalBytesReadRate
}

// UpdateTotalKeysRate updates the total keys write rate and read rate.
func (s *ContainersStats) UpdateTotalKeysRate(f func() []*core.CachedContainer) {
	var totalKeysWriteRate float64
	var totalKeysReadRate float64
	var writeRate, readRate float64
	ss := f()
	s.RLock()
	defer s.RUnlock()
	for _, container := range ss {
		if container.IsUp() {
			stats, ok := s.rollingContainersStats[container.Meta.ID()]
			if !ok {
				continue
			}
			writeRate, readRate = stats.GetKeysRate()
			totalKeysWriteRate += writeRate
			totalKeysReadRate += readRate
		}
	}
	s.keysWriteRate = totalKeysWriteRate
	s.keysReadRate = totalKeysReadRate
}

// TotalBytesWriteRate returns the total written bytes rate of all CachedContainer.
func (s *ContainersStats) TotalBytesWriteRate() float64 {
	return s.bytesWriteRate
}

// TotalBytesReadRate returns the total read bytes rate of all CachedContainer.
func (s *ContainersStats) TotalBytesReadRate() float64 {
	return s.bytesReadRate
}

// TotalKeysWriteRate returns the total written keys rate of all CachedContainer.
func (s *ContainersStats) TotalKeysWriteRate() float64 {
	return s.keysWriteRate
}

// TotalKeysReadRate returns the total read keys rate of all CachedContainer.
func (s *ContainersStats) TotalKeysReadRate() float64 {
	return s.keysReadRate
}

// GetContainerBytesRate returns the bytes write stat of the specified container.
func (s *ContainersStats) GetContainerBytesRate(containerID uint64) (writeRate float64, readRate float64) {
	s.RLock()
	defer s.RUnlock()
	if containerStat, ok := s.rollingContainersStats[containerID]; ok {
		return containerStat.GetBytesRate()
	}
	return 0, 0
}

// GetContainerCPUUsage returns the total cpu usages of threads of the specified container.
func (s *ContainersStats) GetContainerCPUUsage(containerID uint64) float64 {
	s.RLock()
	defer s.RUnlock()
	if containerStat, ok := s.rollingContainersStats[containerID]; ok {
		return containerStat.GetCPUUsage()
	}
	return 0
}

// GetContainerDiskReadRate returns the total read disk io rate of threads of the specified container.
func (s *ContainersStats) GetContainerDiskReadRate(containerID uint64) float64 {
	s.RLock()
	defer s.RUnlock()
	if containerStat, ok := s.rollingContainersStats[containerID]; ok {
		return containerStat.GetDiskReadRate()
	}
	return 0
}

// GetContainerDiskWriteRate returns the total write disk io rate of threads of the specified container.
func (s *ContainersStats) GetContainerDiskWriteRate(containerID uint64) float64 {
	s.RLock()
	defer s.RUnlock()
	if containerStat, ok := s.rollingContainersStats[containerID]; ok {
		return containerStat.GetDiskWriteRate()
	}
	return 0
}

// GetContainersCPUUsage returns the cpu usage stat of all CachedContainer.
func (s *ContainersStats) GetContainersCPUUsage(cluster core.ContainerSetInformer) map[uint64]float64 {
	return s.getStat(func(stats *RollingContainerStats) float64 {
		return stats.GetCPUUsage()
	})
}

// GetContainersDiskReadRate returns the disk read rate stat of all CachedContainer.
func (s *ContainersStats) GetContainersDiskReadRate() map[uint64]float64 {
	return s.getStat(func(stats *RollingContainerStats) float64 {
		return stats.GetDiskReadRate()
	})
}

// GetContainersDiskWriteRate returns the disk write rate stat of all CachedContainer.
func (s *ContainersStats) GetContainersDiskWriteRate() map[uint64]float64 {
	return s.getStat(func(stats *RollingContainerStats) float64 {
		return stats.GetDiskWriteRate()
	})
}

// GetContainerBytesWriteRate returns the bytes write stat of the specified container.
func (s *ContainersStats) GetContainerBytesWriteRate(containerID uint64) float64 {
	s.RLock()
	defer s.RUnlock()
	if containerStat, ok := s.rollingContainersStats[containerID]; ok {
		return containerStat.GetBytesWriteRate()
	}
	return 0
}

// GetContainerBytesReadRate returns the bytes read stat of the specified container.
func (s *ContainersStats) GetContainerBytesReadRate(containerID uint64) float64 {
	s.RLock()
	defer s.RUnlock()
	if containerStat, ok := s.rollingContainersStats[containerID]; ok {
		return containerStat.GetBytesReadRate()
	}
	return 0
}

// GetContainersBytesWriteStat returns the bytes write stat of all CachedContainer.
func (s *ContainersStats) GetContainersBytesWriteStat() map[uint64]float64 {
	return s.getStat(func(stats *RollingContainerStats) float64 {
		return stats.GetBytesWriteRate()
	})
}

// GetContainersBytesReadStat returns the bytes read stat of all CachedContainer.
func (s *ContainersStats) GetContainersBytesReadStat() map[uint64]float64 {
	return s.getStat(func(stats *RollingContainerStats) float64 {
		return stats.GetBytesReadRate()
	})
}

// GetContainersKeysWriteStat returns the keys write stat of all CachedContainer.
func (s *ContainersStats) GetContainersKeysWriteStat() map[uint64]float64 {
	return s.getStat(func(stats *RollingContainerStats) float64 {
		return stats.GetKeysWriteRate()
	})
}

// GetContainersKeysReadStat returns the bytes read stat of all CachedContainer.
func (s *ContainersStats) GetContainersKeysReadStat() map[uint64]float64 {
	return s.getStat(func(stats *RollingContainerStats) float64 {
		return stats.GetKeysReadRate()
	})
}

func (s *ContainersStats) getStat(getRate func(*RollingContainerStats) float64) map[uint64]float64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64]float64, len(s.rollingContainersStats))
	for containerID, stats := range s.rollingContainersStats {
		res[containerID] = getRate(stats)
	}
	return res
}

func (s *ContainersStats) containerIsUnhealthy(cluster core.ContainerSetInformer, containerID uint64) bool {
	container := cluster.GetContainer(containerID)
	return container.IsTombstone() || container.IsUnhealthy()
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
	bytesWriteRate          *movingaverage.TimeMedian
	bytesReadRate           *movingaverage.TimeMedian
	keysWriteRate           *movingaverage.TimeMedian
	keysReadRate            *movingaverage.TimeMedian
	totalCPUUsage           movingaverage.MovingAvg
	totalBytesDiskReadRate  movingaverage.MovingAvg
	totalBytesDiskWriteRate movingaverage.MovingAvg
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
	return &RollingContainerStats{
		bytesWriteRate:          movingaverage.NewTimeMedian(DefaultAotSize, DefaultWriteMfSize, ContainerHeartBeatReportInterval),
		bytesReadRate:           movingaverage.NewTimeMedian(DefaultAotSize, DefaultReadMfSize, ContainerHeartBeatReportInterval),
		keysWriteRate:           movingaverage.NewTimeMedian(DefaultAotSize, DefaultWriteMfSize, ContainerHeartBeatReportInterval),
		keysReadRate:            movingaverage.NewTimeMedian(DefaultAotSize, DefaultReadMfSize, ContainerHeartBeatReportInterval),
		totalCPUUsage:           movingaverage.NewMedianFilter(containerStatsRollingWindows),
		totalBytesDiskReadRate:  movingaverage.NewMedianFilter(containerStatsRollingWindows),
		totalBytesDiskWriteRate: movingaverage.NewMedianFilter(containerStatsRollingWindows),
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
	r.bytesWriteRate.Add(float64(stats.WrittenBytes), time.Duration(interval)*time.Second)
	r.bytesReadRate.Add(float64(stats.ReadBytes), time.Duration(interval)*time.Second)
	r.keysWriteRate.Add(float64(stats.WrittenKeys), time.Duration(interval)*time.Second)
	r.keysReadRate.Add(float64(stats.ReadKeys), time.Duration(interval)*time.Second)

	// Updates the cpu usages and disk rw rates of container.
	r.totalCPUUsage.Add(collect(stats.GetCpuUsages()))
	r.totalBytesDiskReadRate.Add(collect(stats.GetReadIORates()))
	r.totalBytesDiskWriteRate.Add(collect(stats.GetWriteIORates()))
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
	r.bytesWriteRate.Set(float64(stats.WrittenBytes) / float64(interval))
	r.bytesReadRate.Set(float64(stats.ReadBytes) / float64(interval))
	r.keysWriteRate.Set(float64(stats.WrittenKeys) / float64(interval))
	r.keysReadRate.Set(float64(stats.ReadKeys) / float64(interval))
}

// GetBytesRate returns the bytes write rate and the bytes read rate.
func (r *RollingContainerStats) GetBytesRate() (writeRate float64, readRate float64) {
	r.RLock()
	defer r.RUnlock()
	return r.bytesWriteRate.Get(), r.bytesReadRate.Get()
}

// GetBytesRateInstantaneous returns the bytes write rate and the bytes read rate instantaneously.
func (r *RollingContainerStats) GetBytesRateInstantaneous() (writeRate float64, readRate float64) {
	r.RLock()
	defer r.RUnlock()
	return r.bytesWriteRate.GetInstantaneous(), r.bytesReadRate.GetInstantaneous()
}

// GetBytesWriteRate returns the bytes write rate.
func (r *RollingContainerStats) GetBytesWriteRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.bytesWriteRate.Get()
}

// GetBytesReadRate returns the bytes read rate.
func (r *RollingContainerStats) GetBytesReadRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.bytesReadRate.Get()
}

// GetKeysRate returns the keys write rate and the keys read rate.
func (r *RollingContainerStats) GetKeysRate() (writeRate float64, readRate float64) {
	r.RLock()
	defer r.RUnlock()
	return r.keysWriteRate.Get(), r.keysReadRate.Get()
}

// GetKeysRateInstantaneous returns the keys write rate and the keys read rate instantaneously.
func (r *RollingContainerStats) GetKeysRateInstantaneous() (writeRate float64, readRate float64) {
	r.RLock()
	defer r.RUnlock()
	return r.keysWriteRate.GetInstantaneous(), r.keysReadRate.GetInstantaneous()
}

// GetKeysWriteRate returns the keys write rate.
func (r *RollingContainerStats) GetKeysWriteRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.keysWriteRate.Get()
}

// GetKeysReadRate returns the keys read rate.
func (r *RollingContainerStats) GetKeysReadRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.keysReadRate.Get()
}

// GetCPUUsage returns the total cpu usages of threads in the container.
func (r *RollingContainerStats) GetCPUUsage() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.totalCPUUsage.Get()
}

// GetDiskReadRate returns the total read disk io rate of threads in the container.
func (r *RollingContainerStats) GetDiskReadRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.totalBytesDiskReadRate.Get()
}

// GetDiskWriteRate returns the total write disk io rate of threads in the container.
func (r *RollingContainerStats) GetDiskWriteRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.totalBytesDiskWriteRate.Get()
}
