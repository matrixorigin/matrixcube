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

package core

import (
	"math"
	"sync"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/util/movingaverage"
)

type containerStats struct {
	mu       sync.RWMutex
	rawStats *metapb.ContainerStats

	// avgAvailable is used to make available smooth, aka no sudden changes.
	avgAvailable *movingaverage.HMA
	// Following two fields are used to trace the deviation when available
	// records' deviation range to make scheduling able to converge.
	// Here `MaxFilter` is used to make the scheduling more conservative, and
	// `HMA` is used to make it smooth.
	maxAvailableDeviation    *movingaverage.MaxFilter
	avgMaxAvailableDeviation *movingaverage.HMA
}

func newContainerStats() *containerStats {
	return &containerStats{
		rawStats:                 &metapb.ContainerStats{},
		avgAvailable:             movingaverage.NewHMA(240),       // take 40 minutes sample under 10s heartbeat rate
		maxAvailableDeviation:    movingaverage.NewMaxFilter(120), // take 20 minutes sample under 10s heartbeat rate
		avgMaxAvailableDeviation: movingaverage.NewHMA(60),        // take 10 minutes sample under 10s heartbeat rate
	}
}

func (ss *containerStats) updateRawStats(rawStats *metapb.ContainerStats) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.rawStats = rawStats

	if ss.avgAvailable == nil {
		return
	}

	ss.avgAvailable.Add(float64(rawStats.GetAvailable()))
	deviation := math.Abs(float64(rawStats.GetAvailable()) - ss.avgAvailable.Get())
	ss.maxAvailableDeviation.Add(deviation)
	ss.avgMaxAvailableDeviation.Add(ss.maxAvailableDeviation.Get())
}

// GetContainerStats returns the statistics information of the container.
func (ss *containerStats) GetContainerStats() *metapb.ContainerStats {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats
}

// GetCapacity returns the capacity size of the container.
func (ss *containerStats) GetCapacity() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetCapacity()
}

// GetAvailable returns the available size of the container.
func (ss *containerStats) GetAvailable() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetAvailable()
}

// GetUsedSize returns the used size of the container.
func (ss *containerStats) GetUsedSize() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetUsedSize()
}

// GetUsedRatio returns the used ratio of the container.
func (ss *containerStats) GetUsedRatio() float64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return float64(ss.rawStats.GetUsedSize()) / float64(ss.rawStats.GetCapacity())
}

// GetBytesWritten returns the bytes written for the container during this period.
func (ss *containerStats) GetBytesWritten() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetWrittenBytes()
}

// GetBytesRead returns the bytes read for the container during this period.
func (ss *containerStats) GetBytesRead() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetReadBytes()
}

// GetKeysWritten returns the keys written for the container during this period.
func (ss *containerStats) GetKeysWritten() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetWrittenKeys()
}

// GetKeysRead returns the keys read for the container during this period.
func (ss *containerStats) GetKeysRead() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetReadKeys()
}

// IsBusy returns if the container is busy.
func (ss *containerStats) IsBusy() bool {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetIsBusy()
}

// GetSendingSnapCount returns the current sending snapshot count of the container.
func (ss *containerStats) GetSendingSnapCount() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetSendingSnapCount()
}

// GetReceivingSnapCount returns the current receiving snapshot count of the container.
func (ss *containerStats) GetReceivingSnapCount() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetReceivingSnapCount()
}

// GetApplyingSnapCount returns the current applying snapshot count of the container.
func (ss *containerStats) GetApplyingSnapCount() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetApplyingSnapCount()
}

// GetAvgAvailable returns available size after the spike changes has been smoothed.
func (ss *containerStats) GetAvgAvailable() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	if ss.avgAvailable == nil {
		return ss.rawStats.Available
	}
	return climp0(ss.avgAvailable.Get())
}

// GetAvailableDeviation returns approximate magnitude of available in the recent period.
func (ss *containerStats) GetAvailableDeviation() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	if ss.avgMaxAvailableDeviation == nil {
		return 0
	}
	return climp0(ss.avgMaxAvailableDeviation.Get())
}

func climp0(v float64) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v)
}
