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
	"github.com/matrixorigin/matrixcube/components/prophet/core"
)

// ShardStats records a list of resources' statistics and distribution status.
type ShardStats struct {
	Count                int              `json:"count"`
	EmptyCount           int              `json:"empty_count"`
	StorageSize          int64            `json:"storage_size"`
	StorageKeys          int64            `json:"storage_keys"`
	StoreLeaderCount map[uint64]int   `json:"container_leader_count"`
	StorePeerCount   map[uint64]int   `json:"container_peer_count"`
	StoreLeaderSize  map[uint64]int64 `json:"container_leader_size"`
	StoreLeaderKeys  map[uint64]int64 `json:"container_leader_keys"`
	StorePeerSize    map[uint64]int64 `json:"container_peer_size"`
	StorePeerKeys    map[uint64]int64 `json:"container_peer_keys"`
}

// GetShardStats sums resources' statistics.
func GetShardStats(resources []*core.CachedShard) *ShardStats {
	stats := newShardStats()
	for _, resource := range resources {
		stats.Observe(resource)
	}
	return stats
}

func newShardStats() *ShardStats {
	return &ShardStats{
		StoreLeaderCount: make(map[uint64]int),
		StorePeerCount:   make(map[uint64]int),
		StoreLeaderSize:  make(map[uint64]int64),
		StoreLeaderKeys:  make(map[uint64]int64),
		StorePeerSize:    make(map[uint64]int64),
		StorePeerKeys:    make(map[uint64]int64),
	}
}

// Observe adds a resource's statistics into ShardStats.
func (s *ShardStats) Observe(r *core.CachedShard) {
	s.Count++
	approximateKeys := r.GetApproximateKeys()
	approximateSize := r.GetApproximateSize()
	if approximateSize <= core.EmptyShardApproximateSize {
		s.EmptyCount++
	}
	s.StorageSize += approximateSize
	s.StorageKeys += approximateKeys
	leader := r.GetLeader()
	if leader != nil {
		containerID := leader.GetStoreID()
		s.StoreLeaderCount[containerID]++
		s.StoreLeaderSize[containerID] += approximateSize
		s.StoreLeaderKeys[containerID] += approximateKeys
	}
	peers := r.Meta.Peers()
	for _, p := range peers {
		containerID := p.GetStoreID()
		s.StorePeerCount[containerID]++
		s.StorePeerSize[containerID] += approximateSize
		s.StorePeerKeys[containerID] += approximateKeys
	}
}
