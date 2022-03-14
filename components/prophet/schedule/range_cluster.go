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

package schedule

import (
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

// RangeCluster isolates the cluster by range.
type RangeCluster struct {
	opt.Cluster
	group             uint64
	subCluster        *core.BasicCluster // Collect all resources belong to the range.
	tolerantSizeRatio float64
}

// GenRangeCluster gets a range cluster by specifying start key and end key.
// The cluster can only know the resources within [startKey, endKey].
func GenRangeCluster(group uint64, cluster opt.Cluster, startKey, endKey []byte) *RangeCluster {
	subCluster := core.NewBasicCluster(cluster.GetLogger())
	for _, r := range cluster.ScanShards(group, startKey, endKey, -1) {
		subCluster.Shards.AddShard(r)
	}
	return &RangeCluster{
		Cluster:    cluster,
		subCluster: subCluster,
		group:      group,
	}
}

func (r *RangeCluster) updateCachedStore(s *core.CachedStore) *core.CachedStore {
	id := s.Meta.GetID()

	used := float64(s.GetUsedSize()) / (1 << 20)
	if used == 0 {
		return s
	}

	groupKeys := r.Cluster.GetScheduleGroupKeysWithPrefix(util.EncodeGroupKey(r.group, nil, nil))
	var amplification float64
	var opts []core.StoreCreateOption
	var totalShardSize int64
	for _, groupKey := range groupKeys {
		amplification += float64(s.GetShardSize(groupKey))
		leaderCount := r.subCluster.GetStoreLeaderCount(groupKey, id)
		opts = append(opts, core.SetLeaderCount(groupKey, leaderCount))

		leaderSize := r.subCluster.GetStoreLeaderShardSize(groupKey, id)
		opts = append(opts, core.SetLeaderSize(groupKey, leaderSize))

		resourceCount := r.subCluster.GetStoreShardCount(groupKey, id)
		opts = append(opts, core.SetShardCount(groupKey, resourceCount))

		resourceSize := r.subCluster.GetStoreShardSize(groupKey, id)
		totalShardSize += resourceSize
		opts = append(opts, core.SetShardSize(groupKey, resourceSize))

		pendingPeerCount := r.subCluster.GetStorePendingPeerCount(groupKey, id)
		opts = append(opts, core.SetPendingPeerCount(groupKey, pendingPeerCount))
	}

	amplification = amplification / used
	newStats := proto.Clone(s.GetStoreStats()).(*metapb.StoreStats)
	newStats.UsedSize = uint64(float64(totalShardSize)/amplification) * (1 << 20)
	newStats.Available = s.GetCapacity() - newStats.UsedSize
	opts = append(opts, core.SetNewStoreStats(newStats))
	newStore := s.Clone(opts...)
	return newStore
}

// GetStore searches for a container by ID.
func (r *RangeCluster) GetStore(id uint64) *core.CachedStore {
	s := r.Cluster.GetStore(id)
	if s == nil {
		return nil
	}
	return r.updateCachedStore(s)
}

// GetStores returns all Stores in the cluster.
func (r *RangeCluster) GetStores() []*core.CachedStore {
	containers := r.Cluster.GetStores()
	newStores := make([]*core.CachedStore, 0, len(containers))
	for _, s := range containers {
		newStores = append(newStores, r.updateCachedStore(s))
	}
	return newStores
}

// SetTolerantSizeRatio sets the tolerant size ratio.
func (r *RangeCluster) SetTolerantSizeRatio(ratio float64) {
	r.tolerantSizeRatio = ratio
}

// GetTolerantSizeRatio gets the tolerant size ratio.
func (r *RangeCluster) GetTolerantSizeRatio() float64 {
	if r.tolerantSizeRatio != 0 {
		return r.tolerantSizeRatio
	}
	return r.Cluster.GetOpts().GetTolerantSizeRatio()
}

// RandFollowerShard returns a random resource that has a follower on the Store.
func (r *RangeCluster) RandFollowerShard(groupKey string, containerID uint64, ranges []core.KeyRange, opts ...core.ShardOption) *core.CachedShard {
	return r.subCluster.RandFollowerShard(groupKey, containerID, ranges, opts...)
}

// RandLeaderShard returns a random resource that has leader on the container.
func (r *RangeCluster) RandLeaderShard(groupKey string, containerID uint64, ranges []core.KeyRange, opts ...core.ShardOption) *core.CachedShard {
	return r.subCluster.RandLeaderShard(groupKey, containerID, ranges, opts...)
}

// GetAverageShardSize returns the average resource approximate size.
func (r *RangeCluster) GetAverageShardSize() int64 {
	return r.subCluster.GetAverageShardSize()
}

// GetShardStores returns all containers that contains the resource's peer.
func (r *RangeCluster) GetShardStores(res *core.CachedShard) []*core.CachedStore {
	containers := r.Cluster.GetShardStores(res)
	newStores := make([]*core.CachedStore, 0, len(containers))
	for _, s := range containers {
		newStores = append(newStores, r.updateCachedStore(s))
	}
	return newStores
}

// GetFollowerStores returns all containers that contains the resource's follower peer.
func (r *RangeCluster) GetFollowerStores(res *core.CachedShard) []*core.CachedStore {
	containers := r.Cluster.GetFollowerStores(res)
	newStores := make([]*core.CachedStore, 0, len(containers))
	for _, s := range containers {
		newStores = append(newStores, r.updateCachedStore(s))
	}
	return newStores
}

// GetLeaderStore returns all containers that contains the resource's leader peer.
func (r *RangeCluster) GetLeaderStore(res *core.CachedShard) *core.CachedStore {
	s := r.Cluster.GetLeaderStore(res)
	if s != nil {
		return r.updateCachedStore(s)
	}
	return s
}
