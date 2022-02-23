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

	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

// SplitTestShards split a set of CachedShard by the middle of resourceKey
func SplitTestShards(resources []*CachedShard) []*CachedShard {
	results := make([]*CachedShard, 0, len(resources)*2)
	for _, res := range resources {
		resStart, resEnd := res.Meta.Range()
		start, end := byte(0), byte(math.MaxUint8)
		if len(resStart) > 0 {
			start = resStart[0]
		}
		if len(resEnd) > 0 {
			end = resEnd[0]
		}
		middle := []byte{start/2 + end/2}

		left := res.Clone()
		left.Meta.SetID(res.Meta.ID() + uint64(len(resources)))
		left.Meta.SetEndKey(middle)
		epoch := left.Meta.Epoch()
		epoch.Version++
		left.Meta.SetEpoch(epoch)

		right := res.Clone()
		right.Meta.SetID(res.Meta.ID() + uint64(len(resources)*2))
		right.Meta.SetStartKey(middle)
		epoch = right.Meta.Epoch()
		epoch.Version++
		right.Meta.SetEpoch(epoch)
		results = append(results, left, right)
	}
	return results
}

// MergeTestShards merge a set of CachedShard by resourceKey
func MergeTestShards(resources []*CachedShard) []*CachedShard {
	results := make([]*CachedShard, 0, len(resources)/2)
	for i := 0; i < len(resources); i += 2 {
		left := resources[i]
		right := resources[i]
		if i+1 < len(resources) {
			right = resources[i+1]
		}

		leftStart, _ := left.Meta.Range()
		_, rightEnd := right.Meta.Range()
		res := &CachedShard{
			Meta: &metadata.Shard{
				Shard: metapb.Shard{
					ID:    left.Meta.ID() + uint64(len(resources)),
					Start: leftStart,
					End:   rightEnd,
				},
			},
		}
		if left.Meta.Epoch().Version > right.Meta.Epoch().Version {
			res.Meta.SetEpoch(left.Meta.Epoch())
		} else {
			res.Meta.SetEpoch(right.Meta.Epoch())
		}

		epoch := res.Meta.Epoch()
		epoch.Version++
		res.Meta.SetEpoch(epoch)
		results = append(results, res)
	}
	return results
}

// NewTestCachedShard creates a CachedShard for test.
func NewTestCachedShard(start, end []byte) *CachedShard {
	return &CachedShard{
		Meta: &metadata.Shard{
			Shard: metapb.Shard{
				Start: start,
				End:   end,
				Epoch: metapb.ShardEpoch{},
			},
		},
	}
}

// NewTestStoreInfoWithLabel is create a container with specified labels.
func NewTestStoreInfoWithLabel(id uint64, resourceCount int, labels map[string]string) *CachedStore {
	containerLabels := make([]metapb.Pair, 0, len(labels))
	for k, v := range labels {
		containerLabels = append(containerLabels, metapb.Pair{
			Key:   k,
			Value: v,
		})
	}
	stats := &metapb.StoreStats{}
	stats.Capacity = uint64(1024)
	stats.Available = uint64(1024)
	container := NewCachedStore(
		&metadata.Store{
			Store: metapb.Store{
				ID:     id,
				Labels: containerLabels,
			},
		},
		SetStoreStats(stats),
		SetShardCount("", resourceCount),
		SetShardSize("", int64(resourceCount)*10),
	)
	return container
}

// NewTestCachedStoreWithSizeCount is create a container with size and count.
func NewTestCachedStoreWithSizeCount(id uint64, resourceCount, leaderCount int, resourceSize, leaderSize int64) *CachedStore {
	stats := &metapb.StoreStats{}
	stats.Capacity = uint64(1024)
	stats.Available = uint64(1024)
	container := NewCachedStore(
		metadata.NewTestStore(id),
		SetStoreStats(stats),
		SetShardCount("", resourceCount),
		SetShardSize("", resourceSize),
		SetLeaderCount("", leaderCount),
		SetLeaderSize("", leaderSize),
	)
	return container
}

func newTestShardItem(start, end []byte) *resourceItem {
	return &resourceItem{res: NewTestCachedShard(start, end)}
}

func newShardWithStat(start, end string, size, keys int64) *CachedShard {
	res := NewTestCachedShard([]byte(start), []byte(end))
	res.stats.ApproximateSize, res.stats.ApproximateKeys = uint64(size), uint64(keys)
	return res
}
