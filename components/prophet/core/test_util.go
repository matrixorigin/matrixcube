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

	"github.com/matrixorigin/matrixcube/pb/metapb"
)

// SplitTestShards split a set of CachedShard by the middle of shardKey
func SplitTestShards(shards []*CachedShard) []*CachedShard {
	results := make([]*CachedShard, 0, len(shards)*2)
	for _, res := range shards {
		resStart, resEnd := res.Meta.GetRange()
		start, end := byte(0), byte(math.MaxUint8)
		if len(resStart) > 0 {
			start = resStart[0]
		}
		if len(resEnd) > 0 {
			end = resEnd[0]
		}
		middle := []byte{start/2 + end/2}

		left := res.Clone()
		left.Meta.SetID(res.Meta.GetID() + uint64(len(shards)))
		left.Meta.SetEndKey(middle)
		epoch := left.Meta.GetEpoch()
		epoch.Generation++
		left.Meta.SetEpoch(epoch)

		right := res.Clone()
		right.Meta.SetID(res.Meta.GetID() + uint64(len(shards)*2))
		right.Meta.SetStartKey(middle)
		epoch = right.Meta.GetEpoch()
		epoch.Generation++
		right.Meta.SetEpoch(epoch)
		results = append(results, left, right)
	}
	return results
}

// MergeTestShards merge a set of CachedShard by shardKey
func MergeTestShards(shards []*CachedShard) []*CachedShard {
	results := make([]*CachedShard, 0, len(shards)/2)
	for i := 0; i < len(shards); i += 2 {
		left := shards[i]
		right := shards[i]
		if i+1 < len(shards) {
			right = shards[i+1]
		}

		leftStart, _ := left.Meta.GetRange()
		_, rightEnd := right.Meta.GetRange()
		res := &CachedShard{
			Meta: metapb.Shard{
				ID:    left.Meta.GetID() + uint64(len(shards)),
				Start: leftStart,
				End:   rightEnd,
			},
		}
		if left.Meta.GetEpoch().Generation > right.Meta.GetEpoch().Generation {
			res.Meta.SetEpoch(left.Meta.GetEpoch())
		} else {
			res.Meta.SetEpoch(right.Meta.GetEpoch())
		}

		epoch := res.Meta.GetEpoch()
		epoch.Generation++
		res.Meta.SetEpoch(epoch)
		results = append(results, res)
	}
	return results
}

// NewTestCachedShard creates a CachedShard for test.
func NewTestCachedShard(start, end []byte) *CachedShard {
	return &CachedShard{
		Meta: metapb.Shard{
			Start: start,
			End:   end,
			Epoch: metapb.ShardEpoch{},
		},
	}
}

// NewTestStoreInfoWithLabel creates a store with specified labels.
func NewTestStoreInfoWithLabel(id uint64, shardCount int, labels map[string]string) *CachedStore {
	containerLabels := make([]metapb.Label, 0, len(labels))
	for k, v := range labels {
		containerLabels = append(containerLabels, metapb.Label{
			Key:   k,
			Value: v,
		})
	}
	stats := &metapb.StoreStats{}
	stats.Capacity = uint64(1024)
	stats.Available = uint64(1024)
	container := NewCachedStore(
		metapb.Store{ID: id, Labels: containerLabels},
		SetStoreStats(stats),
		SetShardCount("", shardCount),
		SetShardSize("", int64(shardCount)*10),
	)
	return container
}

func newTestShardItem(start, end []byte) *shardItem {
	return &shardItem{shard: NewTestCachedShard(start, end)}
}

func newShardWithStat(start, end string, size, keys int64) *CachedShard {
	res := NewTestCachedShard([]byte(start), []byte(end))
	res.stats.ApproximateSize, res.stats.ApproximateKeys = uint64(size), uint64(keys)
	return res
}
