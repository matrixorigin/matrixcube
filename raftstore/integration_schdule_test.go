// Copyright 2021 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless assertd by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package raftstore

import (
	"sort"
	"testing"

	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestRebalanceWithLabel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	statusReader := &customStorageStatsReader{}
	statusReader.setStatsWithGB(100, 90)

	c := NewTestClusterStore(t, withStorageStatsReader(func(s *store) storageStatsReader {
		return &customStorageStatsReader{
			s:            s,
			addShardSize: false,
			capacity:     100 * gb,
			available:    90 * gb,
		}
	}),
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Prophet.Replication.MaxReplicas = 1
			cfg.Customize.CustomInitShardsFactory = func() []metapb.Shard {
				return []Shard{
					{Start: []byte("a"), End: []byte("b"), Labels: []metapb.Label{{Key: "table", Value: "t1"}}},
					{Start: []byte("b"), End: []byte("c"), Labels: []metapb.Label{{Key: "table", Value: "t1"}}},
					{Start: []byte("c"), End: []byte("d"), Labels: []metapb.Label{{Key: "table", Value: "t1"}}},
					{Start: []byte("d"), End: []byte("e"), Labels: []metapb.Label{{Key: "table", Value: "t2"}}},
					{Start: []byte("e"), End: []byte("f"), Labels: []metapb.Label{{Key: "table", Value: "t2"}}},
					{Start: []byte("f"), End: []byte("g"), Labels: []metapb.Label{{Key: "table", Value: "t2"}}},
				}
			}
		}))
	c.Start()
	defer c.Stop()

	for {
		err := c.GetProphet().GetClient().AddSchedulingRule(0, "table", "table")
		if err == nil {
			break
		}
	}

	c.EveryStore(func(i int, s Store) {
		for {
			if s.(*store).handleRefreshScheduleGroupRule() {
				break
			}
		}
	})

	c.WaitVoterReplicaByCountPerNode(2, testWaitTimeout)
	c.EveryStore(func(i int, s Store) {
		var shards []Shard
		s.(*store).forEachReplica(func(r *replica) bool {
			shards = append(shards, r.getShard())
			return true
		})
		assert.Equal(t, 2, len(shards), "node %d", i)
		sort.Slice(shards, func(i, j int) bool {
			return shards[i].ID < shards[j].ID
		})
		assert.Equal(t, "t1", shards[0].Labels[0].Value, "node %d", i)
		assert.Equal(t, "t2", shards[1].Labels[0].Value, "node %d", i)
	})
}

func TestRebalanceOnBalancedClusterWithLabel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewTestClusterStore(t, withStorageStatsReader(func(s *store) storageStatsReader {
		return &customStorageStatsReader{
			s:            s,
			addShardSize: true,
			capacity:     1000 * gb,
			available:    900 * gb,
		}
	}),
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Prophet.Replication.MaxReplicas = 1
			cfg.Prophet.Replication.Groups = []uint64{0, 1}
		}))
	c.Start()
	defer c.Stop()

	p, err := c.GetStore(0).CreateShardPool(metapb.ShardPoolJobMeta{Group: 1, Capacity: 9, RangePrefix: []byte("b")})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	c.WaitVoterReplicaByCountsAndShardGroup([]int{3, 3, 3}, 1, testWaitTimeout*3)

	var labelIDs []uint64
	c.GetStore(0).(*store).forEachReplica(func(r *replica) bool {
		shard := r.getShard()
		if shard.Group == 1 {
			labelIDs = append(labelIDs, shard.ID)
		}
		return true
	})

	kv := c.CreateTestKVClient(0)
	for _, id := range labelIDs {
		assert.NoError(t, kv.UpdateLabel(id, 1, "table", "t1", testWaitTimeout))
	}

	err = c.GetProphet().GetClient().AddSchedulingRule(1, "table", "table")
	assert.NoError(t, err)

	c.WaitVoterReplicaByCountsAndShardGroupAndLabel([]int{1, 1, 1}, 1, "table", "t1", testWaitTimeout)
}

func TestRebalanceLeaderOnBalancedClusterWithLabel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewTestClusterStore(t, withStorageStatsReader(func(s *store) storageStatsReader {
		return &customStorageStatsReader{
			s:            s,
			addShardSize: true,
			capacity:     1000 * gb,
			available:    900 * gb,
		}
	}),
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Prophet.Replication.Groups = []uint64{0, 1}
		}))
	c.Start()
	defer c.Stop()

	p, err := c.GetStore(0).CreateShardPool(metapb.ShardPoolJobMeta{Group: 1, Capacity: 9, RangePrefix: []byte("b")})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	c.WaitVoterReplicaByCountsAndShardGroup([]int{9, 9, 9}, 1, testWaitTimeout*3)

	var label1IDs []uint64
	var label2IDs []uint64
	var label3IDs []uint64
	c.GetStore(0).(*store).forEachReplica(func(r *replica) bool {
		shard := r.getShard()
		if shard.Group == 1 {
			if len(label1IDs) < 3 {
				label1IDs = append(label1IDs, shard.ID)
			} else if len(label2IDs) < 3 {
				label2IDs = append(label2IDs, shard.ID)
			} else if len(label3IDs) < 3 {
				label3IDs = append(label3IDs, shard.ID)
			}

		}
		return true
	})

	kv := c.CreateTestKVClient(0)
	for _, id := range label1IDs {
		assert.NoError(t, kv.UpdateLabel(id, 1, "table", "t1", testWaitTimeout))
	}
	for _, id := range label2IDs {
		assert.NoError(t, kv.UpdateLabel(id, 1, "table", "t2", testWaitTimeout))
	}
	for _, id := range label3IDs {
		assert.NoError(t, kv.UpdateLabel(id, 1, "table", "t3", testWaitTimeout))
	}

	err = c.GetProphet().GetClient().AddSchedulingRule(1, "table", "table")
	assert.NoError(t, err)

	c.EveryStore(func(i int, s Store) {
		s.(*store).handleRefreshScheduleGroupRule()
	})

	c.WaitLeadersByCountsAndShardGroupAndLabel([]int{1, 1, 1}, 1, "table", "t1", testWaitTimeout)
	c.WaitLeadersByCountsAndShardGroupAndLabel([]int{1, 1, 1}, 1, "table", "t2", testWaitTimeout)
	c.WaitLeadersByCountsAndShardGroupAndLabel([]int{1, 1, 1}, 1, "table", "t3", testWaitTimeout)
}
