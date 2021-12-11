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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

var (
	gb = uint64(1 << 30)
)

type customStorageStatsReader struct {
	sync.RWMutex

	capacity  uint64
	available uint64
}

func (s *customStorageStatsReader) setStatsWithGB(capacity, available uint64) {
	s.Lock()
	defer s.Unlock()
	s.capacity = capacity * gb
	s.available = available * gb
}

func (s *customStorageStatsReader) stats() (storageStats, error) {
	s.RLock()
	defer s.RUnlock()

	return storageStats{
		capacity:  s.capacity,
		available: s.available,
		usedSize:  s.capacity - s.available,
	}, nil
}

func TestRebalanceWithLabel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	statusReader := &customStorageStatsReader{}
	statusReader.setStatsWithGB(100, 90)

	c := NewTestClusterStore(t, withStorageStatsReader(statusReader),
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Prophet.Replication.MaxReplicas = 1
			cfg.Customize.CustomInitShardsFactory = func() []meta.Shard {
				return []Shard{
					{Start: []byte("a"), End: []byte("b"), Labels: []metapb.Pair{{Key: "table", Value: "t1"}}},
					{Start: []byte("b"), End: []byte("c"), Labels: []metapb.Pair{{Key: "table", Value: "t1"}}},
					{Start: []byte("c"), End: []byte("d"), Labels: []metapb.Pair{{Key: "table", Value: "t1"}}},
					{Start: []byte("d"), End: []byte("e"), Labels: []metapb.Pair{{Key: "table", Value: "t2"}}},
					{Start: []byte("e"), End: []byte("f"), Labels: []metapb.Pair{{Key: "table", Value: "t2"}}},
					{Start: []byte("f"), End: []byte("g"), Labels: []metapb.Pair{{Key: "table", Value: "t2"}}},
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
