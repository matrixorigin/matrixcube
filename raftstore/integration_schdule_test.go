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
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/util/leaktest"
)

func TestRebalanceWithShardGroup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
		cfg.Prophet.Replication.MaxReplicas = 1
		cfg.Prophet.Replication.Groups = []uint64{0, 1, 2}
		cfg.Customize.CustomInitShardsFactory = func() []meta.Shard {
			return []Shard{{Group: 0}, {Group: 1}, {Group: 2}}
		}
	}))
	c.Start()
	defer c.Stop()

	c.WaitVoterReplicaByCount(1, testWaitTimeout)
}

func TestRebalanceWithLabel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
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

	c.WaitVoterReplicaByCount(2, testWaitTimeout)
	c.EveryStore(func(i int, store Store) {

	})
}
