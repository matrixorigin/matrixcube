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
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"testing"

	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestScheduleReplicasWithRules(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()
	c := NewTestClusterStore(t,
		WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
			cfg.Customize.CustomInitShardsFactory = func() []Shard {
				return []Shard{{Start: []byte("a"), End: []byte("b")}}
			}
		}))

	c.Start()
	defer c.Stop()
	c.WaitShardByCountPerNode(1, testWaitTimeout)

	assert.NoError(t, c.GetProphet().GetClient().PutPlacementRule(rpcpb.PlacementRule{
		GroupID: "g1",
		ID:      "id1",
		Count:   3,
		LabelConstraints: []rpcpb.LabelConstraint{
			{
				Key:    "c",
				Op:     rpcpb.In,
				Values: []string{"0", "1"},
			},
		},
	}))
	res := metapb.Shard{Start: []byte("b"), End: []byte("c"), Unique: "abc", RuleGroups: []string{"g1"}}
	err := c.GetProphet().GetClient().AsyncAddShardsWithLeastPeers([]metapb.Shard{res}, []int{2})
	assert.NoError(t, err)
	c.WaitShardByCounts([]int{2, 2, 1}, testWaitTimeout)
}
