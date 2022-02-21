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
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/hbstream"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/stretchr/testify/assert"
)

type sequencer struct {
	minID uint64
	maxID uint64
	curID uint64
}

func newSequencer(maxID uint64) *sequencer {
	return newSequencerWithMinID(1, maxID)
}

func newSequencerWithMinID(minID, maxID uint64) *sequencer {
	return &sequencer{
		minID: minID,
		maxID: maxID,
		curID: maxID,
	}
}

func (s *sequencer) next() uint64 {
	s.curID++
	if s.curID > s.maxID {
		s.curID = s.minID
	}
	return s.curID
}

type testScatterShard struct{}

func (s *testScatterShard) checkOperator(op *operator.Operator, t *testing.T) {
	for i := 0; i < op.Len(); i++ {
		if rp, ok := op.Step(i).(operator.RemovePeer); ok {
			for j := i + 1; j < op.Len(); j++ {
				if tr, ok := op.Step(j).(operator.TransferLeader); ok {
					assert.NotEqual(t, tr.FromStore, rp.FromStore)
					assert.NotEqual(t, tr.ToStore, rp.FromStore)
				}
			}
		}
	}
}

func (s *testScatterShard) scatter(t *testing.T, numStores, numShards uint64, useRules bool) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableJointConsensus()

	// Add ordinary containers.
	for i := uint64(1); i <= numStores; i++ {
		tc.AddShardStore(i, 0)
	}
	tc.SetEnablePlacementRules(useRules)

	for i := uint64(1); i <= numShards; i++ {
		// resource distributed in same containers.
		tc.AddLeaderShard(i, 1, 2, 3)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scatterer := NewShardScatterer(ctx, tc)

	for i := uint64(1); i <= numShards; i++ {
		resource := tc.GetShard(i)
		if op, _ := scatterer.Scatter(resource, ""); op != nil {
			s.checkOperator(op, t)
			ApplyOperator(tc, op)
		}
	}

	countPeers := make(map[uint64]uint64)
	countLeader := make(map[uint64]uint64)
	for i := uint64(1); i <= numShards; i++ {
		resource := tc.GetShard(i)
		leaderStoreID := resource.GetLeader().GetStoreID()
		for _, peer := range resource.Meta.Peers() {
			countPeers[peer.GetStoreID()]++
			if peer.GetStoreID() == leaderStoreID {
				countLeader[peer.GetStoreID()]++
			}
		}
	}

	// Each container should have the same number of peers.
	for _, count := range countPeers {
		assert.True(t, float64(count) <= 1.1*float64(numShards*3)/float64(numStores))
		assert.True(t, float64(count) >= 0.9*float64(numShards*3)/float64(numStores))
	}

	// Each container should have the same number of leaders.
	assert.Equal(t, int(numStores), len(countPeers))
	assert.Equal(t, int(numStores), len(countLeader))
	for _, count := range countLeader {
		assert.True(t, float64(count) <= 1.1*float64(numShards)/float64(numStores))
		assert.True(t, float64(count) >= 0.9*float64(numShards)/float64(numStores))
	}
}

func (s *testScatterShard) scatterSpecial(t *testing.T, numOrdinaryStores, numSpecialStores, numresources uint64) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableJointConsensus()

	// Add ordinary containers.
	for i := uint64(1); i <= numOrdinaryStores; i++ {
		tc.AddShardStore(i, 0)
	}
	// Add special containers.
	for i := uint64(1); i <= numSpecialStores; i++ {
		tc.AddLabelsStore(numOrdinaryStores+i, 0, map[string]string{"engine": "tiflash"})
	}
	tc.SetEnablePlacementRules(true)
	assert.Nil(t, tc.RuleManager.SetRule(&placement.Rule{
		GroupID: "prophet", ID: "learner", Role: placement.Learner, Count: 3,
		LabelConstraints: []placement.LabelConstraint{{Key: "engine", Op: placement.In, Values: []string{"tiflash"}}}}))

	// resource 1 has the same distribution with the resource 2, which is used to test selectPeerToReplace.
	tc.AddShardWithLearner(1, 1, []uint64{2, 3}, []uint64{numOrdinaryStores + 1, numOrdinaryStores + 2, numOrdinaryStores + 3})
	for i := uint64(2); i <= numresources; i++ {
		tc.AddShardWithLearner(
			i,
			1,
			[]uint64{2, 3},
			[]uint64{numOrdinaryStores + 1, numOrdinaryStores + 2, numOrdinaryStores + 3},
		)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scatterer := NewShardScatterer(ctx, tc)

	for i := uint64(1); i <= numresources; i++ {
		resource := tc.GetShard(i)
		if op, _ := scatterer.Scatter(resource, ""); op != nil {
			s.checkOperator(op, t)
			ApplyOperator(tc, op)
		}
	}

	countOrdinaryPeers := make(map[uint64]uint64)
	countSpecialPeers := make(map[uint64]uint64)
	countOrdinaryLeaders := make(map[uint64]uint64)
	for i := uint64(1); i <= numresources; i++ {
		resource := tc.GetShard(i)
		leaderStoreID := resource.GetLeader().GetStoreID()
		for _, peer := range resource.Meta.Peers() {
			containerID := peer.GetStoreID()
			container := tc.Stores.GetStore(containerID)
			if container.GetLabelValue("engine") == "tiflash" {
				countSpecialPeers[containerID]++
			} else {
				countOrdinaryPeers[containerID]++
			}
			if peer.GetStoreID() == leaderStoreID {
				countOrdinaryLeaders[containerID]++
			}
		}
	}

	// Each container should have the same number of peers.
	for _, count := range countOrdinaryPeers {
		assert.True(t, float64(count) <= 1.1*float64(numresources*3)/float64(numOrdinaryStores))
		assert.True(t, float64(count) >= 0.9*float64(numresources*3)/float64(numOrdinaryStores))
	}
	for _, count := range countSpecialPeers {
		assert.True(t, float64(count) <= 1.1*float64(numresources*3)/float64(numSpecialStores))
		assert.True(t, float64(count) >= 0.9*float64(numresources*3)/float64(numSpecialStores))
	}
	for _, count := range countOrdinaryLeaders {
		assert.True(t, float64(count) <= 1.1*float64(numresources)/float64(numOrdinaryStores))
		assert.True(t, float64(count) >= 0.9*float64(numresources)/float64(numOrdinaryStores))
	}
}

func TestSixStores(t *testing.T) {
	s := testScatterShard{}

	s.scatter(t, 6, 100, false)
	s.scatter(t, 6, 100, true)
	s.scatter(t, 6, 1000, false)
	s.scatter(t, 6, 1000, true)
}

func TestFiveStores(t *testing.T) {
	s := testScatterShard{}

	s.scatter(t, 5, 100, false)
	s.scatter(t, 5, 100, true)
	s.scatter(t, 5, 1000, false)
	s.scatter(t, 5, 1000, true)
}

func TestSixSpecialStores(t *testing.T) {
	s := testScatterShard{}
	s.scatterSpecial(t, 3, 6, 100)
	s.scatterSpecial(t, 3, 6, 1000)
}

func TestFiveSpecialStores(t *testing.T) {
	s := testScatterShard{}

	s.scatterSpecial(t, 5, 5, 100)
	s.scatterSpecial(t, 5, 5, 1000)
}

func TestScatterStoreLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, false, nil)
	oc := NewOperatorController(ctx, tc, stream)

	// Add Stores 1~6.
	for i := uint64(1); i <= 5; i++ {
		tc.AddShardStore(i, 0)
	}

	// Add resources 1~4.
	seq := newSequencer(3)
	// resource 1 has the same distribution with the resource 2, which is used to test selectPeerToReplace.
	tc.AddLeaderShard(1, 1, 2, 3)
	for i := uint64(2); i <= 5; i++ {
		tc.AddLeaderShard(i, seq.next(), seq.next(), seq.next())
	}

	scatterer := NewShardScatterer(ctx, tc)

	for i := uint64(1); i <= 5; i++ {
		resource := tc.GetShard(i)
		if op, _ := scatterer.Scatter(resource, ""); op != nil {
			assert.Equal(t, 1, oc.AddWaitingOperator(op))
		}
	}
}

func TestScatterCheck(t *testing.T) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	// Add 5 Stores.
	for i := uint64(1); i <= 5; i++ {
		tc.AddShardStore(i, 0)
	}
	testcases := []struct {
		name          string
		checkresource *core.CachedShard
		needFix       bool
	}{
		{
			name:          "resource with 4 replicas",
			checkresource: tc.AddLeaderShard(1, 1, 2, 3, 4),
			needFix:       true,
		},
		{
			name:          "resource with 3 replicas",
			checkresource: tc.AddLeaderShard(1, 1, 2, 3),
			needFix:       false,
		},
		{
			name:          "resource with 2 replicas",
			checkresource: tc.AddLeaderShard(1, 1, 2),
			needFix:       true,
		},
	}
	for _, testcase := range testcases {
		t.Log(testcase.name)
		ctx, cancel := context.WithCancel(context.Background())
		scatterer := NewShardScatterer(ctx, tc)
		_, err := scatterer.Scatter(testcase.checkresource, "")
		if testcase.needFix {
			assert.NotNil(t, err)
			assert.True(t, tc.CheckShardUnderSuspect(1))
		} else {
			assert.Nil(t, err)
			assert.False(t, tc.CheckShardUnderSuspect(1))
		}
		tc.ResetSuspectShards()
		cancel()
	}
}

func TestScatterGroup(t *testing.T) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	// Add 5 Stores.
	for i := uint64(1); i <= 5; i++ {
		tc.AddShardStore(i, 0)
	}

	testcases := []struct {
		name       string
		groupCount int
	}{
		{
			name:       "1 group",
			groupCount: 1,
		},
		{
			name:       "2 group",
			groupCount: 2,
		},
		{
			name:       "3 group",
			groupCount: 3,
		},
	}

	for _, testcase := range testcases {
		t.Logf(testcase.name)
		ctx, cancel := context.WithCancel(context.Background())
		scatterer := NewShardScatterer(ctx, tc)
		resourceID := 1
		for i := 0; i < 100; i++ {
			for j := 0; j < testcase.groupCount; j++ {
				_, err := scatterer.Scatter(tc.AddLeaderShard(uint64(resourceID), 1, 2, 3),
					fmt.Sprintf("group-%v", j))
				assert.Nil(t, err)
				resourceID++
			}
			// insert resource with no group
			_, err := scatterer.Scatter(tc.AddLeaderShard(uint64(resourceID), 1, 2, 3), "")
			assert.Nil(t, err)
			resourceID++
		}

		for i := 0; i < testcase.groupCount; i++ {
			// comparing the leader distribution
			group := fmt.Sprintf("group-%v", i)
			max := uint64(0)
			min := uint64(math.MaxUint64)
			groupDistribution, _ := scatterer.ordinaryEngine.selectedLeader.groupDistribution.Get(group)
			for _, count := range groupDistribution.(map[uint64]uint64) {
				if count > max {
					max = count
				}
				if count < min {
					min = count
				}
			}
			// 100 resources divided 5 Stores, each Store expected to have about 20 resources.
			assert.True(t, min <= 20)
			assert.True(t, max >= 20)
			assert.True(t, (max-min) <= 5)
		}
		cancel()
	}
}

func TestScattersGroup(t *testing.T) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	// Add 5 Stores.
	for i := uint64(1); i <= 5; i++ {
		tc.AddShardStore(i, 0)
	}
	testcases := []struct {
		name    string
		failure bool
	}{
		{
			name:    "no failure",
			failure: false,
		},
	}
	group := "group"
	for _, testcase := range testcases {
		ctx, cancel := context.WithCancel(context.Background())
		scatterer := NewShardScatterer(ctx, tc)
		resources := map[uint64]*core.CachedShard{}
		for i := 1; i <= 100; i++ {
			resources[uint64(i)] = tc.AddLeaderShard(uint64(i), 1, 2, 3)
		}
		t.Log(testcase.name)
		failures := map[uint64]error{}
		scatterer.ScatterShards(resources, failures, group, 3)
		max := uint64(0)
		min := uint64(math.MaxUint64)
		groupDistribution, exist := scatterer.ordinaryEngine.selectedLeader.GetGroupDistribution(group)
		assert.True(t, exist)
		for _, count := range groupDistribution {
			if count > max {
				max = count
			}
			if count < min {
				min = count
			}
		}
		// 100 resources divided 5 Stores, each Store expected to have about 20 resources.
		assert.True(t, min <= 20)
		assert.True(t, max >= 20)
		assert.True(t, (max-min) <= 3)
		if testcase.failure {
			assert.Equal(t, 1, len(failures))
			_, ok := failures[1]
			assert.True(t, ok)
		} else {
			assert.Empty(t, failures)
		}
		cancel()
	}
}

func TestSelectedStoreGC(t *testing.T) {
	// use a shorter gcTTL and gcInterval during the test
	gcInterval = time.Second
	gcTTL = time.Second * 3
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	containers := newSelectedStores(ctx)
	containers.put(1, "testgroup")
	_, ok := containers.GetGroupDistribution("testgroup")
	assert.True(t, ok)
	_, ok = containers.GetGroupDistribution("testgroup")
	assert.True(t, ok)
	time.Sleep(gcTTL)
	_, ok = containers.GetGroupDistribution("testgroup")
	assert.False(t, ok)
	_, ok = containers.GetGroupDistribution("testgroup")
	assert.False(t, ok)
}
