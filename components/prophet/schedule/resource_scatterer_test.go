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

type testScatterResource struct{}

func (s *testScatterResource) checkOperator(op *operator.Operator, t *testing.T) {
	for i := 0; i < op.Len(); i++ {
		if rp, ok := op.Step(i).(operator.RemovePeer); ok {
			for j := i + 1; j < op.Len(); j++ {
				if tr, ok := op.Step(j).(operator.TransferLeader); ok {
					assert.NotEqual(t, tr.FromContainer, rp.FromContainer)
					assert.NotEqual(t, tr.ToContainer, rp.FromContainer)
				}
			}
		}
	}
}

func (s *testScatterResource) scatter(t *testing.T, numContainers, numResources uint64, useRules bool) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableJointConsensus()

	// Add ordinary containers.
	for i := uint64(1); i <= numContainers; i++ {
		tc.AddResourceContainer(i, 0)
	}
	tc.SetEnablePlacementRules(useRules)

	for i := uint64(1); i <= numResources; i++ {
		// resource distributed in same containers.
		tc.AddLeaderResource(i, 1, 2, 3)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scatterer := NewResourceScatterer(ctx, tc)

	for i := uint64(1); i <= numResources; i++ {
		resource := tc.GetResource(i)
		if op, _ := scatterer.Scatter(resource, ""); op != nil {
			s.checkOperator(op, t)
			ApplyOperator(tc, op)
		}
	}

	countPeers := make(map[uint64]uint64)
	countLeader := make(map[uint64]uint64)
	for i := uint64(1); i <= numResources; i++ {
		resource := tc.GetResource(i)
		leaderContainerID := resource.GetLeader().GetContainerID()
		for _, peer := range resource.Meta.Peers() {
			countPeers[peer.GetContainerID()]++
			if peer.GetContainerID() == leaderContainerID {
				countLeader[peer.GetContainerID()]++
			}
		}
	}

	// Each container should have the same number of peers.
	for _, count := range countPeers {
		assert.True(t, float64(count) <= 1.1*float64(numResources*3)/float64(numContainers))
		assert.True(t, float64(count) >= 0.9*float64(numResources*3)/float64(numContainers))
	}

	// Each container should have the same number of leaders.
	assert.Equal(t, int(numContainers), len(countPeers))
	assert.Equal(t, int(numContainers), len(countLeader))
	for _, count := range countLeader {
		assert.True(t, float64(count) <= 1.1*float64(numResources)/float64(numContainers))
		assert.True(t, float64(count) >= 0.9*float64(numResources)/float64(numContainers))
	}
}

func (s *testScatterResource) scatterSpecial(t *testing.T, numOrdinaryContainers, numSpecialContainers, numresources uint64) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableJointConsensus()

	// Add ordinary containers.
	for i := uint64(1); i <= numOrdinaryContainers; i++ {
		tc.AddResourceContainer(i, 0)
	}
	// Add special containers.
	for i := uint64(1); i <= numSpecialContainers; i++ {
		tc.AddLabelsContainer(numOrdinaryContainers+i, 0, map[string]string{"engine": "tiflash"})
	}
	tc.SetEnablePlacementRules(true)
	assert.Nil(t, tc.RuleManager.SetRule(&placement.Rule{
		GroupID: "prophet", ID: "learner", Role: placement.Learner, Count: 3,
		LabelConstraints: []placement.LabelConstraint{{Key: "engine", Op: placement.In, Values: []string{"tiflash"}}}}))

	// resource 1 has the same distribution with the resource 2, which is used to test selectPeerToReplace.
	tc.AddResourceWithLearner(1, 1, []uint64{2, 3}, []uint64{numOrdinaryContainers + 1, numOrdinaryContainers + 2, numOrdinaryContainers + 3})
	for i := uint64(2); i <= numresources; i++ {
		tc.AddResourceWithLearner(
			i,
			1,
			[]uint64{2, 3},
			[]uint64{numOrdinaryContainers + 1, numOrdinaryContainers + 2, numOrdinaryContainers + 3},
		)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scatterer := NewResourceScatterer(ctx, tc)

	for i := uint64(1); i <= numresources; i++ {
		resource := tc.GetResource(i)
		if op, _ := scatterer.Scatter(resource, ""); op != nil {
			s.checkOperator(op, t)
			ApplyOperator(tc, op)
		}
	}

	countOrdinaryPeers := make(map[uint64]uint64)
	countSpecialPeers := make(map[uint64]uint64)
	countOrdinaryLeaders := make(map[uint64]uint64)
	for i := uint64(1); i <= numresources; i++ {
		resource := tc.GetResource(i)
		leaderContainerID := resource.GetLeader().GetContainerID()
		for _, peer := range resource.Meta.Peers() {
			containerID := peer.GetContainerID()
			container := tc.Containers.GetContainer(containerID)
			if container.GetLabelValue("engine") == "tiflash" {
				countSpecialPeers[containerID]++
			} else {
				countOrdinaryPeers[containerID]++
			}
			if peer.GetContainerID() == leaderContainerID {
				countOrdinaryLeaders[containerID]++
			}
		}
	}

	// Each container should have the same number of peers.
	for _, count := range countOrdinaryPeers {
		assert.True(t, float64(count) <= 1.1*float64(numresources*3)/float64(numOrdinaryContainers))
		assert.True(t, float64(count) >= 0.9*float64(numresources*3)/float64(numOrdinaryContainers))
	}
	for _, count := range countSpecialPeers {
		assert.True(t, float64(count) <= 1.1*float64(numresources*3)/float64(numSpecialContainers))
		assert.True(t, float64(count) >= 0.9*float64(numresources*3)/float64(numSpecialContainers))
	}
	for _, count := range countOrdinaryLeaders {
		assert.True(t, float64(count) <= 1.1*float64(numresources)/float64(numOrdinaryContainers))
		assert.True(t, float64(count) >= 0.9*float64(numresources)/float64(numOrdinaryContainers))
	}
}

func TestSixContainers(t *testing.T) {
	s := testScatterResource{}

	s.scatter(t, 6, 100, false)
	s.scatter(t, 6, 100, true)
	s.scatter(t, 6, 1000, false)
	s.scatter(t, 6, 1000, true)
}

func TestFiveContainers(t *testing.T) {
	s := testScatterResource{}

	s.scatter(t, 5, 100, false)
	s.scatter(t, 5, 100, true)
	s.scatter(t, 5, 1000, false)
	s.scatter(t, 5, 1000, true)
}

func TestSixSpecialContainers(t *testing.T) {
	s := testScatterResource{}
	s.scatterSpecial(t, 3, 6, 100)
	s.scatterSpecial(t, 3, 6, 1000)
}

func TestFiveSpecialContainers(t *testing.T) {
	s := testScatterResource{}

	s.scatterSpecial(t, 5, 5, 100)
	s.scatterSpecial(t, 5, 5, 1000)
}

func TestScatterContainerLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, false)
	oc := NewOperatorController(ctx, tc, stream)

	// Add Containers 1~6.
	for i := uint64(1); i <= 5; i++ {
		tc.AddResourceContainer(i, 0)
	}

	// Add resources 1~4.
	seq := newSequencer(3)
	// resource 1 has the same distribution with the resource 2, which is used to test selectPeerToReplace.
	tc.AddLeaderResource(1, 1, 2, 3)
	for i := uint64(2); i <= 5; i++ {
		tc.AddLeaderResource(i, seq.next(), seq.next(), seq.next())
	}

	scatterer := NewResourceScatterer(ctx, tc)

	for i := uint64(1); i <= 5; i++ {
		resource := tc.GetResource(i)
		if op, _ := scatterer.Scatter(resource, ""); op != nil {
			assert.Equal(t, 1, oc.AddWaitingOperator(op))
		}
	}
}

func TestScatterCheck(t *testing.T) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	// Add 5 Containers.
	for i := uint64(1); i <= 5; i++ {
		tc.AddResourceContainer(i, 0)
	}
	testcases := []struct {
		name          string
		checkresource *core.CachedResource
		needFix       bool
	}{
		{
			name:          "resource with 4 replicas",
			checkresource: tc.AddLeaderResource(1, 1, 2, 3, 4),
			needFix:       true,
		},
		{
			name:          "resource with 3 replicas",
			checkresource: tc.AddLeaderResource(1, 1, 2, 3),
			needFix:       false,
		},
		{
			name:          "resource with 2 replicas",
			checkresource: tc.AddLeaderResource(1, 1, 2),
			needFix:       true,
		},
	}
	for _, testcase := range testcases {
		t.Log(testcase.name)
		ctx, cancel := context.WithCancel(context.Background())
		scatterer := NewResourceScatterer(ctx, tc)
		_, err := scatterer.Scatter(testcase.checkresource, "")
		if testcase.needFix {
			assert.NotNil(t, err)
			assert.True(t, tc.CheckResourceUnderSuspect(1))
		} else {
			assert.Nil(t, err)
			assert.False(t, tc.CheckResourceUnderSuspect(1))
		}
		tc.ResetSuspectResources()
		cancel()
	}
}

func TestScatterGroup(t *testing.T) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	// Add 5 Containers.
	for i := uint64(1); i <= 5; i++ {
		tc.AddResourceContainer(i, 0)
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
		scatterer := NewResourceScatterer(ctx, tc)
		resourceID := 1
		for i := 0; i < 100; i++ {
			for j := 0; j < testcase.groupCount; j++ {
				_, err := scatterer.Scatter(tc.AddLeaderResource(uint64(resourceID), 1, 2, 3),
					fmt.Sprintf("group-%v", j))
				assert.Nil(t, err)
				resourceID++
			}
			// insert resource with no group
			_, err := scatterer.Scatter(tc.AddLeaderResource(uint64(resourceID), 1, 2, 3), "")
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
			// 100 resources divided 5 Containers, each Container expected to have about 20 resources.
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
	// Add 5 Containers.
	for i := uint64(1); i <= 5; i++ {
		tc.AddResourceContainer(i, 0)
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
		scatterer := NewResourceScatterer(ctx, tc)
		resources := map[uint64]*core.CachedResource{}
		for i := 1; i <= 100; i++ {
			resources[uint64(i)] = tc.AddLeaderResource(uint64(i), 1, 2, 3)
		}
		t.Log(testcase.name)
		failures := map[uint64]error{}
		scatterer.ScatterResources(resources, failures, group, 3)
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
		// 100 resources divided 5 Containers, each Container expected to have about 20 resources.
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

func TestSelectedContainerGC(t *testing.T) {
	// use a shorter gcTTL and gcInterval during the test
	gcInterval = time.Second
	gcTTL = time.Second * 3
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	containers := newSelectedContainers(ctx)
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
