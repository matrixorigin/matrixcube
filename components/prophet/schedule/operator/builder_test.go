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

package operator

import (
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/stretchr/testify/assert"
)

type testBuilder struct {
	cluster *mockcluster.Cluster
}

func (s *testBuilder) setup() {
	opts := config.NewTestOptions()
	s.cluster = mockcluster.NewCluster(opts)
	s.cluster.SetLabelPropertyConfig(config.LabelPropertyConfig{
		opt.RejectLeader: {{Key: "noleader", Value: "true"}},
	})
	s.cluster.SetLocationLabels([]string{"zone", "host"})
	s.cluster.AddLabelsContainer(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsContainer(2, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsContainer(3, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsContainer(4, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsContainer(5, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsContainer(6, 0, map[string]string{"zone": "z1", "host": "h2"})
	s.cluster.AddLabelsContainer(7, 0, map[string]string{"zone": "z1", "host": "h2"})
	s.cluster.AddLabelsContainer(8, 0, map[string]string{"zone": "z2", "host": "h1"})
	s.cluster.AddLabelsContainer(9, 0, map[string]string{"zone": "z2", "host": "h2"})
	s.cluster.AddLabelsContainer(10, 0, map[string]string{"zone": "z3", "host": "h1", "noleader": "true"})
}

func (s *testBuilder) newBuilder() *Builder {
	peers := []metapb.Replica{
		{ID: 11, ContainerID: 1},
		{ID: 12, ContainerID: 2},
		{ID: 13, ContainerID: 3, Role: metapb.ReplicaRole_Learner},
	}
	resource := core.NewCachedResource(&metadata.TestResource{ResID: 1, ResPeers: peers}, &peers[0])
	return NewBuilder("test", s.cluster, resource)
}

func TestRecord(t *testing.T) {
	s := &testBuilder{}
	s.setup()

	assert.Error(t, s.newBuilder().AddPeer(metapb.Replica{ContainerID: 1}).err)
	assert.NoError(t, s.newBuilder().AddPeer(metapb.Replica{ContainerID: 4}).err)
	assert.Error(t, s.newBuilder().PromoteLearner(1).err)
	assert.NoError(t, s.newBuilder().PromoteLearner(3).err)
	assert.NoError(t, s.newBuilder().SetLeader(1).SetLeader(2).err)
	assert.Error(t, s.newBuilder().SetLeader(3).err)
	assert.Error(t, s.newBuilder().RemovePeer(4).err)
	assert.NoError(t, s.newBuilder().AddPeer(metapb.Replica{ContainerID: 4, Role: metapb.ReplicaRole_Learner}).RemovePeer(4).err)
	assert.Error(t, s.newBuilder().SetLeader(2).RemovePeer(2).err)
	assert.Error(t, s.newBuilder().PromoteLearner(4).err)
	assert.Error(t, s.newBuilder().SetLeader(4).err)
	assert.Error(t, s.newBuilder().SetPeers(map[uint64]metapb.Replica{2: {ID: 2}}).err)

	m := map[uint64]metapb.Replica{
		2: {ContainerID: 2},
		3: {ContainerID: 3, Role: metapb.ReplicaRole_Learner},
		4: {ContainerID: 4},
	}
	builder := s.newBuilder().SetPeers(m).EnableLightWeight()
	assert.Equal(t, 3, len(builder.targetPeers))
	assert.True(t, reflect.DeepEqual(m[2], builder.targetPeers[2]))
	assert.True(t, reflect.DeepEqual(m[3], builder.targetPeers[3]))
	assert.True(t, reflect.DeepEqual(m[4], builder.targetPeers[4]))
	assert.Equal(t, uint64(0), builder.targetLeaderContainerID)
	assert.True(t, builder.lightWeight)
}

func TestNewBuilder(t *testing.T) {
	s := &testBuilder{}
	s.setup()

	peers := []metapb.Replica{{ID: 11, ContainerID: 1}, {ID: 12, ContainerID: 2, Role: metapb.ReplicaRole_Learner}}
	resource := core.NewCachedResource(&metadata.TestResource{ResID: 42, ResPeers: peers}, &peers[0])
	builder := NewBuilder("test", s.cluster, resource)
	assert.NoError(t, builder.err)
	assert.Equal(t, 2, len(builder.originPeers))
	assert.True(t, reflect.DeepEqual(peers[0], builder.originPeers[1]))
	assert.True(t, reflect.DeepEqual(peers[1], builder.originPeers[2]))
	assert.Equal(t, uint64(1), builder.originLeaderContainerID)
	assert.Equal(t, 2, len(builder.targetPeers))
	assert.True(t, reflect.DeepEqual(peers[0], builder.targetPeers[1]))
	assert.True(t, reflect.DeepEqual(peers[1], builder.targetPeers[2]))

	resource = resource.Clone(core.WithLeader(nil))
	builder = NewBuilder("test", s.cluster, resource)
	assert.Error(t, builder.err)
}

func TestPrepareBuild(t *testing.T) {
	s := &testBuilder{}
	s.setup()

	// no voter.
	_, err := s.newBuilder().SetPeers(map[uint64]metapb.Replica{4: {ContainerID: 4, Role: metapb.ReplicaRole_Learner}}).prepareBuild()
	assert.Error(t, err)

	// use joint consensus
	builder := s.newBuilder().SetPeers(map[uint64]metapb.Replica{
		1: {ContainerID: 1, Role: metapb.ReplicaRole_Learner},
		3: {ContainerID: 3},
		4: {ContainerID: 4, ID: 14},
		5: {ContainerID: 5, Role: metapb.ReplicaRole_Learner},
	})
	_, err = builder.prepareBuild()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(builder.toAdd))
	assert.NotEqual(t, metapb.ReplicaRole_Learner, builder.toAdd[4].Role)
	assert.Equal(t, uint64(14), builder.toAdd[4].ID)
	assert.Equal(t, metapb.ReplicaRole_Learner, builder.toAdd[5].Role)
	assert.NotEqual(t, uint64(0), builder.toAdd[5].ID)
	assert.Equal(t, 1, len(builder.toRemove))
	assert.NotNil(t, builder.toRemove[2])
	assert.Equal(t, 1, len(builder.toPromote))
	assert.NotNil(t, builder.toPromote[3])
	assert.Equal(t, 1, len(builder.toDemote))
	assert.NotNil(t, builder.toDemote[1])
	assert.Equal(t, uint64(1), builder.currentLeaderContainerID)

	// do not use joint consensus
	builder = s.newBuilder().SetPeers(map[uint64]metapb.Replica{
		1: {ContainerID: 1, Role: metapb.ReplicaRole_Learner},
		2: {ContainerID: 2},
		3: {ContainerID: 3},
		4: {ContainerID: 4, ID: 14},
		5: {ContainerID: 5, Role: metapb.ReplicaRole_Learner},
	})
	builder.allowDemote = false
	builder.useJointConsensus = false
	_, err = builder.prepareBuild()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(builder.toAdd))
	assert.Equal(t, metapb.ReplicaRole_Learner, builder.toAdd[1].Role)
	assert.NotEqual(t, uint64(0), builder.toAdd[1].ID)
	assert.NotEqual(t, metapb.ReplicaRole_Learner, builder.toAdd[4].Role)
	assert.Equal(t, uint64(14), builder.toAdd[4].ID)
	assert.Equal(t, metapb.ReplicaRole_Learner, builder.toAdd[5].Role)
	assert.NotEqual(t, uint64(0), builder.toAdd[5].ID)
	assert.Equal(t, 1, len(builder.toRemove))
	assert.NotNil(t, builder.toRemove[1])
	assert.Equal(t, 1, len(builder.toPromote))
	assert.NotNil(t, builder.toPromote[3])
	assert.Equal(t, uint64(1), builder.currentLeaderContainerID)
}

func TestBuild(t *testing.T) {
	s := &testBuilder{}
	s.setup()

	type testCase struct {
		allowDemote       bool
		useJointConsensus bool
		originPeers       []metapb.Replica // first is leader
		targetPeers       []metapb.Replica // first is leader
		kind              OpKind
		steps             []OpStep // empty means error
	}
	cases := []testCase{
		{ // empty step
			false, false,
			[]metapb.Replica{{ID: 1, ContainerID: 1}, {ID: 2, ContainerID: 2}},
			[]metapb.Replica{{ID: 1, ContainerID: 1}, {ID: 2, ContainerID: 2}},
			0,
			[]OpStep{},
		},
		{ // empty step
			true, true,
			[]metapb.Replica{{ID: 1, ContainerID: 1}, {ID: 2, ContainerID: 2}},
			[]metapb.Replica{{ID: 1, ContainerID: 1}, {ID: 2, ContainerID: 2}},
			0,
			[]OpStep{},
		},
		{ // no valid leader
			false, false,
			[]metapb.Replica{{ID: 1, ContainerID: 1}},
			[]metapb.Replica{{ID: 10, ContainerID: 10}},
			0,
			[]OpStep{},
		},
		{ // no valid leader
			true, true,
			[]metapb.Replica{{ID: 1, ContainerID: 1}},
			[]metapb.Replica{{ID: 10, ContainerID: 10}},
			0,
			[]OpStep{},
		},
		{ // promote learner
			false, false,
			[]metapb.Replica{{ID: 1, ContainerID: 1}, {ID: 2, ContainerID: 2, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ID: 2, ContainerID: 2}, {ID: 1, ContainerID: 1}},
			OpLeader,
			[]OpStep{
				PromoteLearner{ToContainer: 2},
				TransferLeader{FromContainer: 1, ToContainer: 2},
			},
		},
		{ // promote learner
			true, true,
			[]metapb.Replica{{ID: 1, ContainerID: 1}, {ID: 2, ContainerID: 2, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ID: 2, ContainerID: 2}, {ID: 1, ContainerID: 1}},
			OpLeader,
			[]OpStep{
				PromoteLearner{ToContainer: 2},
				TransferLeader{FromContainer: 1, ToContainer: 2},
			},
		},
		{ // not use joint consensus: prefer replace
			false, false,
			[]metapb.Replica{{ID: 1, ContainerID: 1}, {ID: 2, ContainerID: 2}, {ID: 3, ContainerID: 3, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ContainerID: 4}, {ContainerID: 5, Role: metapb.ReplicaRole_Learner}},
			OpLeader | OpResource,
			[]OpStep{
				AddLearner{ToContainer: 4},
				PromoteLearner{ToContainer: 4},
				RemovePeer{FromContainer: 2},
				AddLearner{ToContainer: 5},
				RemovePeer{FromContainer: 3},
				TransferLeader{FromContainer: 1, ToContainer: 4},
				RemovePeer{FromContainer: 1},
			},
		},
		{ // use joint consensus: transfer leader in joint state
			true, true,
			[]metapb.Replica{{ID: 1, ContainerID: 1}, {ID: 2, ContainerID: 2}, {ID: 3, ContainerID: 3, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ContainerID: 4}, {ContainerID: 5, Role: metapb.ReplicaRole_Learner}},
			OpLeader | OpResource,
			[]OpStep{
				AddLearner{ToContainer: 4},
				AddLearner{ToContainer: 5},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToContainer: 4}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 1}, {ToContainer: 2}},
				},
				TransferLeader{FromContainer: 1, ToContainer: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToContainer: 4}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 1}, {ToContainer: 2}},
				},
				RemovePeer{FromContainer: 1},
				RemovePeer{FromContainer: 2},
				RemovePeer{FromContainer: 3},
			},
		},
		{ // not use joint consensus: transfer leader before remove leader
			false, false,
			[]metapb.Replica{{ID: 1, ContainerID: 1}},
			[]metapb.Replica{{ContainerID: 2}},
			OpLeader | OpResource,
			[]OpStep{
				AddLearner{ToContainer: 2},
				PromoteLearner{ToContainer: 2},
				TransferLeader{FromContainer: 1, ToContainer: 2},
				RemovePeer{FromContainer: 1},
			},
		},
		{ // use joint consensus: transfer leader in joint state
			true, true,
			[]metapb.Replica{{ID: 1, ContainerID: 1}},
			[]metapb.Replica{{ContainerID: 2}},
			OpLeader | OpResource,
			[]OpStep{
				AddLearner{ToContainer: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToContainer: 2}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 1}},
				},
				TransferLeader{FromContainer: 1, ToContainer: 2},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToContainer: 2}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 1}},
				},
				RemovePeer{FromContainer: 1},
			},
		},
		{ // not use joint consensus: replace voter with learner
			false, false,
			[]metapb.Replica{{ID: 1, ContainerID: 1}, {ID: 2, ContainerID: 2}},
			[]metapb.Replica{{ID: 1, ContainerID: 1}, {ID: 2, ContainerID: 2, Role: metapb.ReplicaRole_Learner}},
			OpResource,
			[]OpStep{
				RemovePeer{FromContainer: 2},
				AddLearner{ToContainer: 2},
			},
		},
		{ // use joint consensus: demote directly
			true, true,
			[]metapb.Replica{{ID: 1, ContainerID: 1}, {ID: 2, ContainerID: 2}},
			[]metapb.Replica{{ContainerID: 1}, {ContainerID: 2, Role: metapb.ReplicaRole_Learner}},
			0, // Note that there is no OpResource here
			[]OpStep{
				DemoteFollower{ToContainer: 2},
			},
		},
		// not use joint consensus
		{ // prefer replace with nearest peer
			false, false,
			[]metapb.Replica{{ID: 1, ContainerID: 1}, {ID: 6, ContainerID: 6}, {ID: 8, ContainerID: 8}},
			//             z1,h1                z1,h2                 z2,h1
			[]metapb.Replica{{ContainerID: 9}, {ContainerID: 7}, {ContainerID: 10}},
			//             z2,h2         z1,h1         z3,h1
			OpLeader | OpResource,
			[]OpStep{
				// 6->7
				AddLearner{ToContainer: 7},
				PromoteLearner{ToContainer: 7},
				RemovePeer{FromContainer: 6},
				// 8->9
				AddLearner{ToContainer: 9},
				PromoteLearner{ToContainer: 9},
				RemovePeer{FromContainer: 8},
				// 1->10
				AddLearner{ToContainer: 10},
				PromoteLearner{ToContainer: 10},
				TransferLeader{FromContainer: 1, ToContainer: 7}, // transfer oldest voter
				RemovePeer{FromContainer: 1},
				// transfer leader
				TransferLeader{FromContainer: 7, ToContainer: 9},
			},
		},
		{ // promote learner + demote voter
			true, false,
			[]metapb.Replica{{ID: 1, ContainerID: 1, Role: metapb.ReplicaRole_Voter}, {ID: 2, ContainerID: 2, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ID: 2, ContainerID: 2, Role: metapb.ReplicaRole_Voter}, {ID: 1, ContainerID: 1, Role: metapb.ReplicaRole_Learner}, {ID: 3, ContainerID: 3, Role: metapb.ReplicaRole_Learner}},
			OpLeader | OpResource,
			[]OpStep{
				PromoteLearner{ToContainer: 2},
				TransferLeader{FromContainer: 1, ToContainer: 2},
				DemoteFollower{ToContainer: 1},
				AddLearner{ToContainer: 3},
			},
		},
		{ // add learner + promote learner + remove voter
			false, false,
			[]metapb.Replica{{ID: 1, ContainerID: 1, Role: metapb.ReplicaRole_Voter}, {ID: 2, ContainerID: 2, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ID: 2, ContainerID: 2, Role: metapb.ReplicaRole_Voter}, {ID: 3, ContainerID: 3, Role: metapb.ReplicaRole_Learner}},
			OpLeader | OpResource,
			[]OpStep{
				AddLearner{ToContainer: 3},
				PromoteLearner{ToContainer: 2},
				TransferLeader{FromContainer: 1, ToContainer: 2},
				RemovePeer{FromContainer: 1},
			},
		},
		{ // add voter + demote voter + remove learner.
			true, false,
			[]metapb.Replica{{ID: 1, ContainerID: 1, Role: metapb.ReplicaRole_Voter}, {ID: 2, ContainerID: 2, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ID: 3, ContainerID: 3, Role: metapb.ReplicaRole_Voter}, {ID: 1, ContainerID: 1, Role: metapb.ReplicaRole_Learner}},
			OpLeader | OpResource,
			[]OpStep{
				AddLearner{ToContainer: 3},
				PromoteLearner{ToContainer: 3},
				TransferLeader{FromContainer: 1, ToContainer: 3},
				DemoteFollower{ToContainer: 1},
				RemovePeer{FromContainer: 2},
			},
		},
		// use joint consensus
		{ // transfer leader before entering joint state
			true, true,
			[]metapb.Replica{{ID: 1, ContainerID: 1}, {ID: 2, ContainerID: 2}, {ID: 3, ContainerID: 3, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ID: 2, ContainerID: 2}, {ID: 3, ContainerID: 3}},
			OpLeader | OpResource,
			[]OpStep{
				TransferLeader{FromContainer: 1, ToContainer: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToContainer: 3}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 1}},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToContainer: 3}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 1}},
				},
				RemovePeer{FromContainer: 1},
			},
		},
		{ // transfer leader after leaving joint state
			true, true,
			[]metapb.Replica{{ID: 1, ContainerID: 1}, {ID: 2, ContainerID: 2}, {ID: 3, ContainerID: 3, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ID: 3, ContainerID: 3}, {ID: 1, ContainerID: 1}},
			OpLeader | OpResource,
			[]OpStep{
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToContainer: 3}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 2}},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToContainer: 3}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 2}},
				},
				TransferLeader{FromContainer: 1, ToContainer: 3},
				RemovePeer{FromContainer: 2},
			},
		},
	}

	for _, tc := range cases {
		resource := core.NewCachedResource(&metadata.TestResource{ResID: 1, ResPeers: tc.originPeers}, &tc.originPeers[0])
		builder := NewBuilder("test", s.cluster, resource)
		builder.allowDemote = tc.allowDemote
		builder.useJointConsensus = tc.useJointConsensus
		m := make(map[uint64]metapb.Replica)
		for _, p := range tc.targetPeers {
			m[p.GetContainerID()] = p
		}
		builder.SetPeers(m).SetLeader(tc.targetPeers[0].GetContainerID())
		op, err := builder.Build(0)
		if len(tc.steps) == 0 {
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
		assert.Equal(t, tc.kind, op.Kind())
		assert.Equal(t, len(tc.steps), op.Len())
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				assert.Equal(t, tc.steps[i].(TransferLeader).FromContainer, step.FromContainer)
				assert.Equal(t, tc.steps[i].(TransferLeader).ToContainer, step.ToContainer)
			case AddPeer:
				assert.Equal(t, tc.steps[i].(AddPeer).ToContainer, step.ToContainer)
			case AddLightPeer:
				assert.Equal(t, tc.steps[i].(AddLightPeer).ToContainer, step.ToContainer)
			case RemovePeer:
				assert.Equal(t, step.FromContainer, tc.steps[i].(RemovePeer).FromContainer)
			case AddLearner:
				assert.Equal(t, step.ToContainer, tc.steps[i].(AddLearner).ToContainer)
			case AddLightLearner:
				assert.Equal(t, step.ToContainer, tc.steps[i].(AddLightLearner).ToContainer)
			case PromoteLearner:
				assert.Equal(t, step.ToContainer, tc.steps[i].(PromoteLearner).ToContainer)
			case DemoteFollower:
				assert.Equal(t, step.ToContainer, tc.steps[i].(DemoteFollower).ToContainer)
			case ChangePeerV2Enter:
				assert.Equal(t, len(step.PromoteLearners), len(tc.steps[i].(ChangePeerV2Enter).PromoteLearners))
				assert.Equal(t, len(step.DemoteVoters), len(tc.steps[i].(ChangePeerV2Enter).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Enter).PromoteLearners {
					assert.Equal(t, step.PromoteLearners[j].ToContainer, p.ToContainer)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Enter).DemoteVoters {
					assert.Equal(t, step.DemoteVoters[j].ToContainer, d.ToContainer)
				}
			case ChangePeerV2Leave:
				assert.Equal(t, len(step.PromoteLearners), len(tc.steps[i].(ChangePeerV2Leave).PromoteLearners))
				assert.Equal(t, len(step.DemoteVoters), len(tc.steps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Leave).PromoteLearners {
					assert.Equal(t, step.PromoteLearners[j].ToContainer, p.ToContainer)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Leave).DemoteVoters {
					assert.Equal(t, step.DemoteVoters[j].ToContainer, d.ToContainer)
				}
			}
		}
	}
}

// Test for not set unhealthy peer as target for promote learner and transfer leader
func TestTargetUnhealthyPeer(t *testing.T) {
	s := &testBuilder{}
	s.setup()

	p := metapb.Replica{ID: 2, ContainerID: 2, Role: metapb.ReplicaRole_Learner}
	resource := core.NewCachedResource(&metadata.TestResource{ResID: 1, ResPeers: []metapb.Replica{{ID: 1, ContainerID: 1},
		p}}, &metapb.Replica{ID: 1, ContainerID: 1}, core.WithPendingPeers([]metapb.Replica{p}))
	builder := NewBuilder("test", s.cluster, resource)
	builder.PromoteLearner(2)
	assert.Error(t, builder.err)
	resource = core.NewCachedResource(&metadata.TestResource{ResID: 1, ResPeers: []metapb.Replica{{ID: 1, ContainerID: 1},
		p}}, &metapb.Replica{ID: 1, ContainerID: 1}, core.WithDownPeers([]metapb.ReplicaStats{{Replica: p}}))
	builder = NewBuilder("test", s.cluster, resource)
	builder.PromoteLearner(2)
	assert.Error(t, builder.err)

	p = metapb.Replica{ID: 2, ContainerID: 2, Role: metapb.ReplicaRole_Voter}
	resource = core.NewCachedResource(&metadata.TestResource{ResID: 1, ResPeers: []metapb.Replica{{ID: 1, ContainerID: 1},
		p}}, &metapb.Replica{ID: 1, ContainerID: 1}, core.WithPendingPeers([]metapb.Replica{p}))
	builder = NewBuilder("test", s.cluster, resource)
	builder.SetLeader(2)
	assert.Error(t, builder.err)
	resource = core.NewCachedResource(&metadata.TestResource{ResID: 1, ResPeers: []metapb.Replica{{ID: 1, ContainerID: 1},
		p}}, &metapb.Replica{ID: 1, ContainerID: 1}, core.WithDownPeers([]metapb.ReplicaStats{{Replica: p}}))
	builder = NewBuilder("test", s.cluster, resource)
	builder.SetLeader(2)
	assert.Error(t, builder.err)
}
