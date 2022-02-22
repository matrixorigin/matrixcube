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
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	Peers          []metapb.Replica // first is leader
	ConfVerChanged uint64
	IsFinish       bool
	CheckSafety    string
}

func TestDemoteFollower(t *testing.T) {
	df := DemoteFollower{ToStore: 2, PeerID: 2}
	cases := []testCase{
		{ // before step
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			0,
			false,
			"IsNil",
		},
		{ // after step
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Learner},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			1,
			true,
			"IsNil",
		},
		{ // miss peer id
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 4, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			0,
			false,
			"NotNil",
		},
		{ // miss container id
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 4, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			0,
			false,
			"NotNil",
		},
		{ // miss peer id
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 4, StoreID: 2, Role: metapb.ReplicaRole_Learner},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			0,
			false,
			"NotNil",
		},
		{ // demote leader
			[]metapb.Replica{
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			0,
			false,
			"NotNil",
		},
	}
	checkStep(t, df, "demote follower peer 2 on container 2 to learner", cases)
}

func TestChangePeerV2Enter(t *testing.T) {
	cpe := ChangePeerV2Enter{
		PromoteLearners: []PromoteLearner{{PeerID: 3, ToStore: 3}, {PeerID: 4, ToStore: 4}},
		DemoteVoters:    []DemoteVoter{{PeerID: 1, ToStore: 1}, {PeerID: 2, ToStore: 2}},
	}
	cases := []testCase{
		{ // before step
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Learner},
			},
			0,
			false,
			"IsNil",
		},
		{ // after step
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_IncomingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			4,
			true,
			"IsNil",
		},
		{ // miss peer id
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 5, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Learner},
			},
			0,
			false,
			"NotNil",
		},
		{ // miss container id
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 5, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Learner},
			},
			0,
			false,
			"NotNil",
		},
		{ // miss peer id
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 5, StoreID: 2, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_IncomingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			0,
			false,
			"NotNil",
		},
		{ // change is not atomic
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_IncomingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			0,
			false,
			"NotNil",
		},
		{ // change is not atomic
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Learner},
			},
			0,
			false,
			"NotNil",
		},
		{ // there are other peers in the joint state
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_IncomingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
				{ID: 5, StoreID: 5, Role: metapb.ReplicaRole_IncomingVoter},
			},
			4,
			true,
			"NotNil",
		},
		{ // there are other peers in the joint state
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Learner},
				{ID: 5, StoreID: 5, Role: metapb.ReplicaRole_IncomingVoter},
				{ID: 6, StoreID: 6, Role: metapb.ReplicaRole_DemotingVoter},
			},
			0,
			false,
			"NotNil",
		},
	}
	desc := "use joint consensus, " +
		"promote learner peer 3 on container 3 to voter, promote learner peer 4 on container 4 to voter, " +
		"demote voter peer 1 on container 1 to learner, demote voter peer 2 on container 2 to learner"
	checkStep(t, cpe, desc, cases)
}

func TestChangePeerV2Leave(t *testing.T) {
	cpl := ChangePeerV2Leave{
		PromoteLearners: []PromoteLearner{{PeerID: 3, ToStore: 3}, {PeerID: 4, ToStore: 4}},
		DemoteVoters:    []DemoteVoter{{PeerID: 1, ToStore: 1}, {PeerID: 2, ToStore: 2}},
	}
	cases := []testCase{
		{ // before step
			[]metapb.Replica{
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_IncomingVoter},
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			0,
			false,
			"IsNil",
		},
		{ // after step
			[]metapb.Replica{
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Learner},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Learner},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Voter},
			},
			4,
			true,
			"IsNil",
		},
		{ // miss peer id
			[]metapb.Replica{
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_IncomingVoter},
				{ID: 5, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			0,
			false,
			"NotNil",
		},
		{ // miss container id
			[]metapb.Replica{
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_IncomingVoter},
				{ID: 1, StoreID: 5, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			0,
			false,
			"NotNil",
		},
		{ // miss peer id
			[]metapb.Replica{
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 5, StoreID: 1, Role: metapb.ReplicaRole_Learner},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Learner},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Voter},
			},
			0,
			false,
			"NotNil",
		},
		{ // change is not atomic
			[]metapb.Replica{
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_IncomingVoter},
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Learner},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Learner},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			0,
			false,
			"NotNil",
		},
		{ // change is not atomic
			[]metapb.Replica{
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Voter},
			},
			0,
			false,
			"NotNil",
		},
		{ // there are other peers in the joint state
			[]metapb.Replica{
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_IncomingVoter},
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
				{ID: 5, StoreID: 5, Role: metapb.ReplicaRole_IncomingVoter},
			},
			0,
			false,
			"NotNil",
		},
		{ // there are other peers in the joint state
			[]metapb.Replica{
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Learner},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Learner},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Voter},
				{ID: 5, StoreID: 5, Role: metapb.ReplicaRole_IncomingVoter},
				{ID: 6, StoreID: 6, Role: metapb.ReplicaRole_DemotingVoter},
			},
			4,
			false,
			"NotNil",
		},
		{ // demote leader
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_IncomingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			0,
			false,
			"NotNil",
		},
	}
	desc := "leave joint state, " +
		"promote learner peer 3 on container 3 to voter, promote learner peer 4 on container 4 to voter, " +
		"demote voter peer 1 on container 1 to learner, demote voter peer 2 on container 2 to learner"
	checkStep(t, cpl, desc, cases)
}

func checkStep(t *testing.T, step OpStep, desc string, cases []testCase) {
	assert.Equal(t, desc, step.String())
	for _, tc := range cases {
		resource := core.NewCachedShard(&metadata.ShardWithRWLock{
			Shard: metapb.Shard{ID: 1, Replicas: tc.Peers}}, &tc.Peers[0])
		assert.Equal(t, tc.ConfVerChanged, step.ConfVerChanged(resource))
		assert.Equal(t, tc.IsFinish, step.IsFinish(resource))
		switch tc.CheckSafety {
		case "NotNil":
			assert.NotNil(t, step.CheckSafety(resource))
		case "IsNil":
			assert.Nil(t, step.CheckSafety(resource))
		}
	}
}
