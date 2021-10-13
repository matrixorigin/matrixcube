// Copyright 2021 MatrixOrigin.
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

package raftstore

import (
	"testing"

	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpc"
)

func TestStateMachineAddNode(t *testing.T) {
	// these two tests are the same
	TestStateMachineApplyConfigChange(t)
}

func TestStateMachineAddLearner(t *testing.T) {
	h := &testReplicaResultHandler{}
	f := func(sm *stateMachine) {
		batch := rpc.RequestBatch{
			Header: rpc.RequestBatchHeader{
				ID:      []byte{0x1, 0x2, 0x3},
				ShardID: 1,
			},
			AdminRequest: rpc.AdminRequest{
				CmdType: rpc.AdminCmdType_ConfigChange,
				ConfigChange: &rpc.ConfigChangeRequest{
					ChangeType: metapb.ConfigChangeType_AddLearnerNode,
					Replica: metapb.Replica{
						ID:          100,
						ContainerID: 200,
					},
				},
			},
		}
		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  100,
			Context: protoc.MustMarshal(&batch),
		}
		entry := raftpb.Entry{
			Index: 1,
			Term:  1,
			Type:  raftpb.EntryConfChange,
			Data:  protoc.MustMarshal(&cc),
		}
		sm.applyCommittedEntries([]raftpb.Entry{entry})
		shard := sm.getShard()
		require.Equal(t, 1, len(shard.Replicas))
		assert.Equal(t, uint64(100), shard.Replicas[0].ID)
		assert.Equal(t, uint64(200), shard.Replicas[0].ContainerID)
		assert.Equal(t, metapb.ReplicaRole_Learner, shard.Replicas[0].Role)
	}
	runSimpleStateMachineTest(t, f, h)
}

func TestStateMachinePromoteLeanerToVoter(t *testing.T) {
	h := &testReplicaResultHandler{}
	f := func(sm *stateMachine) {
		shard := Shard{
			Replicas: []metapb.Replica{
				{
					ID:          100,
					ContainerID: 200,
					Role:        metapb.ReplicaRole_Learner,
				},
			},
		}
		sm.updateShard(shard)
		batch := rpc.RequestBatch{
			Header: rpc.RequestBatchHeader{
				ID:      []byte{0x1, 0x2, 0x3},
				ShardID: 1,
			},
			AdminRequest: rpc.AdminRequest{
				CmdType: rpc.AdminCmdType_ConfigChange,
				ConfigChange: &rpc.ConfigChangeRequest{
					ChangeType: metapb.ConfigChangeType_AddNode,
					Replica: metapb.Replica{
						ID:          100,
						ContainerID: 200,
					},
				},
			},
		}
		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  100,
			Context: protoc.MustMarshal(&batch),
		}
		entry := raftpb.Entry{
			Index: 1,
			Term:  1,
			Type:  raftpb.EntryConfChange,
			Data:  protoc.MustMarshal(&cc),
		}
		sm.applyCommittedEntries([]raftpb.Entry{entry})
		shard = sm.getShard()
		require.Equal(t, 1, len(shard.Replicas))
		assert.Equal(t, uint64(100), shard.Replicas[0].ID)
		assert.Equal(t, uint64(200), shard.Replicas[0].ContainerID)
		assert.Equal(t, metapb.ReplicaRole_Voter, shard.Replicas[0].Role)
	}
	runSimpleStateMachineTest(t, f, h)
}

func testStateMachineRemoveNode(t *testing.T, role metapb.ReplicaRole) {
	h := &testReplicaResultHandler{}
	f := func(sm *stateMachine) {
		shard := Shard{
			Replicas: []metapb.Replica{
				{
					ID:          100,
					ContainerID: 200,
					Role:        role,
				},
			},
		}
		sm.updateShard(shard)
		batch := rpc.RequestBatch{
			Header: rpc.RequestBatchHeader{
				ID:      []byte{0x1, 0x2, 0x3},
				ShardID: 1,
			},
			AdminRequest: rpc.AdminRequest{
				CmdType: rpc.AdminCmdType_ConfigChange,
				ConfigChange: &rpc.ConfigChangeRequest{
					ChangeType: metapb.ConfigChangeType_RemoveNode,
					Replica: metapb.Replica{
						ID:          100,
						ContainerID: 200,
					},
				},
			},
		}
		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  100,
			Context: protoc.MustMarshal(&batch),
		}
		entry := raftpb.Entry{
			Index: 1,
			Term:  1,
			Type:  raftpb.EntryConfChange,
			Data:  protoc.MustMarshal(&cc),
		}
		sm.applyCommittedEntries([]raftpb.Entry{entry})
		shard = sm.getShard()
		require.Equal(t, 0, len(shard.Replicas))
	}
	runSimpleStateMachineTest(t, f, h)
}

func TestStateMachineRemoveNode(t *testing.T) {
	testStateMachineRemoveNode(t, metapb.ReplicaRole_Learner)
	testStateMachineRemoveNode(t, metapb.ReplicaRole_Voter)
}

// TODO: add tests to cover failed config change
