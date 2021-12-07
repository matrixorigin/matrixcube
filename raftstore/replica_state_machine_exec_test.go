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
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
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

func TestDoExecSplit(t *testing.T) {
	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	pr := newTestReplica(Shard{ID: 1, Epoch: Epoch{Version: 2}, Start: []byte{1}, End: []byte{10}, Replicas: []Replica{{ID: 2}}}, Replica{ID: 2}, s)
	ctx := newApplyContext()

	ch := make(chan bool)
	checkPanicFn := func() {
		defer func() {
			if err := recover(); err != nil {
				ch <- true
			} else {
				ch <- false
			}
		}()
		pr.sm.execAdminRequest(ctx)
	}

	// check split panic
	ctx.req.AdminRequest.Splits = &rpc.BatchSplitRequest{}
	ctx.req.AdminRequest.CmdType = rpc.AdminCmdType_BatchSplit
	go checkPanicFn()
	assert.True(t, <-ch)

	// check range not match
	ctx.req.AdminRequest.Splits = &rpc.BatchSplitRequest{Requests: []rpc.SplitRequest{{Start: []byte{1}}, {End: []byte{5}}}}
	go checkPanicFn()
	assert.True(t, <-ch)

	// check key not in range
	ctx.req.AdminRequest.Splits = &rpc.BatchSplitRequest{Requests: []rpc.SplitRequest{{Start: []byte{1}, End: []byte{20}}, {End: []byte{10}}}}
	go checkPanicFn()
	assert.True(t, <-ch)

	// check range discontinuity
	ctx.req.AdminRequest.Splits = &rpc.BatchSplitRequest{Requests: []rpc.SplitRequest{{Start: []byte{1}, End: []byte{5}, NewShardID: 2, NewReplicas: []Replica{{ID: 200}}}, {Start: []byte{6}, End: []byte{10}}}}
	go checkPanicFn()
	assert.True(t, <-ch)

	// s1 -> s2+s3
	ctx.index = 100
	ctx.req.AdminRequest.Splits = &rpc.BatchSplitRequest{
		Requests: []rpc.SplitRequest{
			{Start: []byte{1}, End: []byte{5}, NewShardID: 2, NewReplicas: []Replica{{ID: 200}}},
			{Start: []byte{5}, End: []byte{10}, NewShardID: 3, NewReplicas: []Replica{{ID: 300}}},
		},
	}
	resp, err := pr.sm.execAdminRequest(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(resp.AdminResponse.Splits.Shards))
	assert.Equal(t, pr.getShard().Start, resp.AdminResponse.Splits.Shards[0].Start)
	assert.Equal(t, ctx.req.AdminRequest.Splits.Requests[0].End, resp.AdminResponse.Splits.Shards[0].End)
	assert.Equal(t, ctx.req.AdminRequest.Splits.Requests[1].Start, resp.AdminResponse.Splits.Shards[1].Start)
	assert.Equal(t, pr.getShard().End, resp.AdminResponse.Splits.Shards[1].End)
	assert.False(t, pr.sm.canApply(raftpb.Entry{}))
	assert.True(t, pr.sm.metadataMu.splited)

	pr.sm.dataStorage.GetInitialStates()
	assert.NoError(t, pr.sm.dataStorage.Sync([]uint64{1, 2, 3}))
	idx, err := pr.sm.dataStorage.GetPersistentLogIndex(1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), idx)

	idx, err = pr.sm.dataStorage.GetPersistentLogIndex(2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), idx)

	idx, err = pr.sm.dataStorage.GetPersistentLogIndex(3)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), idx)

	metadata, err := pr.sm.dataStorage.GetInitialStates()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(metadata))
	assert.Equal(t, meta.ReplicaState_Normal, metadata[0].Metadata.State)
	assert.Equal(t, metapb.ResourceState_Destroying, metadata[0].Metadata.Shard.State)
	assert.Equal(t, meta.ReplicaState_Normal, metadata[1].Metadata.State)
	assert.Equal(t, []byte{1}, metadata[1].Metadata.Shard.Start)
	assert.Equal(t, []byte{5}, metadata[1].Metadata.Shard.End)
	assert.Equal(t, meta.ReplicaState_Normal, metadata[2].Metadata.State)
	assert.Equal(t, []byte{5}, metadata[2].Metadata.Shard.Start)
	assert.Equal(t, []byte{10}, metadata[2].Metadata.Shard.End)
}

func TestDoExecCompactLog(t *testing.T) {
	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	pr := newTestReplica(Shard{ID: 1, Epoch: Epoch{Version: 2}, Replicas: []Replica{{ID: 2}}}, Replica{ID: 2}, s)
	ctx := newApplyContext()

	err := pr.sm.logdb.SaveRaftState(pr.shardID, pr.replicaID, raft.Ready{
		Entries: []raftpb.Entry{
			{
				Index: 1,
				Term:  1,
			},
			{
				Index: 2,
				Term:  1,
			},
			{
				Index: 3,
				Term:  1,
			},
		},
		HardState: raftpb.HardState{
			Commit: 3,
			Term:   1,
		},
	}, pr.sm.logdb.NewWorkerContext())
	assert.NoError(t, err)

	ctx.req.AdminRequest.CompactLog = &rpc.CompactLogRequest{
		CompactIndex: 2,
	}
	ctx.req.AdminRequest.CmdType = rpc.AdminCmdType_CompactLog

	_, err = pr.sm.execAdminRequest(ctx)
	assert.NoError(t, err)
	result := applyResult{
		shardID:     pr.sm.shardID,
		adminResult: ctx.adminResult,
		metrics:     ctx.metrics,
	}
	assert.NotNil(t, result.adminResult)

	pr.lr.SetRange(1, 100)
	pr.handleApplyResult(result)
	state, err := pr.sm.logdb.ReadRaftState(pr.shardID, pr.replicaID)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), state.FirstIndex)
	assert.Equal(t, uint64(1), state.EntryCount)
}
