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
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/stats"
	"github.com/matrixorigin/matrixcube/util/leaktest"
)

func TestStateMachineAddNode(t *testing.T) {
	// these two tests are the same
	TestStateMachineApplyConfigChange(t)
}

func TestStateMachineAddLearner(t *testing.T) {
	h := &testReplicaResultHandler{}
	f := func(sm *stateMachine) {
		batch := newTestAdminRequestBatch(string([]byte{0x1, 0x2, 0x3}), 0,
			rpc.AdminCmdType_ConfigChange,
			protoc.MustMarshal(&rpc.ConfigChangeRequest{
				ChangeType: metapb.ConfigChangeType_AddLearnerNode,
				Replica: metapb.Replica{
					ID:          100,
					ContainerID: 200,
				},
			}))
		batch.Header.ShardID = 1
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

		batch := newTestAdminRequestBatch(string([]byte{0x1, 0x2, 0x3}), 0,
			rpc.AdminCmdType_ConfigChange,
			protoc.MustMarshal(&rpc.ConfigChangeRequest{
				ChangeType: metapb.ConfigChangeType_AddNode,
				Replica: metapb.Replica{
					ID:          100,
					ContainerID: 200,
				},
			}))
		batch.Header.ShardID = 1
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

func testStateMachineRemoveNode(t *testing.T, role metapb.ReplicaRole, removeReplica Replica) {
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

		batch := newTestAdminRequestBatch(string([]byte{0x1, 0x2, 0x3}), 0,
			rpc.AdminCmdType_ConfigChange,
			protoc.MustMarshal(&rpc.ConfigChangeRequest{
				ChangeType: metapb.ConfigChangeType_RemoveNode,
				Replica:    removeReplica,
			}))
		batch.Header.ShardID = 1
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
		if removeReplica.ID == 100 {
			require.Equal(t, 0, len(shard.Replicas))
		} else {
			require.Equal(t, 1, len(shard.Replicas))
		}

	}
	runSimpleStateMachineTest(t, f, h)
}

func TestStateMachineRemoveNode(t *testing.T) {
	testStateMachineRemoveNode(t, metapb.ReplicaRole_Learner, metapb.Replica{
		ID:          100,
		ContainerID: 200,
	})
	testStateMachineRemoveNode(t, metapb.ReplicaRole_Voter, metapb.Replica{
		ID:          100,
		ContainerID: 200,
	})
	testStateMachineRemoveNode(t, metapb.ReplicaRole_Voter, metapb.Replica{
		ID:          1000,
		ContainerID: 2000,
	})
}

// TODO: add tests to cover failed config change

func TestDoExecSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, cancel := newTestStore(t)
	defer cancel()

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
	ctx.req = newTestAdminRequestBatch("", 0, rpc.AdminCmdType_BatchSplit, protoc.MustMarshal(&rpc.BatchSplitRequest{}))
	go checkPanicFn()
	assert.True(t, <-ch)

	// check range not match
	ctx.req = newTestAdminRequestBatch("", 0, rpc.AdminCmdType_BatchSplit, protoc.MustMarshal(&rpc.BatchSplitRequest{Requests: []rpc.SplitRequest{{Start: []byte{1}}, {End: []byte{5}}}}))
	go checkPanicFn()
	assert.True(t, <-ch)

	// check key not in range
	ctx.req = newTestAdminRequestBatch("", 0, rpc.AdminCmdType_BatchSplit, protoc.MustMarshal(&rpc.BatchSplitRequest{Requests: []rpc.SplitRequest{{Start: []byte{1}, End: []byte{20}}, {End: []byte{10}}}}))
	go checkPanicFn()
	assert.True(t, <-ch)

	// check range discontinuity
	ctx.req = newTestAdminRequestBatch("", 0, rpc.AdminCmdType_BatchSplit, protoc.MustMarshal(&rpc.BatchSplitRequest{Requests: []rpc.SplitRequest{{Start: []byte{1}, End: []byte{5}, NewShardID: 2, NewReplicas: []Replica{{ID: 200}}}, {Start: []byte{6}, End: []byte{10}}}}))
	go checkPanicFn()
	assert.True(t, <-ch)

	// s1 -> s2+s3
	ctx.index = 100
	ctx.req = newTestAdminRequestBatch("", 0, rpc.AdminCmdType_BatchSplit, protoc.MustMarshal(&rpc.BatchSplitRequest{
		Requests: []rpc.SplitRequest{
			{Start: []byte{1}, End: []byte{5}, NewShardID: 2, NewReplicas: []Replica{{ID: 200}}},
			{Start: []byte{5}, End: []byte{10}, NewShardID: 3, NewReplicas: []Replica{{ID: 300}}},
		},
	}))
	resp, err := pr.sm.execAdminRequest(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Responses))
	adminResp := resp.GetBatchSplitResponse()
	adminReq := ctx.req.GetBatchSplitRequest()
	assert.Equal(t, 2, len(adminResp.Shards))
	assert.Equal(t, pr.getShard().Start, adminResp.Shards[0].Start)
	assert.Equal(t, adminReq.Requests[0].End, adminResp.Shards[0].End)
	assert.Equal(t, adminReq.Requests[1].Start, adminResp.Shards[1].Start)
	assert.Equal(t, pr.getShard().End, adminResp.Shards[1].End)
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

type testDataStorage struct {
	persistentLogIndex uint64
}

func (t *testDataStorage) Close() error                                     { panic("not implemented") }
func (t *testDataStorage) Stats() stats.Stats                               { panic("not implemented") }
func (t *testDataStorage) NewWriteBatch() storage.Resetable                 { panic("not implemented") }
func (t *testDataStorage) CreateSnapshot(shardID uint64, path string) error { panic("not implemented") }
func (t *testDataStorage) ApplySnapshot(shardID uint64, path string) error  { panic("not implemented") }
func (t *testDataStorage) Write(storage.WriteContext) error                 { panic("not implemented") }
func (t *testDataStorage) Read(storage.ReadContext) ([]byte, error)         { panic("not implemented") }
func (t *testDataStorage) GetInitialStates() ([]meta.ShardMetadata, error) {
	return nil, nil
}
func (t *testDataStorage) GetPersistentLogIndex(shardID uint64) (uint64, error) {
	return t.persistentLogIndex, nil
}
func (t *testDataStorage) SaveShardMetadata([]meta.ShardMetadata) error { panic("not implemented") }
func (t *testDataStorage) RemoveShard(shard meta.Shard, removeData bool) error {
	panic("not implemented")
}
func (t *testDataStorage) Sync([]uint64) error { panic("not implemented") }
func (t *testDataStorage) SplitCheck(shard meta.Shard, size uint64) (currentApproximateSize uint64,
	currentApproximateKeys uint64, splitKeys [][]byte, ctx []byte, err error) {
	panic("not implemented")
}
func (t *testDataStorage) Split(old meta.ShardMetadata, news []meta.ShardMetadata, ctx []byte) error {
	panic("not implemented")
}

func TestDoExecCompactLog(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, cancel := newTestStore(t)
	defer cancel()

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
			{
				Index: 4,
				Term:  1,
			},
			{
				Index: 5,
				Term:  1,
			},
		},
		HardState: raftpb.HardState{
			Commit: 5,
			Term:   1,
		},
	}, pr.sm.logdb.NewWorkerContext())
	assert.NoError(t, err)

	ctx.req = newTestAdminRequestBatch("", 0, rpc.AdminCmdType_CompactLog, protoc.MustMarshal(&rpc.CompactLogRequest{
		CompactIndex: 4,
	}))
	ds := &testDataStorage{
		persistentLogIndex: uint64(2),
	}
	pr.sm.dataStorage = ds

	// our KV based data storage requires the GetInitialStates() to be invoked
	// first
	_, err = ds.GetInitialStates()
	assert.NoError(t, err)

	_, err = pr.sm.execAdminRequest(ctx)
	assert.NoError(t, err)
	result := applyResult{
		shardID:     pr.sm.shardID,
		adminResult: ctx.adminResult,
		metrics:     ctx.metrics,
	}
	assert.NotNil(t, result.adminResult)
	pr.lr.SetRange(0, 100)
	pr.handleApplyResult(result)
	// actual compaction is done as an action by the event worker
	pr.handleAction(make([]interface{}, readyBatchSize))
	state, err := pr.sm.logdb.ReadRaftState(pr.shardID, pr.replicaID, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), state.FirstIndex)
	assert.Equal(t, uint64(3), state.EntryCount)
}
