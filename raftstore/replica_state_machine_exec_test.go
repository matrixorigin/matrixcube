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
	"github.com/matrixorigin/matrixcube/pb/hlcpb"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/stats"
	"github.com/matrixorigin/matrixcube/util/buf"
	"github.com/matrixorigin/matrixcube/util/hlc"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/uuid"
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
		batch := newTestAdminRequestBatch(string([]byte{0x1, 0x2, 0x3}), 0,
			rpcpb.CmdConfigChange,
			protoc.MustMarshal(&rpcpb.ConfigChangeRequest{
				ChangeType: metapb.ConfigChangeType_AddLearnerNode,
				Replica: metapb.Replica{
					ID:      100,
					StoreID: 200,
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
		assert.Equal(t, uint64(200), shard.Replicas[0].StoreID)
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
					ID:      100,
					StoreID: 200,
					Role:    metapb.ReplicaRole_Learner,
				},
			},
		}
		sm.updateShard(shard)

		batch := newTestAdminRequestBatch(string([]byte{0x1, 0x2, 0x3}), 0,
			rpcpb.CmdConfigChange,
			protoc.MustMarshal(&rpcpb.ConfigChangeRequest{
				ChangeType: metapb.ConfigChangeType_AddNode,
				Replica: metapb.Replica{
					ID:      100,
					StoreID: 200,
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
		assert.Equal(t, uint64(200), shard.Replicas[0].StoreID)
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
					ID:      100,
					StoreID: 200,
					Role:    role,
				},
			},
		}
		sm.updateShard(shard)

		batch := newTestAdminRequestBatch(string([]byte{0x1, 0x2, 0x3}), 0,
			rpcpb.CmdConfigChange,
			protoc.MustMarshal(&rpcpb.ConfigChangeRequest{
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
		ID:      100,
		StoreID: 200,
	})
	testStateMachineRemoveNode(t, metapb.ReplicaRole_Voter, metapb.Replica{
		ID:      100,
		StoreID: 200,
	})
	testStateMachineRemoveNode(t, metapb.ReplicaRole_Voter, metapb.Replica{
		ID:      1000,
		StoreID: 2000,
	})
}

// TODO: add tests to cover failed config change

func TestDoExecSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, cancel := newTestStore(t)
	defer cancel()

	pr := newTestReplica(Shard{ID: 1, Epoch: Epoch{Generation: 2}, Start: []byte{1}, End: []byte{10}, Replicas: []Replica{{ID: 2}}}, Replica{ID: 2}, s)
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
		_, err := pr.sm.execAdminRequest(ctx)
		require.NoError(t, err)
	}

	// check split panic
	ctx.req = newTestAdminRequestBatch("", 0, rpcpb.CmdBatchSplit, protoc.MustMarshal(&rpcpb.BatchSplitRequest{}))
	go checkPanicFn()
	assert.True(t, <-ch)

	// check range not match
	ctx.req = newTestAdminRequestBatch("", 0, rpcpb.CmdBatchSplit, protoc.MustMarshal(&rpcpb.BatchSplitRequest{Requests: []rpcpb.SplitRequest{{Start: []byte{1}}, {End: []byte{5}}}}))
	go checkPanicFn()
	assert.True(t, <-ch)

	// check key not in range
	ctx.req = newTestAdminRequestBatch("", 0, rpcpb.CmdBatchSplit, protoc.MustMarshal(&rpcpb.BatchSplitRequest{Requests: []rpcpb.SplitRequest{{Start: []byte{1}, End: []byte{20}}, {End: []byte{10}}}}))
	go checkPanicFn()
	assert.True(t, <-ch)

	// check range discontinuity
	ctx.req = newTestAdminRequestBatch("", 0, rpcpb.CmdBatchSplit, protoc.MustMarshal(&rpcpb.BatchSplitRequest{Requests: []rpcpb.SplitRequest{{Start: []byte{1}, End: []byte{5}, NewShardID: 2, NewReplicas: []Replica{{ID: 200}}}, {Start: []byte{6}, End: []byte{10}}}}))
	go checkPanicFn()
	assert.True(t, <-ch)

	// s1 -> s2+s3
	ctx.index = 100
	ctx.req = newTestAdminRequestBatch("", 0, rpcpb.CmdBatchSplit, protoc.MustMarshal(&rpcpb.BatchSplitRequest{
		Requests: []rpcpb.SplitRequest{
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

	_, err = pr.sm.dataStorage.GetInitialStates()
	require.NoError(t, err)
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
	assert.Equal(t, metapb.ReplicaState_Normal, metadata[0].Metadata.State)
	assert.Equal(t, metapb.ShardState_Destroying, metadata[0].Metadata.Shard.State)
	assert.Equal(t, metapb.ReplicaState_Normal, metadata[1].Metadata.State)
	assert.Equal(t, []byte{1}, metadata[1].Metadata.Shard.Start)
	assert.Equal(t, []byte{5}, metadata[1].Metadata.Shard.End)
	assert.Equal(t, metapb.ReplicaState_Normal, metadata[2].Metadata.State)
	assert.Equal(t, []byte{5}, metadata[2].Metadata.Shard.Start)
	assert.Equal(t, []byte{10}, metadata[2].Metadata.Shard.End)
}

func TestDoExecUpdateLease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, cancel := newTestStore(t)
	defer cancel()

	pr := newTestReplica(Shard{ID: 1,
		Epoch:    Epoch{Generation: 2},
		Replicas: []Replica{{ID: 2}, {ID: 3}, {ID: 4}}},
		Replica{ID: 2}, s)
	ctx := newApplyContext()

	ctx.req = newTestAdminRequestBatch("", 0, rpcpb.CmdUpdateEpochLease, protoc.MustMarshal(&rpcpb.UpdateEpochLeaseRequest{
		ShardID: 1,
		Lease:   metapb.EpochLease{Epoch: 1, ReplicaID: 2},
	}))
	resp, err := pr.sm.execAdminRequest(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Responses))
	assert.Equal(t, &metapb.EpochLease{Epoch: 1, ReplicaID: 2}, pr.getLease())

	ctx.req = newTestAdminRequestBatch("", 0, rpcpb.CmdUpdateEpochLease, protoc.MustMarshal(&rpcpb.UpdateEpochLeaseRequest{
		ShardID: 1,
		Lease:   metapb.EpochLease{Epoch: 2, ReplicaID: 3},
	}))
	resp, err = pr.sm.execAdminRequest(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Responses))
	assert.Equal(t, &metapb.EpochLease{Epoch: 2, ReplicaID: 3}, pr.getLease())

	ctx.req = newTestAdminRequestBatch("", 0, rpcpb.CmdUpdateEpochLease, protoc.MustMarshal(&rpcpb.UpdateEpochLeaseRequest{
		ShardID: 1,
		Lease:   metapb.EpochLease{Epoch: 1, ReplicaID: 1},
	}))
	resp, err = pr.sm.execAdminRequest(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Responses))
	assert.Equal(t, &metapb.EpochLease{Epoch: 2, ReplicaID: 3}, pr.getLease())
}

type testDataStorage struct {
	persistentLogIndex uint64
	feature            storage.Feature
	counts             map[int]int
}

func (t *testDataStorage) Close() error                                     { panic("not implemented") }
func (t *testDataStorage) Stats() stats.Stats                               { panic("not implemented") }
func (t *testDataStorage) NewWriteBatch() storage.Resetable                 { panic("not implemented") }
func (t *testDataStorage) CreateSnapshot(shardID uint64, path string) error { panic("not implemented") }
func (t *testDataStorage) ApplySnapshot(shardID uint64, path string) error  { panic("not implemented") }
func (t *testDataStorage) Write(ctx storage.WriteContext) error {
	for range ctx.Batch().Requests {
		ctx.AppendResponse([]byte("OK"))
	}
	return nil
}
func (t *testDataStorage) Read(storage.ReadContext) ([]byte, error) { panic("not implemented") }
func (t *testDataStorage) GetInitialStates() ([]metapb.ShardMetadata, error) {
	t.counts = make(map[int]int)
	return nil, nil
}
func (t *testDataStorage) GetPersistentLogIndex(shardID uint64) (uint64, error) {
	return t.persistentLogIndex, nil
}
func (t *testDataStorage) SaveShardMetadata([]metapb.ShardMetadata) error { panic("not implemented") }
func (t *testDataStorage) RemoveShard(shard metapb.Shard, removeData bool) error {
	panic("not implemented")
}
func (t *testDataStorage) Sync([]uint64) error { panic("not implemented") }
func (t *testDataStorage) SplitCheck(shard metapb.Shard, size uint64) (currentApproximateSize uint64,
	currentApproximateKeys uint64, splitKeys [][]byte, ctx []byte, err error) {
	panic("not implemented")
}
func (t *testDataStorage) Split(old metapb.ShardMetadata, news []metapb.ShardMetadata, ctx []byte) error {
	panic("not implemented")
}
func (t *testDataStorage) Feature() storage.Feature {
	return t.feature
}

func (t *testDataStorage) UpdateTxnRecord(record txnpb.TxnRecord, wb storage.WriteContext) error {
	t.counts[int(rpcpb.CmdUpdateTxnRecord)]++
	return nil
}
func (t *testDataStorage) DeleteTxnRecord(txnRecordRouteKey, txnID []byte, wb storage.WriteContext) error {
	t.counts[int(rpcpb.CmdDeleteTxnRecord)]++
	return nil
}
func (t *testDataStorage) CommitWrittenData(originKey []byte, commitTS hlc.Timestamp, wb storage.WriteContext) error {
	t.counts[int(rpcpb.CmdCommitTxnData)]++
	return nil
}
func (t *testDataStorage) RollbackWrittenData(originKey []byte, metadata hlc.Timestamp, wb storage.WriteContext) error {
	t.counts[int(rpcpb.CmdRollbackTxnData)]++
	return nil
}
func (t *testDataStorage) CleanMVCCData(shard metapb.Shard, timestamp hlc.Timestamp, wb storage.WriteContext) error {
	t.counts[int(rpcpb.CmdCleanTxnMVCCData)]++
	return nil
}
func (t *testDataStorage) GetTxnRecord(txnRecordRouteKey, txnID []byte) (bool, txnpb.TxnRecord, error) {
	return false, txnpb.TxnRecord{}, nil
}
func (t *testDataStorage) GetCommitted(originKey []byte, timestamp hlc.Timestamp) (exist bool, data []byte, err error) {
	return false, nil, nil
}
func (t *testDataStorage) Get(originKey []byte, timestamp hlcpb.Timestamp) ([]byte, error) {
	return nil, nil
}
func (t *testDataStorage) Scan(startOriginKey, endOriginKey []byte, timestamp hlcpb.Timestamp, filter storage.UncommittedFilter, handler func(key, value []byte) (bool, error)) error {
	return nil
}
func (t *testDataStorage) GetUncommittedOrAnyHighCommitted(originKey []byte, timestamp hlc.Timestamp) (txnpb.TxnConflictData, error) {
	return txnpb.TxnConflictData{}, nil
}
func (t *testDataStorage) GetUncommittedOrAnyHighCommittedByRange(op txnpb.TxnOperation, timestamp hlc.Timestamp) ([]txnpb.TxnConflictData, error) {
	return nil, nil
}

func (t *testDataStorage) GetUncommittedMVCCMetadata(originKey []byte) (bool, txnpb.TxnConflictData, error) {
	return false, txnpb.TxnConflictData{}, nil
}
func (t *testDataStorage) GetUncommittedMVCCMetadataByRange(op txnpb.TxnOperation) ([]txnpb.TxnConflictData, error) {
	return nil, nil
}

func TestDoExecCompactLog(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, cancel := newTestStore(t)
	defer cancel()

	pr := newTestReplica(Shard{ID: 1, Epoch: Epoch{Generation: 2}, Replicas: []Replica{{ID: 2}}}, Replica{ID: 2}, s)
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

	ctx.req = newTestAdminRequestBatch("", 0, rpcpb.CmdCompactLog, protoc.MustMarshal(&rpcpb.CompactLogRequest{
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
	_, err = pr.handleAction(make([]interface{}, readyBatchSize))
	assert.NoError(t, err)
	state, err := pr.sm.logdb.ReadRaftState(pr.shardID, pr.replicaID, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), state.FirstIndex)
	assert.Equal(t, uint64(3), state.EntryCount)
}

func TestExecWriteRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, cancel := newTestStore(t)
	defer cancel()
	pr := newTestReplica(Shard{ID: 1, Replicas: []Replica{{ID: 2}}}, Replica{ID: 2}, s)
	ds := &testDataStorage{}
	_, err := ds.GetInitialStates()
	assert.NoError(t, err)

	pr.sm.dataStorage = ds
	pr.sm.transactionalDataStorage = ds

	cases := []struct {
		request         rpcpb.RequestBatch
		expectResponses [][]byte
		expectCounts    map[rpcpb.InternalCmd]int
	}{
		{
			request:         newTestRequestBatch(1, func(r *rpcpb.Request, i int) { r.CustomType = uint64(rpcpb.CmdReserved) + 1 }),
			expectResponses: [][]byte{[]byte("OK")},
		},
		{
			request: newTestRequestBatch(2, func(r *rpcpb.Request, i int) {
				if i == 0 {
					r.CustomType = uint64(rpcpb.CmdUpdateTxnRecord)
				} else {
					r.CustomType = uint64(rpcpb.CmdReserved) + 1
				}
			}),
			expectResponses: [][]byte{nil, []byte("OK")},
			expectCounts: map[rpcpb.InternalCmd]int{
				rpcpb.CmdUpdateTxnRecord: 1,
			},
		},
		{
			request: newTestRequestBatch(9, func(r *rpcpb.Request, i int) {
				switch i {
				case 0:
					r.CustomType = uint64(rpcpb.CmdUpdateTxnRecord)
				case 1:
					r.CustomType = uint64(rpcpb.CmdReserved) + 1
				case 2:
					r.CustomType = uint64(rpcpb.CmdDeleteTxnRecord)
				case 3:
					r.CustomType = uint64(rpcpb.CmdReserved) + 1
				case 4:
					r.CustomType = uint64(rpcpb.CmdCommitTxnData)
				case 5:
					r.CustomType = uint64(rpcpb.CmdReserved) + 1
				case 6:
					r.CustomType = uint64(rpcpb.CmdRollbackTxnData)
				case 7:
					r.CustomType = uint64(rpcpb.CmdReserved) + 1
				case 8:
					r.CustomType = uint64(rpcpb.CmdCleanTxnMVCCData)
				}
			}),
			expectResponses: [][]byte{nil, []byte("OK"), nil, []byte("OK"), nil, []byte("OK"), nil, []byte("OK"), nil},
			expectCounts: map[rpcpb.InternalCmd]int{
				rpcpb.CmdUpdateTxnRecord:  1,
				rpcpb.CmdDeleteTxnRecord:  1,
				rpcpb.CmdCommitTxnData:    1,
				rpcpb.CmdRollbackTxnData:  1,
				rpcpb.CmdCleanTxnMVCCData: 1,
			},
		},
	}

	for _, c := range cases {
		ctx := newApplyContext()
		ctx.req = c.request
		ds.counts = make(map[int]int)
		var responses [][]byte
		for _, resp := range pr.sm.execWriteRequest(ctx).Responses {
			responses = append(responses, resp.Value)
		}
		assert.Equal(t, c.expectResponses, responses)
		if c.expectCounts == nil {
			c.expectCounts = make(map[rpcpb.InternalCmd]int)
		}
		for k, v := range ds.counts {
			ev := c.expectCounts[rpcpb.InternalCmd(k)]
			assert.Equal(t, ev, v, "%s", rpcpb.InternalCmd(k).String())
		}
	}
}

func newTestRequestBatch(n int, builder func(*rpcpb.Request, int)) rpcpb.RequestBatch {
	rb := rpcpb.RequestBatch{
		Header: rpcpb.RequestBatchHeader{ID: uuid.NewV4().Bytes()}}
	for i := 0; i < n; i++ {
		req := rpcpb.Request{
			ID:   buf.Int2Bytes(i),
			Key:  buf.Int2Bytes(i),
			Type: rpcpb.Write,
		}
		builder(&req, i)
		rb.Requests = append(rb.Requests, req)
	}
	return rb
}
