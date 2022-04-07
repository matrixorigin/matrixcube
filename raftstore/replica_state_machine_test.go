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

	cpebble "github.com/cockroachdb/pebble"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor/simple"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestStateMachineApplyContextCanBeInitialized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	req := rpcpb.RequestBatch{
		Header: rpcpb.RequestBatchHeader{
			ID: []byte{0x1, 0x2, 0x3},
		},
		Requests: []rpcpb.Request{
			{
				ID:         []byte{100, 200, 200},
				Type:       rpcpb.Write,
				Key:        []byte{101, 202, 203},
				CustomType: uint64(rpcpb.CmdReserved) + 100,
				Cmd:        []byte{200, 201, 202},
			},
		},
	}
	entry := raftpb.Entry{
		Index: 10001,
		Data:  protoc.MustMarshal(&req),
	}

	ctx := newApplyContext()
	ctx.initialize(entry)
	assert.Equal(t, entry.Index, ctx.index)
	assert.Empty(t, ctx.metrics)
	assert.Empty(t, ctx.v2cc)
	assert.Equal(t, ctx.req, req)
}

func TestStateMachineApplyContextCanBeInitializedForConfigChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
	ctx := newApplyContext()
	ctx.initialize(entry)
	assert.Equal(t, entry.Index, ctx.index)
	assert.Empty(t, ctx.metrics)
	assert.Equal(t, batch, ctx.req)
	assert.Equal(t, cc.AsV2(), ctx.v2cc)
}

func runSimpleStateMachineTest(t *testing.T,
	f func(sm *stateMachine), h replicaResultHandler) {
	l := log.GetDefaultZapLogger(zap.OnFatal(zapcore.WriteThenPanic))
	shard := Shard{ID: 100}
	fs := vfs.NewMemFS()
	defer vfs.ReportLeakedFD(fs, t)
	defer leaktest.AfterTest(t)()
	opts := &cpebble.Options{
		FS: vfs.NewPebbleFS(fs),
	}
	st, err := pebble.NewStorage("test-data", nil, opts)
	require.NoError(t, err)
	defer st.Close()
	executor := simple.NewSimpleKVExecutor(st)
	base := kv.NewBaseStorage(st, fs)
	ds := kv.NewKVDataStorage(base, executor)
	sm := newStateMachine(l, ds, nil, shard, Replica{ID: 100}, h, nil)
	f(sm)
}

func TestStateMachineCanUpdateShard(t *testing.T) {
	f := func(sm *stateMachine) {
		assert.Equal(t, uint64(100), sm.getShard().ID)
		shard := Shard{ID: 200}
		sm.updateShard(shard)
		assert.Equal(t, uint64(200), sm.getShard().ID)
	}
	runSimpleStateMachineTest(t, f, nil)
}

func TestStateMachineSetShardState(t *testing.T) {
	f := func(sm *stateMachine) {
		assert.Equal(t, metapb.ShardState_Running, sm.getShard().State)
		sm.setShardState(metapb.ShardState_Destroyed)
		assert.Equal(t, metapb.ShardState_Destroyed, sm.getShard().State)
	}
	runSimpleStateMachineTest(t, f, nil)
}

func TestStateMachineRemovedStateCanBeSet(t *testing.T) {
	f := func(sm *stateMachine) {
		assert.False(t, sm.isRemoved())
		sm.setRemoved()
		assert.True(t, sm.isRemoved())
	}
	runSimpleStateMachineTest(t, f, nil)
}

func TestStateMachineCanSetAppliedIndexTerm(t *testing.T) {
	f := func(sm *stateMachine) {
		index, term := sm.getAppliedIndexTerm()
		assert.Equal(t, uint64(0), index)
		assert.Equal(t, uint64(0), term)
		sm.updateAppliedIndexTerm(100, 200)
		index, term = sm.getAppliedIndexTerm()
		assert.Equal(t, uint64(100), index)
		assert.Equal(t, uint64(200), term)
	}
	runSimpleStateMachineTest(t, f, nil)
}

func TestStateMachineIsConfigChangeEntry(t *testing.T) {
	tests := []struct {
		typ    raftpb.EntryType
		result bool
	}{
		{raftpb.EntryConfChange, true},
		{raftpb.EntryConfChangeV2, true},
		{raftpb.EntryNormal, false},
	}

	for _, tt := range tests {
		entry := raftpb.Entry{
			Type: tt.typ,
		}
		assert.Equal(t, tt.result, isConfigChangeEntry(entry))
	}
}

func TestStateMachineCheckEntryIndex(t *testing.T) {
	f := func(sm *stateMachine) {
		{
			sm.updateAppliedIndexTerm(1, 1)
			entry1 := raftpb.Entry{Index: 2, Term: 1}
			sm.checkEntryIndexTerm(entry1)
		}
		{
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("failed to trigger panic")
				}
			}()
			entry2 := raftpb.Entry{Index: 3, Term: 1}
			sm.checkEntryIndexTerm(entry2)
		}
	}
	runSimpleStateMachineTest(t, f, nil)
}

func TestStateMachineCheckEntryTerm(t *testing.T) {
	f := func(sm *stateMachine) {
		{
			sm.updateAppliedIndexTerm(1, 1)
			entry1 := raftpb.Entry{Index: 2, Term: 1}
			sm.checkEntryIndexTerm(entry1)
			sm.updateAppliedIndexTerm(2, 1)
			entry2 := raftpb.Entry{Index: 3, Term: 2}
			sm.checkEntryIndexTerm(entry2)
			sm.updateAppliedIndexTerm(3, 2)
		}
		{
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("failed to trigger panic")
				}
			}()
			entry3 := raftpb.Entry{Index: 4, Term: 1}
			sm.checkEntryIndexTerm(entry3)
		}
	}
	runSimpleStateMachineTest(t, f, nil)
}

type testReplicaResultHandler struct {
	appliedIndex uint64
	notified     uint64
	id           []byte
	resp         rpcpb.ResponseBatch
	isConfChange bool
}

var _ replicaResultHandler = (*testReplicaResultHandler)(nil)

func (t *testReplicaResultHandler) handleApplyResult(a applyResult) {
	t.appliedIndex = a.index
}

func (t *testReplicaResultHandler) notifyPendingProposal(id []byte,
	resp rpcpb.ResponseBatch, isConfChange bool) {
	t.id = id
	t.resp = resp
	t.isConfChange = isConfChange
	t.notified++
}

func TestStateMachineApplyNoopEntry(t *testing.T) {
	h := &testReplicaResultHandler{}
	f := func(sm *stateMachine) {
		entry := raftpb.Entry{
			Index: 1,
			Term:  1,
			Type:  raftpb.EntryNormal,
		}
		index, term := sm.getAppliedIndexTerm()
		assert.Equal(t, uint64(0), index)
		assert.Equal(t, uint64(0), term)
		sm.applyCommittedEntries([]raftpb.Entry{entry})
		index, term = sm.getAppliedIndexTerm()
		assert.Equal(t, uint64(1), index)
		assert.Equal(t, uint64(1), term)
		assert.Equal(t, uint64(1), h.appliedIndex)
	}
	runSimpleStateMachineTest(t, f, h)
}

// TODO: add checks to ensure responses are expected
// TODO: add checks to ensure epoch is checked

func TestStateMachineApplyNormalEntries(t *testing.T) {
	h := &testReplicaResultHandler{}
	f := func(sm *stateMachine) {
		key1 := []byte("test-key")
		value1 := []byte("test-value")
		batch1 := rpcpb.RequestBatch{
			Header: rpcpb.RequestBatchHeader{
				ID:      []byte{0x1, 0x2, 0x3},
				ShardID: 1,
			},
			Requests: []rpcpb.Request{
				{
					ID:         []byte{100, 200, 200},
					Type:       rpcpb.Write,
					Key:        key1,
					CustomType: uint64(rpcpb.CmdReserved) + 1,
					Cmd:        value1,
				},
			},
		}
		entry1 := raftpb.Entry{
			Index: 1,
			Term:  1,
			Type:  raftpb.EntryNormal,
			Data:  protoc.MustMarshal(&batch1),
		}
		key2 := []byte("test-key-2")
		value2 := []byte("test-value-2")
		batch2 := rpcpb.RequestBatch{
			Header: rpcpb.RequestBatchHeader{
				ID:      []byte{0x4, 0x5, 0x6},
				ShardID: 1,
			},
			Requests: []rpcpb.Request{
				{
					ID:         []byte{220, 230, 235},
					Type:       rpcpb.Write,
					Key:        key2,
					CustomType: uint64(rpcpb.CmdReserved) + 1,
					Cmd:        value2,
				},
			},
		}
		entry2 := raftpb.Entry{
			Index: 2,
			Term:  1,
			Type:  raftpb.EntryNormal,
			Data:  protoc.MustMarshal(&batch2),
		}
		index, term := sm.getAppliedIndexTerm()
		assert.Equal(t, uint64(0), index)
		assert.Equal(t, uint64(0), term)
		sm.applyCommittedEntries([]raftpb.Entry{entry1, entry2})
		index, term = sm.getAppliedIndexTerm()
		assert.Equal(t, uint64(2), index)
		assert.Equal(t, uint64(1), term)
		assert.Equal(t, uint64(2), h.appliedIndex)

		assert.Equal(t, uint64(2), h.notified)
		assert.Equal(t, batch2.Header.ID, h.id)
		assert.Equal(t, false, h.isConfChange)
		require.Equal(t, 1, len(h.resp.Responses))
		assert.Equal(t, []byte("OK"), h.resp.Responses[0].Value)

		readContext := newReadContext()
		sr := storage.Request{
			Key:     key1,
			CmdType: uint64(rpcpb.CmdReserved) + 2,
		}
		readContext.reset(sm.metadataMu.shard, sr)
		data, err := sm.dataStorage.Read(readContext)
		assert.NoError(t, err)
		assert.Equal(t, value1, data)

		sr = storage.Request{
			Key:     key2,
			CmdType: uint64(rpcpb.CmdReserved) + 2,
		}
		readContext.reset(sm.metadataMu.shard, sr)
		data, err = sm.dataStorage.Read(readContext)
		assert.NoError(t, err)
		assert.Equal(t, value2, data)
	}
	runSimpleStateMachineTest(t, f, h)
}

func TestStateMachineApplyConfigChange(t *testing.T) {
	h := &testReplicaResultHandler{}
	f := func(sm *stateMachine) {
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
		index, term := sm.getAppliedIndexTerm()
		assert.Equal(t, uint64(1), index)
		assert.Equal(t, uint64(1), term)
		assert.Equal(t, uint64(1), h.appliedIndex)

		assert.Equal(t, uint64(1), h.notified)
		assert.Equal(t, batch.Header.ID, h.id)
		assert.Equal(t, true, h.isConfChange)
		require.Equal(t, 1, len(h.resp.Responses))
		assert.Equal(t, rpcpb.CmdConfigChange, rpcpb.InternalCmd(h.resp.Responses[0].CustomType))
		// TODO: add a check to test whether the error field in the resp is empty

		shard := sm.getShard()
		require.Equal(t, 1, len(shard.Replicas))
		assert.Equal(t, uint64(100), shard.Replicas[0].ID)
		assert.Equal(t, uint64(200), shard.Replicas[0].StoreID)
	}
	runSimpleStateMachineTest(t, f, h)
}

func TestStateMachineRejectsStaleEpochEntries(t *testing.T) {
	h := &testReplicaResultHandler{}
	f := func(sm *stateMachine) {
		batch := newTestAdminRequestBatch(string([]byte{0x1, 0x2, 0x3}), 0,
			rpcpb.CmdConfigChange,
			protoc.MustMarshal(&rpcpb.ConfigChangeRequest{}))
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
		index, term := sm.getAppliedIndexTerm()
		assert.Equal(t, uint64(1), index)
		assert.Equal(t, uint64(1), term)
		assert.Equal(t, uint64(1), h.appliedIndex)

		assert.Equal(t, uint64(1), h.notified)
		assert.Equal(t, batch.Header.ID, h.id)
		assert.Equal(t, true, h.isConfChange)
		require.Equal(t, 0, len(h.resp.Responses))
		assert.Equal(t, "stale command", h.resp.Header.Error.Message)
	}
	runSimpleStateMachineTest(t, f, h)
}

func TestStateMachineUpdatesAppliedIndexAfterSkippingEntries(t *testing.T) {
	h := &testReplicaResultHandler{}
	f := func(sm *stateMachine) {
		batch := newTestAdminRequestBatch(string([]byte{0x1, 0x2, 0x3}), 0,
			rpcpb.CmdConfigChange,
			protoc.MustMarshal(&rpcpb.ConfigChangeRequest{}))
		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  100,
			Context: protoc.MustMarshal(&batch),
		}
		entry1 := raftpb.Entry{
			Index: 1,
			Term:  1,
			Type:  raftpb.EntryNormal,
		}
		entry2 := raftpb.Entry{
			Index: 2,
			Term:  1,
			Type:  raftpb.EntryNormal,
		}
		entry3 := raftpb.Entry{
			Index: 3,
			Term:  1,
			Type:  raftpb.EntryConfChange,
			Data:  protoc.MustMarshal(&cc),
		}

		// mark as splitted
		sm.metadataMu.splited = true
		assert.False(t, sm.canApply(entry1))
		assert.False(t, sm.canApply(entry2))
		sm.applyCommittedEntries([]raftpb.Entry{entry1, entry2, entry3})
		index, term := sm.getAppliedIndexTerm()
		assert.Equal(t, uint64(3), index)
		assert.Equal(t, uint64(1), term)
	}
	runSimpleStateMachineTest(t, f, h)
}

func TestStateMachineApplyCommittedEntriesAllowEmptyInput(t *testing.T) {
	h := &testReplicaResultHandler{}
	f := func(sm *stateMachine) {
		sm.applyCommittedEntries(nil)
		sm.applyCommittedEntries([]raftpb.Entry{})
	}
	runSimpleStateMachineTest(t, f, h)
}

func TestStateMachineCannotApplyInDestroyingAfterRestart(t *testing.T) {
	h := &testReplicaResultHandler{}
	f := func(sm *stateMachine) {
		sm.metadataMu.removed = false
		sm.metadataMu.splited = false
		sm.metadataMu.shard.State = metapb.ShardState_Destroying

		assert.False(t, sm.canApply(raftpb.Entry{}))
		assert.True(t, sm.canApply(raftpb.Entry{Type: raftpb.EntryConfChange}))
	}
	runSimpleStateMachineTest(t, f, h)
}

func TestStateMachineCannotApplyIfRemovedFlagSetted(t *testing.T) {
	h := &testReplicaResultHandler{}
	f := func(sm *stateMachine) {
		sm.metadataMu.removed = true

		assert.False(t, sm.canApply(raftpb.Entry{}))
	}
	runSimpleStateMachineTest(t, f, h)
}
