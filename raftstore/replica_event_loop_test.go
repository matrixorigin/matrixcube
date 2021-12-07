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
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"
	trackerPkg "go.etcd.io/etcd/raft/v3/tracker"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/util/fileutil"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/task"
	"github.com/matrixorigin/matrixcube/vfs"
)

func getTestStorage() storage.KVStorage {
	fs := vfs.NewMemFS()
	opts := &cpebble.Options{
		FS: vfs.NewPebbleFS(fs),
	}
	st, err := pebble.NewStorage("test-data", nil, opts)
	if err != nil {
		panic(err)
	}
	return st
}

// TODO: we need this here largely because it is pretty difficult to write unit
// tests for the replica type when it has an injected store instance in it.
func getCloseableReplica() (*replica, func()) {
	l := log.GetDefaultZapLogger()
	r := Replica{}
	shardID := uint64(1)
	kv := getTestStorage()
	ldb := logdb.NewKVLogDB(kv, log.GetDefaultZapLogger())
	c := &raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         NewLogReader(l, 1, 1, ldb),
		MaxInflightMsgs: 100,
		CheckQuorum:     true,
		PreVote:         true,
	}
	rn, err := raft.NewRawNode(c)
	if err != nil {
		panic(err)
	}
	return &replica{
		logger:            l,
		replica:           r,
		shardID:           shardID,
		rn:                rn,
		logdb:             ldb,
		pendingProposals:  newPendingProposals(),
		incomingProposals: newProposalBatch(l, 0, shardID, r),
		pendingReads:      &readIndexQueue{shardID: shardID, logger: l},
		ticks:             task.New(32),
		messages:          task.New(32),
		requests:          task.New(32),
		actions:           task.New(32),
		feedbacks:         task.New(32),
		snapshotStatus:    task.New(32),
		items:             make([]interface{}, 1024),
		startedC:          make(chan struct{}),
		closedC:           make(chan struct{}),
		unloadedC:         make(chan struct{}),
		sm:                &stateMachine{},
	}, func() { kv.Close() }
}

func TestReplicaCanBeClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r, closer := getCloseableReplica()
	defer r.close()
	defer closer()
	// we just check whether the replica can be created and closed
	_, err := r.handleEvent(nil)
	assert.NoError(t, err)
}

func TestApplyInitialSnapshot(t *testing.T) {
	fn := func(t *testing.T, r *replica, fs vfs.FS) {
		ss, created, err := r.createSnapshot()
		if err != nil {
			t.Fatalf("failed to create snapshot %v", err)
		}
		assert.Equal(t, uint64(100), ss.Metadata.Index)
		assert.True(t, created)

		rd := raft.Ready{Snapshot: ss}
		assert.NoError(t, r.logdb.SaveRaftState(1, 1, rd, r.logdb.NewWorkerContext()))
		// reset the data storage
		dsMem := mem.NewStorage()
		base := kv.NewBaseStorage(dsMem, fs)
		ds := kv.NewKVDataStorage(base, nil)
		defer ds.Close()
		shard := Shard{ID: 1}
		replicaRec := Replica{ID: 1}
		r.sm = newStateMachine(r.logger, ds, r.logdb, shard, replicaRec, nil, nil)

		assert.False(t, r.initialized)
		hasEvent, err := r.handleEvent(r.logdb.NewWorkerContext())
		assert.NoError(t, err)
		assert.True(t, hasEvent)
		assert.True(t, r.initialized)
		assert.Equal(t, ss.Metadata.Index, r.sm.metadataMu.index)
		assert.Equal(t, ss.Metadata.Term, r.sm.metadataMu.term)
		assert.Equal(t, Shard{ID: 1}, r.sm.metadataMu.shard)

		sms, err := r.sm.dataStorage.GetInitialStates()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(sms))
		assert.Equal(t, shard, sms[0].Metadata.Shard)

		env := r.snapshotter.getRecoverSnapshotEnv(ss)
		exist, err := fileutil.Exist(env.GetFinalDir(), fs)
		assert.NoError(t, err)
		assert.False(t, exist)
		_, err = r.logdb.GetSnapshot(1)
		assert.Equal(t, logdb.ErrNoSnapshot, err)
	}
	fs := vfs.GetTestFS()
	runReplicaSnapshotTest(t, fn, fs)
}

func TestDoCheckCompactLog(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	pr := newTestReplica(Shard{ID: 1}, Replica{ID: 1}, s)
	pr.leaderID = 2

	// check not leader
	pr.doCheckLogCompact(nil, 0)
	assert.Equal(t, int64(0), pr.requests.Len())

	// check min replicated index > last
	pr.leaderID = 1
	hasPanic := false
	func() {
		defer func() {
			if err := recover(); err != nil {
				hasPanic = true
			}
		}()

		pr.doCheckLogCompact(map[uint64]trackerPkg.Progress{
			1: {Match: 101},
		}, 100)
	}()
	assert.True(t, hasPanic)

	// check minReplicatedIndex < firstIndex
	pr.sm.setFirstIndex(102)
	pr.doCheckLogCompact(map[uint64]trackerPkg.Progress{
		1: {Match: 101},
	}, 102)
	assert.Equal(t, int64(0), pr.requests.Len())

	// minReplicatedIndex-firstIndex <= CompactThreshold
	pr.store.cfg.Raft.RaftLog.CompactThreshold = 1
	pr.sm.setFirstIndex(100)
	pr.doCheckLogCompact(map[uint64]trackerPkg.Progress{
		1: {Match: 101},
	}, 102)
	assert.Equal(t, int64(0), pr.requests.Len())

	// force count
	pr.store.cfg.Raft.RaftLog.ForceCompactCount = 1
	pr.store.cfg.Raft.RaftLog.ForceCompactBytes = 1000
	pr.store.cfg.Raft.RaftLog.CompactThreshold = 1
	pr.stats.raftLogSizeHint = 0
	pr.sm.setFirstIndex(99)
	pr.appliedIndex = 101
	pr.doCheckLogCompact(map[uint64]trackerPkg.Progress{
		1: {Match: 101},
	}, 101)
	v, _ := pr.requests.Peek()
	req := &rpc.CompactLogRequest{}
	protoc.MustUnmarshal(req, v.(reqCtx).req.Cmd)
	assert.Equal(t, uint64(100), req.CompactIndex)

	// force bytes
	pr.store.cfg.Raft.RaftLog.ForceCompactCount = 1000
	pr.store.cfg.Raft.RaftLog.ForceCompactBytes = 1
	pr.store.cfg.Raft.RaftLog.CompactThreshold = 1
	pr.stats.raftLogSizeHint = 1
	pr.requests = task.New(32)
	pr.sm.setFirstIndex(99)
	pr.appliedIndex = 101
	pr.doCheckLogCompact(map[uint64]trackerPkg.Progress{
		1: {Match: 101},
	}, 101)
	v, _ = pr.requests.Peek()
	req.Reset()
	protoc.MustUnmarshal(req, v.(reqCtx).req.Cmd)
	assert.Equal(t, uint64(100), req.CompactIndex)
}
