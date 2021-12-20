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
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/util/fileutil"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/task"
	"github.com/matrixorigin/matrixcube/vfs"
)

func runReplicaSnapshotTest(t *testing.T,
	fn func(t *testing.T, r *replica, fs vfs.FS), fs vfs.FS) {
	defer leaktest.AfterTest(t)()
	defer vfs.ReportLeakedFD(fs, t)
	m := mem.NewStorage()
	defer m.Close()
	logger := log.GetPanicZapLogger()
	ldb := logdb.NewKVLogDB(m, logger)
	lr := NewLogReader(logger, 1, 1, ldb)
	fp := fs.PathJoin(snapshotterTestDir, "snapshot")
	fs.RemoveAll(snapshotterTestDir)
	if err := fs.MkdirAll(fp, 0777); err != nil {
		panic(err)
	}
	defer fs.RemoveAll(snapshotterTestDir)
	replicaSnapshotDir := func(shardID uint64, replicaID uint64) string {
		return fp
	}
	snapshotter := newSnapshotter(1, 1, logger, replicaSnapshotDir, ldb, fs)
	replicaRec := Replica{ID: 1, ContainerID: 100}
	shard := Shard{ID: 1, Replicas: []Replica{replicaRec}}
	dsMem := mem.NewStorage()
	base := kv.NewBaseStorage(dsMem, fs)
	ds := kv.NewKVDataStorage(base, nil)
	defer ds.Close()

	assert.NoError(t, ds.SaveShardMetadata([]meta.ShardMetadata{
		{ShardID: 1, LogIndex: 100, Metadata: meta.ShardLocalState{Shard: shard}},
	}))
	assert.NoError(t, ds.Sync([]uint64{1}))

	sm := newStateMachine(logger, ds, ldb, shard, replicaRec, nil, nil)
	sm.updateAppliedIndexTerm(100, 1)
	r := &replica{
		startedC: make(chan struct{}),
		store: &store{
			workerPool: newWorkerPool(logger, ldb, nil, 96),
		},
		actions:     task.New(32),
		storeID:     100,
		logger:      logger,
		logdb:       ldb,
		sm:          sm,
		snapshotter: snapshotter,
		shardID:     1,
		replica:     replicaRec,
		lr:          lr,
	}
	r.setStarted()
	fn(t, r, fs)
}

func TestReplicaSnapshotCanBeCreated(t *testing.T) {
	fn := func(t *testing.T, r *replica, fs vfs.FS) {
		ss, created, err := r.createSnapshot()
		if err != nil {
			t.Fatalf("failed to create snapshot %v", err)
		}
		assert.Equal(t, uint64(100), ss.Metadata.Index)
		assert.True(t, created)

		var si meta.SnapshotInfo
		protoc.MustUnmarshal(&si, ss.Data)
		env := snapshot.NewSSEnv(r.snapshotter.rootDirFunc,
			1, 1, ss.Metadata.Index, si.Extra, snapshot.CreatingMode, r.snapshotter.fs)
		env.FinalizeIndex(ss.Metadata.Index)
		snapshotDir := env.GetFinalDir()
		if _, err := fs.Stat(snapshotDir); vfs.IsNotExist(err) {
			t.Errorf("snapshot final dir not created, %v", err)
		}
		dbf := fs.PathJoin(snapshotDir, "db.data")
		if _, err := fs.Stat(dbf); vfs.IsNotExist(err) {
			t.Errorf("snapshot data file not created, %v", err)
		}
		_, err = r.logdb.GetSnapshot(1)
		assert.Equal(t, logdb.ErrNoSnapshot, err)
	}
	fs := vfs.GetTestFS()
	runReplicaSnapshotTest(t, fn, fs)
}

// other related tests
// TestApplyInitialSnapshot
// TestApplyReceivedSnapshot
func TestReplicaSnapshotCanBeApplied(t *testing.T) {
	fn := func(t *testing.T, r *replica, fs vfs.FS) {
		// setup key range and old metadata
		r.store.updateShardKeyRange(r.getShard().Group, r.getShard())
		r.aware = newTestShardAware(0)
		r.aware.Created(r.getShard())

		// update shard
		replicaRec := Replica{ID: 1, ContainerID: 100}
		shard := Shard{ID: 1, Replicas: []Replica{replicaRec}, Start: []byte{1}, End: []byte{2}}
		assert.NoError(t, r.sm.dataStorage.SaveShardMetadata([]meta.ShardMetadata{
			{ShardID: 1, LogIndex: 100, Metadata: meta.ShardLocalState{Shard: shard}},
		}))

		ss, created, err := r.createSnapshot()
		if err != nil {
			t.Fatalf("failed to create snapshot %v", err)
		}
		assert.Equal(t, uint64(100), ss.Metadata.Index)
		assert.True(t, created)

		// reset the data storage
		dsMem := mem.NewStorage()
		base := kv.NewBaseStorage(dsMem, fs)
		ds := kv.NewKVDataStorage(base, nil)
		defer ds.Close()

		r.sm = newStateMachine(r.logger, ds, r.logdb, shard, replicaRec, nil, nil)
		_, err = r.sm.dataStorage.GetInitialStates()
		assert.NoError(t, err)
		persistentLogIndex, err := r.getPersistentLogIndex()
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), persistentLogIndex)

		r.replica = Replica{}
		assert.NoError(t, r.applySnapshot(ss))
		r.handleAction(make([]interface{}, readyBatchSize))

		// applySnapshot will have the persistentLogIndex value updated
		persistentLogIndex, err = r.getPersistentLogIndex()
		assert.NoError(t, err)
		assert.Equal(t, uint64(100), persistentLogIndex)

		assert.Equal(t, r.replica, replicaRec)
		assert.Equal(t, ss.Metadata.Index, r.sm.metadataMu.index)
		assert.Equal(t, ss.Metadata.Term, r.sm.metadataMu.term)
		assert.Equal(t, shard, r.sm.metadataMu.shard)
		assert.Equal(t, r.aware.(*testShardAware).getShardByIndex(0), r.getShard())
		assert.Equal(t, shard, r.store.searchShard(shard.Group, shard.Start))

		sms, err := r.sm.dataStorage.GetInitialStates()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(sms))
		assert.Equal(t, shard, sms[0].Metadata.Shard)

		env := r.snapshotter.getRecoverSnapshotEnv(ss)
		exist, err := fileutil.Exist(env.GetFinalDir(), fs)
		assert.NoError(t, err)
		assert.False(t, exist)
	}
	fs := vfs.GetTestFS()
	runReplicaSnapshotTest(t, fn, fs)
}

func TestCreatingTheSameSnapshotAgainIsTolerated(t *testing.T) {
	fn := func(t *testing.T, r *replica, fs vfs.FS) {
		ss1, created, err := r.createSnapshot()
		assert.Equal(t, uint64(100), ss1.Metadata.Index)
		assert.NoError(t, err)
		assert.True(t, created)

		var si1 meta.SnapshotInfo
		protoc.MustUnmarshal(&si1, ss1.Data)
		env1 := snapshot.NewSSEnv(r.snapshotter.rootDirFunc,
			1, 1, ss1.Metadata.Index, si1.Extra, snapshot.CreatingMode, r.snapshotter.fs)
		env1.FinalizeIndex(ss1.Metadata.Index)
		snapshotDir1 := env1.GetFinalDir()
		if _, err := fs.Stat(snapshotDir1); vfs.IsNotExist(err) {
			t.Errorf("snapshot final dir not created, %v", err)
		}

		ss2, created, err := r.createSnapshot()
		assert.Equal(t, uint64(100), ss2.Metadata.Index)
		assert.NoError(t, err)
		assert.True(t, created)

		var si2 meta.SnapshotInfo
		protoc.MustUnmarshal(&si2, ss2.Data)
		env2 := snapshot.NewSSEnv(r.snapshotter.rootDirFunc,
			1, 1, ss2.Metadata.Index, si2.Extra, snapshot.CreatingMode, r.snapshotter.fs)
		env2.FinalizeIndex(ss2.Metadata.Index)
		snapshotDir2 := env2.GetFinalDir()
		if _, err := fs.Stat(snapshotDir2); vfs.IsNotExist(err) {
			t.Errorf("snapshot final dir not created, %v", err)
		}

		assert.NotEqual(t, snapshotDir1, snapshotDir2)
	}
	fs := vfs.GetTestFS()
	runReplicaSnapshotTest(t, fn, fs)
}

func TestCreatingOutOfDateSnapshotWillCausePanic(t *testing.T) {
	fn := func(t *testing.T, r *replica, fs vfs.FS) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("failed to trigger panic")
			}
		}()
		ss := raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: 200,
				Term:  200,
			},
		}
		assert.NoError(t, r.lr.CreateSnapshot(ss))
		r.createSnapshot()
	}
	fs := vfs.GetTestFS()
	runReplicaSnapshotTest(t, fn, fs)
}

func TestSnapshotCompactionWithMatchedPersistentLogIndex(t *testing.T) {
	testSnapshotCompaction(t, 200, true)
}

func TestSnapshotCompactionWithMismatchedPersistentLogIndex(t *testing.T) {
	testSnapshotCompaction(t, 199, false)
}

func testSnapshotCompaction(t *testing.T, index uint64, matched bool) {
	fn := func(t *testing.T, r *replica, fs vfs.FS) {
		// setup key range and old metadata
		r.store.updateShardKeyRange(r.getShard().Group, r.getShard())
		r.aware = newTestShardAware(0)
		r.aware.Created(r.getShard())

		// update shard
		replicaRec := Replica{ID: 1, ContainerID: 100}
		shard := Shard{ID: 1, Replicas: []Replica{replicaRec}, Start: []byte{1}, End: []byte{2}}
		assert.NoError(t, r.sm.dataStorage.SaveShardMetadata([]meta.ShardMetadata{
			{ShardID: 1, LogIndex: 100, Metadata: meta.ShardLocalState{Shard: shard}},
		}))
		ss1, created, err := r.createSnapshot()
		if err != nil {
			t.Fatalf("failed to create snapshot %v", err)
		}
		assert.Equal(t, uint64(100), ss1.Metadata.Index)
		assert.True(t, created)

		assert.NoError(t, r.sm.dataStorage.SaveShardMetadata([]meta.ShardMetadata{
			{ShardID: 1, LogIndex: 200, Metadata: meta.ShardLocalState{Shard: shard}},
		}))
		r.sm.updateAppliedIndexTerm(200, 2)
		ss2, created, err := r.createSnapshot()
		if err != nil {
			t.Fatalf("failed to create snapshot %v", err)
		}
		assert.Equal(t, uint64(200), ss2.Metadata.Index)
		assert.True(t, created)

		rd1 := raft.Ready{Snapshot: ss1}
		rd2 := raft.Ready{Snapshot: ss2}
		wc := r.logdb.NewWorkerContext()
		assert.NoError(t, r.logdb.SaveRaftState(1, 1, rd1, wc))
		wc.Reset()
		assert.NoError(t, r.logdb.SaveRaftState(1, 1, rd2, wc))
		snapshots, err := r.logdb.GetAllSnapshots(r.shardID)
		assert.NoError(t, err)
		assert.Equal(t, []raftpb.Snapshot{ss1, ss2}, snapshots)

		env1 := r.snapshotter.getRecoverSnapshotEnv(ss1)
		env2 := r.snapshotter.getRecoverSnapshotEnv(ss2)
		assert.True(t, env1.FinalDirExists())
		assert.True(t, env2.FinalDirExists())

		assert.NoError(t, r.snapshotCompaction(ss2, index))

		// when matched, both snapshot images are suppose to be removed
		// otherwise, the latest image should be kept
		// the latest snapshot record is always kept
		assert.False(t, env1.FinalDirExists())
		if matched {
			assert.False(t, env2.FinalDirExists())
		} else {
			assert.True(t, env2.FinalDirExists())
		}
		snapshots, err = r.logdb.GetAllSnapshots(r.shardID)
		assert.NoError(t, err)
		assert.Equal(t, []raftpb.Snapshot{ss2}, snapshots)
	}
	fs := vfs.GetTestFS()
	runReplicaSnapshotTest(t, fn, fs)
}
