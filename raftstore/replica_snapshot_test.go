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
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/util/fileutil"
	"github.com/matrixorigin/matrixcube/util/leaktest"
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
	shard := Shard{ID: 1}
	replicaRec := Replica{ID: 1}
	dsMem := mem.NewStorage()
	base := kv.NewBaseStorage(dsMem, fs)
	ds := kv.NewKVDataStorage(base, nil)
	defer ds.Close()

	assert.NoError(t, ds.SaveShardMetadata([]meta.ShardMetadata{
		{ShardID: 1, LogIndex: 100, Metadata: meta.ShardLocalState{Shard: shard}},
	}))
	assert.NoError(t, ds.Sync([]uint64{1}))

	sm := newStateMachine(logger, ds, ldb, shard, replicaRec, nil, nil)
	r := &replica{
		logger:      logger,
		logdb:       ldb,
		sm:          sm,
		snapshotter: snapshotter,
		shardID:     1,
		replica:     replicaRec,
		lr:          lr,
	}
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
		mf := fs.PathJoin(snapshotDir, snapshot.MetadataFilename)
		if _, err := fs.Stat(mf); vfs.IsNotExist(err) {
			t.Errorf("snapshot metadata file not created, %v", err)
		}
		dbf := fs.PathJoin(snapshotDir, "db.data")
		if _, err := fs.Stat(dbf); vfs.IsNotExist(err) {
			t.Errorf("snapshot data file not created, %v", err)
		}
		var ssFromDir raftpb.SnapshotMetadata
		if err := fileutil.GetFlagFileContent(snapshotDir,
			snapshot.MetadataFilename, &ssFromDir, fs); err != nil {
			t.Errorf("failed to get flag file content %v", err)
		}
		assert.Equal(t, ss.Metadata.Index, ssFromDir.Index)
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
		shard := Shard{ID: 1}
		replicaRec := Replica{ID: 1}
		r.sm = newStateMachine(r.logger, ds, r.logdb, shard, replicaRec, nil, nil)

		assert.NoError(t, r.applySnapshot(ss))
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
