// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
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

package logdb

import (
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/errors"
	cpebble "github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/keys"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/vfs"
)

var (
	testShardID    uint64 = 101
	testReplicaID  uint64 = 202
	testStorageDir        = "/tmp/test_data_safe_to_delete"
)

func getTestStorage(fs vfs.FS) storage.KVStorage {
	opts := &cpebble.Options{
		FS: vfs.NewPebbleFS(fs),
	}
	_ = fs.RemoveAll(testStorageDir)
	st, err := pebble.NewStorage(testStorageDir, nil, opts)
	if err != nil {
		panic(err)
	}
	return st
}

func runLogDBTest(t *testing.T, tf func(t *testing.T, db *KVLogDB), fs vfs.FS) {
	defer func() {
		assert.NoError(t, fs.RemoveAll(testStorageDir))
	}()
	defer vfs.ReportLeakedFD(fs, t)
	defer leaktest.AfterTest(t)()
	kv := getTestStorage(fs)
	defer kv.Close()
	db := NewKVLogDB(kv, log.GetPanicZapLogger())
	tf(t, db)
}

func TestLogDBGetMaxIndexReturnsNoSavedLogErrorWhenMaxIndexIsNotSaved(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		if _, err := db.getMaxIndex(testShardID, testReplicaID); !errors.Is(err, ErrNoSavedLog) {
			t.Fatalf("failed to return the expected error")
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBGetSnapshot(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		v, err := db.GetSnapshot(testShardID)
		assert.Equal(t, ErrNoSnapshot, err)
		assert.Empty(t, v)
		rd := raft.Ready{
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{Index: 100},
			},
		}
		wc := db.NewWorkerContext()
		if err := db.SaveRaftState(testShardID, 100, rd, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		v, err = db.GetSnapshot(testShardID)
		assert.NoError(t, err)
		assert.Equal(t, rd.Snapshot, v)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBGetAllSnapshots(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		v, err := db.GetSnapshot(testShardID)
		assert.Equal(t, ErrNoSnapshot, err)
		assert.Empty(t, v)
		rd1 := raft.Ready{
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{Index: 100},
			},
		}
		rd2 := raft.Ready{
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{Index: 150},
			},
		}
		rd3 := raft.Ready{
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{Index: 200},
			},
		}
		wc := db.NewWorkerContext()
		if err := db.SaveRaftState(testShardID, 100, rd1, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		wc.Reset()
		if err := db.SaveRaftState(testShardID, 100, rd2, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		wc.Reset()
		if err := db.SaveRaftState(testShardID, 100, rd3, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		results, err := db.GetAllSnapshots(testShardID)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(results))
		assert.Equal(t, []raftpb.Snapshot{rd1.Snapshot, rd2.Snapshot, rd3.Snapshot}, results)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBRemoveSnapshot(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		rd1 := raft.Ready{
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{Index: 100},
			},
		}
		rd2 := raft.Ready{
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{Index: 200},
			},
		}
		wc := db.NewWorkerContext()
		defer wc.Close()
		if err := db.SaveRaftState(testShardID, 100, rd1, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		wc.Reset()
		if err := db.SaveRaftState(testShardID, 100, rd2, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		assert.NoError(t, db.RemoveSnapshot(testShardID, rd1.Snapshot.Metadata.Index))

		v, err := db.GetAllSnapshots(testShardID)
		assert.NoError(t, err)
		assert.Equal(t, []raftpb.Snapshot{rd2.Snapshot}, v)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBRemovingTheMostRecentSnapshotWillPanic(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("panic not triggered")
			}
		}()
		rd1 := raft.Ready{
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{Index: 100},
			},
		}
		wc := db.NewWorkerContext()
		defer wc.Close()
		if err := db.SaveRaftState(testShardID, 100, rd1, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		assert.NoError(t, db.RemoveSnapshot(testShardID, rd1.Snapshot.Metadata.Index))
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBGetSnapshotReturnsTheMostRecentSnapshot(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		rd1 := raft.Ready{
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{Index: 99},
			},
		}
		wc := db.NewWorkerContext()
		if err := db.SaveRaftState(testShardID-1, rd1.Snapshot.Metadata.Index, rd1, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		wc.Reset()
		rd2 := raft.Ready{
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{Index: 99},
			},
		}
		rd3 := raft.Ready{
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{Index: 100},
			},
		}
		if err := db.SaveRaftState(testShardID, rd2.Snapshot.Metadata.Index, rd2, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		wc.Reset()
		if err := db.SaveRaftState(testShardID, rd3.Snapshot.Metadata.Index, rd3, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		wc.Reset()
		rd4 := raft.Ready{
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{Index: 200},
			},
		}
		if err := db.SaveRaftState(testShardID+1, rd4.Snapshot.Metadata.Index, rd4, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		wc.Reset()
		v, err := db.GetSnapshot(testShardID)
		assert.NoError(t, err)
		assert.Equal(t, rd3.Snapshot, v)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBGetMaxIndex(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		rd := raft.Ready{
			Entries:   []raftpb.Entry{{Index: 4, Term: 1}, {Index: 5, Term: 1}, {Index: 6, Term: 1}},
			HardState: raftpb.HardState{Commit: 4, Term: 1, Vote: 2},
		}
		wc := db.NewWorkerContext()
		if err := db.SaveRaftState(testShardID, testReplicaID, rd, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		index, err := db.getMaxIndex(testShardID, testReplicaID)
		if err != nil {
			t.Fatalf("failed to get max inde")
		}
		if index != 6 {
			t.Errorf("unexpected max index %d, want 6", index)
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBReadRaftStateReturnsNoSavedLogErrorWhenStateIsNeverSaved(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		if _, err := db.ReadRaftState(testShardID, testReplicaID, 0); !errors.Is(err, ErrNoSavedLog) {
			t.Fatalf("failed to return the expected error")
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBSaveRaftState(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		rd := raft.Ready{
			Entries:   []raftpb.Entry{{Index: 4, Term: 1}, {Index: 5, Term: 1}, {Index: 6, Term: 1}},
			HardState: raftpb.HardState{Commit: 4, Term: 1, Vote: 2},
		}
		wc := db.NewWorkerContext()
		if err := db.SaveRaftState(testShardID, testReplicaID, rd, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		rs, err := db.ReadRaftState(testShardID, testReplicaID, 0)
		if errors.Is(err, ErrNoSavedLog) {
			t.Fatalf("failed to get raft state, %v", err)
		}
		if rs.FirstIndex != 4 {
			t.Errorf("first index %d, want 4", rs.FirstIndex)
		}
		if rs.EntryCount != 3 {
			t.Errorf("entry count %d, want 3", rs.EntryCount)
		}
		if !reflect.DeepEqual(&rs.State, &rd.HardState) {
			t.Errorf("hard state changed")
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBReadRaftState(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		rd := raft.Ready{
			Entries:   []raftpb.Entry{{Index: 4, Term: 1}, {Index: 5, Term: 1}, {Index: 6, Term: 1}},
			HardState: raftpb.HardState{Commit: 4, Term: 1, Vote: 2},
		}
		wc := db.NewWorkerContext()
		if err := db.SaveRaftState(testShardID, testReplicaID, rd, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		rs, err := db.ReadRaftState(testShardID, testReplicaID, 5)
		if errors.Is(err, ErrNoSavedLog) {
			t.Fatalf("failed to get raft state, %v", err)
		}
		assert.Equal(t, uint64(5), rs.FirstIndex)
		assert.Equal(t, uint64(2), rs.EntryCount)
		assert.Equal(t, rd.HardState, rs.State)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBIterateEntries(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		rd := raft.Ready{
			Entries:   []raftpb.Entry{{Index: 4, Term: 1}, {Index: 5, Term: 1}, {Index: 6, Term: 1}},
			HardState: raftpb.HardState{Commit: 4, Term: 1, Vote: 2},
		}
		size := uint64(0)
		for _, e := range rd.Entries {
			size += uint64(e.Size())
		}
		wc := db.NewWorkerContext()
		if err := db.SaveRaftState(testShardID, testReplicaID, rd, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		var ents []raftpb.Entry
		ents, rs, err := db.IterateEntries(ents, 0, testShardID, testReplicaID, 4, 7, math.MaxUint64)
		if err != nil {
			t.Fatalf("failed to get entries, %v", err)
		}
		if len(ents) != 3 {
			t.Errorf("failed to get all entries")
		}
		if rs != size {
			t.Errorf("unexpected size")
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBRemoveEntriesTo(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		rd := raft.Ready{
			Entries:   []raftpb.Entry{{Index: 4, Term: 1}, {Index: 5, Term: 1}, {Index: 6, Term: 1}},
			HardState: raftpb.HardState{Commit: 4, Term: 1, Vote: 2},
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 5,
					Term:  1,
				},
			},
		}
		wc := db.NewWorkerContext()
		if err := db.SaveRaftState(testShardID, testReplicaID, rd, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		if err := db.RemoveEntriesTo(testShardID, testReplicaID, 5); err != nil {
			t.Fatalf("remove entries to failed, %v", err)
		}
		var ents []raftpb.Entry
		ents, _, err := db.IterateEntries(ents, 0, testShardID, testReplicaID, 5, 6, math.MaxUint64)
		if err != raft.ErrUnavailable {
			t.Errorf("failed to remove entries, %v, %v", err, ents)
		}
		ents, _, err = db.IterateEntries(ents, 0, testShardID, testReplicaID, 6, 7, math.MaxUint64)
		if err != nil {
			t.Fatalf("failed to get entry, %v", err)
		}
		if len(ents) != 1 {
			t.Errorf("unexpected entry count")
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBRemovingTooManyLogEntriesWillPanic(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("failed to trigger panic")
			}
		}()
		rd := raft.Ready{
			Entries: []raftpb.Entry{{Index: 4, Term: 1}, {Index: 5, Term: 1}, {Index: 6, Term: 1}},
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 4,
					Term:  1,
				},
			},
		}
		wc := db.NewWorkerContext()
		if err := db.SaveRaftState(testShardID, testReplicaID, rd, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		if err := db.RemoveEntriesTo(testShardID, testReplicaID, 5); err != nil {
			t.Fatalf("remove entries to failed, %v", err)
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBRemoveEntriesWithoutSnapshotWillPanic(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("failed to trigger panic")
			}
		}()
		rd := raft.Ready{
			Entries: []raftpb.Entry{{Index: 4, Term: 1}, {Index: 5, Term: 1}, {Index: 6, Term: 1}},
		}
		wc := db.NewWorkerContext()
		if err := db.SaveRaftState(testShardID, testReplicaID, rd, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		if err := db.RemoveEntriesTo(testShardID, testReplicaID, 5); err != nil {
			t.Fatalf("remove entries to failed, %v", err)
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBRemoveReplicaData(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		rd1 := raft.Ready{
			Entries:   []raftpb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}},
			HardState: raftpb.HardState{Term: 1, Vote: 3, Commit: 100},
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 4,
					Term:  1,
				},
			},
		}
		wc := db.NewWorkerContext()
		if err := db.SaveRaftState(testShardID, testReplicaID, rd1, wc); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		assert.NoError(t, db.RemoveReplicaData(testShardID))
		first, length, err := db.getRange(testShardID, testReplicaID, 0)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), first)
		assert.Equal(t, uint64(0), length)

		_, err = db.getMaxIndex(testShardID, testReplicaID)
		assert.Equal(t, ErrNoSavedLog, err)

		snapshots, err := db.GetAllSnapshots(testShardID)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(snapshots))

		v, err := db.ms.Get(keys.GetHardStateKey(testShardID, testReplicaID, nil))
		assert.NoError(t, err)
		assert.Equal(t, 0, len(v))
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}
