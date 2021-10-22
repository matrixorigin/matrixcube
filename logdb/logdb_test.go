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
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/vfs"
)

var (
	testShardID uint64 = 101
	testPeerID  uint64 = 202
)

func getTestMetadataStorage(fs vfs.FS) storage.MetadataStorage {
	opts := &cpebble.Options{
		FS: vfs.NewPebbleFS(fs),
	}
	st, err := pebble.NewStorage("test-data", fs, opts)
	if err != nil {
		panic(err)
	}
	return st
}

func runLogDBTest(t *testing.T, tf func(t *testing.T, db *KVLogDB), fs vfs.FS) {
	ms := getTestMetadataStorage(fs)
	defer ms.Close()
	db := NewKVLogDB(ms, log.GetDefaultZapLogger())
	tf(t, db)
}

func TestLogDBGetMaxIndexReturnsNoSavedLogErrorWhenMaxIndexIsNotSaved(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		if _, err := db.getMaxIndex(testShardID, testPeerID); !errors.Is(err, ErrNoSavedLog) {
			t.Fatalf("failed to return the expected error")
		}
	}
	fs := vfs.NewMemFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBGetMaxIndex(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		rd := raft.Ready{
			Entries:   []raftpb.Entry{{Index: 4, Term: 1}, {Index: 5, Term: 1}, {Index: 6, Term: 1}},
			HardState: raftpb.HardState{Commit: 4, Term: 1, Vote: 2},
		}
		if err := db.SaveRaftState(testShardID, testPeerID, rd); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		index, err := db.getMaxIndex(testShardID, testPeerID)
		if err != nil {
			t.Fatalf("failed to get max inde")
		}
		if index != 6 {
			t.Errorf("unexpected max index %d, want 6", index)
		}
	}
	fs := vfs.NewMemFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBReadRaftStateReturnsNoSavedLogErrorWhenStateIsNeverSaved(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		if _, err := db.ReadRaftState(testShardID, testPeerID); !errors.Is(err, ErrNoSavedLog) {
			t.Fatalf("failed to return the expected error")
		}
	}
	fs := vfs.NewMemFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBSaveRaftState(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		rd := raft.Ready{
			Entries:   []raftpb.Entry{{Index: 4, Term: 1}, {Index: 5, Term: 1}, {Index: 6, Term: 1}},
			HardState: raftpb.HardState{Commit: 4, Term: 1, Vote: 2},
		}
		if err := db.SaveRaftState(testShardID, testPeerID, rd); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		rs, err := db.ReadRaftState(testShardID, testPeerID)
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
	fs := vfs.NewMemFS()
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
		if err := db.SaveRaftState(testShardID, testPeerID, rd); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		var ents []raftpb.Entry
		ents, rs, err := db.IterateEntries(ents, 0, testShardID, testPeerID, 4, 7, math.MaxUint64)
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
	fs := vfs.NewMemFS()
	runLogDBTest(t, tf, fs)
}

func TestLogDBRemoveEntriesTo(t *testing.T) {
	tf := func(t *testing.T, db *KVLogDB) {
		rd := raft.Ready{
			Entries:   []raftpb.Entry{{Index: 4, Term: 1}, {Index: 5, Term: 1}, {Index: 6, Term: 1}},
			HardState: raftpb.HardState{Commit: 4, Term: 1, Vote: 2},
		}
		if err := db.SaveRaftState(testShardID, testPeerID, rd); err != nil {
			t.Fatalf("failed to save raft state, %v", err)
		}
		if err := db.RemoveEntriesTo(testShardID, testPeerID, 5); err != nil {
			t.Fatalf("remove entries to failed, %v", err)
		}
		var ents []raftpb.Entry
		ents, _, err := db.IterateEntries(ents, 0, testShardID, testPeerID, 5, 6, math.MaxUint64)
		if err != raft.ErrUnavailable {
			t.Errorf("failed to remove entries, %v, %v", err, ents)
		}
		ents, _, err = db.IterateEntries(ents, 0, testShardID, testPeerID, 6, 7, math.MaxUint64)
		if err != nil {
			t.Fatalf("failed to get entry, %v", err)
		}
		if len(ents) != 1 {
			t.Errorf("unexpected entry count")
		}
	}
	fs := vfs.NewMemFS()
	runLogDBTest(t, tf, fs)
}
