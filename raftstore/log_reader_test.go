// Copyright 2015 The etcd Authors
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

// some tests in this file are ported from etcd raft

package raftstore

import (
	"math"
	"reflect"
	"testing"

	cpebble "github.com/cockroachdb/pebble"
	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/vfs"
)

var (
	testShardID uint64 = 101
	testPeerID  uint64 = 202
)

func getNewLogReaderTestDB(entries []pb.Entry, fs vfs.FS) *pebble.Storage {
	opts := &cpebble.Options{
		FS: vfs.NewPebbleFS(fs),
	}
	st, err := pebble.NewStorage("st_test", nil, opts)
	if err != nil {
		panic(err)
	}
	return st
}

func getTestLogReader(entries []pb.Entry,
	fs vfs.FS) (*LogReader, storage.WriteBatchCreator, func()) {
	db := getNewLogReaderTestDB(entries, fs)
	ldb := logdb.NewKVLogDB(db, log.GetDefaultZapLogger())
	rd := raft.Ready{
		Entries: entries,
	}
	wc := logdb.NewWorkerContext(db)
	if err := ldb.SaveRaftState(testShardID, testPeerID, rd, wc); err != nil {
		panic(err)
	}
	ls := NewLogReader(nil, testShardID, testPeerID, ldb)
	ls.markerIndex = entries[0].Index
	ls.markerTerm = entries[0].Term
	ls.length = uint64(len(entries))
	return ls, db, func() { db.Close() }
}

func TestLogReaderTerm(t *testing.T) {
	fs := vfs.NewMemFS()
	testLogReaderTerm(t, fs)
}

func testLogReaderTerm(t *testing.T, fs vfs.FS) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		i      uint64
		werr   error
		wterm  uint64
		wpanic bool
	}{
		{2, raft.ErrCompacted, 0, false},
		{3, nil, 3, false},
		{4, nil, 4, false},
		{5, nil, 5, false},
		{6, raft.ErrUnavailable, 0, false},
	}
	for i, tt := range tests {
		s, _, closer := getTestLogReader(ents, fs)
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()
			term, err := s.Term(tt.i)
			if err != tt.werr {
				t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
			}
			if term != tt.wterm {
				t.Errorf("#%d: term = %d, want %d", i, term, tt.wterm)
			}
		}()
		closer()
	}
}

func TestLogReaderEntries(t *testing.T) {
	fs := vfs.NewMemFS()
	testLogReaderEntries(t, fs)
}

func testLogReaderEntries(t *testing.T, fs vfs.FS) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}
	tests := []struct {
		lo, hi, maxsize uint64
		werr            error
		wentries        []pb.Entry
	}{
		{2, 6, math.MaxUint64, raft.ErrCompacted, nil},
		{3, 4, math.MaxUint64, raft.ErrCompacted, nil},
		{4, 5, math.MaxUint64, nil, []pb.Entry{{Index: 4, Term: 4}}},
		{4, 6, math.MaxUint64, nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{4, 7, math.MaxUint64, nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
		// even if maxsize is zero, the first entry should be returned
		{4, 7, 0, nil, []pb.Entry{{Index: 4, Term: 4}}},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size()), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()/2), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size() - 1), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// all
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
	}

	for i, tt := range tests {
		s, _, closer := getTestLogReader(ents, fs)
		entries, err := s.Entries(tt.lo, tt.hi, tt.maxsize)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(entries, tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, entries, tt.wentries)
		}
		closer()
	}
}

func TestLogReaderLastIndex(t *testing.T) {
	fs := vfs.NewMemFS()
	testLogReaderLastIndex(t, fs)
}

func testLogReaderLastIndex(t *testing.T, fs vfs.FS) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	s, _, closer := getTestLogReader(ents, fs)
	defer closer()
	last, err := s.LastIndex()
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}
	if last != 5 {
		t.Errorf("term = %d, want %d", last, 5)
	}
	if err := s.Append([]pb.Entry{{Index: 6, Term: 5}}); err != nil {
		t.Fatalf("%v", err)
	}
	last, err = s.LastIndex()
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}
	if last != 6 {
		t.Errorf("last = %d, want %d", last, 5)
	}
}

func TestLogReaderFirstIndex(t *testing.T) {
	fs := vfs.NewMemFS()
	testLogReaderFirstIndex(t, fs)
}

func testLogReaderFirstIndex(t *testing.T, fs vfs.FS) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	s, _, closer := getTestLogReader(ents, fs)
	defer closer()
	first, err := s.FirstIndex()
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}
	if first != 4 {
		t.Errorf("first = %d, want %d", first, 4)
	}
	li, err := s.LastIndex()
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}
	if li != 5 {
		t.Errorf("last index = %d, want 5", li)
	}
	if err := s.Compact(4); err != nil {
		t.Fatalf("%v", err)
	}
	first, err = s.FirstIndex()
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}
	if first != 5 {
		t.Errorf("first = %d, want %d", first, 5)
	}
	li, err = s.LastIndex()
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}
	if li != 5 {
		t.Errorf("last index = %d, want 5", li)
	}
}

func TestLogReaderAppend(t *testing.T) {
	fs := vfs.NewMemFS()
	testLogReaderAppend(t, fs)
}

func testLogReaderAppend(t *testing.T, fs vfs.FS) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		entries  []pb.Entry
		werr     error
		wentries []pb.Entry
	}{
		{
			[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}},
		},
		{
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}},
		},
		{
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 6}, {Index: 5, Term: 6}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 6}, {Index: 5, Term: 6}},
		},
		{
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
		},
		// truncate incoming entries, truncate the existing entries and append
		{
			[]pb.Entry{{Index: 2, Term: 3}, {Index: 3, Term: 3}, {Index: 4, Term: 5}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 5}},
		},
		// truncate the existing entries and append
		{
			[]pb.Entry{{Index: 4, Term: 5}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 5}},
		},
		// direct append
		{
			[]pb.Entry{{Index: 6, Term: 5}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
		},
	}
	for i, tt := range tests {
		s, kv, closer := getTestLogReader(ents, fs)
		if err := s.Append(tt.entries); err != tt.werr {
			t.Fatalf("%v", err)
		}
		rd := raft.Ready{
			Entries: tt.entries,
		}
		wc := logdb.NewWorkerContext(kv)
		if err := s.logdb.SaveRaftState(testShardID, testPeerID, rd, wc); err != nil {
			panic(err)
		}
		bfi := tt.wentries[0].Index - 1
		_, err := s.Term(bfi)
		if err == nil {
			t.Errorf("suppose to fail")
		}
		ali := tt.wentries[len(tt.wentries)-1].Index + 1
		_, err = s.Term(ali)
		if err == nil {
			t.Errorf("suppose to fail, but it didn't fail, i %d", i)
		}
		for ii, e := range tt.wentries {
			term, err := s.Term(e.Index)
			if err != nil {
				t.Errorf("idx %d, ii %d Term() failed", i, ii)
			}
			if term != e.Term {
				t.Errorf("term %d, want %d", term, e.Term)
			}
		}
		closer()
	}
}

func TestLogReaderApplySnapshot(t *testing.T) {
	fs := vfs.NewMemFS()
	ents := []pb.Entry{{Index: 0, Term: 0}}
	cs := &pb.ConfState{
		Voters: []uint64{1, 2, 3},
	}
	tests := []pb.Snapshot{
		{Metadata: pb.SnapshotMetadata{Index: 4, Term: 4, ConfState: *cs}},
		{Metadata: pb.SnapshotMetadata{Index: 3, Term: 3, ConfState: *cs}},
	}
	s, _, closer := getTestLogReader(ents, fs)
	defer closer()
	//Apply Snapshot successful
	i := 0
	tt := tests[i]
	err := s.ApplySnapshot(tt)
	if err != nil {
		t.Errorf("#%d: err = %v, want %v", i, err, nil)
	}
	if fi, _ := s.FirstIndex(); fi != 5 {
		t.Errorf("first index %d, want 5", fi)
	}
	if li, _ := s.LastIndex(); li != 4 {
		t.Errorf("last index %d, want 4", li)
	}
	//Apply Snapshot fails due to ErrSnapOutOfDate
	i = 1
	tt = tests[i]
	err = s.ApplySnapshot(tt)
	if err != raft.ErrSnapOutOfDate {
		t.Errorf("#%d: err = %v, want %v", i, err, raft.ErrSnapOutOfDate)
	}
}

func TestLogReaderCreateSnapshot(t *testing.T) {
	fs := vfs.NewMemFS()
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	cs := &pb.ConfState{Voters: []uint64{1, 2, 3}}

	tests := []struct {
		i     uint64
		term  uint64
		werr  error
		wsnap pb.Snapshot
	}{
		{4, 4, nil, pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 4, ConfState: *cs}}},
		{5, 5, nil, pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 5, Term: 5, ConfState: *cs}}},
	}
	for i, tt := range tests {
		s, _, closer := getTestLogReader(ents, fs)
		err := s.CreateSnapshot(tt.wsnap)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if s.snapshot.Metadata.Index != tt.wsnap.Metadata.Index {
			t.Errorf("#%d: snap = %+v, want %+v", i, s.snapshot, tt.wsnap)
		}
		closer()
	}
}

func TestLogReaderSetRange(t *testing.T) {
	fs := vfs.NewMemFS()
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		firstIndex     uint64
		length         uint64
		expLength      uint64
		expMarkerIndex uint64
	}{
		{2, 2, 3, 3},
		{2, 5, 4, 3},
		{3, 5, 5, 3},
		{6, 6, 9, 3},
	}
	for idx, tt := range tests {
		s, _, closer := getTestLogReader(ents, fs)
		s.SetRange(tt.firstIndex, tt.length)
		if s.markerIndex != tt.expMarkerIndex {
			t.Errorf("%d, marker index %d, want %d", idx, s.markerIndex, tt.expMarkerIndex)
		}
		if s.length != tt.expLength {
			t.Errorf("%d, length %d, want %d", idx, s.length, tt.expLength)
		}
		closer()
	}
}

func TestLogReaderGetSnapshot(t *testing.T) {
	fs := vfs.NewMemFS()
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	cs := &pb.ConfState{
		Voters: []uint64{1, 2, 3},
	}
	ss := pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 4, ConfState: *cs}}
	s, _, closer := getTestLogReader(ents, fs)
	defer closer()
	if err := s.ApplySnapshot(ss); err != nil {
		t.Errorf("create snapshot failed %v", err)
	}
	rs, err := s.Snapshot()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if rs.Metadata.Index != ss.Metadata.Index {
		t.Errorf("unexpected snapshot rec")
	}
}

func TestLogReaderInitialState(t *testing.T) {
	fs := vfs.NewMemFS()
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	cs := pb.ConfState{
		Voters: []uint64{1, 2, 3},
	}
	s, _, closer := getTestLogReader(ents, fs)
	defer closer()
	s.SetConfState(cs)
	ps := pb.HardState{
		Term:   2,
		Vote:   3,
		Commit: 5,
	}
	s.SetState(ps)
	rps, ms, err := s.InitialState()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(&ms, &cs) {
		t.Errorf("conf state changed")
	}
	if !reflect.DeepEqual(&rps, &ps) {
		t.Errorf("hard state changed")
	}
}
