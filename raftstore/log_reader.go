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
//
// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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
// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
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

package raftstore

import (
	"fmt"
	"sync"
	"unsafe"

	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/logdb"
)

const (
	maxEntrySliceSize uint64 = 4 * 1024 * 1024
)

// LogReader code below is based on dragonboat's internal/logdb/logreader.go,
// which in turn made reference to CockraochDB's replicaRaftStorage and etcd
// raft's MemoryStorage.

// LogReader is the struct used to manage logs that have already been persisted
// into LogDB. LogReader implements the raft.Storage interface.
type LogReader struct {
	sync.Mutex
	logger      *zap.Logger
	snapshot    pb.Snapshot
	state       pb.HardState
	confState   pb.ConfState
	markerIndex uint64
	markerTerm  uint64
	length      uint64
	logdb       logdb.LogDB
	shardID     uint64
	peerID      uint64
}

var _ raft.Storage = (*LogReader)(nil)

// NewLogReader creates and returns a new LogReader instance.
func NewLogReader(logger *zap.Logger, shardID uint64, peerID uint64,
	db logdb.LogDB) *LogReader {
	return &LogReader{
		logger:  log.Adjust(logger),
		logdb:   db,
		shardID: shardID,
		peerID:  peerID,
		length:  1,
	}
}

func (lr *LogReader) id() string {
	return fmt.Sprintf("logreader %s index %d term %d length %d",
		dn(lr.shardID, lr.peerID), lr.markerIndex, lr.markerTerm, lr.length)
}

// InitialState returns the saved HardState and ConfState information.
func (lr *LogReader) InitialState() (pb.HardState, pb.ConfState, error) {
	lr.Lock()
	defer lr.Unlock()
	return lr.state, lr.confState, nil
}

func (lr *LogReader) FirstIndex() (uint64, error) {
	lr.Lock()
	defer lr.Unlock()
	return lr.firstIndex(), nil
}

func (lr *LogReader) LastIndex() (uint64, error) {
	lr.Lock()
	defer lr.Unlock()
	return lr.lastIndex(), nil
}

func (lr *LogReader) firstIndex() uint64 {
	return lr.markerIndex + 1
}

func (lr *LogReader) lastIndex() uint64 {
	return lr.markerIndex + lr.length - 1
}

// Entries returns persisted entries between [low, high) with a total limit of
// up to maxSize bytes.
func (lr *LogReader) Entries(low uint64,
	high uint64, maxSize uint64) ([]pb.Entry, error) {
	ents, _, err := lr.entries(low, high, maxSize)
	if err != nil {
		return nil, err
	}
	return ents, nil
}

func (lr *LogReader) entries(low uint64,
	high uint64, maxSize uint64) ([]pb.Entry, uint64, error) {
	lr.Lock()
	defer lr.Unlock()
	return lr.entriesLocked(low, high, maxSize)
}

func (lr *LogReader) entriesLocked(low uint64,
	high uint64, maxSize uint64) ([]pb.Entry, uint64, error) {
	if low > high {
		return nil, 0, fmt.Errorf("high (%d) < low (%d)", high, low)
	}
	if low <= lr.markerIndex {
		return nil, 0, raft.ErrCompacted
	}
	if high > lr.lastIndex()+1 {
		lr.logger.Error("log reader unavailable",
			zap.String("id", lr.id()),
			zap.Uint64("low", low),
			zap.Uint64("high", high),
			zap.Uint64("last-index", lr.lastIndex()))
		return nil, 0, raft.ErrUnavailable
	}
	// limit the size the ents slice to handle the extreme situation in which
	// high-low can be tens of millions, slice cap is > 50,000 when
	// maxEntrySliceSize is 4MBytes
	maxEntries := maxEntrySliceSize / uint64(unsafe.Sizeof(pb.Entry{}))
	if high-low > maxEntries {
		high = low + maxEntries
		lr.logger.Warn("limited high in logReader.entriesLocked",
			zap.String("id", lr.id()),
			zap.Uint64("high", high))
	}
	ents := make([]pb.Entry, 0, high-low)
	ents, size, err := lr.logdb.IterateEntries(ents, 0, lr.shardID, lr.peerID, low, high, maxSize)
	if err != nil {
		return nil, 0, err
	}
	if uint64(len(ents)) == high-low || size >= maxSize {
		return ents, size, nil
	}
	if len(ents) > 0 {
		if ents[0].Index > low {
			return nil, 0, raft.ErrCompacted
		}
		expected := ents[len(ents)-1].Index + 1
		if lr.lastIndex() <= expected {
			lr.logger.Error("log reader unavailable",
				zap.String("id", lr.id()),
				zap.Uint64("low", low),
				zap.Uint64("high", high),
				zap.Uint64("expected", expected),
				zap.Uint64("last-index", lr.lastIndex()))
			return nil, 0, raft.ErrUnavailable
		}
		return nil, 0, fmt.Errorf("gap found between [%d:%d) at %d",
			low, high, expected)
	}
	lr.logger.Warn("failed to get anything from logreader",
		zap.String("id", lr.id()))
	return nil, 0, raft.ErrUnavailable
}

// Term returns the term of the entry specified by the entry index.
func (lr *LogReader) Term(index uint64) (uint64, error) {
	lr.Lock()
	defer lr.Unlock()
	return lr.termLocked(index)
}

func (lr *LogReader) termLocked(index uint64) (uint64, error) {
	if index == lr.markerIndex {
		t := lr.markerTerm
		return t, nil
	}
	ents, _, err := lr.entriesLocked(index, index+1, 0)
	if err != nil {
		return 0, err
	}
	if len(ents) == 0 {
		return 0, nil
	}
	return ents[0].Term, nil
}

// Snapshot returns the metadata of the lastest snapshot.
func (lr *LogReader) Snapshot() (pb.Snapshot, error) {
	lr.Lock()
	defer lr.Unlock()
	return lr.snapshot, nil
}

// ApplySnapshot applies the specified snapshot.
func (lr *LogReader) ApplySnapshot(snapshot pb.Snapshot) error {
	lr.Lock()
	defer lr.Unlock()
	if err := lr.setSnapshot(snapshot); err != nil {
		return err
	}
	lr.markerIndex = snapshot.Metadata.Index
	lr.markerTerm = snapshot.Metadata.Term
	lr.length = 1
	return nil
}

// CreateSnapshot keeps the metadata of the specified snapshot.
func (lr *LogReader) CreateSnapshot(snapshot pb.Snapshot) error {
	lr.Lock()
	defer lr.Unlock()
	return lr.setSnapshot(snapshot)
}

func (lr *LogReader) setSnapshot(snapshot pb.Snapshot) error {
	if lr.snapshot.Metadata.Index >= snapshot.Metadata.Index {
		lr.logger.Debug("called setSnapshot",
			zap.String("id", lr.id()),
			zap.Uint64("existing", lr.snapshot.Metadata.Index),
			zap.Uint64("new", snapshot.Metadata.Index))
		return raft.ErrSnapOutOfDate
	}
	lr.logger.Debug("set snapshot",
		zap.String("id", lr.id()),
		zap.Uint64("new", snapshot.Metadata.Index))
	lr.snapshot = snapshot
	return nil
}

// Append marks the specified entries as persisted and make them available from
// logreader.
func (lr *LogReader) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	if len(entries) > 0 {
		if entries[0].Index+uint64(len(entries))-1 != entries[len(entries)-1].Index {
			panic("gap in entries")
		}
	}
	lr.SetRange(entries[0].Index, uint64(len(entries)))
	return nil
}

// SetRange updates the LogReader to reflect what is available in it.
func (lr *LogReader) SetRange(firstIndex uint64, length uint64) {
	if length == 0 {
		return
	}
	lr.Lock()
	defer lr.Unlock()
	first := lr.firstIndex()
	last := firstIndex + length - 1
	if last < first {
		return
	}
	if first > firstIndex {
		cut := first - firstIndex
		firstIndex = first
		length -= cut
	}
	offset := firstIndex - lr.markerIndex
	switch {
	case lr.length > offset:
		lr.length = offset + length
	case lr.length == offset:
		lr.length += length
	default:
		lr.logger.Fatal("gap in log entries, marker %d, len %d, first %d, len %d",
			zap.String("id", lr.id()),
			zap.Uint64("marker", lr.markerIndex),
			zap.Uint64("lr.len", lr.length),
			zap.Uint64("first", lr.markerIndex),
			zap.Uint64("len", length))
	}
}

// SetState sets the persistent state.
func (lr *LogReader) SetState(s pb.HardState) {
	lr.Lock()
	defer lr.Unlock()
	lr.state = s
}

func (lr *LogReader) SetConfState(cs pb.ConfState) {
	lr.Lock()
	defer lr.Unlock()
	lr.confState = cs
}

// Compact compacts raft log entries up to index.
func (lr *LogReader) Compact(index uint64) error {
	lr.Lock()
	defer lr.Unlock()
	if index < lr.markerIndex {
		return raft.ErrCompacted
	}
	if index > lr.lastIndex() {
		return raft.ErrUnavailable
	}
	term, err := lr.termLocked(index)
	if err != nil {
		return err
	}
	i := index - lr.markerIndex
	lr.length -= i
	lr.markerIndex = index
	lr.markerTerm = term
	return nil
}
