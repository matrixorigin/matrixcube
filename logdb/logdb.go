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
	"encoding/binary"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/fagongzi/util/protoc"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/keys"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util"
)

var (
	ErrNoSnapshot = errors.New("no snapshot")
	ErrNoSavedLog = errors.New("no saved log")
	ErrNotFound   = errors.New("not found")
)

// LogDB interface and its concrete implements are derived from dragonboat's
// logdb designs.

// RaftState is the persistent Raft state found in the LogDB.
type RaftState struct {
	// State is the Raft state persistent to the disk
	State raftpb.HardState
	// FirstIndex is the index of the first entry to iterate
	FirstIndex uint64
	// EntryCount is the number of entries to iterate
	EntryCount uint64
}

// WorkerContext is the per worker context owned and used by each raft worker.
// It contains write batch and buffers that can be reused across iterations.
type WorkerContext struct {
	idBuf []byte
	wb    util.WriteBatch
}

func (w *WorkerContext) Close() {
	w.wb.Close()
}

// Reset resets the worker context so it can be reused.
func (w *WorkerContext) Reset() {
	w.wb.Reset()
}

// LogDB is the interface to be implemented for concrete LogDB types used for
// saving raft logs and states.
type LogDB interface {
	// Name returns the type name of the ILogDB instance.
	Name() string
	// Close closes the ILogDB instance.
	Close() error
	// NewWorkerContext creates a new worker context which used by `SaveRaftState`.
	NewWorkerContext() *WorkerContext
	// SaveRaftState atomically saves the Raft states, log entries and snapshots
	// metadata found in the pb.Update list to the log DB. shardID is a 1-based
	// ID of the worker invoking the SaveRaftState method, as each worker
	// accesses the log DB from its own thread, SaveRaftState will never be
	// concurrently called with the same shardID.
	SaveRaftState(shardID uint64,
		replicaID uint64, rd raft.Ready, ctx *WorkerContext) error
	// IterateEntries returns the continuous Raft log entries of the specified
	// Raft node between the index value range of [low, high) up to a max size
	// limit of maxSize bytes. It returns the located log entries, their total
	// size in bytes and the occurred error.
	IterateEntries(ents []raftpb.Entry,
		size uint64, shardID uint64, replicaID uint64, low uint64,
		high uint64, maxSize uint64) ([]raftpb.Entry, uint64, error)
	// ReadRaftState returns the persistented raft state found in Log DB.
	ReadRaftState(shardID uint64, replicaID uint64) (RaftState, error)
	// RemoveEntriesTo removes entries with indexes between (0, index].
	RemoveEntriesTo(shardID uint64, replicaID uint64, index uint64) error
	// GetSnapshot returns the most recent snapshot metadata for the specified
	// replica.
	GetSnapshot(shardID uint64) (raftpb.Snapshot, error)
	// RemoveSnapshot removes the specified snapshot.
	RemoveSnapshot(shardID uint64, index uint64) error
}

// KVLogDB is a LogDB implementation built on top of a Key-Value store.
type KVLogDB struct {
	logger *zap.Logger
	state  raftpb.HardState
	ms     storage.KVMetadataStore
}

var _ LogDB = (*KVLogDB)(nil)

func NewKVLogDB(ms storage.KVMetadataStore, logger *zap.Logger) *KVLogDB {
	return &KVLogDB{
		logger: logger,
		ms:     ms,
	}
}

func (l *KVLogDB) Name() string {
	return "KVLogDB"
}

func (l *KVLogDB) Close() error {
	return nil
}

func (l *KVLogDB) NewWorkerContext() *WorkerContext {
	return &WorkerContext{
		idBuf: make([]byte, 8),
		wb:    l.ms.NewWriteBatch().(util.WriteBatch),
	}
}

func (l *KVLogDB) RemoveSnapshot(shardID uint64, index uint64) error {
	key := keys.GetSnapshotKey(shardID, index, nil)
	return l.ms.Delete(key, true)
}

func (l *KVLogDB) GetSnapshot(shardID uint64) (raftpb.Snapshot, error) {
	fk := keys.GetSnapshotKey(shardID, 0, nil)
	lk := keys.GetSnapshotKey(shardID, math.MaxUint64, nil)
	var v []byte
	if err := l.ms.Scan(fk, lk, func(key, value []byte) (bool, error) {
		v = value
		return true, nil
	}, true); err != nil {
		return raftpb.Snapshot{}, err
	}
	if len(v) == 0 {
		return raftpb.Snapshot{}, ErrNoSnapshot
	}
	var ss raftpb.Snapshot
	protoc.MustUnmarshal(&ss, v)
	return ss, nil
}

func (l *KVLogDB) SaveRaftState(shardID uint64,
	replicaID uint64, rd raft.Ready, ctx *WorkerContext) error {
	if isEmptyRaftReady(rd) {
		return nil
	}

	l.logger.Debug("save raft state",
		log.ShardIDField(shardID),
		log.ReplicaIDField(replicaID),
		zap.Uint64("commit", rd.HardState.Commit),
		zap.Uint64("term", rd.HardState.Commit),
		zap.Uint64("vote", rd.HardState.Vote))

	if !raft.IsEmptyHardState(rd.HardState) {
		v := protoc.MustMarshal(&rd.HardState)
		ctx.wb.Set(keys.GetHardStateKey(shardID, replicaID, nil), v)
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		ctx.wb.Set(keys.GetSnapshotKey(shardID, rd.Snapshot.Metadata.Index, nil),
			protoc.MustMarshal(&rd.Snapshot))
	}

	for _, e := range rd.Entries {
		// TODO: use reusable buf here
		d := protoc.MustMarshal(&e)
		ctx.wb.Set(keys.GetRaftLogKey(shardID, e.Index, nil), d)
	}
	if len(rd.Entries) > 0 {
		binary.BigEndian.PutUint64(ctx.idBuf, rd.Entries[len(rd.Entries)-1].Index)
		ctx.wb.Set(keys.GetMaxIndexKey(shardID, nil), ctx.idBuf)
	}
	return l.ms.Write(ctx.wb, true)
}

func (l *KVLogDB) IterateEntries(ents []raftpb.Entry,
	size uint64, shardID uint64, replicaID uint64, low uint64,
	high uint64, maxSize uint64) ([]raftpb.Entry, uint64, error) {
	nextIndex := low
	startKey := keys.GetRaftLogKey(shardID, low, nil)
	if low+1 == high {
		v, err := l.ms.Get(startKey)
		if err != nil {
			return nil, 0, err
		}
		if len(v) == 0 {
			return nil, 0, raft.ErrUnavailable
		}
		e := raftpb.Entry{}
		protoc.MustUnmarshal(&e, v)
		if e.Index != nextIndex {
			l.logger.Fatal("raft log index not match",
				log.ShardIDField(shardID),
				log.IndexField(e.Index),
				zap.Uint64("expected-index", nextIndex))
		}
		ents = append(ents, e)
		return ents, uint64(e.Size()), nil
	}

	maxIndex, err := l.getMaxIndex(shardID, replicaID)
	if err != nil {
		return nil, 0, err
	}
	if high > maxIndex+1 {
		high = maxIndex + 1
	}

	endKey := keys.GetRaftLogKey(shardID, high, nil)
	if err := l.ms.Scan(startKey, endKey, func(key, value []byte) (bool, error) {
		e := raftpb.Entry{}
		protoc.MustUnmarshal(&e, value)
		// May meet gap or has been compacted.
		if e.Index != nextIndex {
			return false, nil
		}
		nextIndex++
		size += uint64(e.Size())
		exceededMaxSize := size > maxSize
		if !exceededMaxSize || len(ents) == 0 {
			ents = append(ents, e)
		}

		return !exceededMaxSize, nil
	}, false); err != nil {
		return nil, 0, err
	}
	return ents, size, nil
}

func (l *KVLogDB) ReadRaftState(shardID uint64,
	replicaID uint64, snapshotIndex uint64) (RaftState, error) {
	firstIndex, length, err := r.getRange(shardID, replicaID, snapshotIndex)
	if err != nil {
		return RaftState{}, err
	}

	var st raftpb.HardState
	v, err := l.ms.Get(keys.GetHardStateKey(shardID, replicaID, nil))
	if err != nil {
		return RaftState{}, err
	}
	if len(v) == 0 {
		return RaftState{}, ErrNoSavedLog
	}
	protoc.MustUnmarshal(&st, v)

	return RaftState{
		State:      st,
		FirstIndex: startIndex,
		EntryCount: length,
	}, nil
}

// TODO: check whether index below is larger than the max index
// RemoveEntriesTo deletes all raft log entries between [0, index].
func (l *KVLogDB) RemoveEntriesTo(shardID uint64, replicaID uint64, index uint64) error {
	startKey := keys.GetRaftLogKey(shardID, 0, nil)
	endKey := keys.GetRaftLogKey(shardID, index+1, nil)
	return l.ms.RangeDelete(startKey, endKey, true)
}

func (l *KVLogDB) getRange(shardID uint64,
	replicaID uint64, snapshotIndex uint64) (uint64, uint64, error) {
	maxIndex, err := r.getMaxIndex(clusterID, nodeID)
	if err == ErrNoSavedLog {
		return snapshotIndex, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	if snapshotIndex == maxIndex {
		return snapshotIndex, 0, nil
	}

	target := keys.GetRaftLogKey(shardID, snapshotIndex, nil)
	key, _, err := l.ms.Seek(target)
	if err != nil {
		return 0, 0, err
	}
	if len(key) == 0 || !keys.IsRaftLogKey(key) {
		return snapshotIndex, 0, nil
	}
	startIndex, err := keys.GetRaftLogIndex(key)
	if err != nil {
		return 0, 0, err
	}
	if startIndex == 0 && maxIndex != 0 {
		l.logger.Fatal("maxIndex recorded, but no log entry found",
			zap.Uint64("max-index", maxIndex))
	}
	return startIndex, maxIndex - startIndex + 1, nil
}

func (l *KVLogDB) getMaxIndex(shardID uint64, replicaID uint64) (uint64, error) {
	v, err := l.ms.Get(keys.GetMaxIndexKey(shardID, nil))
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return 0, ErrNoSavedLog
	}
	if len(v) != 8 {
		panic("unexpected max index value")
	}
	return binary.BigEndian.Uint64(v), nil
}

func isEmptyRaftReady(rd raft.Ready) bool {
	return raft.IsEmptyHardState(rd.HardState) &&
		raft.IsEmptySnap(rd.Snapshot) &&
		len(rd.Entries) == 0
}
