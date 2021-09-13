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

	"github.com/cockroachdb/errors"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util"
)

var (
	logger = log.NewLoggerWithPrefix("[logdb]")
)

var (
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

type LogDB interface {
	// Name returns the type name of the ILogDB instance.
	Name() string
	// Close closes the ILogDB instance.
	Close() error
	// SaveBootstrapInfo saves the specified bootstrap info to the log DB.
	SaveBootstrapInfo(shardID uint64,
		peerID uint64, bootstrap bhraftpb.BootstrapInfo) error
	// GetBootstrapInfo returns saved bootstrap info from log DB. It returns
	// ErrNoBootstrapInfo when there is no previously saved bootstrap info for
	// the specified node.
	GetBootstrapInfo(shardID uint64, peerID uint64) (bhraftpb.BootstrapInfo, error)
	// SaveRaftState atomically saves the Raft states, log entries and snapshots
	// metadata found in the pb.Update list to the log DB. shardID is a 1-based
	// ID of the worker invoking the SaveRaftState method, as each worker
	// accesses the log DB from its own thread, SaveRaftState will never be
	// concurrently called with the same shardID.
	SaveRaftState(shardID uint64, peerID uint64, rd raft.Ready) error
	// IterateEntries returns the continuous Raft log entries of the specified
	// Raft node between the index value range of [low, high) up to a max size
	// limit of maxSize bytes. It returns the located log entries, their total
	// size in bytes and the occurred error.
	IterateEntries(ents []raftpb.Entry,
		size uint64, shardID uint64, peerID uint64, low uint64,
		high uint64, maxSize uint64) ([]raftpb.Entry, uint64, error)
	// ReadRaftState returns the persistented raft state found in Log DB.
	ReadRaftState(shardID uint64, peerID uint64) (RaftState, error)
	// RemoveEntriesTo removes entries with indexes between (0, index].
	RemoveEntriesTo(shardID uint64, peerID uint64, index uint64) error
}

type KVLogDB struct {
	state raftpb.HardState
	ms    storage.MetadataStorage
	wb    *util.WriteBatch
	// FIXME: wbuf is unsafe as it will be concurrently accessed from multiple
	// worker goroutines.
	wbuf []byte
}

var _ LogDB = (*KVLogDB)(nil)

func NewKVLogDB(ms storage.MetadataStorage) *KVLogDB {
	return &KVLogDB{
		ms:   ms,
		wb:   util.NewWriteBatch(),
		wbuf: make([]byte, 8),
	}
}

func (l *KVLogDB) Name() string {
	return "KVLogDB"
}

func (l *KVLogDB) Close() error {
	return nil
}

func (l *KVLogDB) SaveBootstrapInfo(shardID uint64, peerID uint64,
	bootstrap bhraftpb.BootstrapInfo) error {
	wb := util.NewWriteBatch()
	v := protoc.MustMarshal(&bootstrap)
	if err := wb.Set(keys.GetBootstrapInfoKey(shardID, peerID), v); err != nil {
		panic(err)
	}
	return l.ms.Write(wb, true)
}

func (l *KVLogDB) GetBootstrapInfo(shardID uint64,
	peerID uint64) (bhraftpb.BootstrapInfo, error) {
	v, err := l.ms.Get(keys.GetBootstrapInfoKey(shardID, peerID))
	if err != nil {
		return bhraftpb.BootstrapInfo{}, err
	}
	if len(v) == 0 {
		return bhraftpb.BootstrapInfo{}, ErrNotFound
	}
	var result bhraftpb.BootstrapInfo
	protoc.MustUnmarshal(&result, v)
	return result, nil
}

func (l *KVLogDB) SaveRaftState(shardID uint64, peerID uint64, rd raft.Ready) error {
	if len(rd.Entries) == 0 {
		return nil
	}

	l.wb.Reset()
	for _, e := range rd.Entries {
		d := protoc.MustMarshal(&e)
		if err := l.wb.Set(keys.GetRaftLogKey(shardID, e.Index), d); err != nil {
			panic(err)
		}
	}

	v := protoc.MustMarshal(&rd.HardState)
	if err := l.wb.Set(keys.GetHardStateKey(shardID, peerID), v); err != nil {
		panic(err)
	}

	binary.BigEndian.PutUint64(l.wbuf, rd.Entries[len(rd.Entries)-1].Index)
	if err := l.wb.Set(keys.GetMaxIndexKey(shardID), l.wbuf); err != nil {
		panic(err)
	}

	return l.ms.Write(l.wb, true)
}

func (l *KVLogDB) IterateEntries(ents []raftpb.Entry,
	size uint64, shardID uint64, peerID uint64, low uint64,
	high uint64, maxSize uint64) ([]raftpb.Entry, uint64, error) {

	nextIndex := low
	startKey := keys.GetRaftLogKey(shardID, low)
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
			logger.Fatalf("shard %d raft log index not match, logIndex %d expect %d",
				shardID,
				e.Index,
				nextIndex)
		}
		ents = append(ents, e)
		return ents, uint64(e.Size()), nil
	}

	maxIndex, err := l.getMaxIndex(shardID, peerID)
	if err != nil {
		return nil, 0, err
	}
	if high > maxIndex+1 {
		high = maxIndex + 1
	}

	endKey := keys.GetRaftLogKey(shardID, high)
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

func (l *KVLogDB) ReadRaftState(shardID uint64, peerID uint64) (RaftState, error) {
	target := keys.GetRaftLogKey(shardID, 0)
	key, _, err := l.ms.Seek(target)
	if err != nil {
		return RaftState{}, err
	}
	if len(key) == 0 || !keys.IsRaftLogKey(key) {
		return RaftState{}, ErrNoSavedLog
	}
	startIndex, err := keys.GetRaftLogIndex(key)
	if err != nil {
		return RaftState{}, err
	}
	maxIndex, err := l.getMaxIndex(shardID, peerID)
	if err != nil {
		return RaftState{}, err
	}
	if maxIndex < startIndex {
		panic("invalid maxIndex or startIndex")
	}

	var st raftpb.HardState
	v, err := l.ms.Get(keys.GetHardStateKey(shardID, peerID))
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
		EntryCount: maxIndex - startIndex + 1,
	}, nil
}

// TODO: check whether index below is larger than the max index
func (l *KVLogDB) RemoveEntriesTo(shardID uint64, peerID uint64, index uint64) error {
	startKey := keys.GetRaftLogKey(shardID, 0)
	endKey := keys.GetRaftLogKey(shardID, index+1)
	return l.ms.RangeDelete(startKey, endKey)
}

func (l *KVLogDB) getMaxIndex(shardID uint64, peerID uint64) (uint64, error) {
	v, err := l.ms.Get(keys.GetMaxIndexKey(shardID))
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
