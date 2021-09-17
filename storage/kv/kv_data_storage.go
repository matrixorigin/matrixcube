// Copyright 2020 MatrixOrigin.
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

package kv

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/stats"
)

// Option option func
type Option func(*options)

type options struct {
	sampleSync uint64
}

// WithSampleSync set sync sample interval. `Cube` will call the `GetPersistentLogIndex` method of `DataStorage` to obtain
// the last `PersistentLogIndex`, which is used for log compression. Since all the writes of `DataStorage` are not written
// by fsync, we need to sample some sync writes to ensure that the returned `PersistentLogIndex` will not exceed the real
// `PersistentLogIndex` to avoid the log being compressed by mistake.
func WithSampleSync(value uint64) Option {
	return func(opts *options) {
		opts.sampleSync = value
	}
}

func newOptions() *options {
	return &options{}
}

func (opts *options) adjust() {
	if opts.sampleSync == 0 {
		opts.sampleSync = 100
	}
}

type kvStorage struct {
	opts       *options
	base       storage.KVBaseDataStorage
	executor   storage.CommandExecutor
	writeCount uint64
	mu         struct {
		sync.RWMutex
		lastAppliedIndexes       map[uint64]uint64
		persistentAppliedIndexes map[uint64]uint64
	}
}

// NewKVStorage returns data storage based on a kv storage, support KV operations.
func NewKVStorage(base storage.KVBaseDataStorage, executor storage.CommandExecutor, opts ...Option) storage.DataStorage {
	s := &kvStorage{base: base, executor: executor, opts: newOptions()}
	s.mu.lastAppliedIndexes = make(map[uint64]uint64)
	s.mu.persistentAppliedIndexes = make(map[uint64]uint64)

	for _, opt := range opts {
		opt(s.opts)
	}
	s.opts.adjust()
	return s
}

func (kv *kvStorage) GetCommandExecutor() storage.CommandExecutor {
	return kv
}

func (kv *kvStorage) ExecuteWrite(ctx storage.Context) error {
	err := kv.executor.ExecuteWrite(ctx)
	if err != nil {
		return err
	}

	kv.updateAppliedIndex(ctx.Shard().ID, ctx.Requests()[len(ctx.Requests())-1].Index)
	return kv.simpleSync()
}

func (kv *kvStorage) ExecuteRead(ctx storage.Context) error {
	return kv.executor.ExecuteRead(ctx)
}

func (kv *kvStorage) SaveShardMetadata(metadatas ...storage.ShardMetadata) error {
	wb := kv.base.NewWriteBatch()
	defer wb.Close()

	kv.mu.Lock()
	for _, m := range metadatas {
		wb.Set(keys.GetDataStorageMetadataKey(m.ShardID, m.LogIndex), m.Metadata)
		wb.Set(keys.GetDataStorageAppliedIndexKey(m.ShardID), format.Uint64ToBytes(m.LogIndex))

		kv.mu.lastAppliedIndexes[m.ShardID] = m.LogIndex
	}
	kv.mu.Unlock()

	err := kv.base.Write(wb, false)
	if err != nil {
		return err
	}

	return kv.simpleSync()
}

func (kv *kvStorage) GetInitialStates() ([]storage.ShardMetadata, error) {
	minApplied := keys.GetDataStorageAppliedIndexKey(0)
	maxApplied := keys.GetDataStorageAppliedIndexKey(math.MaxUint64)
	var shards []uint64
	var lastAppliedIndexs []uint64
	err := kv.base.Scan(minApplied, maxApplied, func(key, value []byte) (bool, error) {
		if keys.IsDataStorageAppliedIndexKey(key) {
			shards = append(shards, keys.DecodeDataStorageAppliedIndexKey(key))
			lastAppliedIndexs = append(lastAppliedIndexs, buf.Byte2UInt64(value))
		}
		return true, nil
	}, false)
	if err != nil {
		return nil, err
	}

	var values []storage.ShardMetadata
	for idx, shard := range shards {
		v, err := kv.base.Get(keys.GetDataStorageMetadataKey(shard, lastAppliedIndexs[idx]))
		if err != nil {
			return nil, err
		}

		values = append(values, storage.ShardMetadata{
			ShardID:  shard,
			LogIndex: lastAppliedIndexs[idx],
			Metadata: v,
		})
	}
	return values, nil
}

func (kv *kvStorage) GetPersistentLogIndex(shardID uint64) (uint64, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.mu.persistentAppliedIndexes[shardID], nil
}

func (kv *kvStorage) Sync(...uint64) error {
	return kv.base.Sync()
}

func (kv *kvStorage) RemoveShardData(shard bhmetapb.Shard, encodedStartKey, encodedEndKey []byte) error {
	// This is not an atomic operation, but it is idempotent, and the metadata is deleted afterwards,
	// so the cleanup will not be lost.

	err := kv.base.RangeDelete(encodedStartKey, encodedEndKey)
	if err != nil {
		return err
	}

	return kv.base.RangeDelete(keys.GetRaftPrefix(shard.ID), keys.GetRaftPrefix(shard.ID+1))
}

func (kv *kvStorage) updateAppliedIndex(shard uint64, index uint64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.mu.lastAppliedIndexes[shard] = index
}

// simpleSync we write and sync the data to disk every interval, and then record the appliedIndex value of
// the raft log when sync.
func (kv *kvStorage) simpleSync() error {
	n := atomic.AddUint64(&kv.writeCount, 1)
	if n%kv.opts.sampleSync != 0 {
		return nil
	}
	kv.mu.Lock()
	for k, v := range kv.mu.lastAppliedIndexes {
		kv.mu.persistentAppliedIndexes[k] = v
	}
	kv.mu.Unlock()

	return kv.base.Sync()
}

// delegate method
func (kv *kvStorage) Close() error {
	return kv.base.Close()
}

func (kv *kvStorage) SplitCheck(start []byte, end []byte, size uint64) (currentSize uint64, currentKeys uint64, splitKeys [][]byte, err error) {
	return kv.base.SplitCheck(start, end, size)
}

func (kv *kvStorage) CreateSnapshot(path string, start, end []byte) error {
	return kv.base.CreateSnapshot(path, start, end)
}

func (kv *kvStorage) ApplySnapshot(path string) error {
	return kv.base.ApplySnapshot(path)
}

func (kv *kvStorage) Stats() stats.Stats {
	return kv.base.Stats()
}
