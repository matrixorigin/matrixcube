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
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/fagongzi/util/format"

	"github.com/matrixorigin/matrixcube/keys"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/stats"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/buf"
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

type kvDataStorage struct {
	opts       *options
	base       storage.KVStorage
	executor   storage.Executor
	writeCount uint64
	mu         struct {
		sync.RWMutex
		loaded                   bool
		lastAppliedIndexes       map[uint64]uint64
		persistentAppliedIndexes map[uint64]uint64
	}
}

var _ storage.DataStorage = (*kvDataStorage)(nil)

// NewKVDataStorage returns data storage based on a kv base storage.
func NewKVDataStorage(base storage.KVStorage,
	executor storage.Executor, opts ...Option) storage.DataStorage {
	s := &kvDataStorage{
		base:     base,
		executor: executor,
		opts:     newOptions(),
	}
	s.mu.lastAppliedIndexes = make(map[uint64]uint64)
	s.mu.persistentAppliedIndexes = make(map[uint64]uint64)

	for _, opt := range opts {
		opt(s.opts)
	}
	s.opts.adjust()
	return s
}

func (kv *kvDataStorage) GetKVStorage() storage.KVStorage {
	return kv.base
}

func (kv *kvDataStorage) NewWriteBatch() storage.Resetable {
	return kv.base.NewWriteBatch()
}

func (kv *kvDataStorage) Write(ctx storage.WriteContext) error {
	if err := kv.executor.UpdateWriteBatch(ctx); err != nil {
		return err
	}
	r := ctx.WriteBatch()
	defer r.Reset()
	batch := ctx.Batch()
	if batch.Index == 0 {
		panic("empty batch?")
	}
	kv.setAppliedIndexToWriteBatch(ctx, batch.Index)
	kv.updateAppliedIndex(ctx.Shard().ID, batch.Index)
	if err := kv.executor.ApplyWriteBatch(r); err != nil {
		return err
	}
	return kv.trySync()
}

func (kv *kvDataStorage) setAppliedIndexToWriteBatch(ctx storage.WriteContext, index uint64) {
	r := ctx.WriteBatch()
	wb := r.(util.WriteBatch)
	ctx.ByteBuf().MarkWrite()
	ctx.ByteBuf().WriteUInt64(index)
	key := keys.GetAppliedIndexKey(ctx.Shard().ID, nil)
	val := ctx.ByteBuf().WrittenDataAfterMark().Data()
	wb.Set(key, val)
}

func (kv *kvDataStorage) Read(ctx storage.ReadContext) ([]byte, error) {
	return kv.executor.Read(ctx)
}

func (kv *kvDataStorage) SaveShardMetadata(metadatas []storage.ShardMetadata) error {
	r := kv.base.NewWriteBatch()
	wb := r.(util.WriteBatch)
	defer wb.Close()

	seen := make(map[uint64]struct{})
	kv.mu.Lock()
	for _, m := range metadatas {
		fmt.Fprintf(os.Stderr, "shard: %d, index: %d\n", m.ShardID, m.LogIndex)
		wb.Set(keys.GetMetadataKey(m.ShardID, m.LogIndex, nil), m.Metadata)
		wb.Set(keys.GetAppliedIndexKey(m.ShardID, nil), format.Uint64ToBytes(m.LogIndex))
		kv.mu.lastAppliedIndexes[m.ShardID] = m.LogIndex
		if _, ok := seen[m.ShardID]; ok {
			panic("more than one instance of metadata from the same shard")
		} else {
			seen[m.ShardID] = struct{}{}
		}
	}
	kv.mu.Unlock()

	if err := kv.base.Write(wb, false); err != nil {
		return err
	}

	return kv.trySync()
}

func (kv *kvDataStorage) GetInitialStates() ([]storage.ShardMetadata, error) {
	// TODO: this assumes that all shards have applied index records saved.
	// double check to make sure this is actually true.
	min := keys.GetAppliedIndexKey(0, nil)
	max := keys.GetAppliedIndexKey(math.MaxUint64, nil)
	var shards []uint64
	var lastApplied []uint64
	// find out all shards and their last applied indexes
	fmt.Printf(" ===> going to call scan!!!\n")
	if err := kv.base.Scan(min, max, func(key, value []byte) (bool, error) {
		fmt.Printf(" ===> got a key!!!\n")
		if keys.IsAppliedIndexKey(key) {
			shardID, err := keys.GetShardIDFromAppliedIndexKey(key)
			if err != nil {
				panic(err)
			}
			shards = append(shards, shardID)
			lastApplied = append(lastApplied, buf.Byte2UInt64(value))
		}
		return true, nil
	}, false); err != nil {
		return nil, err
	}
	fmt.Printf("shards len: %d\n", len(shards))
	// update the persistentAppliedIndexes
	kv.mu.Lock()
	for idx, appliedIndex := range lastApplied {
		kv.mu.persistentAppliedIndexes[shards[idx]] = appliedIndex
	}
	kv.mu.loaded = true
	kv.mu.Unlock()
	// for each shard,
	var values []storage.ShardMetadata
	for _, shard := range shards {
		min := keys.GetMetadataKey(shard, 0, nil)
		max := keys.GetMetadataKey(shard, math.MaxUint64, nil)
		var v []byte
		var logIndex uint64
		var err error
		if err := kv.base.Scan(min, max, func(key, value []byte) (bool, error) {
			if keys.IsMetadataKey(key) {
				v = value
				logIndex, err = keys.GetMetadataIndex(key)
				if err != nil {
					panic(err)
				}
			} else {
				panic("unexpected key/value")
			}
			return true, nil
		}, true); err != nil {
			return nil, err
		}

		if v == nil && logIndex == 0 {
			panic("failed to get shard metadata")
		}
		values = append(values, storage.ShardMetadata{
			ShardID:  shard,
			LogIndex: logIndex,
			Metadata: v,
		})
	}
	return values, nil
}

// TODO: handle shardID not found error, maybe define ShardNotFound?

func (kv *kvDataStorage) GetPersistentLogIndex(shardID uint64) (uint64, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if !kv.mu.loaded {
		panic("GetInitialStates must be invoked first")
	}
	return kv.mu.persistentAppliedIndexes[shardID], nil
}

func (kv *kvDataStorage) Sync(_ []uint64) error {
	return kv.base.Sync()
}

func (kv *kvDataStorage) RemoveShardData(shard meta.Shard,
	encodedStartKey, encodedEndKey []byte) error {
	// This is not an atomic operation, but it is idempotent, and the metadata is
	// deleted afterwards, so the cleanup will not be lost.
	if err := kv.base.RangeDelete(encodedStartKey, encodedEndKey, false); err != nil {
		return err
	}
	return kv.base.RangeDelete(keys.GetRaftPrefix(shard.ID),
		keys.GetRaftPrefix(shard.ID+1), true)
}

func (kv *kvDataStorage) updateAppliedIndex(shard uint64, index uint64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.mu.lastAppliedIndexes[shard] = index
}

// trySync syncs the data to disk every interval and then mark the appliedIndex
// values of the raft log as persistented.
func (kv *kvDataStorage) trySync() error {
	n := atomic.AddUint64(&kv.writeCount, 1)
	if n%kv.opts.sampleSync != 0 {
		return nil
	}
	if err := kv.base.Sync(); err != nil {
		return err
	}
	kv.mu.Lock()
	for k, v := range kv.mu.lastAppliedIndexes {
		kv.mu.persistentAppliedIndexes[k] = v
	}
	kv.mu.Unlock()

	return nil
}

// delegate method
func (kv *kvDataStorage) Close() error {
	return kv.base.Close()
}

func (kv *kvDataStorage) SplitCheck(start, end []byte,
	size uint64) (currentSize uint64, currentKeys uint64, splitKeys [][]byte, err error) {
	return kv.base.SplitCheck(start, end, size)
}

func (kv *kvDataStorage) CreateSnapshot(path string, start, end []byte) error {
	return kv.base.CreateSnapshot(path, start, end)
}

func (kv *kvDataStorage) ApplySnapshot(path string) error {
	return kv.base.ApplySnapshot(path)
}

func (kv *kvDataStorage) Stats() stats.Stats {
	return kv.base.Stats()
}
