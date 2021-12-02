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
	"sync"
	"sync/atomic"

	"github.com/fagongzi/util/protoc"
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
	base       storage.KVBaseStorage
	executor   storage.Executor
	writeCount uint64

	mu struct {
		sync.RWMutex
		loaded                   bool
		lastAppliedIndexes       map[uint64]uint64
		persistentAppliedIndexes map[uint64]uint64
	}
}

var _ storage.DataStorage = (*kvDataStorage)(nil)
var _ storage.KVStorageWrapper = (*kvDataStorage)(nil)

// NewKVDataStorage returns data storage based on a kv base storage.
func NewKVDataStorage(base storage.KVBaseStorage,
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
	// TODO: see keys encode
	return kv.base
}

func (kv *kvDataStorage) NewWriteBatch() storage.Resetable {
	return kv.base.NewWriteBatch()
}

func (kv *kvDataStorage) Write(ctx storage.WriteContext) error {
	batch := ctx.Batch()
	if batch.Index == 0 {
		panic("empty batch?")
	}

	// append data key
	for idx := range batch.Requests {
		batch.Requests[idx].Key = EncodeDataKey(batch.Requests[idx].Key, ctx.ByteBuf())
	}
	if err := kv.executor.UpdateWriteBatch(ctx); err != nil {
		return err
	}
	r := ctx.WriteBatch()
	defer r.Reset()

	kv.setAppliedIndexToWriteBatch(ctx, batch.Index)
	kv.updateAppliedIndex(ctx.Shard().ID, batch.Index)
	if err := kv.executor.ApplyWriteBatch(r); err != nil {
		return err
	}
	return kv.trySync()
}

func (kv *kvDataStorage) Read(ctx storage.ReadContext) ([]byte, error) {
	return kv.executor.Read(readContext{base: ctx})
}

func (kv *kvDataStorage) SaveShardMetadata(metadatas []meta.ShardMetadata) error {
	r := kv.base.NewWriteBatch()
	wb := r.(util.WriteBatch)
	defer wb.Close()

	seen := make(map[uint64]struct{})
	kv.mu.Lock()
	for _, m := range metadatas {
		if m.ShardID != m.Metadata.Shard.ID {
			panic(fmt.Errorf("BUG: shard ID mismatch, %+v", m))
		}
		key := EncodeShardMetadataKey(keys.GetMetadataKey(m.ShardID, m.LogIndex, nil), nil)
		wb.Set(key, protoc.MustMarshal(&m))

		logIndex := meta.LogIndex{Index: m.LogIndex}
		key = EncodeShardMetadataKey(keys.GetAppliedIndexKey(m.ShardID, nil), nil)
		wb.Set(key, protoc.MustMarshal(&logIndex))
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

func (kv *kvDataStorage) GetInitialStates() ([]meta.ShardMetadata, error) {
	// TODO: this assumes that all shards have applied index records saved.
	// double check to make sure this is actually true.
	min := EncodeShardMetadataKey(keys.GetAppliedIndexKey(0, nil), nil)
	max := EncodeShardMetadataKey(keys.GetAppliedIndexKey(math.MaxUint64, nil), nil)
	var shards []uint64
	var lastApplied []uint64
	// find out all shards and their last applied indexes
	if err := kv.base.Scan(min, max, func(key, value []byte) (bool, error) {
		key = key[1:]
		if keys.IsAppliedIndexKey(key) {
			shardID, err := keys.GetShardIDFromAppliedIndexKey(key)
			if err != nil {
				panic(err)
			}
			shards = append(shards, shardID)
			var logIndex meta.LogIndex
			protoc.MustUnmarshal(&logIndex, value)
			lastApplied = append(lastApplied, logIndex.Index)
		}
		return true, nil
	}, false); err != nil {
		return nil, err
	}
	// update the persistentAppliedIndexes
	kv.mu.Lock()
	for idx, appliedIndex := range lastApplied {
		kv.mu.persistentAppliedIndexes[shards[idx]] = appliedIndex
	}
	kv.mu.loaded = true
	kv.mu.Unlock()
	// for each shard,
	var values []meta.ShardMetadata
	for _, shard := range shards {
		min := EncodeShardMetadataKey(keys.GetMetadataKey(shard, 0, nil), nil)
		max := EncodeShardMetadataKey(keys.GetMetadataKey(shard, math.MaxUint64, nil), nil)
		var v []byte
		var logIndex uint64
		var err error
		if err := kv.base.Scan(min, max, func(key, value []byte) (bool, error) {
			key = key[1:]
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

		sm := meta.ShardMetadata{}
		protoc.MustUnmarshal(&sm, v)
		if sm.LogIndex != logIndex {
			panic(fmt.Sprintf("LogIndex not match, expect %d, but %d", logIndex, sm.LogIndex))
		}

		values = append(values, sm)
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
	err := kv.base.Sync()
	if err != nil {
		return err
	}

	kv.updatePersistentAppliedIndexes()
	return nil
}

func (kv *kvDataStorage) RemoveShard(shard meta.Shard, removeData bool) error {
	// This is not an atomic operation, but it is idempotent, and the metadata is
	// deleted afterwards, so the cleanup will not be lost.
	if removeData {
		min := EncodeShardStart(shard.Start, nil)
		max := EncodeShardEnd(shard.End, nil)
		if err := kv.base.RangeDelete(min, max, false); err != nil {
			return err
		}
	}

	min := EncodeShardMetadataKey(keys.GetRaftPrefix(shard.ID), nil)
	max := EncodeShardMetadataKey(keys.GetRaftPrefix(shard.ID+1), nil)
	return kv.base.RangeDelete(min, max, false)
}

// SplitCheck find keys from [start, end), so that the sum of bytes of the
// value of [start, key) <=size, returns the current bytes in [start,end),
// and the founded keys.
func (kv *kvDataStorage) SplitCheck(shard meta.Shard,
	size uint64) (uint64, uint64, [][]byte, []byte, error) {
	total := uint64(0)
	keys := uint64(0)
	sum := uint64(0)
	appendSplitKey := false
	var splitKeys [][]byte

	start := EncodeShardStart(shard.Start, nil)
	end := EncodeShardEnd(shard.End, nil)
	if err := kv.base.Scan(start, end, func(key, val []byte) (bool, error) {
		if appendSplitKey {
			splitKeys = append(splitKeys, key[1:])
			appendSplitKey = false
			sum = 0
		}
		n := uint64(len(key[1:]) + len(val))
		sum += n
		total += n
		keys++
		if sum >= size {
			appendSplitKey = true
		}
		return true, nil
	}, true); err != nil {
		return 0, 0, nil, nil, err
	}

	return total, keys, splitKeys, nil, nil
}

func (kv *kvDataStorage) Split(old meta.ShardMetadata,
	news []meta.ShardMetadata, ctx []byte) error {
	return kv.SaveShardMetadata(append(news, old))
}

func (kv *kvDataStorage) setAppliedIndexToWriteBatch(ctx storage.WriteContext, index uint64) {
	r := ctx.WriteBatch()
	wb := r.(util.WriteBatch)
	buffer := ctx.ByteBuf()
	// TODO(fagongzi): avoid allocate for get applied index key
	key := EncodeShardMetadataKey(keys.GetAppliedIndexKey(ctx.Shard().ID, nil), buffer)
	val := protoc.MustMarshal(&meta.LogIndex{Index: index})
	wb.Set(key, val)
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

	kv.updatePersistentAppliedIndexes()
	return nil
}

// delegate method
func (kv *kvDataStorage) Close() error {
	return kv.base.Close()
}

func (kv *kvDataStorage) CreateSnapshot(shardID uint64, path string) error {
	return kv.base.CreateSnapshot(shardID, path)
}

func (kv *kvDataStorage) ApplySnapshot(shardID uint64, path string) error {
	return kv.base.ApplySnapshot(shardID, path)
}

func (kv *kvDataStorage) Stats() stats.Stats {
	return kv.base.Stats()
}

func (kv *kvDataStorage) updatePersistentAppliedIndexes() {
	kv.mu.Lock()
	for k, v := range kv.mu.lastAppliedIndexes {
		kv.mu.persistentAppliedIndexes[k] = v
	}
	kv.mu.Unlock()
}

type readContext struct {
	base storage.ReadContext
}

func (c readContext) ByteBuf() *buf.ByteBuf { return c.base.ByteBuf() }
func (c readContext) Shard() meta.Shard     { return c.base.Shard() }
func (c readContext) SetReadBytes(v uint64) { c.base.SetReadBytes(v) }
func (c readContext) Request() storage.Request {
	req := c.base.Request()
	req.Key = EncodeDataKey(req.Key, c.base.ByteBuf())
	return req
}
