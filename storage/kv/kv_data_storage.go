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
	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/stats"
)

type kvStorage struct {
	base     storage.KVBaseDataStorage
	executor storage.CommandExecutor
}

// NewKVStorage returns data storage based on a kv storage, support KV operations.
func NewKVStorage(base storage.KVBaseDataStorage, executor storage.CommandExecutor) storage.DataStorage {
	return &kvStorage{base: base, executor: executor}
}

func (kv *kvStorage) GetCommandExecutor() storage.CommandExecutor {
	return kv.executor
}

func (kv *kvStorage) SaveShardMetadata(shardID uint64, logIndex uint64, data []byte) error {
	wb := kv.base.NewWriteBatch()
	defer wb.Close()

	wb.Set(keys.GetDataStorageMetadataKey(shardID, logIndex), data)
	wb.Set(keys.GetDataStorageAppliedIndexKey(shardID), format.Uint64ToBytes(logIndex))

	return kv.base.Write(wb, false)
}

func (kv *kvStorage) GetShardMetadata(shardID uint64, logIndex uint64) ([]byte, error) {
	min := keys.GetDataStorageMetadataKey(shardID, 0)
	max := keys.GetDataStorageMetadataKey(shardID, logIndex+1)

	var last []byte
	err := kv.base.Scan(min, max, func(key, value []byte) (bool, error) {
		last = value
		return true, nil
	}, true)
	if err != nil {
		return nil, err
	}
	return last, nil
}

func (kv *kvStorage) GetAppliedLogIndex(shardID uint64) (uint64, error) {
	v, err := kv.base.Get(keys.GetDataStorageAppliedIndexKey(shardID))
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}

	return format.MustBytesToUint64(v), nil
}

func (kv *kvStorage) RemoveShardData(shard bhmetapb.Shard, encodedStartKey, encodedEndKey []byte) error {
	return kv.base.RangeDelete(encodedStartKey, encodedEndKey)
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
