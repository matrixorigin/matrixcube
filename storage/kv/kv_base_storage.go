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

package kv

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/fagongzi/util/protoc"

	"github.com/matrixorigin/matrixcube/keys"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/stats"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/vfs"
)

var (
	ErrNoMetadata = errors.New("no metadata")
)

type BaseStorage struct {
	kv storage.KVStorage
	fs vfs.FS
}

func NewBaseStorage(kv storage.KVStorage, fs vfs.FS) storage.KVBaseStorage {
	return &BaseStorage{
		kv: kv,
		fs: fs,
	}
}

func (s *BaseStorage) GetView() storage.View {
	return s.kv.GetView()
}

func (s *BaseStorage) ScanInView(view storage.View,
	start, end []byte, handler func(key, value []byte) (bool, error)) error {
	return s.kv.ScanInView(view, start, end, handler)
}

func (s *BaseStorage) Close() error {
	return s.kv.Close()
}

func (s *BaseStorage) NewWriteBatch() storage.Resetable {
	return s.kv.NewWriteBatch()
}

func (s *BaseStorage) Stats() stats.Stats {
	return s.kv.Stats()
}

func (s *BaseStorage) Write(wb util.WriteBatch, sync bool) error {
	return s.kv.Write(wb, sync)
}

func (s *BaseStorage) Set(key []byte, value []byte, sync bool) error {
	return s.kv.Set(key, value, sync)
}

func (s *BaseStorage) Get(key []byte) ([]byte, error) {
	return s.kv.Get(key)
}

func (s *BaseStorage) Delete(key []byte, sync bool) error {
	return s.kv.Delete(key, sync)
}

func (s *BaseStorage) Scan(start, end []byte,
	handler func(key, value []byte) (bool, error), copy bool) error {
	return s.kv.Scan(start, end, handler, copy)
}

func (s *BaseStorage) PrefixScan(prefix []byte,
	handler func(key, value []byte) (bool, error), copy bool) error {
	return s.kv.PrefixScan(prefix, handler, copy)
}

func (s *BaseStorage) RangeDelete(start, end []byte, sync bool) error {
	return s.kv.RangeDelete(start, end, sync)
}

func (s *BaseStorage) Seek(key []byte) ([]byte, []byte, error) {
	return s.kv.Seek(key)
}

func (s *BaseStorage) Sync() error {
	return s.kv.Sync()
}

func (s *BaseStorage) getAppliedIndex(ss *pebble.Snapshot,
	shardID uint64) ([]byte, []byte, error) {
	key := EncodeShardMetadataKey(keys.GetAppliedIndexKey(shardID, nil), nil)
	v, closer, err := ss.Get(key)
	if err != nil {
		return nil, nil, err
	}
	defer closer.Close()
	return key, v, nil
}

func (s *BaseStorage) getShardMetadata(ss *pebble.Snapshot,
	shardID uint64) ([]byte, []byte, error) {
	ios := &pebble.IterOptions{
		LowerBound: EncodeShardMetadataKey(keys.GetMetadataKey(shardID, 0, nil), nil),
		UpperBound: EncodeShardMetadataKey(keys.GetMetadataKey(shardID, math.MaxUint64, nil), nil),
	}
	iter := ss.NewIter(ios)
	defer iter.Close()

	clone := func(value []byte) []byte {
		v := make([]byte, len(value))
		copy(v, value)
		return v
	}

	var value []byte
	var key []byte
	iter.First()
	for iter.Valid() {
		if err := iter.Error(); err != nil {
			return nil, nil, err
		}
		keyShardID, err := keys.GetShardIDFromMetadataKey(iter.Key()[1:])
		if err == nil && keyShardID == shardID {
			value = clone(iter.Value())
			key = clone(iter.Key())
		} else {
			break
		}
		iter.Next()
	}

	if len(value) == 0 || len(key) == 0 {
		return nil, nil, ErrNoMetadata
	}
	return key, value, nil
}

// TODO: change the snapshot ops below to sst ingestion based with
// special attention paid to its sync state.

// CreateSnapshot create a snapshot file under the giving path
func (s *BaseStorage) CreateSnapshot(shardID uint64,
	path string) (uint64, error) {
	if err := s.fs.MkdirAll(path, 0755); err != nil {
		return 0, err
	}
	file := s.fs.PathJoin(path, "db.data")
	f, err := s.fs.Create(file)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	view := s.kv.GetView()
	defer view.Close()

	snap := view.Raw().(*pebble.Snapshot)
	appliedIndexKey, appliedIndexValue, err := s.getAppliedIndex(snap, shardID)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get applied index in CreateSnapshot")
	}
	metadataKey, metadataValue, err := s.getShardMetadata(snap, shardID)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get shard in CreateSnapshot")
	}

	var sls meta.ShardMetadata
	var logIndex meta.LogIndex
	protoc.MustUnmarshal(&sls, metadataValue)
	protoc.MustUnmarshal(&logIndex, appliedIndexValue)
	shard := sls.Metadata.Shard

	if err := writeBytes(f, EncodeShardStart(shard.Start, nil)); err != nil {
		return 0, err
	}
	if err := writeBytes(f, EncodeShardEnd(shard.End, nil)); err != nil {
		return 0, err
	}
	if err := writeBytes(f, appliedIndexKey); err != nil {
		return 0, err
	}
	if err := writeBytes(f, appliedIndexValue); err != nil {
		return 0, err
	}
	if err := writeBytes(f, metadataKey); err != nil {
		return 0, err
	}
	if err := writeBytes(f, metadataValue); err != nil {
		return 0, err
	}

	ios := &pebble.IterOptions{
		LowerBound: EncodeShardStart(shard.Start, nil),
		UpperBound: EncodeShardEnd(shard.End, nil),
	}

	iter := snap.NewIter(ios)
	defer iter.Close()
	iter.First()
	for iter.Valid() {
		if err := iter.Error(); err != nil {
			return 0, err
		}
		if err := writeBytes(f, iter.Key()); err != nil {
			return 0, err
		}
		if err = writeBytes(f, iter.Value()); err != nil {
			return 0, err
		}
		iter.Next()
	}

	return logIndex.Index, nil
}

// ApplySnapshot apply a snapshort file from giving path
func (s *BaseStorage) ApplySnapshot(shardID uint64, path string) error {
	f, err := s.fs.Open(s.fs.PathJoin(path, "db.data"))
	if err != nil {
		return err
	}
	defer f.Close()
	start, err := readBytes(f)
	if err != nil {
		return err
	}
	if len(start) == 0 {
		panic("range start not specified in snapshot")
	}
	end, err := readBytes(f)
	if err != nil {
		return err
	}
	if len(end) == 0 {
		panic("range end not specified in snapshot")
	}
	appliedIndexKey, err := readBytes(f)
	if err != nil {
		return err
	}
	appliedIndexValue, err := readBytes(f)
	if err != nil {
		return err
	}
	metadataKey, err := readBytes(f)
	if err != nil {
		return err
	}
	metadataValue, err := readBytes(f)
	if err != nil {
		return err
	}
	if err := s.kv.RangeDelete(start, end, false); err != nil {
		return errors.Wrapf(err, "RangeDelete failed in ApplySnapshot")
	}
	if err := s.kv.Set(appliedIndexKey, appliedIndexValue, false); err != nil {
		return err
	}
	if err := s.kv.Set(metadataKey, metadataValue, false); err != nil {
		return err
	}

	for {
		key, err := readBytes(f)
		if err != nil {
			return err
		}
		if len(key) == 0 {
			break
		}
		value, err := readBytes(f)
		if err != nil {
			return err
		}
		if len(value) == 0 {
			panic("key specified without value")
		}
		if err := s.kv.Set(key, value, false); err != nil {
			return err
		}
	}

	return s.kv.Sync()
}

func writeBytes(f vfs.File, data []byte) error {
	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, uint32(len(data)))
	if _, err := f.Write(size); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	return nil
}

func readBytes(f vfs.File) ([]byte, error) {
	size := make([]byte, 4)
	n, err := f.Read(size)
	if n == 0 && err == io.EOF {
		return nil, nil
	}

	total := int(binary.BigEndian.Uint32(size))
	written := 0
	data := make([]byte, total)
	for {
		n, err = f.Read(data[written:])
		if err != nil && err != io.EOF {
			return nil, err
		}
		written += n
		if written == total {
			return data, nil
		}
	}
}
