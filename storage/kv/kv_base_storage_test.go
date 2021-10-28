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
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixcube/keys"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor/simple"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/util/buf"
	"github.com/matrixorigin/matrixcube/vfs"
)

func TestReadWriteBytes(t *testing.T) {
	tests := []struct {
		data []byte
	}{
		{nil},
		{[]byte("")},
		{[]byte("test-data")},
	}

	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	fn := "test-data-safe-to-delete"
	fs.RemoveAll(fn)
	for _, tt := range tests {
		defer vfs.ReportLeakedFD(fs, t)
		func() {
			f, err := fs.Create(fn)
			assert.NoError(t, err)
			defer f.Close()
			assert.NoError(t, writeBytes(f, tt.data))
		}()

		func() {
			f, err := fs.Open(fn)
			assert.NoError(t, err)
			defer fs.RemoveAll(fn)
			defer f.Close()
			result, err := readBytes(f)
			assert.NoError(t, err)
			if tt.data == nil && len(result) == 0 {
				result = nil
			}
			assert.Equal(t, tt.data, result)
		}()
	}
}

func TestGetAppliedIndexReturnsErrorOnEmptyDB(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := mem.NewStorage()
	base := NewBaseStorage(kv, fs)
	defer base.Close()
	view := base.GetView()
	defer view.Close()
	key, val, err := base.(*BaseStorage).getAppliedIndex(view.Raw().(*pebble.Snapshot), 100)
	assert.Empty(t, key)
	assert.Empty(t, val)
	assert.Equal(t, pebble.ErrNotFound, err)
}

func TestGetAppliedIndex(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := mem.NewStorage()
	base := NewBaseStorage(kv, fs)
	ds := NewKVDataStorage(base, simple.NewSimpleKVExecutor(kv))
	defer ds.Close()
	ctx := storage.NewSimpleWriteContext(100, kv, storage.Batch{Index: 200})
	assert.NoError(t, ds.Write(ctx))
	view := base.GetView()
	defer view.Close()
	key, val, err := base.(*BaseStorage).getAppliedIndex(view.Raw().(*pebble.Snapshot), 100)
	assert.NoError(t, err)
	assert.Equal(t, keys.GetAppliedIndexKey(100, nil), key)
	assert.Equal(t, uint64(200), buf.Byte2UInt64(val))
}

func TestGetShardMetadataReturnsErrorOnEmptyDB(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := mem.NewStorage()
	base := NewBaseStorage(kv, fs)
	defer base.Close()
	view := base.GetView()
	defer view.Close()
	key, val, err := base.(*BaseStorage).getShardMetadata(view.Raw().(*pebble.Snapshot), 100)
	assert.Empty(t, key)
	assert.Empty(t, val)
	assert.Equal(t, ErrNoMetadata, err)
}

func TestGetShardMetadata(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := mem.NewStorage()
	base := NewBaseStorage(kv, fs)
	ds := NewKVDataStorage(base, simple.NewSimpleKVExecutor(kv))
	defer ds.Close()
	sm1 := meta.ShardMetadata{
		ShardID:  100,
		LogIndex: 110,
		// Metadata: []byte("test-data-1"),
	}
	sm2 := meta.ShardMetadata{
		ShardID:  100,
		LogIndex: 120,
		// Metadata: []byte("test-data-2"),
	}
	assert.NoError(t, ds.SaveShardMetadata([]meta.ShardMetadata{sm1}))
	assert.NoError(t, ds.SaveShardMetadata([]meta.ShardMetadata{sm2}))
	view := base.GetView()
	defer view.Close()
	key, val, err := base.(*BaseStorage).getShardMetadata(view.Raw().(*pebble.Snapshot), 100)
	assert.NoError(t, err)
	assert.Equal(t, keys.GetMetadataKey(uint64(100), uint64(120), nil), key)
	assert.Equal(t, protoc.MustMarshal(&sm2), val)
}

func TestCreateAndApplySnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	dir := "snapshot-dir-safe-to-delete"
	shardID := uint64(100)
	fs.RemoveAll(dir)
	defer fs.RemoveAll(dir)
	var metadata []byte
	func() {
		kv := mem.NewStorage()
		base := NewBaseStorage(kv, fs)
		ds := NewKVDataStorage(base, simple.NewSimpleKVExecutor(kv))
		defer ds.Close()
		assert.NoError(t, base.Set([]byte("bb"), []byte("v"), false))
		assert.NoError(t, base.Set([]byte("mmm"), []byte("vv"), false))
		assert.NoError(t, base.Set([]byte("yy"), []byte("vvv"), false))
		shard := meta.Shard{
			Start: []byte("aa"),
			End:   []byte("xx"),
		}
		sls := meta.ShardLocalState{
			Shard: shard,
		}
		sm := meta.ShardMetadata{
			ShardID:  shardID,
			LogIndex: 110,
			Metadata: sls,
		}
		metadata = protoc.MustMarshal(&sm)
		assert.NoError(t, ds.SaveShardMetadata([]meta.ShardMetadata{sm}))
		index, err := base.CreateSnapshot(sm.ShardID, dir)
		assert.Equal(t, sm.LogIndex, index)
		assert.NoError(t, err)
	}()

	func() {
		kv := mem.NewStorage()
		base := NewBaseStorage(kv, fs)
		ds := NewKVDataStorage(base, simple.NewSimpleKVExecutor(kv))
		defer ds.Close()
		assert.NoError(t, base.Set([]byte("cc"), []byte("vv"), false))
		assert.NoError(t, base.Set([]byte("yy"), []byte("zzz"), false))
		assert.NoError(t, base.ApplySnapshot(shardID, dir))
		v, err := base.Get([]byte("cc"))
		assert.NoError(t, err)
		assert.Empty(t, v)
		v, err = base.Get([]byte("yy"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("zzz"), v)
		v, err = base.Get([]byte("bb"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("v"), v)
		v, err = base.Get([]byte("mmm"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("vv"), v)
		view := base.GetView()
		defer view.Close()
		key, val, err := base.(*BaseStorage).getAppliedIndex(view.Raw().(*pebble.Snapshot), shardID)
		assert.NoError(t, err)
		assert.Equal(t, keys.GetAppliedIndexKey(shardID, nil), key)
		assert.Equal(t, uint64(110), buf.Byte2UInt64(val))

		key, val, err = base.(*BaseStorage).getShardMetadata(view.Raw().(*pebble.Snapshot), shardID)
		assert.NoError(t, err)
		assert.Equal(t, keys.GetMetadataKey(shardID, uint64(110), nil), key)
		assert.Equal(t, metadata, val)
	}()
}

// TODO: add test for BaseStorage.SplitCheck()
