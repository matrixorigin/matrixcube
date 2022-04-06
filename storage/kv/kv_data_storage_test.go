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
	"fmt"
	"reflect"
	"testing"

	cpebble "github.com/cockroachdb/pebble"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
	pvfs "github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixcube/keys"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor/simple"
	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/util/buf"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/vfs"
)

const (
	testDir = "/tmp/test-dir-safe-to-delete"
)

func getTestPebbleStorage(t *testing.T, fs vfs.FS) *pebble.Storage {
	require.NoError(t, fs.RemoveAll(testDir))
	base, err := pebble.NewStorage(testDir, nil, &cpebble.Options{FS: vfs.NewPebbleFS(fs)})
	require.NoError(t, err)
	return base
}

func TestSaveShardMetadataUpdatesLastAppliedIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		inputs []metapb.ShardMetadata
	}{
		{
			inputs: newTestShardMetadata(2),
		},
		{
			inputs: newTestShardMetadata(100),
		},
	}

	for _, tt := range tests {
		func() {
			fs := vfs.GetTestFS()
			defer vfs.ReportLeakedFD(fs, t)
			kv := getTestPebbleStorage(t, fs)
			base := NewBaseStorage(kv, fs)
			s := NewKVDataStorage(base, nil)
			defer func() {
				require.NoError(t, fs.RemoveAll(testDir))
			}()
			defer s.Close()

			assert.NoError(t, s.SaveShardMetadata(tt.inputs))
			kvd := s.(*kvDataStorage)
			for _, m := range tt.inputs {
				index, ok := kvd.mu.lastAppliedIndexes[m.ShardID]
				assert.True(t, ok)
				assert.Equal(t, m.LogIndex, index)
				v, err := kvd.base.Get(EncodeShardMetadataKey(keys.GetAppliedIndexKey(m.ShardID, nil), nil))
				assert.NoError(t, err)
				var logIndex metapb.LogIndex
				protoc.MustUnmarshal(&logIndex, v)
				assert.Equal(t, m.LogIndex, logIndex.Index)
			}
		}()
	}
}

func TestSaveShardMetadataAndGetInitialStates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cases := []struct {
		inputs []metapb.ShardMetadata
	}{
		{
			inputs: newTestShardMetadata(2),
		},
		{
			inputs: newTestShardMetadata(10),
		},
		{
			inputs: newTestShardMetadata(100),
		},
	}

	for i, c := range cases {
		func() {
			fs := vfs.GetTestFS()
			defer vfs.ReportLeakedFD(fs, t)
			kv := getTestPebbleStorage(t, fs)
			base := NewBaseStorage(kv, fs)
			s := NewKVDataStorage(base, nil)
			defer func() {
				require.NoError(t, fs.RemoveAll(testDir))
			}()
			defer s.Close()

			assert.NoError(t, s.SaveShardMetadata(c.inputs), "index %d", i)

			values, err := s.GetInitialStates()
			assert.NoError(t, err)
			assert.True(t, reflect.DeepEqual(c.inputs, values), "index %d", i)
		}()
	}
}

func TestGetInitialStatesReturnsTheMostRecentMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	inputs := newTestShardMetadata(2)
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, nil)
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()
	assert.NoError(t, s.SaveShardMetadata(inputs))

	for idx := range inputs {
		inputs[idx].LogIndex = 100
	}
	assert.NoError(t, s.SaveShardMetadata(inputs))
	values, err := s.GetInitialStates()
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(inputs, values))
	for _, v := range values {
		assert.Equal(t, uint64(100), v.LogIndex)
	}
}

func TestGetInitialStatesLoadsPersistentLogIndexValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	inputs := newTestShardMetadata(2)
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, nil)
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()
	assert.NoError(t, s.SaveShardMetadata(inputs))
	values, err := s.GetInitialStates()
	assert.NoError(t, err)
	for _, v := range values {
		index, err := s.GetPersistentLogIndex(v.ShardID)
		assert.NoError(t, err)
		assert.Equal(t, v.LogIndex, index)
	}
}

func TestGetPersistentLogIndexWillPanicWhenPersistentIndexesAreNotLoaded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, nil)
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("panic not triggered")
		}
	}()
	_, err := s.GetPersistentLogIndex(10)
	require.NoError(t, err)
}

func TestGetPersistentLogIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, nil)
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	cases := []struct {
		sample       uint64
		requests     int
		appliedIndex uint64
	}{
		{
			sample:       3,
			requests:     2,
			appliedIndex: 0,
		},
		{
			sample:       2,
			requests:     2,
			appliedIndex: 2,
		},
	}

	for i, c := range cases {
		func() {
			fs := vfs.GetTestFS()
			defer vfs.ReportLeakedFD(fs, t)
			kv := getTestPebbleStorage(t, fs)
			base := NewBaseStorage(kv, fs)
			s := NewKVDataStorage(base, simple.NewSimpleKVExecutor(base), WithSampleSync(c.sample))
			defer func() {
				require.NoError(t, fs.RemoveAll(testDir))
			}()
			defer s.Close()

			s.(*kvDataStorage).mu.loaded = true
			for i := 1; i <= c.requests; i++ {
				var batch storage.Batch
				batch.Index = uint64(i)
				k := []byte(fmt.Sprintf("%d", i))
				batch.Requests = append(batch.Requests, simple.NewWriteRequest(k, k))
				ctx := storage.NewSimpleWriteContext(0, base, batch)
				assert.NoError(t, s.Write(ctx), "index %d", i)
			}

			appliedIndex, err := s.GetPersistentLogIndex(0)
			assert.NoError(t, err, "index %d", i)
			assert.Equal(t, c.appliedIndex, appliedIndex, "index %d", i)

			kvd := s.(*kvDataStorage)
			v, err := kvd.base.Get(EncodeShardMetadataKey(keys.GetAppliedIndexKey(0, nil), nil))
			assert.NoError(t, err)
			var logIndex metapb.LogIndex
			protoc.MustUnmarshal(&logIndex, v)
			assert.Equal(t, uint64(c.requests), logIndex.Index)
		}()
	}
}

func TestKVDataStorageRestartWithNotSyncedDataLost(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, sample := range []uint64{10, 11} {
		memfs := vfs.NewMemFS()
		defer vfs.ReportLeakedFD(memfs, t)
		opts := &cpebble.Options{
			FS: vfs.NewPebbleFS(memfs),
		}
		require.NoError(t, memfs.MkdirAll("/test-data", 0755))
		dir, err := memfs.OpenDir("/")
		assert.NoError(t, err)
		require.NoError(t, dir.Sync())
		shardID := uint64(1)
		persistentLogIndex, metadataLogIndex := func() (uint64, uint64) {
			kv, err := pebble.NewStorage("test-data", nil, opts)
			assert.NoError(t, err)
			base := NewBaseStorage(kv, memfs)
			s := NewKVDataStorage(base, simple.NewSimpleKVExecutor(base), WithSampleSync(sample))
			defer func() {
				// to emulate a crash
				memfs.(*pvfs.MemFS).SetIgnoreSyncs(true)
				s.Close()
			}()
			_, err = s.GetInitialStates()
			assert.NoError(t, err)
			write := func(index uint64) {
				var batch storage.Batch
				batch.Index = index
				k := []byte(fmt.Sprintf("%d", 100))
				batch.Requests = append(batch.Requests, simple.NewWriteRequest(k, k))
				ctx := storage.NewSimpleWriteContext(shardID, base, batch)
				assert.NoError(t, s.Write(ctx))
			}

			save := func(index uint64) {
				metadata := metapb.ShardMetadata{
					ShardID:  shardID,
					LogIndex: index,
					Metadata: metapb.ShardLocalState{Shard: metapb.Shard{ID: shardID}},
				}
				assert.NoError(t, s.SaveShardMetadata([]metapb.ShardMetadata{metadata}))
			}

			persistent := uint64(0)
			metadataLogIndex := uint64(0)
			index := uint64(1)
			for {
				write(index)
				index++
				v, err := s.GetPersistentLogIndex(shardID)
				assert.NoError(t, err)
				if v != 0 {
					persistent = v
					break
				}
				save(index)
				metadataLogIndex = index
				index++
				v, err = s.GetPersistentLogIndex(shardID)
				assert.NoError(t, err)
				if v != 0 {
					persistent = v
					break
				}
			}

			write(index)
			index++
			save(index)
			index++
			write(index)
			v, err := s.GetPersistentLogIndex(shardID)
			assert.NoError(t, err)
			assert.Equal(t, persistent, v)
			return persistent, metadataLogIndex
		}()
		assert.True(t, metadataLogIndex > 0)
		memfs.(*pvfs.MemFS).ResetToSyncedState()
		memfs.(*pvfs.MemFS).SetIgnoreSyncs(false)
		kv, err := pebble.NewStorage("test-data", nil, opts)
		assert.NoError(t, err)
		base := NewBaseStorage(kv, memfs)
		s := NewKVDataStorage(base, simple.NewSimpleKVExecutor(base), WithSampleSync(sample))
		defer s.Close()
		md, err := s.GetInitialStates()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(md))
		assert.Equal(t, metadataLogIndex, md[0].LogIndex)
		index, err := s.GetPersistentLogIndex(shardID)
		assert.NoError(t, err)
		assert.Equal(t, persistentLogIndex, index)
	}
}

func TestRemoveShard(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	ds := NewKVDataStorage(base, nil)
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer ds.Close()

	require.NoError(t, kv.Set(EncodeShardMetadataKey(keys.GetAppliedIndexKey(1, nil), nil), format.Uint64ToBytes(100), false))
	require.NoError(t, kv.Set(EncodeShardMetadataKey(keys.GetMetadataKey(1, 99, nil), nil), []byte{99}, false))
	require.NoError(t, kv.Set(EncodeShardMetadataKey(keys.GetMetadataKey(1, 100, nil), nil), []byte{100}, false))
	require.NoError(t, kv.Set(EncodeDataKey([]byte{1}, nil), []byte{1}, false))

	assert.NoError(t, ds.RemoveShard(metapb.Shard{ID: 1, End: []byte{2}}, false))
	v, err := kv.Get(EncodeShardMetadataKey(keys.GetAppliedIndexKey(1, nil), nil))
	assert.NoError(t, err)
	assert.Empty(t, v)
	c := 0
	require.NoError(t, kv.Scan(EncodeShardMetadataKey(keys.GetMetadataKey(1, 99, nil), nil), EncodeShardMetadataKey(keys.GetMetadataKey(1, 200, nil), nil), func(key, value []byte) (bool, error) {
		c++
		return true, nil
	}, false))
	assert.Equal(t, 0, c)
	c = 0
	require.NoError(t, kv.Scan(EncodeShardStart(nil, nil), EncodeShardEnd([]byte{2}, nil), func(key, value []byte) (bool, error) {
		c++
		return true, nil
	}, false))
	assert.Equal(t, 1, c)

	require.NoError(t, kv.Set(EncodeShardMetadataKey(keys.GetAppliedIndexKey(2, nil), nil), format.Uint64ToBytes(200), false))
	require.NoError(t, kv.Set(EncodeShardMetadataKey(keys.GetMetadataKey(2, 99, nil), nil), []byte{199}, false))
	require.NoError(t, kv.Set(EncodeShardMetadataKey(keys.GetMetadataKey(2, 100, nil), nil), []byte{200}, false))
	require.NoError(t, kv.Set(EncodeDataKey([]byte{2}, nil), []byte{2}, false))
	ds.(*kvDataStorage).mu.Lock()
	ds.(*kvDataStorage).mu.lastAppliedIndexes[2] = 100
	ds.(*kvDataStorage).mu.persistentAppliedIndexes[2] = 100
	ds.(*kvDataStorage).mu.Unlock()

	assert.NoError(t, ds.RemoveShard(metapb.Shard{ID: 2, Start: []byte{2}}, true))
	ds.(*kvDataStorage).mu.RLock()
	assert.Equal(t, uint64(0), ds.(*kvDataStorage).mu.lastAppliedIndexes[2])
	assert.Equal(t, uint64(0), ds.(*kvDataStorage).mu.persistentAppliedIndexes[2])
	ds.(*kvDataStorage).mu.RUnlock()

	v, err = kv.Get(EncodeShardMetadataKey(keys.GetAppliedIndexKey(2, nil), nil))
	assert.NoError(t, err)
	assert.Empty(t, v)
	c = 0
	require.NoError(t, kv.Scan(EncodeShardMetadataKey(keys.GetMetadataKey(2, 99, nil), nil), EncodeShardMetadataKey(keys.GetMetadataKey(2, 200, nil), nil), func(key, value []byte) (bool, error) {
		c++
		return true, nil
	}, false))
	assert.Equal(t, 0, c)
	c = 0
	require.NoError(t, kv.Scan(EncodeShardStart([]byte{2}, nil), EncodeShardEnd(nil, nil), func(key, value []byte) (bool, error) {
		c++
		return true, nil
	}, false))
	assert.Equal(t, 0, c)
}

func TestSplitCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	ds := NewKVDataStorage(base, nil)
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer ds.Close()

	require.NoError(t, kv.Set(EncodeDataKey([]byte{1}, nil), []byte{1}, false))
	require.NoError(t, kv.Set(EncodeDataKey([]byte{2}, nil), []byte{2}, false))
	require.NoError(t, kv.Set(EncodeDataKey([]byte{3}, nil), []byte{3}, false))

	size, keys, splitKeys, ctx, err := ds.SplitCheck(metapb.Shard{}, 100)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), size)
	assert.Equal(t, uint64(3), keys)
	assert.Empty(t, splitKeys)
	assert.Empty(t, ctx)

	size, keys, splitKeys, ctx, err = ds.SplitCheck(metapb.Shard{}, 2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), size)
	assert.Equal(t, uint64(3), keys)
	assert.Equal(t, [][]byte{{2}, {3}}, splitKeys)
	assert.Empty(t, ctx)

	size, keys, splitKeys, ctx, err = ds.SplitCheck(metapb.Shard{}, 4)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), size)
	assert.Equal(t, uint64(3), keys)
	assert.Equal(t, [][]byte{{3}}, splitKeys)
	assert.Empty(t, ctx)
}

func TestSplitCheckWithSplitKeyFunc(t *testing.T) {
	// mvcc encode: key+uint64, fix key length 4
	decode := func(k []byte) []byte {
		return k[:4]
	}
	encode := func(k int, v uint64) []byte {
		newK := make([]byte, 12)
		buf.Int2BytesTo(k, newK)
		buf.Uint64ToBytesTo(v, newK[4:])
		return newK
	}

	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	ds := NewKVDataStorage(base, nil, WithFeature(storage.Feature{
		SplitKeyAdjustFunc: func(splitKey []byte) []byte {
			if len(splitKey) == 4 {
				return splitKey
			}
			return buf.Int2Bytes(buf.Byte2Int(decode(splitKey)) + 1)
		},
	}))
	defer func() {
		assert.NoError(t, fs.RemoveAll(testDir))
	}()
	defer func() {
		assert.NoError(t, ds.Close())
	}()

	v := []byte("v")

	// k 1, and has 1, 2, 3 version
	assert.NoError(t, base.Set(EncodeDataKey(buf.Int2Bytes(1), nil), v, false)) // 5          bytes
	assert.NoError(t, base.Set(EncodeDataKey(encode(1, 1), nil), v, false))     // 5 +13 = 18 bytes
	assert.NoError(t, base.Set(EncodeDataKey(encode(1, 2), nil), v, false))     // 18+13 = 31 bytes
	assert.NoError(t, base.Set(EncodeDataKey(encode(1, 3), nil), v, false))     // 31+13 = 44 bytes
	// k 2, and has 1, 2, 3 version
	assert.NoError(t, base.Set(EncodeDataKey(buf.Int2Bytes(2), nil), v, false))
	assert.NoError(t, base.Set(EncodeDataKey(encode(2, 1), nil), v, false))
	assert.NoError(t, base.Set(EncodeDataKey(encode(2, 2), nil), v, false))
	assert.NoError(t, base.Set(EncodeDataKey(encode(2, 3), nil), v, false))

	_, _, keys, _, err := ds.SplitCheck(metapb.Shard{}, 5)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(keys))
	assert.Equal(t, buf.Int2Bytes(2), keys[0])
	assert.Equal(t, buf.Int2Bytes(3), keys[1])

	_, _, keys, _, err = ds.SplitCheck(metapb.Shard{}, 44)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(keys))
	assert.Equal(t, buf.Int2Bytes(2), keys[0])
}

func newTestShardMetadata(n uint64) []metapb.ShardMetadata {
	var values []metapb.ShardMetadata
	for i := uint64(1); i < n; i++ {
		values = append(values, metapb.ShardMetadata{
			ShardID:  i,
			LogIndex: i,
			Metadata: metapb.ShardLocalState{Shard: metapb.Shard{ID: i}},
		})
	}
	return values
}
