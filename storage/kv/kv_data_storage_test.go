package kv

import (
	"fmt"
	"reflect"
	"testing"

	cpebble "github.com/cockroachdb/pebble"
	"github.com/fagongzi/util/format"
	pvfs "github.com/lni/vfs"
	"github.com/matrixorigin/matrixcube/keys"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor/simple"
	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/util/buf"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testDir = "test-dir-safe-to-delete"
)

func getTestPebbleStorage(t *testing.T, fs vfs.FS) *pebble.Storage {
	fs.RemoveAll(testDir)
	base, err := pebble.NewStorage(testDir, nil, &cpebble.Options{FS: vfs.NewPebbleFS(fs)})
	require.NoError(t, err)
	return base
}

func TestSaveShardMetadataUpdatesLastAppliedIndex(t *testing.T) {
	tests := []struct {
		inputs []meta.ShardMetadata
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
			defer s.Close()
			defer fs.RemoveAll(testDir)
			assert.NoError(t, s.SaveShardMetadata(tt.inputs))
			kvd := s.(*kvDataStorage)
			for _, m := range tt.inputs {
				index, ok := kvd.mu.lastAppliedIndexes[m.ShardID]
				assert.True(t, ok)
				assert.Equal(t, m.LogIndex, index)
				v, err := kvd.base.Get(EncodeShardMetadataKey(keys.GetAppliedIndexKey(m.ShardID, nil), nil))
				assert.NoError(t, err)
				assert.Equal(t, m.LogIndex, buf.Byte2UInt64(v))
			}
		}()
	}
}

func TestSaveShardMetadataAndGetInitialStates(t *testing.T) {
	cases := []struct {
		inputs []meta.ShardMetadata
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
			defer s.Close()
			defer fs.RemoveAll(testDir)

			assert.NoError(t, s.SaveShardMetadata(c.inputs), "index %d", i)

			values, err := s.GetInitialStates()
			assert.NoError(t, err)
			assert.True(t, reflect.DeepEqual(c.inputs, values), "index %d", i)
		}()
	}
}

func TestGetInitialStatesReturnsTheMostRecentMetadata(t *testing.T) {
	inputs := newTestShardMetadata(2)
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, nil)
	defer s.Close()
	defer fs.RemoveAll(testDir)
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
	inputs := newTestShardMetadata(2)
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, nil)
	defer s.Close()
	defer fs.RemoveAll(testDir)
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
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, nil)
	defer s.Close()
	defer fs.RemoveAll(testDir)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("panic not triggered")
		}
	}()
	s.GetPersistentLogIndex(10)
}

func TestGetPersistentLogIndex(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, nil)
	defer s.Close()
	defer fs.RemoveAll(testDir)

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
			defer s.Close()
			defer fs.RemoveAll(testDir)

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
			assert.Equal(t, uint64(c.requests), buf.Byte2UInt64(v))
		}()
	}
}

func TestKVDataStorageRestartWithNotSyncedDataLost(t *testing.T) {
	for _, sample := range []uint64{10, 11} {
		memfs := vfs.NewMemFS()
		defer vfs.ReportLeakedFD(memfs, t)
		opts := &cpebble.Options{
			FS: vfs.NewPebbleFS(memfs),
		}
		memfs.MkdirAll("/test-data", 0755)
		dir, err := memfs.OpenDir("/")
		assert.NoError(t, err)
		dir.Sync()
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
				metadata := meta.ShardMetadata{
					ShardID:  shardID,
					LogIndex: index,
					Metadata: meta.ShardLocalState{Shard: meta.Shard{ID: shardID}},
				}
				assert.NoError(t, s.SaveShardMetadata([]meta.ShardMetadata{metadata}))
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
			index++
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
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	ds := NewKVDataStorage(base, nil)
	defer ds.Close()

	kv.Set(EncodeShardMetadataKey(keys.GetAppliedIndexKey(1, nil), nil), format.Uint64ToBytes(100), false)
	kv.Set(EncodeShardMetadataKey(keys.GetMetadataKey(1, 99, nil), nil), []byte{99}, false)
	kv.Set(EncodeShardMetadataKey(keys.GetMetadataKey(1, 100, nil), nil), []byte{100}, false)
	kv.Set(EncodeDataKey([]byte{1}, nil), []byte{1}, false)

	assert.NoError(t, ds.RemoveShard(meta.Shard{ID: 1, End: []byte{2}}, false))
	v, err := kv.Get(EncodeShardMetadataKey(keys.GetAppliedIndexKey(1, nil), nil))
	assert.NoError(t, err)
	assert.Empty(t, v)
	c := 0
	kv.Scan(EncodeShardMetadataKey(keys.GetMetadataKey(1, 99, nil), nil), EncodeShardMetadataKey(keys.GetMetadataKey(1, 200, nil), nil), func(key, value []byte) (bool, error) {
		c++
		return true, nil
	}, false)
	assert.Equal(t, 0, c)
	c = 0
	kv.Scan(EncodeShardStart(nil, nil), EncodeShardEnd([]byte{2}, nil), func(key, value []byte) (bool, error) {
		c++
		return true, nil
	}, false)
	assert.Equal(t, 1, c)

	kv.Set(EncodeShardMetadataKey(keys.GetAppliedIndexKey(2, nil), nil), format.Uint64ToBytes(200), false)
	kv.Set(EncodeShardMetadataKey(keys.GetMetadataKey(2, 99, nil), nil), []byte{199}, false)
	kv.Set(EncodeShardMetadataKey(keys.GetMetadataKey(2, 100, nil), nil), []byte{200}, false)
	kv.Set(EncodeDataKey([]byte{2}, nil), []byte{2}, false)

	assert.NoError(t, ds.RemoveShard(meta.Shard{ID: 2, Start: []byte{2}}, true))
	v, err = kv.Get(EncodeShardMetadataKey(keys.GetAppliedIndexKey(2, nil), nil))
	assert.NoError(t, err)
	assert.Empty(t, v)
	c = 0
	kv.Scan(EncodeShardMetadataKey(keys.GetMetadataKey(2, 99, nil), nil), EncodeShardMetadataKey(keys.GetMetadataKey(2, 200, nil), nil), func(key, value []byte) (bool, error) {
		c++
		return true, nil
	}, false)
	assert.Equal(t, 0, c)
	c = 0
	kv.Scan(EncodeShardStart([]byte{2}, nil), EncodeShardEnd(nil, nil), func(key, value []byte) (bool, error) {
		c++
		return true, nil
	}, false)
	assert.Equal(t, 0, c)
}

func TestSplitCheck(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	ds := NewKVDataStorage(base, nil)
	defer ds.Close()

	kv.Set(EncodeDataKey([]byte{1}, nil), []byte{1}, false)
	kv.Set(EncodeDataKey([]byte{2}, nil), []byte{2}, false)
	kv.Set(EncodeDataKey([]byte{3}, nil), []byte{3}, false)

	size, keys, splitKeys, ctx, err := ds.SplitCheck(meta.Shard{}, 100)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), size)
	assert.Equal(t, uint64(3), keys)
	assert.Empty(t, splitKeys)
	assert.Empty(t, ctx)

	size, keys, splitKeys, ctx, err = ds.SplitCheck(meta.Shard{}, 2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), size)
	assert.Equal(t, uint64(3), keys)
	assert.Equal(t, [][]byte{{2}, {3}}, splitKeys)
	assert.Empty(t, ctx)

	size, keys, splitKeys, ctx, err = ds.SplitCheck(meta.Shard{}, 4)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), size)
	assert.Equal(t, uint64(3), keys)
	assert.Equal(t, [][]byte{{3}}, splitKeys)
	assert.Empty(t, ctx)
}

func newTestShardMetadata(n uint64) []meta.ShardMetadata {
	var values []meta.ShardMetadata
	for i := uint64(1); i < n; i++ {
		values = append(values, meta.ShardMetadata{
			ShardID:  i,
			LogIndex: i,
			Metadata: meta.ShardLocalState{Shard: meta.Shard{ID: i}},
		})
	}
	return values
}
