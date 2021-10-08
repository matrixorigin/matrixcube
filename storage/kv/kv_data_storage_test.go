package kv

import (
	"fmt"
	"reflect"
	"testing"

	cpebble "github.com/cockroachdb/pebble"
	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/util/format"
	pvfs "github.com/lni/vfs"

	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor/simple"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

func TestSaveShardMetadataUpdatesLastAppliedIndex(t *testing.T) {
	tests := []struct {
		inputs []storage.ShardMetadata
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
			base := mem.NewStorage(fs)
			s := NewKVDataStorage(base, nil)
			defer s.Close()
			assert.NoError(t, s.SaveShardMetadata(tt.inputs))
			kv := s.(*kvDataStorage)
			for _, m := range tt.inputs {
				index, ok := kv.mu.lastAppliedIndexes[m.ShardID]
				assert.True(t, ok)
				assert.Equal(t, m.LogIndex, index)
				v, err := kv.base.Get(keys.GetAppliedIndexKey(m.ShardID))
				assert.NoError(t, err)
				assert.Equal(t, m.LogIndex, buf.Byte2UInt64(v))
			}
		}()
	}
}

func TestSaveShardMetadataAndGetInitialStates(t *testing.T) {
	cases := []struct {
		inputs []storage.ShardMetadata
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
			base := mem.NewStorage(fs)

			s := NewKVDataStorage(base, nil)
			defer s.Close()

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
	base := mem.NewStorage(fs)
	s := NewKVDataStorage(base, nil)
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
	inputs := newTestShardMetadata(2)
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	base := mem.NewStorage(fs)
	s := NewKVDataStorage(base, nil)
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
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	base := mem.NewStorage(fs)
	s := NewKVDataStorage(base, nil)
	defer s.Close()
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
	base := mem.NewStorage(fs)

	s := NewKVDataStorage(base, nil)
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
			base := mem.NewStorage(fs)

			s := NewKVDataStorage(base, simple.NewSimpleKVExecutor(base), WithSampleSync(c.sample))
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

			kv := s.(*kvDataStorage)
			v, err := kv.base.Get(keys.GetAppliedIndexKey(0))
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
			base, err := pebble.NewStorage("test-data", opts)
			assert.NoError(t, err)
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
				metadata := storage.ShardMetadata{
					ShardID:  shardID,
					LogIndex: index,
					Metadata: make([]byte, 100),
				}
				assert.NoError(t, s.SaveShardMetadata([]storage.ShardMetadata{metadata}))
			}

			persistent := uint64(0)
			metadataLogIndex := uint64(0)
			index := uint64(1)
			for {
				write(index)
				index++
				v, err := s.GetPersistentLogIndex(shardID)
				assert.NoError(t, err)
				if 0 != v {
					persistent = v
					break
				}
				save(index)
				metadataLogIndex = index
				index++
				v, err = s.GetPersistentLogIndex(shardID)
				assert.NoError(t, err)
				if 0 != v {
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
		base, err := pebble.NewStorage("test-data", opts)
		assert.NoError(t, err)
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

func newTestShardMetadata(n uint64) []storage.ShardMetadata {
	var values []storage.ShardMetadata
	for i := uint64(1); i < n; i++ {
		values = append(values, storage.ShardMetadata{
			ShardID:  i,
			LogIndex: i,
			Metadata: format.Uint64ToBytes(i),
		})
	}
	return values
}
