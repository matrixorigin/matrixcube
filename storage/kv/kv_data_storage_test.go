package kv

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor/simple"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

func TestSaveShardMetadataAndGetInitialStates(t *testing.T) {
	cases := []struct {
		inputs  []storage.ShardMetadata
		outputs []storage.ShardMetadata
	}{
		{
			inputs:  newTestShardMetadata(1),
			outputs: newTestShardMetadata(1),
		},
		{
			inputs:  newTestShardMetadata(10),
			outputs: newTestShardMetadata(10),
		},
		{
			inputs:  newTestShardMetadata(100),
			outputs: newTestShardMetadata(100),
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
			assert.True(t, reflect.DeepEqual(c.outputs, values), "index %d", i)
		}()
	}
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
			appliedIndex: 1,
		},
	}

	for i, c := range cases {
		func() {
			fs := vfs.GetTestFS()
			defer vfs.ReportLeakedFD(fs, t)
			base := mem.NewStorage(fs)

			s := NewKVDataStorage(base, simple.NewSimpleKVExecutor(base), WithSampleSync(c.sample))
			defer s.Close()

			for i := 0; i < c.requests; i++ {
				var batch storage.Batch
				batch.Index = uint64(i)
				k := []byte(fmt.Sprintf("%d", i))
				batch.Requests = append(batch.Requests, simple.NewWriteRequest(k, k))
				ctx := storage.NewSimpleContext(0, base, []storage.Batch{batch})
				assert.NoError(t, s.Write(ctx), "index %d", i)
			}

			appliedIndex, err := s.GetPersistentLogIndex(0)
			assert.NoError(t, err, "index %d", i)
			assert.Equal(t, c.appliedIndex, appliedIndex, "index %d", i)
		}()
	}
}

func newTestShardMetadata(n uint64) []storage.ShardMetadata {
	var values []storage.ShardMetadata
	for i := uint64(0); i < n; i++ {
		values = append(values, storage.ShardMetadata{
			ShardID:  i,
			LogIndex: i,
			Metadata: format.Uint64ToBytes(i),
		})
	}
	return values
}
