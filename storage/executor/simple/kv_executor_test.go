package simple

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

func TestReadAndWrite(t *testing.T) {
	cases := []struct {
		shard        uint64
		requests     storage.Batch
		responses    [][]byte
		appliedIndex uint64
		write        bool
	}{
		{
			shard:     1,
			requests:  newReadRequests(1, 0, 2),
			responses: newReadResponses(1, 0, 2, false),
		},
		{
			shard:        1,
			requests:     newWriteRequests(1, 1, 2),
			responses:    newWriteResponses(1, 1, 2),
			appliedIndex: 1,
			write:        true,
		},
		{
			shard:     1,
			requests:  newReadRequests(1, 0, 2),
			responses: joinResponses(newReadResponses(1, 0, 1, false), newReadResponses(1, 1, 2, true)),
		},

		{
			shard:     2,
			requests:  newReadRequests(2, 0, 2),
			responses: newReadResponses(2, 0, 2, false),
		},
		{
			shard:        2,
			requests:     newWriteRequests(2, 1, 2),
			responses:    newWriteResponses(1, 1, 2),
			appliedIndex: 1,
			write:        true,
		},
		{
			shard:     2,
			requests:  newReadRequests(2, 0, 2),
			responses: joinResponses(newReadResponses(2, 0, 1, false), newReadResponses(2, 1, 2, true)),
		},
	}

	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)

	kv := mem.NewStorage()
	executor := NewSimpleKVExecutor(kv)

	for i, c := range cases {
		if c.write {
			ctx := storage.NewSimpleWriteContext(c.shard, kv, c.requests)
			assert.NoError(t, executor.UpdateWriteBatch(ctx), "index %d", i)
			assert.NoError(t, executor.ApplyWriteBatch(ctx.WriteBatch()), "index %d", i)
			assert.True(t, reflect.DeepEqual(c.responses, ctx.Responses()), "index %d, responses %+v", i, ctx.Responses())
			assert.True(t, ctx.GetWrittenBytes() > 0, "index %d", i)
			assert.True(t, ctx.GetDiffBytes() > 0, "index %d", i)
		} else {
			for idx, req := range c.requests.Requests {
				ctx := storage.NewSimpleReadContext(c.shard, req)
				rsp, err := executor.Read(ctx)
				assert.NoError(t, err, "index %d", i)
				assert.Equal(t, c.responses[idx], rsp, "index %d", i)
			}
		}
	}
}

func newWriteRequests(shard uint64, keyStart, keyEnd uint64) storage.Batch {
	r := storage.Batch{}
	r.Index = keyEnd - 1

	for j := keyStart; j < keyEnd; j++ {
		r.Requests = append(r.Requests, NewWriteRequest([]byte(fmt.Sprintf("%d-%d", shard, j)),
			[]byte(fmt.Sprintf("%d-%d", shard, j))))
	}
	return r
}

func newWriteResponses(logN, keyStart, keyEnd uint64) [][]byte {
	var responses [][]byte
	for i := uint64(0); i < logN; i++ {
		for j := keyStart; j < keyEnd; j++ {
			responses = append(responses, OK)
		}

	}
	return responses
}

func newReadRequests(shard uint64, keyStart, keyEnd uint64) storage.Batch {
	r := storage.Batch{}
	for j := keyStart; j < keyEnd; j++ {
		r.Requests = append(r.Requests, NewReadRequest([]byte(fmt.Sprintf("%d-%d", shard, j))))
	}
	return r
}

func newReadResponses(shard uint64, keyStart, keyEnd uint64, exists bool) [][]byte {
	var responses [][]byte
	for j := keyStart; j < keyEnd; j++ {
		if exists {
			responses = append(responses, []byte(fmt.Sprintf("%d-%d", shard, j)))
		} else {
			responses = append(responses, nil)
		}
	}
	return responses
}

func joinResponses(values ...[][]byte) [][]byte {
	var res [][]byte
	for _, v := range values {
		res = append(res, v...)
	}
	return res
}
