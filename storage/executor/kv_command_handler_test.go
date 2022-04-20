// Copyright 2022 MatrixOrigin.
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

package executor

import (
	"testing"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/buf"
	keysutil "github.com/matrixorigin/matrixcube/util/keys"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

func TestHandleSetAndGet(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)

	kvStore := mem.NewStorage()
	defer kvStore.Close()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	wb := kvStore.NewWriteBatch().(util.WriteBatch)
	result, err := handleSet(metapb.Shard{}, newTestSetRequest("k1", "v1"), wb, buffer, kvStore)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), result.DiffBytes)
	assert.Equal(t, uint64(5), result.WrittenBytes)

	assert.NoError(t, kvStore.Write(wb, false))
	v, err := kvStore.Get(keysutil.EncodeDataKey([]byte("k1"), buffer))
	assert.NoError(t, err)
	assert.Equal(t, "v1", string(v))

	readed, err := handleGet(metapb.Shard{}, newTestGetRequest("k1"), buffer, kvStore)
	assert.NoError(t, err)
	assert.True(t, readed.ReadBytes > 0)
	assert.Equal(t, "v1", string(getTestGetResponseValue(readed.Response)))
}

func TestHandleBatchSetAndBatchGet(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)

	kvStore := mem.NewStorage()
	defer kvStore.Close()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	wb := kvStore.NewWriteBatch().(util.WriteBatch)
	result, err := handleBatchSet(metapb.Shard{}, newTestBatchSetRequest("k1", "v1", "k2", "v2"), wb, buffer, kvStore)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), result.DiffBytes)
	assert.Equal(t, uint64(10), result.WrittenBytes)

	assert.NoError(t, kvStore.Write(wb, false))
	v, err := kvStore.Get(keysutil.EncodeDataKey([]byte("k1"), buffer))
	assert.NoError(t, err)
	assert.Equal(t, "v1", string(v))
	v, err = kvStore.Get(keysutil.EncodeDataKey([]byte("k2"), buffer))
	assert.NoError(t, err)
	assert.Equal(t, "v2", string(v))

	readed, err := handleBatchGet(metapb.Shard{}, newTestBatchGetRequest("k1"), buffer, kvStore)
	assert.NoError(t, err)
	assert.True(t, readed.ReadBytes > 0)
	assert.Equal(t, [][]byte{[]byte("v1")}, getTestBatchGetResponseValue(readed.Response))

	readed, err = handleBatchGet(metapb.Shard{}, newTestBatchGetRequest("k2", "k3", "k1"), buffer, kvStore)
	assert.NoError(t, err)
	assert.True(t, readed.ReadBytes > 0)
	assert.Equal(t, [][]byte{[]byte("v2"), {}, []byte("v1")}, getTestBatchGetResponseValue(readed.Response))
}

func TestHandleDelete(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)

	kvStore := mem.NewStorage()
	defer kvStore.Close()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	wb := kvStore.NewWriteBatch().(util.WriteBatch)
	_, err := handleSet(metapb.Shard{}, newTestSetRequest("k1", "v1"), wb, buffer, kvStore)
	assert.NoError(t, err)

	result, err := handleDelete(metapb.Shard{}, newTestDeleteRequest("k1"), wb, buffer, kvStore)
	assert.NoError(t, err)
	assert.Equal(t, int64(-3), result.DiffBytes)
	assert.Equal(t, uint64(3), result.WrittenBytes)

	assert.NoError(t, kvStore.Write(wb, false))
	v, err := kvStore.Get(keysutil.EncodeDataKey([]byte("k1"), buffer))
	assert.NoError(t, err)
	assert.Equal(t, "", string(v))
}

func TestHandleBatchDelete(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)

	kvStore := mem.NewStorage()
	defer kvStore.Close()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	wb := kvStore.NewWriteBatch().(util.WriteBatch)
	_, err := handleBatchSet(metapb.Shard{}, newTestBatchSetRequest("k1", "v1", "k2", "v2", "k3", "v3"), wb, buffer, kvStore)
	assert.NoError(t, err)

	result, err := handleBatchDelete(metapb.Shard{}, newTestBatchDeleteRequest("k1", "k3"), wb, buffer, kvStore)
	assert.NoError(t, err)
	assert.Equal(t, int64(-6), result.DiffBytes)
	assert.Equal(t, uint64(6), result.WrittenBytes)

	assert.NoError(t, kvStore.Write(wb, false))
	v, err := kvStore.Get(keysutil.EncodeDataKey([]byte("k1"), buffer))
	assert.NoError(t, err)
	assert.Equal(t, "", string(v))
	v, err = kvStore.Get(keysutil.EncodeDataKey([]byte("k2"), buffer))
	assert.NoError(t, err)
	assert.Equal(t, "v2", string(v))
	v, err = kvStore.Get(keysutil.EncodeDataKey([]byte("k3"), buffer))
	assert.NoError(t, err)
	assert.Equal(t, "", string(v))
}

func TestHandleRangeDelete(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)

	kvStore := mem.NewStorage()
	defer kvStore.Close()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	wb := kvStore.NewWriteBatch().(util.WriteBatch)
	_, err := handleBatchSet(metapb.Shard{}, newTestBatchSetRequest("k1", "v1", "k2", "v2", "k3", "v3"), wb, buffer, kvStore)
	assert.NoError(t, err)

	result, err := handleRangeDelete(metapb.Shard{}, newTestRangeDeleteRequest("k1", "k3"), wb, buffer, kvStore)
	assert.NoError(t, err)
	assert.Equal(t, int64(-6), result.DiffBytes)
	assert.Equal(t, uint64(6), result.WrittenBytes)

	assert.NoError(t, kvStore.Write(wb, false))
	v, err := kvStore.Get(keysutil.EncodeDataKey([]byte("k1"), buffer))
	assert.NoError(t, err)
	assert.Equal(t, "", string(v))
	v, err = kvStore.Get(keysutil.EncodeDataKey([]byte("k2"), buffer))
	assert.NoError(t, err)
	assert.Equal(t, "", string(v))
	v, err = kvStore.Get(keysutil.EncodeDataKey([]byte("k3"), buffer))
	assert.NoError(t, err)
	assert.Equal(t, "v3", string(v))

	wb.Reset()
	_, err = handleRangeDelete(metapb.Shard{}, newTestRangeDeleteRequest("", ""), wb, buffer, kvStore)
	assert.NoError(t, err)
	assert.NoError(t, kvStore.Write(wb, false))
	v, err = kvStore.Get(keysutil.EncodeDataKey([]byte("k3"), buffer))
	assert.NoError(t, err)
	assert.Equal(t, "", string(v))
}

func TestHandleScan(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)

	kvStore := mem.NewStorage()
	defer kvStore.Close()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	assert.NoError(t, kvStore.Set(keysutil.EncodeDataKey([]byte("a"), nil), []byte("a"), false))
	assert.NoError(t, kvStore.Set(keysutil.EncodeDataKey([]byte("b"), nil), []byte("b"), false))
	assert.NoError(t, kvStore.Set(keysutil.EncodeDataKey([]byte("c"), nil), []byte("c"), false))
	assert.NoError(t, kvStore.Set(keysutil.EncodeDataKey([]byte("d"), nil), []byte("d"), false))

	cases := []struct {
		shard           metapb.Shard
		start, end      []byte
		withValue       bool
		onlyCount       bool
		limit           uint64
		limitBytes      uint64
		expectCount     uint64
		expectCompleted bool
		expectKeys      [][]byte
		expectValues    [][]byte
	}{
		{
			shard:           metapb.Shard{},
			start:           nil,
			end:             nil,
			onlyCount:       true,
			expectCompleted: true,
			expectCount:     4,
		},
		{
			shard:           metapb.Shard{End: []byte("c")},
			start:           nil,
			end:             nil,
			onlyCount:       true,
			expectCompleted: true,
			expectCount:     2,
		},
		{
			shard:           metapb.Shard{},
			start:           nil,
			end:             []byte("d"),
			onlyCount:       true,
			expectCompleted: true,
			expectCount:     3,
		},
		{
			shard:           metapb.Shard{},
			start:           []byte("b"),
			end:             []byte("d"),
			onlyCount:       true,
			expectCompleted: true,
			expectCount:     2,
		},
		{
			shard:           metapb.Shard{},
			start:           nil,
			end:             nil,
			onlyCount:       false,
			limit:           1,
			expectCompleted: false,
			expectCount:     1,
			expectKeys:      [][]byte{[]byte("a")},
		},
		{
			shard:           metapb.Shard{},
			start:           nil,
			end:             nil,
			onlyCount:       false,
			limitBytes:      1,
			expectCompleted: false,
			expectCount:     1,
			expectKeys:      [][]byte{[]byte("a")},
		},
		{
			shard:           metapb.Shard{},
			start:           nil,
			end:             nil,
			withValue:       true,
			onlyCount:       false,
			expectCompleted: true,
			expectCount:     4,
			expectKeys:      [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
			expectValues:    [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		},
		{
			shard:           metapb.Shard{},
			start:           nil,
			end:             nil,
			withValue:       false,
			onlyCount:       false,
			expectCompleted: true,
			expectCount:     4,
			expectKeys:      [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		},
	}

	for _, c := range cases {
		req := &rpcpb.KVScanRequest{}
		req.Start = c.start
		req.End = c.end
		req.Limit = c.limit
		req.LimitBytes = c.limitBytes
		req.WithValue = c.withValue
		req.OnlyCount = c.onlyCount
		result, err := handleScan(c.shard, protoc.MustMarshal(req), buffer, kvStore)
		assert.NoError(t, err)

		resp := &rpcpb.KVScanResponse{}
		protoc.MustUnmarshal(resp, result.Response)

		assert.Equal(t, c.expectCompleted, resp.Completed)
		assert.Equal(t, c.expectKeys, resp.Keys)
		assert.Equal(t, c.expectValues, resp.Values)
		assert.Equal(t, c.expectCount, resp.Count)
	}
}

func newTestSetRequest(k, v string) []byte {
	return protoc.MustMarshal(&rpcpb.KVSetRequest{
		Key:   []byte(k),
		Value: []byte(v),
	})
}

func newTestGetRequest(k string) []byte {
	return protoc.MustMarshal(&rpcpb.KVGetRequest{
		Key: []byte(k),
	})
}

func newTestBatchGetRequest(keys ...string) []byte {
	var req rpcpb.KVBatchGetRequest
	for _, key := range keys {
		req.Keys = append(req.Keys, []byte(key))
	}
	return protoc.MustMarshal(&req)
}

func newTestBatchSetRequest(keysAndValues ...string) []byte {
	var req rpcpb.KVBatchSetRequest

	n := len(keysAndValues) / 2
	for i := 0; i < n; i++ {
		req.Keys = append(req.Keys, []byte(keysAndValues[2*i]))
		req.Values = append(req.Values, []byte(keysAndValues[2*i+1]))
	}
	return protoc.MustMarshal(&req)
}

func newTestDeleteRequest(k string) []byte {
	return protoc.MustMarshal(&rpcpb.KVDeleteRequest{
		Key: []byte(k),
	})
}

func newTestRangeDeleteRequest(start, end string) []byte {
	return protoc.MustMarshal(&rpcpb.KVRangeDeleteRequest{
		Start: []byte(start),
		End:   []byte(end),
	})
}

func newTestBatchDeleteRequest(keys ...string) []byte {
	var req rpcpb.KVBatchDeleteRequest

	for _, k := range keys {
		req.Keys = append(req.Keys, []byte(k))
	}
	return protoc.MustMarshal(&req)
}

func getTestGetResponseValue(data []byte) []byte {
	var resp rpcpb.KVGetResponse
	protoc.MustUnmarshal(&resp, data)
	return resp.Value
}

func getTestBatchGetResponseValue(data []byte) [][]byte {
	var resp rpcpb.KVBatchGetResponse
	protoc.MustUnmarshal(&resp, data)
	return resp.Values
}
