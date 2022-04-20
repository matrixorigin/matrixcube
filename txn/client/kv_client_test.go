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

package client

import (
	"testing"

	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/stretchr/testify/assert"
)

func TestRouteBatch(t *testing.T) {
	router := raftstore.NewMockRouter()
	addTestShardWithAdjust(router, 0, "10/11,20/21,30/31", func(s *metapb.Shard) { s.Start = nil })
	addTestShard(router, 1, "100/101,200/201,300/301")
	addTestShard(router, 2, "1000/1001,2000/2001,3000/3001")
	addTestShardWithAdjust(router, 3, "1000/10001,20000/20001,30000/30001", func(s *metapb.Shard) { s.End = nil })

	k0 := format.Uint64ToBytes(0)
	k1 := format.Uint64ToBytes(1)
	k2 := format.Uint64ToBytes(2)
	k3 := format.Uint64ToBytes(3)
	k4 := format.Uint64ToBytes(4)

	cases := []struct {
		keys         [][]byte
		values       [][]byte
		expectKeys   [][][]byte
		expectValues [][][]byte
		expectShard  []uint64
	}{
		{
			keys:        [][]byte{k0},
			expectKeys:  [][][]byte{{k0}},
			expectShard: []uint64{0},
		},
		{
			keys:         [][]byte{k0},
			values:       [][]byte{k0},
			expectKeys:   [][][]byte{{k0}},
			expectValues: [][][]byte{{k0}},
			expectShard:  []uint64{0},
		},
		{
			keys:        [][]byte{k0, k1},
			expectKeys:  [][][]byte{{k0}, {k1}},
			expectShard: []uint64{0, 1},
		},
		{
			keys:         [][]byte{k0, k1},
			values:       [][]byte{k0, k1},
			expectKeys:   [][][]byte{{k0}, {k1}},
			expectValues: [][][]byte{{k0}, {k1}},
			expectShard:  []uint64{0, 1},
		},
		{
			keys:        [][]byte{k0, k1, k2},
			expectKeys:  [][][]byte{{k0}, {k1}, {k2}},
			expectShard: []uint64{0, 1, 2},
		},
		{
			keys:         [][]byte{k0, k1, k2},
			values:       [][]byte{k0, k1, k2},
			expectKeys:   [][][]byte{{k0}, {k1}, {k2}},
			expectValues: [][][]byte{{k0}, {k1}, {k2}},
			expectShard:  []uint64{0, 1, 2},
		},
		{
			keys:        [][]byte{k0, k1, k2, k3},
			expectKeys:  [][][]byte{{k0}, {k1}, {k2}, {k3}},
			expectShard: []uint64{0, 1, 2, 3},
		},
		{
			keys:         [][]byte{k0, k1, k2, k3},
			values:       [][]byte{k0, k1, k2, k3},
			expectKeys:   [][][]byte{{k0}, {k1}, {k2}, {k3}},
			expectValues: [][][]byte{{k0}, {k1}, {k2}, {k3}},
			expectShard:  []uint64{0, 1, 2, 3},
		},
		{
			keys:        [][]byte{k0, k1, k2, k3, k4},
			expectKeys:  [][][]byte{{k0}, {k1}, {k2}, {k3, k4}},
			expectShard: []uint64{0, 1, 2, 3},
		},
		{
			keys:         [][]byte{k0, k1, k2, k3, k4},
			values:       [][]byte{k0, k1, k2, k3, k4},
			expectKeys:   [][][]byte{{k0}, {k1}, {k2}, {k3, k4}},
			expectValues: [][][]byte{{k0}, {k1}, {k2}, {k3, k4}},
			expectShard:  []uint64{0, 1, 2, 3},
		},
	}

	for _, c := range cases {
		shards := []uint64{}
		var resultKeys [][][]byte
		var resultValues [][][]byte
		routeBatch(router, 0, nil, c.keys, c.values, func(shardID uint64, keys, values [][]byte) {
			shards = append(shards, shardID)
			resultKeys = append(resultKeys, keys)
			if len(values) > 0 {
				resultValues = append(resultValues, values)
			}
		})
		assert.Equal(t, c.expectKeys, resultKeys)
		assert.Equal(t, c.expectValues, resultValues)
		assert.Equal(t, c.expectShard, shards)
	}
}

func TestRouteRange(t *testing.T) {
	router := raftstore.NewMockRouter()
	addTestShardWithAdjust(router, 0, "10/11,20/21,30/31", func(s *metapb.Shard) { s.Start = nil })
	addTestShard(router, 1, "100/101,200/201,300/301")
	addTestShard(router, 2, "1000/1001,2000/2001,3000/3001")
	addTestShardWithAdjust(router, 3, "1000/10001,20000/20001,30000/30001", func(s *metapb.Shard) { s.End = nil })

	k0 := format.Uint64ToBytes(0)
	k1 := format.Uint64ToBytes(1)
	k2 := format.Uint64ToBytes(2)
	k3 := format.Uint64ToBytes(3)
	k4 := format.Uint64ToBytes(4)

	cases := []struct {
		start        []byte
		end          []byte
		expectShards []uint64
		expectRanges [][][]byte
	}{
		{
			start:        k0,
			end:          k1,
			expectShards: []uint64{0},
			expectRanges: [][][]byte{{k0, k1}},
		},
		{
			start:        k0,
			end:          k2,
			expectShards: []uint64{0, 1},
			expectRanges: [][][]byte{{k0, k1}, {k1, k2}},
		},
		{
			start:        k0,
			end:          k3,
			expectShards: []uint64{0, 1, 2},
			expectRanges: [][][]byte{{k0, k1}, {k1, k2}, {k2, k3}},
		},
		{
			start:        k0,
			end:          k4,
			expectShards: []uint64{0, 1, 2, 3},
			expectRanges: [][][]byte{{k0, k1}, {k1, k2}, {k2, k3}, {k3, nil}},
		},
	}

	for _, c := range cases {
		shards := []uint64{}
		var resultRanges [][][]byte
		routeRange(router, 0, c.start, c.end, func(shardID uint64, start, end []byte) {
			resultRanges = append(resultRanges, [][]byte{start, end})
			shards = append(shards, shardID)
		})
		assert.Equal(t, c.expectShards, shards)
		assert.Equal(t, c.expectRanges, resultRanges)
	}
}

func TestRouteWithToShardWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			assert.Fail(t, "must panic")
		}
	}()

	router := raftstore.NewMockRouter()
	kvRouter := NewKVOperationRouter(router)
	_, _, err := kvRouter.Route(&txnpb.TxnOperation{
		ToShard: 1,
	})
	assert.NoError(t, err)
}

func TestRouteWithSingleOperation(t *testing.T) {
	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "100/101,200/201,300/301")
	addTestShard(router, 2, "1000/1001,2000/2001,3000/3001")
	kvRouter := NewKVOperationRouter(router)

	cases := []struct {
		op          rpcpb.InternalCmd
		key         []byte
		expectShard uint64
	}{
		{
			op:          rpcpb.CmdKVSet,
			key:         format.Uint64ToBytes(1),
			expectShard: 1,
		},
		{
			op:          rpcpb.CmdKVDelete,
			key:         format.Uint64ToBytes(1),
			expectShard: 1,
		},
		{
			op:          rpcpb.CmdKVGet,
			key:         format.Uint64ToBytes(1),
			expectShard: 1,
		},
		{
			op:          rpcpb.CmdKVScan,
			key:         format.Uint64ToBytes(1),
			expectShard: 1,
		},
	}
	for _, c := range cases {
		op := &txnpb.TxnOperation{Op: uint32(c.op), Impacted: txnpb.KeySet{PointKeys: [][]byte{c.key}, Ranges: []txnpb.KeyRange{{Start: c.key}}}}
		changed, ops, err := kvRouter.Route(op)
		assert.False(t, changed)
		assert.Empty(t, ops)
		assert.NoError(t, err)
		assert.Equal(t, c.expectShard, op.ToShard)
	}
}

func TestRouteWithMultiKeysOperation(t *testing.T) {
	router := raftstore.NewMockRouter()
	addTestShardWithAdjust(router, 1, "100/101,200/201,300/301", func(s *metapb.Shard) { s.Start = nil })
	addTestShardWithAdjust(router, 2, "1000/1001,2000/2001,3000/3001", func(s *metapb.Shard) { s.End = nil })
	kvRouter := NewKVOperationRouter(router)

	k1 := format.Uint64ToBytes(1)
	k2 := format.Uint64ToBytes(2)

	cases := []struct {
		op           rpcpb.InternalCmd
		keys         [][]byte
		expectShards []uint64
		expectKeys   [][][]byte
	}{
		{
			op:           rpcpb.CmdKVBatchGet,
			keys:         [][]byte{k1, k2},
			expectShards: []uint64{1, 2},
			expectKeys:   [][][]byte{{k1}, {k2}},
		},
		{
			op:           rpcpb.CmdKVBatchDelete,
			keys:         [][]byte{k1, k2},
			expectShards: []uint64{1, 2},
			expectKeys:   [][][]byte{{k1}, {k2}},
		},
	}
	for _, c := range cases {
		op := &txnpb.TxnOperation{Op: uint32(c.op), Impacted: txnpb.KeySet{PointKeys: c.keys}}
		changed, ops, err := kvRouter.Route(op)
		assert.True(t, changed)
		assert.NotEmpty(t, ops)
		assert.NoError(t, err)

		for idx := range ops {
			assert.Equal(t, c.expectKeys[idx], ops[idx].Impacted.PointKeys)
			assert.Equal(t, c.expectShards[idx], ops[idx].ToShard)
		}
	}
}

func TestRouteWithBatchSetOperation(t *testing.T) {
	router := raftstore.NewMockRouter()
	addTestShardWithAdjust(router, 1, "100/101,200/201,300/301", func(s *metapb.Shard) { s.Start = nil })
	addTestShardWithAdjust(router, 2, "1000/1001,2000/2001,3000/3001", func(s *metapb.Shard) { s.End = nil })

	k1 := format.Uint64ToBytes(1)
	k2 := format.Uint64ToBytes(2)

	kvRouter := NewKVOperationRouter(router)
	op := &txnpb.TxnOperation{Op: uint32(rpcpb.CmdKVBatchSet), Payload: protoc.MustMarshal(&rpcpb.KVBatchSetRequest{
		Keys:   [][]byte{k1, k2},
		Values: [][]byte{k1, k2},
	})}

	changed, ops, err := kvRouter.Route(op)
	assert.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, 2, len(ops))

	assert.Equal(t, op.Op, ops[0].Op)
	assert.Equal(t, op.Op, ops[1].Op)

	req := &rpcpb.KVBatchSetRequest{}
	protoc.MustUnmarshal(req, ops[0].Payload)
	assert.Equal(t, req.Keys, [][]byte{k1})
	assert.Equal(t, req.Values, [][]byte{k1})

	req.Reset()
	protoc.MustUnmarshal(req, ops[1].Payload)
	assert.Equal(t, req.Keys, [][]byte{k2})
	assert.Equal(t, req.Values, [][]byte{k2})
}

func TestRouteWithRangeDeleteOperation(t *testing.T) {
	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "100/101,200/201,300/301")
	addTestShard(router, 2, "1000/1001,2000/2001,3000/3001")
	addTestShard(router, 3, "10000/10001,20000/20001,30000/30001")

	k1 := format.Uint64ToBytes(1)
	k2 := format.Uint64ToBytes(2)
	k3 := format.Uint64ToBytes(3)

	kvRouter := NewKVOperationRouter(router)
	op := &txnpb.TxnOperation{Op: uint32(rpcpb.CmdKVRangeDelete), Impacted: txnpb.KeySet{Ranges: []txnpb.KeyRange{{Start: k1, End: k3}}}}

	changed, ops, err := kvRouter.Route(op)
	assert.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, 2, len(ops))

	assert.Equal(t, op.Op, ops[0].Op)
	assert.Equal(t, op.Op, ops[1].Op)

	req := &rpcpb.KVRangeDeleteRequest{}
	protoc.MustUnmarshal(req, ops[0].Payload)
	assert.Equal(t, req.Start, k1)
	assert.Equal(t, req.End, k2)

	req.Reset()
	protoc.MustUnmarshal(req, ops[1].Payload)
	assert.Equal(t, req.Start, k2)
	assert.Equal(t, req.End, k3)
}

func TestSet(t *testing.T) {
	txn := &kvTxn{}
	k1 := []byte("k1")
	v1 := []byte("v1")
	txn.Set(k1, v1)

	assert.Equal(t, 1, len(txn.ops))
	assert.Equal(t, [][]byte{k1}, txn.ops[0].Impacted.PointKeys)
	assert.Equal(t, txnpb.ImpactedType_WriteImpacted, txn.ops[0].ImpactedType)
}

func TestDelete(t *testing.T) {
	txn := &kvTxn{}
	k1 := []byte("k1")
	txn.Delete(k1)

	assert.Equal(t, 1, len(txn.ops))
	assert.Equal(t, [][]byte{k1}, txn.ops[0].Impacted.PointKeys)
	assert.Equal(t, txnpb.ImpactedType_WriteImpacted, txn.ops[0].ImpactedType)
}

func TestBatchSet(t *testing.T) {
	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "100/101,200/201,300/301")
	addTestShard(router, 2, "1000/1001,2000/2001,3000/3001")
	addTestShard(router, 3, "10000/10001,20000/20001,30000/30001")

	txn := &kvTxn{router: router}
	k1 := format.Uint64ToBytes(1)
	v1 := []byte("v1")
	k2 := format.Uint64ToBytes(2)
	v2 := []byte("v2")

	txn.BatchSet([][]byte{k1, k2}, [][]byte{v1, v2})

	assert.Equal(t, 2, len(txn.ops))
	assert.Equal(t, [][]byte{k1}, txn.ops[0].Impacted.PointKeys)
	assert.Equal(t, txnpb.ImpactedType_WriteImpacted, txn.ops[0].ImpactedType)
	assert.Equal(t, [][]byte{k2}, txn.ops[1].Impacted.PointKeys)
	assert.Equal(t, txnpb.ImpactedType_WriteImpacted, txn.ops[0].ImpactedType)
}

func TestBatchDelete(t *testing.T) {
	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "100/101,200/201,300/301")
	addTestShard(router, 2, "1000/1001,2000/2001,3000/3001")
	addTestShard(router, 3, "10000/10001,20000/20001,30000/30001")

	txn := &kvTxn{router: router}
	k1 := format.Uint64ToBytes(1)
	k2 := format.Uint64ToBytes(2)

	txn.BatchDelete([][]byte{k1, k2})

	assert.Equal(t, 2, len(txn.ops))
	assert.Equal(t, [][]byte{k1}, txn.ops[0].Impacted.PointKeys)
	assert.Equal(t, txnpb.ImpactedType_WriteImpacted, txn.ops[0].ImpactedType)
	assert.Equal(t, [][]byte{k2}, txn.ops[1].Impacted.PointKeys)
	assert.Equal(t, txnpb.ImpactedType_WriteImpacted, txn.ops[0].ImpactedType)
}

func TestRangeDelete(t *testing.T) {
	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "100/101,200/201,300/301")
	addTestShard(router, 2, "1000/1001,2000/2001,3000/3001")
	addTestShard(router, 3, "10000/10001,20000/20001,30000/30001")

	txn := &kvTxn{router: router}
	k1 := format.Uint64ToBytes(1)
	k2 := format.Uint64ToBytes(2)
	k3 := format.Uint64ToBytes(3)

	txn.RangeDelete(k1, k3)

	assert.Equal(t, 2, len(txn.ops))
	assert.Equal(t, txnpb.KeyRange{Start: k1, End: k2}, txn.ops[0].Impacted.Ranges[0])
	assert.Equal(t, txnpb.ImpactedType_WriteImpacted, txn.ops[0].ImpactedType)
	assert.Equal(t, txnpb.KeyRange{Start: k2, End: k3}, txn.ops[1].Impacted.Ranges[0])
	assert.Equal(t, txnpb.ImpactedType_WriteImpacted, txn.ops[0].ImpactedType)
}

// TODO: more read and commit txn tests
