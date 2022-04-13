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

package kv

import (
	"testing"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/txn"
	"github.com/matrixorigin/matrixcube/util/buf"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/testutil"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

func TestHandleGet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	dr := txn.NewTransactionDataReader(ts)

	k1 := []byte("k1")
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k1, normalValuePrefix, 1)

	rw := newKVReaderAndWriter(buffer, testutil.GetTestTxnOpMeta(0, 0, 2), dr, nil)
	v, err := handleGet(metapb.Shard{}, txnpb.TxnRequest{
		Operation: txnpb.TxnOperation{
			Payload: protoc.MustMarshal(&rpcpb.KVGetRequest{
				Key: k1,
			}),
		},
	}, rw)
	assert.NoError(t, err)
	var resp rpcpb.KVGetResponse
	protoc.MustUnmarshal(&resp, v)
	assert.Equal(t, "k1-1(c)", string(resp.Value))
}

func TestHandleGetWithLowerTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	dr := txn.NewTransactionDataReader(ts)

	k1 := []byte("k1")
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k1, normalValuePrefix, 1)

	rw := newKVReaderAndWriter(buffer, testutil.GetTestTxnOpMeta(0, 0, 1), dr, nil)
	v, err := handleGet(metapb.Shard{}, txnpb.TxnRequest{
		Operation: txnpb.TxnOperation{
			Payload: protoc.MustMarshal(&rpcpb.KVGetRequest{
				Key: k1,
			}),
		},
	}, rw)
	assert.NoError(t, err)
	var resp rpcpb.KVGetResponse
	protoc.MustUnmarshal(&resp, v)
	assert.Equal(t, "", string(resp.Value))
}

func TestHandleGetWithNoCommitted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, _, closer := getTestTransactionDataStorage(t)
	defer closer()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	dr := txn.NewTransactionDataReader(ts)

	rw := newKVReaderAndWriter(buffer, testutil.GetTestTxnOpMeta(0, 0, 1), dr, nil)
	v, err := handleGet(metapb.Shard{}, txnpb.TxnRequest{
		Operation: txnpb.TxnOperation{
			Payload: protoc.MustMarshal(&rpcpb.KVGetRequest{
				Key: []byte("k1"),
			}),
		},
	}, rw)
	assert.NoError(t, err)
	var resp rpcpb.KVGetResponse
	protoc.MustUnmarshal(&resp, v)
	assert.Equal(t, "", string(resp.Value))
}

func TestHandleBatchGet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	dr := txn.NewTransactionDataReader(ts)

	k1 := []byte("k1")
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k1, normalValuePrefix, 1)
	k2 := []byte("k2")
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k2, normalValuePrefix, 1)

	rw := newKVReaderAndWriter(buffer, testutil.GetTestTxnOpMeta(0, 0, 2), dr, nil)
	v, err := handleBatchGet(metapb.Shard{}, txnpb.TxnRequest{
		Operation: txnpb.TxnOperation{
			Payload: protoc.MustMarshal(&rpcpb.KVBatchGetRequest{
				Keys: [][]byte{k1, k2},
			}),
		},
	}, rw)
	assert.NoError(t, err)
	var resp rpcpb.KVBatchGetResponse
	protoc.MustUnmarshal(&resp, v)
	assert.Equal(t, [][]byte{[]byte("k1-1(c)"), []byte("k2-1(c)")}, resp.Values)
}

func TestHandleScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	k1 := []byte("k1")
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k1, deletedValuePrefix, 1)
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k1, normalValuePrefix, 2)
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k1, normalValuePrefix, 3)

	k2 := []byte("k2")
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k2, normalValuePrefix, 1)
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k2, normalValuePrefix, 2)
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k2, normalValuePrefix, 3)

	k3 := []byte("k3")
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k3, normalValuePrefix, 1)
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k3, normalValuePrefix, 2)
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k3, normalValuePrefix, 3)

	k4 := []byte("k4")
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k4, normalValuePrefix, 1)
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k4, normalValuePrefix, 2)
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k4, normalValuePrefix, 3)

	dr := txn.NewTransactionDataReader(ts)

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

		seq   uint32
		epoch uint32
		ts    int64
	}{
		{
			onlyCount:       true,
			expectCompleted: true,
			expectCount:     0,
			ts:              1,
		},
		{
			expectCompleted: true,
			expectCount:     0,
			ts:              1,
		},
		{
			onlyCount:       true,
			expectCompleted: true,
			expectCount:     3,
			ts:              2,
		},
		{
			shard:           metapb.Shard{End: k3},
			onlyCount:       true,
			expectCompleted: true,
			expectCount:     1,
			ts:              2,
		},
		{
			onlyCount:       true,
			expectCompleted: true,
			expectCount:     4,
			ts:              3,
		},
		{
			shard:           metapb.Shard{End: k3},
			onlyCount:       true,
			expectCompleted: true,
			expectCount:     2,
			ts:              3,
		},
		{
			expectCompleted: true,
			expectCount:     3,
			ts:              2,
			expectKeys:      [][]byte{k2, k3, k4},
		},
		{
			withValue:       true,
			expectCompleted: true,
			expectCount:     3,
			ts:              2,
			expectKeys:      [][]byte{k2, k3, k4},
			expectValues:    [][]byte{[]byte("k2-1(c)"), []byte("k3-1(c)"), []byte("k4-1(c)")},
		},
		{
			expectCompleted: false,
			expectCount:     1,
			ts:              2,
			limit:           1,
			expectKeys:      [][]byte{k2},
		},
		{
			expectCompleted: false,
			expectCount:     1,
			ts:              2,
			limitBytes:      1,
			expectKeys:      [][]byte{k2},
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

		rw := newKVReaderAndWriter(buffer, testutil.GetTestTxnOpMeta(c.seq, c.epoch, c.ts), dr, nil)
		result, err := handleScan(c.shard, txnpb.TxnRequest{
			Operation: txnpb.TxnOperation{
				Payload: protoc.MustMarshal(req),
			},
		}, rw)
		assert.NoError(t, err)

		resp := &rpcpb.KVScanResponse{}
		protoc.MustUnmarshal(resp, result)
		assert.Equal(t, c.expectCompleted, resp.Completed)
		assert.Equal(t, c.expectKeys, resp.Keys)
		assert.Equal(t, c.expectValues, resp.Values)
		assert.Equal(t, c.expectCount, resp.Count)
	}
}

func newKVReaderAndWriter(buffer *buf.ByteBuf,
	txn txnpb.TxnOpMeta,
	reader txn.TransactionDataReader,
	uncommittedTree txn.UncommittedDataTree) KVReaderAndWriter {
	return &kvReaderAndWriter{
		buffer:      buffer,
		txn:         txn,
		reader:      reader,
		uncommitted: uncommittedTree,
	}
}

func getTestTransactionDataStorage(t *testing.T) (storage.TransactionalDataStorage, storage.KVBaseStorage, func()) {
	fs := vfs.GetTestFS()
	kvStore := mem.NewStorage()
	base := kv.NewBaseStorage(kvStore, fs)
	s := kv.NewKVDataStorage(base, executor.NewKVExecutor(base),
		kv.WithFeature(storage.Feature{SupportTransaction: true}))
	return s.(storage.TransactionalDataStorage), base, func() {
		assert.NoError(t, s.Close())
		vfs.ReportLeakedFD(fs, t)
	}
}
