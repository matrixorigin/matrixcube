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
	"github.com/matrixorigin/matrixcube/pb/hlcpb"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/txn"
	"github.com/matrixorigin/matrixcube/txn/util"
	"github.com/matrixorigin/matrixcube/util/buf"
	keysutil "github.com/matrixorigin/matrixcube/util/keys"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/testutil"
	"github.com/stretchr/testify/assert"
)

func TestRegisterWrite(t *testing.T) {
	rp := NewKVTxnCommandProcessor()
	op := uint32(rpcpb.CmdReserved + 100)
	n1 := 0
	rp.RegisterWrite(op, func(s metapb.Shard, tr txnpb.TxnRequest, kaw KVReaderAndWriter) error {
		n1++
		return nil
	})
	n2 := 0
	rp.RegisterWrite(op+1, func(s metapb.Shard, tr txnpb.TxnRequest, kaw KVReaderAndWriter) error {
		n2++
		return nil
	})
	n3 := 0
	rp.RegisterWrite(op+2, func(s metapb.Shard, tr txnpb.TxnRequest, kaw KVReaderAndWriter) error {
		n3++
		return nil
	})

	_, err := rp.HandleWrite(metapb.Shard{}, testutil.GetTestTxnOpMeta(0, 0, 1), []txnpb.TxnRequest{
		{
			Operation: txnpb.TxnOperation{
				Op: op,
			},
		},
		{
			Operation: txnpb.TxnOperation{
				Op: op + 1,
			},
		},
	}, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, n1)
	assert.Equal(t, 1, n2)
	assert.Equal(t, 0, n3)
}

func TestRegisterRead(t *testing.T) {
	rp := NewKVTxnCommandProcessor()
	op := uint32(rpcpb.CmdReserved + 100)
	rp.RegisterRead(op, func(s metapb.Shard, tr txnpb.TxnRequest, kaw KVReader) ([]byte, error) {
		return []byte{0}, nil
	})
	rp.RegisterRead(op+1, func(s metapb.Shard, tr txnpb.TxnRequest, kaw KVReader) ([]byte, error) {
		return []byte{1}, nil
	})

	v, err := rp.HandleRead(metapb.Shard{}, testutil.GetTestTxnOpMeta(0, 0, 1), txnpb.TxnRequest{
		Operation: txnpb.TxnOperation{
			Op: op,
		},
	}, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0}, v)

	v, err = rp.HandleRead(metapb.Shard{}, testutil.GetTestTxnOpMeta(0, 0, 1), txnpb.TxnRequest{
		Operation: txnpb.TxnOperation{
			Op: op + 1,
		},
	}, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, []byte{1}, v)
}

func TestHandleSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, _, closer := getTestTransactionDataStorage(t)
	defer closer()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	k1 := []byte("k1")
	dr := txn.NewTransactionDataReader(ts)
	rp := NewKVTxnCommandProcessor()
	txn := testutil.GetTestTxnOpMeta(0, 0, 1)
	data, err := rp.HandleWrite(metapb.Shard{}, txn, []txnpb.TxnRequest{
		{
			Operation: txnpb.TxnOperation{
				Op: uint32(rpcpb.CmdKVSet),
				Payload: protoc.MustMarshal(&rpcpb.KVSetRequest{
					Key:   k1,
					Value: k1,
				}),
			},
		},
	}, dr, nil)
	assert.NoError(t, err)
	assert.Equal(t, data.RequestType, uint64(rpcpb.CmdKVBatchMixedWrite))

	var req rpcpb.KVBatchMixedWriteRequest
	protoc.MustUnmarshal(&req, data.Data)

	mvcc := testutil.GetUncommittedWithTxnOp(txn)

	assert.Equal(t, 2, len(req.Requests))
	assert.Equal(t, k1, req.Requests[0].Set.Key)
	assert.Equal(t, protoc.MustMarshal(&mvcc), req.Requests[0].Set.Value)
	assert.Equal(t, keysutil.EncodeTxnMVCCKey(k1, txn.WriteTimestamp, buffer, false), req.Requests[1].Set.Key)
	assert.Equal(t, keysutil.Join(normalValuePrefix, k1), req.Requests[1].Set.Value)
}

func TestHandleSetWithCreateTxnRecord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, _, closer := getTestTransactionDataStorage(t)
	defer closer()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	k1 := []byte("k1")
	dr := txn.NewTransactionDataReader(ts)
	rp := NewKVTxnCommandProcessor()
	txn := testutil.GetTestTxnOpMeta(0, 0, 1)
	txn.TxnRecordRouteKey = k1
	txnReq := txnpb.TxnRequest{
		Operation: txnpb.TxnOperation{
			Op: uint32(rpcpb.CmdKVSet),
			Payload: protoc.MustMarshal(&rpcpb.KVSetRequest{
				Key:   k1,
				Value: k1,
			}),
		},
		Options: txnpb.RequestOptions{
			CreateTxnRecord: true,
		},
	}
	data, err := rp.HandleWrite(metapb.Shard{}, txn, []txnpb.TxnRequest{
		txnReq,
	}, dr, nil)
	assert.NoError(t, err)
	assert.Equal(t, data.RequestType, uint64(rpcpb.CmdKVBatchMixedWrite))

	var req rpcpb.KVBatchMixedWriteRequest
	protoc.MustUnmarshal(&req, data.Data)

	mvcc := testutil.GetUncommittedWithTxnOp(txn)
	assert.Equal(t, 3, len(req.Requests))
	assert.Equal(t, keysutil.EncodeTxnRecordKey(k1, txn.ID, buffer, false), req.Requests[0].Set.Key)
	assert.Equal(t, getTxnRecord(txn.TxnMeta, txnReq), req.Requests[0].Set.Value)
	assert.Equal(t, k1, req.Requests[1].Set.Key)
	assert.Equal(t, protoc.MustMarshal(&mvcc), req.Requests[1].Set.Value)
	assert.Equal(t, keysutil.EncodeTxnMVCCKey(k1, txn.WriteTimestamp, buffer, false), req.Requests[2].Set.Key)
	assert.Equal(t, keysutil.Join(normalValuePrefix, k1), req.Requests[2].Set.Value)
}

func TestHandleSetWithUncommitted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, _, closer := getTestTransactionDataStorage(t)
	defer closer()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	uncommitted := util.NewUncommittedTree()
	k1 := []byte("k1")
	dr := txn.NewTransactionDataReader(ts)
	rp := NewKVTxnCommandProcessor()
	txn := testutil.GetTestTxnOpMeta(0, 0, 1)
	data, err := rp.HandleWrite(metapb.Shard{}, txn, []txnpb.TxnRequest{
		{
			Operation: txnpb.TxnOperation{
				Op: uint32(rpcpb.CmdKVSet),
				Payload: protoc.MustMarshal(&rpcpb.KVSetRequest{
					Key:   k1,
					Value: k1,
				}),
			},
		},
	}, dr, uncommitted)
	assert.NoError(t, err)
	assert.Equal(t, data.RequestType, uint64(rpcpb.CmdKVBatchMixedWrite))

	var req rpcpb.KVBatchMixedWriteRequest
	protoc.MustUnmarshal(&req, data.Data)

	mvcc := testutil.GetUncommittedWithTxnOp(txn)

	assert.Equal(t, 2, len(req.Requests))
	assert.Equal(t, k1, req.Requests[0].Set.Key)
	assert.Equal(t, protoc.MustMarshal(&mvcc), req.Requests[0].Set.Value)
	assert.Equal(t, keysutil.EncodeTxnMVCCKey(k1, txn.WriteTimestamp, buffer, false), req.Requests[1].Set.Key)
	assert.Equal(t, keysutil.Join(normalValuePrefix, k1), req.Requests[1].Set.Value)
	assert.Equal(t, 1, uncommitted.Len())
	v, ok := uncommitted.Get(k1)
	assert.True(t, ok)
	assert.Equal(t, v, mvcc)
}

func TestHandleSetWithOldUncommittedAndTimestampEqual(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, _, closer := getTestTransactionDataStorage(t)
	defer closer()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	uncommitted := util.NewUncommittedTree()
	k1 := []byte("k1")
	dr := txn.NewTransactionDataReader(ts)
	rp := NewKVTxnCommandProcessor()
	txn := testutil.GetTestTxnOpMeta(0, 0, 1)
	uncommitted.Add(k1, testutil.GetUncommitted(0, 0, 1))

	data, err := rp.HandleWrite(metapb.Shard{}, txn, []txnpb.TxnRequest{
		{
			Operation: txnpb.TxnOperation{
				Op: uint32(rpcpb.CmdKVSet),
				Payload: protoc.MustMarshal(&rpcpb.KVSetRequest{
					Key:   k1,
					Value: k1,
				}),
			},
		},
	}, dr, uncommitted)
	assert.NoError(t, err)
	assert.Equal(t, data.RequestType, uint64(rpcpb.CmdKVBatchMixedWrite))

	var req rpcpb.KVBatchMixedWriteRequest
	protoc.MustUnmarshal(&req, data.Data)

	mvcc := testutil.GetUncommittedWithTxnOp(txn)

	assert.Equal(t, 1, len(req.Requests))
	assert.Equal(t, keysutil.EncodeTxnMVCCKey(k1, txn.WriteTimestamp, buffer, false), req.Requests[0].Set.Key)
	assert.Equal(t, keysutil.Join(normalValuePrefix, k1), req.Requests[0].Set.Value)
	assert.Equal(t, 1, uncommitted.Len())
	v, ok := uncommitted.Get(k1)
	assert.True(t, ok)
	assert.Equal(t, v, mvcc)
}

func TestHandleSetWithOldUncommittedAndTimestampChanged(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, _, closer := getTestTransactionDataStorage(t)
	defer closer()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	uncommitted := util.NewUncommittedTree()
	k1 := []byte("k1")
	dr := txn.NewTransactionDataReader(ts)
	rp := NewKVTxnCommandProcessor()
	txn := testutil.GetTestTxnOpMeta(0, 0, 2)
	uncommitted.Add(k1, testutil.GetUncommitted(0, 0, 1))

	data, err := rp.HandleWrite(metapb.Shard{}, txn, []txnpb.TxnRequest{
		{
			Operation: txnpb.TxnOperation{
				Op: uint32(rpcpb.CmdKVSet),
				Payload: protoc.MustMarshal(&rpcpb.KVSetRequest{
					Key:   k1,
					Value: k1,
				}),
			},
		},
	}, dr, uncommitted)
	assert.NoError(t, err)
	assert.Equal(t, data.RequestType, uint64(rpcpb.CmdKVBatchMixedWrite))

	var req rpcpb.KVBatchMixedWriteRequest
	protoc.MustUnmarshal(&req, data.Data)

	mvcc := testutil.GetUncommittedWithTxnOp(txn)

	assert.Equal(t, 3, len(req.Requests))
	assert.Equal(t, keysutil.EncodeTxnMVCCKey(k1, hlcpb.Timestamp{PhysicalTime: 1}, buffer, false), req.Requests[0].Delete.Key)
	assert.Equal(t, k1, req.Requests[1].Set.Key)
	assert.Equal(t, protoc.MustMarshal(&mvcc), req.Requests[1].Set.Value)
	assert.Equal(t, keysutil.EncodeTxnMVCCKey(k1, txn.WriteTimestamp, buffer, false), req.Requests[2].Set.Key)
	assert.Equal(t, keysutil.Join(normalValuePrefix, k1), req.Requests[2].Set.Value)
	assert.Equal(t, 1, uncommitted.Len())
	v, ok := uncommitted.Get(k1)
	assert.True(t, ok)
	assert.Equal(t, v, mvcc)
}

func TestHandleBatchSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, _, closer := getTestTransactionDataStorage(t)
	defer closer()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	k1 := []byte("k1")
	k2 := []byte("k2")

	dr := txn.NewTransactionDataReader(ts)
	rp := NewKVTxnCommandProcessor()
	txn := testutil.GetTestTxnOpMeta(0, 0, 1)
	data, err := rp.HandleWrite(metapb.Shard{}, txn, []txnpb.TxnRequest{
		{
			Operation: txnpb.TxnOperation{
				Op: uint32(rpcpb.CmdKVBatchSet),
				Payload: protoc.MustMarshal(&rpcpb.KVBatchSetRequest{
					Keys:   [][]byte{k1, k2},
					Values: [][]byte{k1, k2},
				}),
			},
		},
	}, dr, nil)
	assert.NoError(t, err)
	assert.Equal(t, data.RequestType, uint64(rpcpb.CmdKVBatchMixedWrite))

	var req rpcpb.KVBatchMixedWriteRequest
	protoc.MustUnmarshal(&req, data.Data)

	mvcc := testutil.GetUncommittedWithTxnOp(txn)

	assert.Equal(t, 4, len(req.Requests))

	assert.Equal(t, k1, req.Requests[0].Set.Key)
	assert.Equal(t, protoc.MustMarshal(&mvcc), req.Requests[0].Set.Value)
	assert.Equal(t, keysutil.EncodeTxnMVCCKey(k1, txn.WriteTimestamp, buffer, false), req.Requests[1].Set.Key)
	assert.Equal(t, keysutil.Join(normalValuePrefix, k1), req.Requests[1].Set.Value)

	assert.Equal(t, k2, req.Requests[2].Set.Key)
	assert.Equal(t, protoc.MustMarshal(&mvcc), req.Requests[2].Set.Value)
	assert.Equal(t, keysutil.EncodeTxnMVCCKey(k2, txn.WriteTimestamp, buffer, false), req.Requests[3].Set.Key)
	assert.Equal(t, keysutil.Join(normalValuePrefix, k2), req.Requests[3].Set.Value)
}

func TestHandleDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, _, closer := getTestTransactionDataStorage(t)
	defer closer()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	k1 := []byte("k1")
	dr := txn.NewTransactionDataReader(ts)
	rp := NewKVTxnCommandProcessor()
	txn := testutil.GetTestTxnOpMeta(0, 0, 1)
	data, err := rp.HandleWrite(metapb.Shard{}, txn, []txnpb.TxnRequest{
		{
			Operation: txnpb.TxnOperation{
				Op: uint32(rpcpb.CmdKVDelete),
				Payload: protoc.MustMarshal(&rpcpb.KVDeleteRequest{
					Key: k1,
				}),
			},
		},
	}, dr, nil)
	assert.NoError(t, err)
	assert.Equal(t, data.RequestType, uint64(rpcpb.CmdKVBatchMixedWrite))

	var req rpcpb.KVBatchMixedWriteRequest
	protoc.MustUnmarshal(&req, data.Data)

	mvcc := testutil.GetUncommittedWithTxnOp(txn)

	assert.Equal(t, 2, len(req.Requests))
	assert.Equal(t, k1, req.Requests[0].Set.Key)
	assert.Equal(t, protoc.MustMarshal(&mvcc), req.Requests[0].Set.Value)
	assert.Equal(t, keysutil.EncodeTxnMVCCKey(k1, txn.WriteTimestamp, buffer, false), req.Requests[1].Set.Key)
	assert.Equal(t, deletedValuePrefix, req.Requests[1].Set.Value)
}

func TestHandleRangeDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	k1 := []byte("k1")
	k2 := []byte("k2")

	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k1, normalValuePrefix, 1)
	testutil.AddTestCommittedMVCCRecordWithPrefix(t, base, k2, normalValuePrefix, 1)

	dr := txn.NewTransactionDataReader(ts)
	rp := NewKVTxnCommandProcessor()
	txn := testutil.GetTestTxnOpMeta(0, 0, 2)
	data, err := rp.HandleWrite(metapb.Shard{}, txn, []txnpb.TxnRequest{
		{
			Operation: txnpb.TxnOperation{
				Op: uint32(rpcpb.CmdKVRangeDelete),
				Payload: protoc.MustMarshal(&rpcpb.KVRangeDeleteRequest{
					Start: k1,
					End:   k2,
				}),
			},
		},
	}, dr, nil)
	assert.NoError(t, err)
	assert.Equal(t, data.RequestType, uint64(rpcpb.CmdKVBatchMixedWrite))

	var req rpcpb.KVBatchMixedWriteRequest
	protoc.MustUnmarshal(&req, data.Data)

	mvcc := testutil.GetUncommittedWithTxnOp(txn)

	assert.Equal(t, 2, len(req.Requests))
	assert.Equal(t, k1, req.Requests[0].Set.Key)
	assert.Equal(t, protoc.MustMarshal(&mvcc), req.Requests[0].Set.Value)
	assert.Equal(t, keysutil.EncodeTxnMVCCKey(k1, txn.WriteTimestamp, buffer, false), req.Requests[1].Set.Key)
	assert.Equal(t, deletedValuePrefix, req.Requests[1].Set.Value)
}
