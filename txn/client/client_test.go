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
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestNewTxn(t *testing.T) {
	client := NewTxnClient(newMockBatchDispatcher(nil), WithTxnClock(newHLCTxnClock(time.Millisecond*500)))
	txn := client.NewTxn(WithTxnOptionName("mock-txn")).getTxnMeta()
	assert.Equal(t, "mock-txn", txn.Name)
	assert.Equal(t, txnpb.IsolationLevel_SnapshotSerializable, txn.IsolationLevel)
	assert.NotEmpty(t, txn.ID)
	assert.True(t, txn.Priority > 0)
	assert.True(t, txn.ReadTimestamp.Equal(txn.WriteTimestamp))
	assert.True(t, txn.MaxTimestamp.Greater(txn.ReadTimestamp))
	assert.Equal(t, uint32(0), txn.Epoch)
	assert.Empty(t, txn.TxnRecordRouteKey)
}

func TestWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()

	client := NewTxnClient(newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	}), WithTxnClock(newHLCTxnClock(time.Millisecond*500)))

	ctx := context.TODO()
	op := client.NewTxn()
	defer func() {
		assert.NoError(t, op.Rollback(ctx))
	}()

	assert.NoError(t, op.Write(ctx, []txnpb.TxnOperation{{Op: 10000, Payload: []byte("10000"), Impacted: txnpb.KeySet{PointKeys: [][]byte{[]byte("k1")}}}}))
}

func TestWriteAndCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	client := NewTxnClient(newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	}), WithTxnClock(newHLCTxnClock(time.Millisecond*500)))

	ctx := context.TODO()
	op := client.NewTxn()
	assert.NoError(t, op.WriteAndCommit(ctx, []txnpb.TxnOperation{{Op: 10000, Payload: []byte("10000"), Impacted: txnpb.KeySet{PointKeys: [][]byte{[]byte("k1")}}}}))
}

func TestRead(t *testing.T) {
	defer leaktest.AfterTest(t)()

	client := NewTxnClient(newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}, Responses: make([]txnpb.TxnResponse, len(req.Requests))}, nil
	}), WithTxnClock(newHLCTxnClock(time.Millisecond*500)))

	ctx := context.TODO()
	op := client.NewTxn()
	data, err := op.Read(ctx, []txnpb.TxnOperation{{Op: 10000, Payload: []byte("10000"), Impacted: txnpb.KeySet{PointKeys: [][]byte{[]byte("k1")}}}})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data))
}

func TestCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	client := NewTxnClient(newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	}), WithTxnClock(newHLCTxnClock(time.Millisecond*500)))

	ctx := context.TODO()
	op := client.NewTxn()
	assert.NoError(t, op.Write(ctx, []txnpb.TxnOperation{{Op: 10000, Payload: []byte("10000"), Impacted: txnpb.KeySet{PointKeys: [][]byte{[]byte("k1")}}}}))
	assert.NoError(t, op.Commit(ctx))
}

func TestRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()

	client := NewTxnClient(newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	}), WithTxnClock(newHLCTxnClock(time.Millisecond*500)))

	ctx := context.TODO()
	op := client.NewTxn()
	assert.NoError(t, op.Write(ctx, []txnpb.TxnOperation{{Op: 10000, Payload: []byte("10000"), Impacted: txnpb.KeySet{PointKeys: [][]byte{[]byte("k1")}}}}))
	assert.NoError(t, op.Rollback(ctx))
}
