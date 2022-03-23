package client

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestNewTxn(t *testing.T) {
	client := NewTxnClient(newMockBatchSender(nil), WithTxnClocker(newMockTxnClocker(5)))
	txn := client.NewTxn("mock-txn", txnpb.Isolation_SI).getTxnMeta()
	assert.Equal(t, "mock-txn", txn.Name)
	assert.Equal(t, txnpb.Isolation_SI, txn.Isolation)
	assert.NotEmpty(t, txn.ID)
	assert.True(t, txn.Priority > 0)
	assert.Equal(t, uint64(6), txn.MaxTimestamp)
	assert.Equal(t, uint64(1), txn.ReadTimestamp)
	assert.Equal(t, uint64(1), txn.WriteTimestamp)
	assert.Equal(t, uint32(0), txn.Epoch)
	assert.Empty(t, txn.TxnRecordRouteKey)
}

func TestWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()

	client := NewTxnClient(newMockBatchSender(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	}), WithTxnClocker(newMockTxnClocker(5)))

	ctx := context.TODO()
	op := client.NewTxn("mock-txn", txnpb.Isolation_SI)
	defer op.Rollback(ctx)

	assert.NoError(t, op.Write(ctx, []txnpb.TxnOperation{{Op: 10000, Payload: []byte("10000"), Impacted: txnpb.KeySet{PointKeys: [][]byte{[]byte("k1")}}}}))
}

func TestWriteAndCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	client := NewTxnClient(newMockBatchSender(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	}), WithTxnClocker(newMockTxnClocker(5)))

	ctx := context.TODO()
	op := client.NewTxn("mock-txn", txnpb.Isolation_SI)
	assert.NoError(t, op.WriteAndCommit(ctx, []txnpb.TxnOperation{{Op: 10000, Payload: []byte("10000"), Impacted: txnpb.KeySet{PointKeys: [][]byte{[]byte("k1")}}}}))
}

func TestRead(t *testing.T) {
	defer leaktest.AfterTest(t)()

	client := NewTxnClient(newMockBatchSender(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}, Responses: make([]txnpb.TxnResponse, len(req.Requests))}, nil
	}), WithTxnClocker(newMockTxnClocker(5)))

	ctx := context.TODO()
	op := client.NewTxn("mock-txn", txnpb.Isolation_SI)
	data, err := op.Read(ctx, []txnpb.TxnOperation{{Op: 10000, Payload: []byte("10000"), Impacted: txnpb.KeySet{PointKeys: [][]byte{[]byte("k1")}}}})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data))
}

func TestCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	client := NewTxnClient(newMockBatchSender(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	}), WithTxnClocker(newMockTxnClocker(5)))

	ctx := context.TODO()
	op := client.NewTxn("mock-txn", txnpb.Isolation_SI)
	assert.NoError(t, op.Write(ctx, []txnpb.TxnOperation{{Op: 10000, Payload: []byte("10000"), Impacted: txnpb.KeySet{PointKeys: [][]byte{[]byte("k1")}}}}))
	assert.NoError(t, op.Commit(ctx))
}

func TestRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()

	client := NewTxnClient(newMockBatchSender(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	}), WithTxnClocker(newMockTxnClocker(5)))

	ctx := context.TODO()
	op := client.NewTxn("mock-txn", txnpb.Isolation_SI)
	assert.NoError(t, op.Write(ctx, []txnpb.TxnOperation{{Op: 10000, Payload: []byte("10000"), Impacted: txnpb.KeySet{PointKeys: [][]byte{[]byte("k1")}}}}))
	assert.NoError(t, op.Rollback(ctx))
}
