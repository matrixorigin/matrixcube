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

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/txn/util"
	"github.com/matrixorigin/matrixcube/util/hlc"
	"go.uber.org/zap"
)

// TxnClient distributed transaction client.
type TxnClient interface {
	// NewTxn create transaction operation handles to perform transaction operations.
	NewTxn(opts ...TxnOption) TxnOperator
}

var _ TxnClient = (*txnClient)(nil)

type txnClient struct {
	logger               *zap.Logger
	txnIDGenerator       TxnIDGenerator
	txnPriorityGenerator TxnPriorityGenerator
	txnClocker           hlc.Clock
	dispatcher           BatchDispatcher
}

// NewTxnClient create a txn client
func NewTxnClient(dispatcher BatchDispatcher, opts ...Option) TxnClient {
	tc := &txnClient{dispatcher: dispatcher}
	for _, opt := range opts {
		opt(tc)
	}
	tc.adjust()
	return tc
}

// NewTxn create a txn and returns the txn operator.
func (tc *txnClient) NewTxn(opts ...TxnOption) TxnOperator {
	options := txnOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	options.adjust()

	now := tc.txnClocker.Now()
	maxOffet := tc.txnClocker.MaxOffset()
	// tc.txnClocker

	txn := txnpb.TxnMeta{
		Name:           options.name,
		IsolationLevel: options.isolationLevel,
		ReadTimestamp:  now,
		WriteTimestamp: now,
		MaxTimestamp:   now,
	}
	txn.ID = tc.txnIDGenerator.Generate()
	txn.Priority = tc.txnPriorityGenerator.Generate()
	util.LogTxnMeta(tc.logger, zap.DebugLevel, "txn created", txn)
	return newTxnOperator(txn,
		tc.dispatcher,
		tc.txnClocker,
		tc.logger,
		options)
}

func (tc *txnClient) adjust() {
	if tc.logger == nil {
		tc.logger = log.Adjust(nil).Named("txn")
	}

	if tc.txnIDGenerator == nil {
		tc.txnIDGenerator = newUUIDTxnIDGenerator()
	}

	if tc.txnPriorityGenerator == nil {
		tc.txnPriorityGenerator = newTxnPriorityGenerator()
	}
}

type txnMetaGetter interface {
	getTxnMeta() txnpb.TxnMeta
}

// TxnOperator txn operator, support transaction read, write, commit and rollback operations.
type TxnOperator interface {
	txnMetaGetter

	// Write write operation, operation type, data and context are stored in the TxnOperation.
	// Each TxnOperation can be split into multiple `TxnOperations` by the `Splitter` and
	// the framework will send these split `TxnOperations` to the corresponding Shard for
	// execution. The Write method will only return when all TxnOperations have been executed.
	// Note: use `txnpb.NewWriteOnlyOperation` or `txnpb.NewReadWriteOperation` to build
	// TxnOperation.
	Write(ctx context.Context, operations []txnpb.TxnOperation) error
	// WriteAndCommit similar to `Wirte`, but commit the transaction after completing the write.
	WriteAndCommit(ctx context.Context, operations []txnpb.TxnOperation) error
	// Read transactional read operations, each read operation needs to specify a shard to a shard,
	// or provide a RouteKey to route to a shard.
	// Note: use `txnpb.NewReadOperation` to build TxnOperation.
	Read(ctx context.Context, operations []txnpb.TxnOperation) ([][]byte, error)
	// Rollback rollback transactions, once the transaction is rolled back, the transaction's temporary
	// write data is cleaned up asynchronously.
	Rollback(ctx context.Context) error
	// Commit commit transaction will return as soon as the status of TxnRecord will be changed to
	// `Committed`. The data written in the transaction still exists as temporary data, but this data
	// needs to be treated as committed data, and TxnManager will start an asynchronous task to convert
	// the temporary data to committed data after modifying the status of TxnRecord.
	Commit(ctx context.Context) error
}

var _ TxnOperator = (*txnOperator)(nil)

func newTxnOperator(txnMeta txnpb.TxnMeta,
	dispatcher BatchDispatcher,
	txnClocker TxnClocker,
	logger *zap.Logger,
	opts txnOptions) TxnOperator {
	return &txnOperator{
		tc: newTxnCoordinator(txnMeta,
			dispatcher,
			txnClocker,
			logger,
			opts),
	}
}

type txnOperator struct {
	tc txnCoordinator
}

func (to *txnOperator) Write(ctx context.Context, operations []txnpb.TxnOperation) error {
	_, err := to.doSend(ctx, operations, txnpb.TxnRequestType_Write, false)
	return err
}

func (to *txnOperator) WriteAndCommit(ctx context.Context, operations []txnpb.TxnOperation) error {
	defer to.close()

	_, err := to.doSend(ctx, operations, txnpb.TxnRequestType_Write, true)
	return err
}

func (to *txnOperator) Read(ctx context.Context, operations []txnpb.TxnOperation) ([][]byte, error) {
	resp, err := to.doSend(ctx, operations, txnpb.TxnRequestType_Read, false)
	if err != nil {
		return nil, err
	}
	data := make([][]byte, 0, len(resp.Responses))
	for idx := range resp.Responses {
		data = append(data, resp.Responses[idx].Data)
	}
	return data, nil
}

func (to *txnOperator) Rollback(ctx context.Context) error {
	defer to.close()

	_, err := to.tc.send(ctx, endTxn(txnpb.InternalTxnOp_Rollback))
	return err
}

func (to *txnOperator) Commit(ctx context.Context) error {
	defer to.close()

	_, err := to.tc.send(ctx, endTxn(txnpb.InternalTxnOp_Commit))
	return err
}

func (to *txnOperator) close() {
	to.tc.stop()
}

func (to *txnOperator) doSend(ctx context.Context, operations []txnpb.TxnOperation, requestType txnpb.TxnRequestType, commit bool) (txnpb.TxnBatchResponse, error) {
	n := len(operations)
	if commit {
		n += 1
	}

	var request txnpb.TxnBatchRequest
	request.Header.Type = requestType
	request.Requests = make([]txnpb.TxnRequest, 0, n)
	for _, op := range operations {
		request.Requests = append(request.Requests, txnpb.TxnRequest{Operation: op})
	}
	if commit {
		request.Requests = append(request.Requests, txnpb.TxnRequest{
			Operation: txnpb.TxnOperation{
				Op: uint32(txnpb.InternalTxnOp_Commit),
			},
		})
	}
	return to.tc.send(ctx, request)
}

func (to *txnOperator) getTxnMeta() txnpb.TxnMeta {
	return to.tc.getTxnMeta()
}

func endTxn(op txnpb.InternalTxnOp) txnpb.TxnBatchRequest {
	var req txnpb.TxnBatchRequest
	req.Header.Type = txnpb.TxnRequestType_Write
	req.Requests = append(req.Requests, txnpb.TxnRequest{
		Operation: txnpb.TxnOperation{
			Op: uint32(op),
		},
	})
	return req
}
