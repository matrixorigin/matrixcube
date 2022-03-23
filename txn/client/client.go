package client

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/txn/util"
	"go.uber.org/zap"
)

// TxnClient distributed transaction client.
type TxnClient interface {
	// NewTxn create transaction operation handles to perform transaction operations.
	NewTxn(name string, isolation txnpb.Isolation) TxnOperator
}

var _ TxnClient = (*txnClient)(nil)

type txnClient struct {
	logger               *zap.Logger
	txnIDGenerator       TxnIDGenerator
	txnPriorityGenerator TxnPriorityGenerator
	txnClocker           TxnClocker
	sender               BatchSender
	txnHeartbeatDuration time.Duration
}

// NewTxnClient create a txn client
func NewTxnClient(sender BatchSender, opts ...Option) TxnClient {
	tc := &txnClient{sender: sender}
	for _, opt := range opts {
		opt(tc)
	}
	tc.adjust()
	return tc
}

// NewTxn create a txn and returns the txn operator.
func (tc *txnClient) NewTxn(name string, isolation txnpb.Isolation) TxnOperator {
	now, maxSkew := tc.txnClocker.Now()
	txn := txnpb.TxnMeta{
		Name:           name,
		Isolation:      isolation,
		ReadTimestamp:  now,
		WriteTimestamp: now,
		MaxTimestamp:   now + maxSkew,
	}
	txn.ID = tc.txnIDGenerator.Generate()
	txn.Priority = tc.txnPriorityGenerator.Generate()
	util.LogTxnMeta(tc.logger, zap.DebugLevel, "txn created", txn)
	return NewTxnOperator(txn,
		tc.sender,
		tc.txnClocker,
		tc.txnHeartbeatDuration,
		tc.logger)
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

	if tc.txnHeartbeatDuration == 0 {
		tc.txnHeartbeatDuration = time.Second * 5
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
	Write(ctx context.Context, operations []txnpb.TxnOperation) error
	// WriteAndCommit similar to `Wirte`, but commit the transaction after completing the write.
	WriteAndCommit(ctx context.Context, operations []txnpb.TxnOperation) error
	// Read transactional read operations, each read operation needs to specify a shard to a shard,
	// or provide a RouteKey to route to a shard.
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

// NewTxnOperator create txn operator, a txn corresponds to a TxnOperator instance.
func NewTxnOperator(txnMeta txnpb.TxnMeta,
	sender BatchSender,
	txnClocker TxnClocker,
	txnHeartbeatDuration time.Duration,
	logger *zap.Logger) TxnOperator {
	return &txnOperator{
		tc: newTxnCoordinator(txnMeta,
			sender,
			txnClocker,
			txnHeartbeatDuration,
			logger),
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
