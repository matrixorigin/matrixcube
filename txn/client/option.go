package client

import (
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/util/uuid"
	"go.uber.org/zap"
)

// Option the option create txn client
type Option func(*txnClient)

// WithLogger set logger for txn client
func WithLogger(logger *zap.Logger) Option {
	return func(tc *txnClient) {
		tc.logger = logger.Named("txn")
	}
}

// WithSplitter set Splitter for txn client
func WithSplitter(splitter Splitter) Option {
	return func(tc *txnClient) {
		tc.splitter = splitter
	}
}

// WithTxnIDGenerator set TxnIDGenerator for txn client
func WithTxnIDGenerator(txnIDGenerator TxnIDGenerator) Option {
	return func(tc *txnClient) {
		tc.txnIDGenerator = txnIDGenerator
	}
}

// TxnIDGenerator generate a unique transaction ID for the cluster
type TxnIDGenerator interface {
	// Generate returns a unique transaction ID
	Generate() []byte
}

// Splitter used to split TxnOperation, as the transaction framework does not know how the
// TxnOperation data is organized, the caller needs to split the data managed in a TxnOperation
// into multiple TxnOperations according to the Shard.
type Splitter interface {
	// Split according to the TxnOperation internal management of data split into multiple
	// TxnOperation, split each TxnOperation with a Shard correspondence. The transaction
	// framework will concurrently send the split TxnOperations to the corresponding Shard for
	// execution.
	Split(request txnpb.TxnOperation) (payloads []txnpb.TxnOperation, shards []uint64, err error)
}

var _ TxnIDGenerator = (*uuidTxnIDGenerator)(nil)

type uuidTxnIDGenerator struct {
}

func newUUIDTxnIDGenerator() TxnIDGenerator {
	return &uuidTxnIDGenerator{}
}

func (gen *uuidTxnIDGenerator) Generate() []byte {
	return uuid.NewV4().Bytes()
}
