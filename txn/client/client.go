package client

import (
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"go.uber.org/zap"
)

// TxnClient distributed transaction client.
type TxnClient interface {
	// New create transaction operation handles to perform transaction operations.
	New(name string, isolation txnpb.Isolation) TxnOperator
}

var _ TxnClient = (*txnClient)(nil)

type txnClient struct {
	logger         *zap.Logger
	txnIDGenerator TxnIDGenerator
	splitter       Splitter
}

// NewTxnClient create a txn client
func NewTxnClient(opts ...Option) TxnClient {
	tc := &txnClient{}
	for _, opt := range opts {
		opt(tc)
	}

	return tc
}

// New create a txn
func (tc *txnClient) New(name string, isolation txnpb.Isolation) TxnOperator {
	id := tc.txnIDGenerator.Generate()
	if ce := tc.logger.Check(zap.DebugLevel, "txn created"); ce != nil {
		ce.Write(log.TxnIDField(id),
			zap.String("txn-name", name),
			zap.String("txn-isolation",
				isolation.String()))
	}
	return NewTxnOperator(id, name, isolation, tc.logger)
}

func (tc *txnClient) adjust() {
	if tc.logger == nil {
		tc.logger = log.Adjust(nil).Named("txn")
	}

	if tc.txnIDGenerator == nil {
		tc.txnIDGenerator = newUUIDTxnIDGenerator()
	}
}
