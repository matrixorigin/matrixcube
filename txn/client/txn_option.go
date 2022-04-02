package client

import (
	"time"

	"github.com/matrixorigin/matrixcube/pb/txnpb"
)

// TxnOption txn option used to create Transaction
type TxnOption func(*txnOptions)

type txnOptions struct {
	name              string
	isolationLevel    txnpb.IsolationLevel
	heartbeatDuration time.Duration
	optimize          struct {
		asynchronousConsensus bool
		maxInfilghtKeysBytes  int
	}
}

func (opts *txnOptions) adjust() {
	if opts.name == "" {
		opts.name = "unnamed"
	}
	if opts.heartbeatDuration == 0 {
		opts.heartbeatDuration = time.Second
	}
}

// WithTxnOptionName set txn name
func WithTxnOptionName(name string) TxnOption {
	return func(opts *txnOptions) {
		opts.name = name
	}
}

// WithTxnOptionHeartbeatDuration set txn heartbeat duration
func WithTxnOptionHeartbeatDuration(heartbeatDuration time.Duration) TxnOption {
	return func(opts *txnOptions) {
		opts.heartbeatDuration = heartbeatDuration
	}
}

// WithTxnOptionSetIsolationLevel set the isolation level for current transaction
func WithTxnOptionSetIsolationLevel(isolationLevel txnpb.IsolationLevel) TxnOption {
	return func(opts *txnOptions) {
		opts.isolationLevel = isolationLevel
	}
}

// WithTxnOptionEnableAsynchronousConsensus enable asynchronous consensus. The transaction client sends to
// TxnMananger to process the transaction operation and waits for a response from TxnMananger
// synchronously. By default TxnManager needs to wait for the data to be written to the storage
// to complete the consensus before responding to the transaction client. If asynchronous consensus
// is enabled, TxnManager will complete the consensus asynchronously and return the success of the
// operation to the transaction client immediately, so that the transaction client can send the next
// transaction operation immediately, thus saving the consensus delay. So once the transaction needs
// to read the previous write data, then it needs to wait for the previous asynchronous consensus to
// complete, so it needs to record again in memory which data is being consensus asynchronously and
// which data data has completed consensus. So we need to set the memory size to store the consensus
// data that the asynchronous consensus has not yet completed.
func WithTxnOptionEnableAsynchronousConsensus(maxInfilghtKeysBytes int) TxnOption {
	return func(opts *txnOptions) {
		opts.optimize.asynchronousConsensus = true
		opts.optimize.maxInfilghtKeysBytes = maxInfilghtKeysBytes
	}
}
