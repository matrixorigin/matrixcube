package txn

import (
	"context"

	"github.com/matrixorigin/matrixcube/pb/txnpb"
)

// TransactionDataReader the reader for transaction data. Reads transaction data and is
// responsible for the details of whether the data is visible to the current transaction.
type TransactionDataReader interface {
	// Read read the latest visible data of the specified key according to the transaction
	// metadata。
	Read(originKey []byte, txn txnpb.TxnMeta) (data []byte, err error)

	// ReadRange read the latest visible data of the specified key range according to the
	// transaction metadata
	ReadRange(fromOriginKey, toOriginKey []byte, txn txnpb.TxnMeta, handler func(originKey, data []byte) (bool, error)) error
}

// TransactionCommandProcessor transaction read and write command processor, responsible for
// handling the transaction read and write logic.
type TransactionCommandProcessor interface {
	// HandleWrite execute the write command logic of the transaction at the node where TxnManager
	// is located, and return the data that requires consensus.
	// The transaction client calls the Execute* method once, which triggers the execution of a
	// HandleWrite. In addition there are some additional responsibilities as follows:
	// 1. The data written by this method is `Uncommitted data`, which contains `Uncommitted Data`
	// and `UncommittedMVCCMetadata`.
	// 2. If `TxnOperation` requires the creation of a `TxnRecord`, the returned consensus data
	// needs to include the `TxnRecord`.
	HandleWrite(ctx context.Context, ops []txnpb.TxnOperation, reader TransactionDataReader) (txnpb.ConsensusData, error)
	// HandleRead execute the read command logic of the transaction at the node where TxnManager
	// is located.
	HandleRead(ctx context.Context, op txnpb.TxnOperation, reader TransactionDataReader) ([]byte, error)
}
