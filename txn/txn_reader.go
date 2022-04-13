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

package txn

import (
	"github.com/matrixorigin/matrixcube/pb/hlcpb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/storage"
)

type transactionDataReader struct {
	ds storage.TransactionalDataStorage
}

// NewTransactionDataReader reutrns a transaction data reader. Hide details of transaction data
// visibility from outside.
func NewTransactionDataReader(ds storage.TransactionalDataStorage) TransactionDataReader {
	return &transactionDataReader{
		ds: ds,
	}
}

func (tr *transactionDataReader) Read(key []byte,
	txn txnpb.TxnOpMeta,
	uncommittedTree UncommittedDataTree) (data []byte, err error) {
	// no uncommitted, read the committed
	if uncommittedTree == nil ||
		uncommittedTree.Len() == 0 {
		return tr.readCommitted(key, txn.ReadTimestamp)
	}

	uncommitted, ok := uncommittedTree.Get(key)
	if !ok {
		return tr.readCommitted(key, txn.ReadTimestamp)
	}

	if canReadUncommitted(uncommitted, txn) {
		return tr.readUncommitted(key, uncommitted.Timestamp)
	}
	return tr.readCommitted(key, txn.ReadTimestamp)
}

func (tr *transactionDataReader) ReadRange(startOriginKey, endOriginKey []byte,
	txn txnpb.TxnOpMeta,
	handler func(originKey, data []byte) (bool, error),
	uncommittedTree UncommittedDataTree) error {

	return tr.ds.Scan(startOriginKey, endOriginKey, txn.ReadTimestamp, func(key []byte, uncommitted txnpb.TxnUncommittedMVCCMetadata) (accept bool) {
		if uncommittedTree == nil || uncommittedTree.Len() == 0 {
			return false
		}

		v, ok := uncommittedTree.Get(key)
		if !ok {
			return false
		}
		return canReadUncommitted(v, txn)
	}, func(key, value []byte) (bool, error) {
		return handler(key, value)
	})
}

func (tr *transactionDataReader) readCommitted(key []byte, ts hlcpb.Timestamp) ([]byte, error) {
	_, value, err := tr.ds.GetCommitted(key, ts)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (tr *transactionDataReader) readUncommitted(key []byte, ts hlcpb.Timestamp) ([]byte, error) {
	return tr.ds.Get(key, ts)
}

func canReadUncommitted(uncommitted txnpb.TxnUncommittedMVCCMetadata, txn txnpb.TxnOpMeta) bool {
	return txn.Epoch == uncommitted.Epoch && txn.Sequence >= uncommitted.Sequence
}
