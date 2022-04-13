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
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
)

// UncommittedDataTree uncommitted tree
type UncommittedDataTree interface {
	// Add add a uncommitted data
	Add([]byte, txnpb.TxnUncommittedMVCCMetadata)
	// Get returns the uncommitted data of the key
	Get(key []byte) (txnpb.TxnUncommittedMVCCMetadata, bool)
	// Len returns the count of uncommitted data
	Len() int
	// AscendRange is simliar to Ascend, but perform scan in [start, end)
	AscendRange(start, end []byte, fn func(key []byte, data txnpb.TxnUncommittedMVCCMetadata) (bool, error)) error
}

// TransactionDataReader the reader for transaction data. Reads transaction data and is
// responsible for the details of whether the data is visible to the current transaction.
type TransactionDataReader interface {
	// Read read the latest visible data of the specified key according to the transaction
	// metadata
	Read(originKey []byte, txn txnpb.TxnOpMeta,
		uncommittedTree UncommittedDataTree) (data []byte, err error)

	// ReadRange read the latest visible data of the specified key range according to the
	// transaction metadata. The key and value of the handler are only valid for the current
	// call, if you want to save them, you need to copy.
	ReadRange(fromOriginKey, toOriginKey []byte, txn txnpb.TxnOpMeta,
		handler func(originKey, data []byte) (bool, error),
		uncommittedTree UncommittedDataTree) error
}

// TransactionCommandProcessor transaction read and write command processor, responsible for
// handling the transaction read and write logic.
type TransactionCommandProcessor interface {
	// HandleWrite execute the write command logic of the transaction at the node where TxnManager
	// is located, and return the data that requires consensus.
	// The transaction client calls the Execute* method once, which triggers the execution of a
	// HandleWrite. In addition there are some additional responsibilities as follows:
	// 1. The data written by this method is `Uncommitted data`, which contains `Uncommitted Data`
	//    and `UncommittedMVCCMetadata`. And added the `uncommitted` into the UncommittedDataTree.
	// 2. If `TxnRequest` requires the creation of a `TxnRecord`, the returned consensus data
	//    needs to include the `TxnRecord`.
	HandleWrite(shard metapb.Shard, txn txnpb.TxnOpMeta, ops []txnpb.TxnRequest, reader TransactionDataReader, uncommitted UncommittedDataTree) (txnpb.ConsensusData, error)
	// HandleRead execute the read command logic of the transaction at the node where TxnManager
	// is located.
	HandleRead(shard metapb.Shard, txn txnpb.TxnOpMeta, op txnpb.TxnRequest, reader TransactionDataReader, uncommitted UncommittedDataTree) ([]byte, error)
}
