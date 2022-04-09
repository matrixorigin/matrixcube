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

package kv

import (
	"context"

	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/txn"
)

var _ txn.TransactionCommandProcessor = (*kvBasedTxnCommandProcessor)(nil)

// kvBasedTxnCommandProcessor is a transactional command executor based on the underlying
// kv store, with the following supported operations:
// 1. Set
// 2. Delete
// 3. Scan
// 4. Get
// 5. MGet
type kvBasedTxnCommandProcessor struct {
}

func (kp *kvBasedTxnCommandProcessor) HandleWrite(ctx context.Context, ops []txnpb.TxnOperation, reader txn.TransactionDataReader) (txnpb.ConsensusData, error) {
	return txnpb.ConsensusData{}, nil
}

func (kp *kvBasedTxnCommandProcessor) HandleRead(ctx context.Context, op txnpb.TxnOperation, reader txn.TransactionDataReader) ([]byte, error) {
	return nil, nil
}
