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

package txnmanager

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixcube/pb/txnpb"
)

// HandleRequest handles TxnBatchRequest sent from transaction coordinator
func (t *TxnManager) HandleRequest(
	parentCtx context.Context,
	request txnpb.TxnBatchRequest,
) (
	response txnpb.TxnBatchResponse,
	err error,
) {

	txnMeta := request.Header.Txn.TxnMeta
	defer func() {
		// set modified meta
		response.Header.Txn = txnMeta
	}()

	for _, req := range request.Requests {

		switch op := txnpb.InternalTxnOp(req.Operation.Op); op {

		case txnpb.InternalTxnOp_Heartbeat:
			// heartbeat
			if err = t.handleHeartbeat(parentCtx, &txnMeta, req); err != nil {
				return
			}

		case txnpb.InternalTxnOp_Commit:
			// commit
			if err = t.handleCommit(parentCtx, &txnMeta, &request.Header.Txn); err != nil {
				return
			}
			response.Header.Status = txnpb.TxnStatus_Committed

		case txnpb.InternalTxnOp_Rollback:
			// rollback
			if err = t.handleRollback(parentCtx, &txnMeta, &request.Header.Txn); err != nil {
				return
			}
			response.Header.Status = txnpb.TxnStatus_Aborted

		default:
			// non-internal operations

			if op <= txnpb.InternalTxnOp_Reserved {
				// invalid op
				panic(fmt.Errorf("unknown internal op: %v", op))
			}

			switch request.Header.Type {

			case txnpb.TxnRequestType_Read:
				// read
				var data []byte
				var conflict txnpb.TxnConflictData
				if conflict, data, err = t.handleRead(parentCtx, request.Header.Txn, req); err != nil {
					return
				} else if !conflict.IsEmpty() {
					response.Header.Error = txnConflictDataToTxnError(&conflict)
					return
				} else {
					response.Responses = append(response.Responses, txnpb.TxnResponse{
						Data: data,
					})
				}

			case txnpb.TxnRequestType_Write:
				// write
				var conflict txnpb.TxnConflictData
				if conflict, err = t.handleWrite(parentCtx, request.Header.Txn, req); err != nil {
					return
				} else if !conflict.IsEmpty() {
					response.Header.Error = txnConflictDataToTxnError(&conflict)
					return
				}

			default:
				panic("impossible")
			}

		}

	}

	return
}

func txnConflictDataToTxnError(conflict *txnpb.TxnConflictData) *txnpb.TxnError {
	if conflict.ConflictWithCommitted() {
		return &txnpb.TxnError{
			ConflictWithCommittedError: &txnpb.ConflictWithCommittedError{
				MinTimestamp: conflict.WithCommitted,
			},
		}
	} else if conflict.ConflictWithUncommitted() {
		return &txnpb.TxnError{
			//FIXME
		}
	}
	panic("impossible")
}
