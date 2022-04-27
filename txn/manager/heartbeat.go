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

	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/util/uuid"
)

func (t *TxnManager) handleHeartbeat(
	parentCtx context.Context,
	txnMeta *txnpb.TxnMeta,
	req txnpb.TxnRequest,
) error {

	_, record, err := t.storage.GetTxnRecord(txnMeta.TxnRecordRouteKey, txnMeta.ID)
	if err != nil {
		return err
	}
	record.LastHeartbeat = req.Operation.Timestamp

	var rpcRequest rpcpb.Request
	id := uuid.NewV4()
	rpcRequest.ID = id.Bytes()
	rpcRequest.Type = rpcpb.Write
	rpcRequest.CustomType = uint64(rpcpb.CmdUpdateTxnRecord)
	rpcRequest.UpdateTxnRecord = rpcpb.UpdateTxnRecordRequest{
		TxnRecord: record,
	}
	if err := t.proxy.Dispatch(rpcRequest); err != nil {
		return err
	}

	return nil
}
