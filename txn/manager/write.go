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

	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/txn"
	"github.com/matrixorigin/matrixcube/util/uuid"
)

func (t *TxnManager) handleWrite(
	parentCtx context.Context,
	meta txnpb.TxnOpMeta,
	req txnpb.TxnRequest,
) (
	conflict txnpb.TxnConflictData,
	err error,
) {

	for _, key := range req.Operation.Impacted.PointKeys {
		// latch
		unlock := t.lockWrite(meta.TxnMeta.ID, key)
		defer unlock()

		// check ts cache
		if t.IsStaleWrite(key, req.Operation.Timestamp) {
			//FIXME restart transaction
		}

		//FIXME lock table

		conflict, err = t.storage.GetUncommittedOrAnyHighCommitted(key, req.Operation.Timestamp)
		if err != nil {
			return
		}

		if err != nil || !conflict.IsEmpty() {
			return
		}

		//FIXME 2.5.4 2.5.6 2.5.7

	}

	shard := metapb.Shard{
		ID:    req.Operation.ToShard,
		Group: req.Operation.ShardGroup,
	}
	var reader txn.TransactionDataReader //FIXME
	var tree txn.UncommittedDataTree     //FIXME
	var data txnpb.ConsensusData
	data, err = t.processor.HandleWrite(shard, meta, []txnpb.TxnRequest{req}, reader, tree)
	if err != nil {
		return
	}

	var rpcRequest rpcpb.Request
	id := uuid.NewV4()
	rpcRequest.ID = id.Bytes()
	rpcRequest.Type = rpcpb.Write
	rpcRequest.CustomType = data.RequestType
	rpcRequest.Cmd = data.Data
	if err = t.proxy.Dispatch(rpcRequest); err != nil {
		return
	}

	return
}
