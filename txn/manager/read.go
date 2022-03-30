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
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/txn"
)

func (t *TxnManager) handleRead(
	parentCtx context.Context,
	meta txnpb.TxnOpMeta,
	req txnpb.TxnRequest,
) (
	conflict txnpb.TxnConflictData,
	data []byte,
	err error,
) {

	for _, key := range req.Operation.Impacted.PointKeys {
		// latch
		unlock := t.lockRead(meta.TxnMeta.ID, key)
		defer unlock()

		// ts cache
		t.SetTSCache(key, req.Operation.Timestamp)

		//FIXME lock table for si

		// check write intent
		conflict, err = t.storage.GetUncommittedOrAnyHighCommitted(
			key,
			req.Operation.Timestamp,
		)
		if err != nil || !conflict.IsEmpty() {
			return
		}

	}

	shard := metapb.Shard{
		ID:    req.Operation.ToShard,
		Group: req.Operation.ShardGroup,
	}
	var tree txn.UncommittedDataTree     //FIXME
	var reader txn.TransactionDataReader //FIXME
	data, err = t.processor.HandleRead(shard, meta, req, reader, tree)
	if err != nil {
		return
	}

	return
}
