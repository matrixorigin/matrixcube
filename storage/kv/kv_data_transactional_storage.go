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
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/hlcpb"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/buf"
	keysutil "github.com/matrixorigin/matrixcube/util/keys"
)

var _ storage.TransactionalDataStorage = (*kvDataStorage)(nil)

// TxnOperationKeysResolver parsing a TxnOperation involves the operation of a collection of keys
type TxnOperationKeysResolver interface {
	// Resolve return all the Keys involved in the operation.
	Resolve(txnpb.TxnOperation) [][]byte
}

func (kv *kvDataStorage) UpdateTxnRecord(record txnpb.TxnRecord, ctx storage.WriteContext) error {
	buffer := ctx.(storage.InternalContext).ByteBuf()
	defer buffer.ResetWrite()

	txnRecordKey := encodeTxnRecordKey(record.TxnRecordRouteKey, record.TxnMeta.ID, buffer)
	wb := ctx.WriteBatch()
	wb.(util.WriteBatch).Set(txnRecordKey, protoc.MustMarshal(&record))
	return nil
}

func (kv *kvDataStorage) DeleteTxnRecord(txnRecordRouteKey, txnID []byte, ctx storage.WriteContext) error {
	buffer := ctx.(storage.InternalContext).ByteBuf()
	defer buffer.ResetWrite()

	txnRecordKey := encodeTxnRecordKey(txnRecordRouteKey, txnID, buffer)
	wb := ctx.WriteBatch()
	wb.(util.WriteBatch).Delete(txnRecordKey)
	return nil
}

func (kv *kvDataStorage) CommitWrittenData(originKey []byte, commitTS hlcpb.Timestamp, ctx storage.WriteContext) error {
	buffer := ctx.(storage.InternalContext).ByteBuf()
	defer buffer.ResetWrite()

	originKeyDataKey := EncodeDataKey(originKey, buffer)
	exists, meta, err := kv.getTxnUncommittedMVCCMetadata(originKeyDataKey)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	if commitTS.Less(meta.Timestamp) {
		return fmt.Errorf("invalid commit timestamp %s, provisional %s",
			meta.Timestamp.String(),
			commitTS.String())
	}

	wb := ctx.WriteBatch().(util.WriteBatch)
	wb.Delete(originKeyDataKey)
	// txn write timestamp changed, use commit timestamp as new mvcc version
	if commitTS.Greater(meta.Timestamp) {
		oldMVCCKey, v, err := kv.getTxnMVCCData(originKey, meta.Timestamp, true, buffer)
		if err != nil {
			return err
		}
		wb.Delete(oldMVCCKey)
		wb.Set(encodeTxnMVCCKey(originKey, commitTS, buffer), v)
	}
	return nil
}

func (kv *kvDataStorage) RollbackWrittenData(originKey []byte, timestamp hlcpb.Timestamp, ctx storage.WriteContext) error {
	buffer := ctx.(storage.InternalContext).ByteBuf()
	defer buffer.ResetWrite()

	originKeyDataKey := EncodeDataKey(originKey, buffer)
	exists, meta, err := kv.getTxnUncommittedMVCCMetadata(originKeyDataKey)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if !meta.Timestamp.Equal(timestamp) {
		return nil
	}

	wb := ctx.WriteBatch().(util.WriteBatch)
	wb.Delete(originKeyDataKey)
	wb.Delete(encodeTxnMVCCKey(originKey, timestamp, buffer))
	return nil
}

func (kv *kvDataStorage) CleanMVCCData(shard metapb.Shard, timestamp hlcpb.Timestamp, ctx storage.WriteContext) error {
	buffer := ctx.(storage.InternalContext).ByteBuf()
	wb := ctx.WriteBatch().(util.WriteBatch)

	view := kv.base.GetView()
	defer view.Close()

	// Scan all originKey in the current shard, and skip all related transaction keys of the same key (MVCCKeys,
	// TxnRecordKeys).
	err := kv.base.ScanInViewWithOptions(view,
		EncodeShardStart(shard.Start, buffer),
		EncodeShardEnd(shard.End, buffer),
		func(key, value []byte) (storage.NextIterOptions, error) {
			defer buffer.ResetWrite()

			originKey, kt, _ := decodeTxnKey(key)
			// keep uncommitted data
			if kt == txnOriginKeyType {
				var uncommitted txnpb.TxnUncommittedMVCCMetadata
				protoc.MustUnmarshal(&uncommitted, value)
				wb.DeleteRange(encodeTxnMVCCKey(originKey, hlcpb.Timestamp{}, buffer),
					encodeTxnMVCCKey(originKey, uncommitted.Timestamp, buffer))
				wb.DeleteRange(encodeTxnMVCCKey(originKey, uncommitted.Timestamp.Next(), buffer),
					encodeTxnMVCCKey(originKey, timestamp, buffer))
			} else {
				wb.DeleteRange(encodeTxnMVCCKey(originKey, hlcpb.Timestamp{}, buffer),
					encodeTxnMVCCKey(originKey, timestamp, buffer))
			}
			return storage.NextIterOptions{SeekGE: txnNextScanKey(originKey, buffer)}, nil
		})
	if err != nil {
		return err
	}
	return nil
}

func (kv *kvDataStorage) GetTxnRecord(txnRecordRouteKey, txnID []byte) (bool, txnpb.TxnRecord, error) {
	buffer := buf.NewByteBuf(txnRecordKeyLen(txnRecordRouteKey, txnID))
	defer buffer.Release()

	var record txnpb.TxnRecord
	v, err := kv.base.Get(encodeTxnRecordKey(txnRecordRouteKey, txnID, buffer))
	if err != nil {
		return false, record, err
	}
	if len(v) == 0 {
		return false, record, nil
	}

	protoc.MustUnmarshal(&record, v)
	return true, record, nil
}

func (kv *kvDataStorage) GetCommitted(originKey []byte, timestamp hlcpb.Timestamp) (exist bool, data []byte, err error) {
	buffer := buf.NewByteBuf(txnMVCCKeyLen(originKey))
	defer buffer.Release()

	_, v, err := kv.base.SeekLT(encodeTxnMVCCKey(originKey, timestamp, buffer))
	if err != nil {
		return false, nil, err
	}
	if len(v) == 0 {
		return false, nil, nil
	}
	return true, v, nil
}

func (kv *kvDataStorage) GetUncommittedOrAnyHighCommitted(originKey []byte, timestamp hlcpb.Timestamp) (txnpb.TxnConflictData, error) {
	buffer := buf.NewByteBuf(txnMVCCKeyLen(originKey))
	defer buffer.Release()

	// check conflict with uncommitted
	ok, v, err := kv.getTxnUncommittedMVCCMetadata(EncodeDataKey(originKey, buffer))
	if err != nil {
		return txnpb.TxnConflictData{}, err
	}
	if ok {
		return txnpb.TxnConflictData{WithUncommitted: v}, nil
	}

	// check conflict with committed
	k, _, err := kv.base.Seek(encodeTxnMVCCKey(originKey, timestamp, buffer))
	if err != nil {
		return txnpb.TxnConflictData{}, err
	}
	if len(k) == 0 {
		return txnpb.TxnConflictData{}, err
	}

	k, kt, timestampValue := decodeTxnKey(k)
	if kt != txnMVCCKeyType || !bytes.Equal(k, originKey) {
		return txnpb.TxnConflictData{}, nil
	}

	ts := decodeTimestamp(timestampValue)
	return txnpb.TxnConflictData{WithCommitted: ts}, nil
}

func (kv *kvDataStorage) GetUncommittedOrAnyHighCommittedByRange(op txnpb.TxnOperation, timestamp hlcpb.Timestamp) ([]txnpb.TxnConflictData, error) {
	keys := kv.txn.keysResolver.Resolve(op)
	if len(keys) == 0 {
		return nil, nil
	}

	buffer := buf.NewByteBuf(txnMVCCKeyLen(keys[0]))
	defer buffer.Release()

	tree := keysutil.NewKeyTree(64)
	tree.AddMany(keys)

	from := EncodeDataKey(tree.Min(), buffer)
	to := txnNextScanKey(tree.Max(), buffer)

	view := kv.base.GetView()
	defer view.Close()

	var conflicts []txnpb.TxnConflictData
	seekToNextKeyInTree := func(k []byte, seekFn func([]byte) []byte) (storage.NextIterOptions, error) {
		nextK := seekFn(k)
		if len(nextK) == 0 {
			return storage.NextIterOptions{Stop: true}, nil
		}
		return storage.NextIterOptions{SeekGE: EncodeDataKey(nextK, buffer)}, nil
	}
	err := kv.base.ScanInViewWithOptions(view, from, to, func(key, value []byte) (storage.NextIterOptions, error) {
		buffer.MarkWrite()
		defer buffer.ResetWrite()

		k, kt, v := decodeTxnKey(key)

		// seek to next key in the tree
		if !tree.Contains(k) {
			return seekToNextKeyInTree(k, tree.Seek)
		}

		switch kt {
		case txnOriginKeyType:
			meta := txnpb.TxnUncommittedMVCCMetadata{}
			protoc.MustUnmarshal(&meta, value)
			// conflict with uncommitted
			conflicts = append(conflicts, txnpb.TxnConflictData{
				WithUncommitted: meta,
			})
			return seekToNextKeyInTree(k, tree.SeekGT)
		case txnMVCCKeyType:
			ts := decodeTimestamp(v)
			if ts.Less(timestamp) {
				return storage.NextIterOptions{SeekGE: encodeTxnMVCCKey(k, timestamp, buffer)}, nil
			}
			// conflict with committed
			conflicts = append(conflicts, txnpb.TxnConflictData{
				WithCommitted: ts,
			})
			return seekToNextKeyInTree(k, tree.SeekGT)
		case txnRecordKeyType:
			return seekToNextKeyInTree(k, tree.SeekGT)
		}
		return storage.NextIterOptions{}, nil
	})
	if err != nil {
		return nil, err
	}

	return conflicts, nil
}

func (kv *kvDataStorage) getTxnUncommittedMVCCMetadata(originDataKey []byte) (bool, txnpb.TxnUncommittedMVCCMetadata, error) {
	meta := txnpb.TxnUncommittedMVCCMetadata{}

	v, err := kv.base.Get(originDataKey)
	if err != nil {
		return false, meta, err
	}

	if len(v) == 0 {
		return false, meta, err
	}

	protoc.MustUnmarshal(&meta, v)
	return true, meta, nil
}

func (kv *kvDataStorage) getTxnMVCCData(originKey []byte, timestamp hlcpb.Timestamp, mustExists bool, buffer *buf.ByteBuf) ([]byte, []byte, error) {
	oldMVCCKey := encodeTxnMVCCKey(originKey, timestamp, buffer)
	v, err := kv.base.Get(oldMVCCKey)
	if err != nil {
		return oldMVCCKey, v, err
	}
	if len(v) == 0 && mustExists {
		return nil, nil, fmt.Errorf("%s missing provisional mvcc",
			hex.EncodeToString(originKey))
	}
	return oldMVCCKey, v, nil
}
