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

func (kv *kvDataStorage) UpdateTxnRecord(record txnpb.TxnRecord, ctx storage.WriteContext) error {
	buffer := ctx.(storage.InternalContext).ByteBuf()
	defer buffer.ResetWrite()

	txnRecordKey := keysutil.EncodeTxnRecordKey(record.TxnRecordRouteKey, record.TxnMeta.ID, buffer, true)
	wb := ctx.WriteBatch()
	wb.(util.WriteBatch).Set(txnRecordKey, protoc.MustMarshal(&record))
	return nil
}

func (kv *kvDataStorage) DeleteTxnRecord(txnRecordRouteKey, txnID []byte, ctx storage.WriteContext) error {
	buffer := ctx.(storage.InternalContext).ByteBuf()
	defer buffer.ResetWrite()

	txnRecordKey := keysutil.EncodeTxnRecordKey(txnRecordRouteKey, txnID, buffer, true)
	wb := ctx.WriteBatch()
	wb.(util.WriteBatch).Delete(txnRecordKey)
	return nil
}

func (kv *kvDataStorage) CommitWrittenData(originKey []byte, commitTS hlcpb.Timestamp, ctx storage.WriteContext) error {
	buffer := ctx.(storage.InternalContext).ByteBuf()
	defer buffer.ResetWrite()

	originKeyDataKey := keysutil.EncodeDataKey(originKey, buffer)
	exists, meta, err := kv.doGetUncommittedMVCCMetadata(originKeyDataKey)
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
		wb.Set(keysutil.EncodeTxnMVCCKey(originKey, commitTS, buffer, true), v)
	}
	return nil
}

func (kv *kvDataStorage) RollbackWrittenData(originKey []byte, timestamp hlcpb.Timestamp, ctx storage.WriteContext) error {
	buffer := ctx.(storage.InternalContext).ByteBuf()
	defer buffer.ResetWrite()

	originKeyDataKey := keysutil.EncodeDataKey(originKey, buffer)
	exists, meta, err := kv.doGetUncommittedMVCCMetadata(originKeyDataKey)
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
	wb.Delete(keysutil.EncodeTxnMVCCKey(originKey, timestamp, buffer, true))
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
		keysutil.EncodeShardStart(shard.Start, buffer),
		keysutil.EncodeShardEnd(shard.End, buffer),
		func(key, value []byte) (storage.NextIterOptions, error) {
			defer buffer.ResetWrite()

			originKey, kt, _ := keysutil.DecodeTxnKey(key)
			// keep uncommitted data
			if kt == keysutil.TxnOriginKeyType {
				var uncommitted txnpb.TxnUncommittedMVCCMetadata
				protoc.MustUnmarshal(&uncommitted, value)
				wb.DeleteRange(keysutil.EncodeTxnMVCCKey(originKey, hlcpb.Timestamp{}, buffer, true),
					keysutil.EncodeTxnMVCCKey(originKey, uncommitted.Timestamp, buffer, true))
				wb.DeleteRange(keysutil.EncodeTxnMVCCKey(originKey, uncommitted.Timestamp.Next(), buffer, true),
					keysutil.EncodeTxnMVCCKey(originKey, timestamp, buffer, true))
			} else {
				wb.DeleteRange(keysutil.EncodeTxnMVCCKey(originKey, hlcpb.Timestamp{}, buffer, true),
					keysutil.EncodeTxnMVCCKey(originKey, timestamp, buffer, true))
			}
			return storage.NextIterOptions{SeekGE: keysutil.TxnNextScanKey(originKey, buffer, true)}, nil
		})
	if err != nil {
		return err
	}
	return nil
}

func (kv *kvDataStorage) GetTxnRecord(txnRecordRouteKey, txnID []byte) (bool, txnpb.TxnRecord, error) {
	buffer := buf.NewByteBuf(keysutil.TxnRecordKeyLen(txnRecordRouteKey, txnID))
	defer buffer.Release()

	var record txnpb.TxnRecord
	v, err := kv.base.Get(keysutil.EncodeTxnRecordKey(txnRecordRouteKey, txnID, buffer, true))
	if err != nil {
		return false, record, err
	}
	if len(v) == 0 {
		return false, record, nil
	}

	protoc.MustUnmarshal(&record, v)
	return true, record, nil
}

func (kv *kvDataStorage) Get(originKey []byte, timestamp hlcpb.Timestamp) ([]byte, error) {
	buffer := buf.NewByteBuf(keysutil.TxnMVCCKeyLen(originKey))
	defer buffer.Release()
	return kv.base.Get(keysutil.EncodeTxnMVCCKey(originKey, timestamp, buffer, true))
}

func (kv *kvDataStorage) GetCommitted(originKey []byte, timestamp hlcpb.Timestamp) (exist bool, data []byte, err error) {
	buffer := buf.NewByteBuf(keysutil.TxnMVCCKeyLen(originKey) * 2)
	defer buffer.Release()

	_, v, err := kv.base.SeekLTAndGE(keysutil.EncodeTxnMVCCKey(originKey, timestamp, buffer, true),
		keysutil.EncodeTxnMVCCKey(originKey, hlcpb.Timestamp{}, buffer, true))
	if err != nil {
		return false, nil, err
	}
	if len(v) == 0 {
		return false, nil, nil
	}

	return true, v, nil
}

func (kv *kvDataStorage) Scan(startOriginKey, endOriginKey []byte,
	timestamp hlcpb.Timestamp,
	filter storage.UncommittedFilter,
	handler func(key, value []byte) (bool, error)) error {
	buffer := buf.NewByteBuf(keysutil.TxnMVCCKeyLen(startOriginKey) + keysutil.TxnMVCCKeyLen(endOriginKey))
	defer buffer.Release()

	protectedKey := buf.NewByteBuf(keysutil.TxnMVCCKeyLen(startOriginKey))
	defer protectedKey.Release()

	view := kv.base.GetView()
	defer view.Close()

	start := keysutil.EncodeShardStart(startOriginKey, buffer)
	end := keysutil.EncodeShardEnd(endOriginKey, buffer)

	accepted := false
	hasReadableCommitted := false
	acceptFunc := func(key, value []byte) (storage.NextIterOptions, error) {
		ok, err := handler(key, value)
		if err != nil {
			return storage.NextIterOptions{}, err
		}
		if !ok {
			return storage.NextIterOptions{Stop: true}, nil
		}

		accepted = false
		hasReadableCommitted = false
		return storage.NextIterOptions{SeekGE: keysutil.TxnNextScanKey(key, buffer, true)}, nil
	}

	seekToLatestFunc := func(rawKey, key []byte, timestamp hlcpb.Timestamp) (storage.NextIterOptions, error) {
		protectedKey.Clear()
		_, err := protectedKey.Write(rawKey)
		if err != nil {
			return storage.NextIterOptions{}, err
		}
		return storage.NextIterOptions{SeekLT: keysutil.EncodeTxnMVCCKey(key, timestamp, buffer, true)}, nil
	}

	seekToNextKey := func(k []byte) (storage.NextIterOptions, error) {
		return storage.NextIterOptions{SeekGE: keysutil.TxnNextScanKey(k, buffer, true)}, nil
	}

	var uncommitted txnpb.TxnUncommittedMVCCMetadata
	return kv.base.ScanInViewWithOptions(view, start, end, func(key, value []byte) (storage.NextIterOptions, error) {
		buffer.MarkWrite()
		defer buffer.ResetWrite()

		k, kt, v := keysutil.DecodeTxnKey(key)
		if bytes.Equal(protectedKey.ReadableBytes(), key) {
			if hasReadableCommitted {
				return acceptFunc(k, value)
			}
			return seekToNextKey(k)
		}

		switch kt {
		case keysutil.TxnOriginKeyType:
			// uncommitted data
			uncommitted.Reset()
			protoc.MustUnmarshal(&uncommitted, value)

			// if accept, seek to the uncommitted timestamp
			if filter(k, uncommitted) {
				accepted = true
				return storage.NextIterOptions{SeekGE: keysutil.EncodeTxnMVCCKey(k, uncommitted.Timestamp, buffer, true)}, nil
			}

			return seekToLatestFunc(key, k, timestamp)
		case keysutil.TxnMVCCKeyType:
			if accepted {
				return acceptFunc(k, value)
			}

			ts := keysutil.DecodeTimestamp(v)
			if ts.GreaterEq(timestamp) {
				return seekToNextKey(k)
			}

			// maybe has larger version
			hasReadableCommitted = true
			return seekToLatestFunc(key, k, timestamp)
		case keysutil.TxnRecordKeyType:
			return seekToNextKey(k)
		}
		return storage.NextIterOptions{}, nil
	})
}

func (kv *kvDataStorage) GetUncommittedMVCCMetadata(originKey []byte) (bool, txnpb.TxnUncommittedMVCCMetadata, error) {
	buffer := buf.NewByteBuf(keysutil.TxnMVCCKeyLen(originKey))
	defer buffer.Release()

	return kv.doGetUncommittedMVCCMetadata(keysutil.EncodeDataKey(originKey, buffer))
}

func (kv *kvDataStorage) GetUncommittedMVCCMetadataByRange(op txnpb.TxnOperation) ([]txnpb.TxnUncommittedMVCCMetadata, error) {
	op.Impacted.Sort()
	if op.Impacted.IsEmpty() {
		return nil, nil
	}

	min, max := op.Impacted.GetKeyRange()
	buffer := buf.NewByteBuf(keysutil.TxnMVCCKeyLen(min))
	defer buffer.Release()

	tree := keysutil.NewMixedKeysTree(op.Impacted.PointKeys)
	for _, r := range op.Impacted.Ranges {
		tree.AddKeyRange(r.Start, r.End)
	}
	from := keysutil.EncodeDataKey(min, buffer)
	to := keysutil.TxnNextScanKey(max, buffer, true)

	view := kv.base.GetView()
	defer view.Close()

	var uncommitted []txnpb.TxnUncommittedMVCCMetadata
	seekToNextKeyInTree := func(k []byte, seekFn func([]byte) []byte) (storage.NextIterOptions, error) {
		nextK := seekFn(k)
		if len(nextK) == 0 {
			return storage.NextIterOptions{Stop: true}, nil
		}
		return storage.NextIterOptions{SeekGE: keysutil.EncodeDataKey(nextK, buffer)}, nil
	}
	err := kv.base.ScanInViewWithOptions(view, from, to, func(key, value []byte) (storage.NextIterOptions, error) {
		buffer.MarkWrite()
		defer buffer.ResetWrite()

		k, kt, _ := keysutil.DecodeTxnKey(key)
		// seek to next key in the
		if !tree.Contains(k) {
			return seekToNextKeyInTree(k, tree.Seek)
		}

		switch kt {
		case keysutil.TxnOriginKeyType:
			meta := txnpb.TxnUncommittedMVCCMetadata{}
			protoc.MustUnmarshal(&meta, value)
			// conflict with uncommitted
			uncommitted = append(uncommitted, meta)
			return seekToNextKeyInTree(k, tree.SeekGT)
		default:
			return seekToNextKeyInTree(k, tree.SeekGT)
		}
	})
	if err != nil {
		return nil, err
	}
	return uncommitted, nil
}

func (kv *kvDataStorage) GetUncommittedOrAnyHighCommitted(originKey []byte, timestamp hlcpb.Timestamp) (txnpb.TxnConflictData, error) {
	buffer := buf.NewByteBuf(keysutil.TxnMVCCKeyLen(originKey))
	defer buffer.Release()

	// check conflict with uncommitted
	ok, v, err := kv.doGetUncommittedMVCCMetadata(keysutil.EncodeDataKey(originKey, buffer))
	if err != nil {
		return txnpb.TxnConflictData{}, err
	}
	if ok {
		return txnpb.TxnConflictData{WithUncommitted: v}, nil
	}

	// check conflict with committed
	k, _, err := kv.base.Seek(keysutil.EncodeTxnMVCCKey(originKey, timestamp, buffer, true))
	if err != nil {
		return txnpb.TxnConflictData{}, err
	}
	if len(k) == 0 {
		return txnpb.TxnConflictData{}, err
	}

	k, kt, timestampValue := keysutil.DecodeTxnKey(k)
	if kt != keysutil.TxnMVCCKeyType || !bytes.Equal(k, originKey) {
		return txnpb.TxnConflictData{}, nil
	}

	ts := keysutil.DecodeTimestamp(timestampValue)
	return txnpb.TxnConflictData{OriginKey: k, WithCommitted: ts}, nil
}

func (kv *kvDataStorage) GetUncommittedOrAnyHighCommittedByRange(op txnpb.TxnOperation, timestamp hlcpb.Timestamp) ([]txnpb.TxnConflictData, error) {
	op.Impacted.Sort()
	if op.Impacted.IsEmpty() {
		return nil, nil
	}

	min, max := op.Impacted.GetKeyRange()
	buffer := buf.NewByteBuf(keysutil.TxnMVCCKeyLen(min))
	defer buffer.Release()

	tree := keysutil.NewMixedKeysTree(op.Impacted.PointKeys)
	for _, r := range op.Impacted.Ranges {
		tree.AddKeyRange(r.Start, r.End)
	}
	from := keysutil.EncodeDataKey(min, buffer)
	to := keysutil.TxnNextScanKey(max, buffer, true)

	view := kv.base.GetView()
	defer view.Close()

	var conflicts []txnpb.TxnConflictData
	seekToNextKeyInTree := func(k []byte, seekFn func([]byte) []byte) (storage.NextIterOptions, error) {
		nextK := seekFn(k)
		if len(nextK) == 0 {
			return storage.NextIterOptions{Stop: true}, nil
		}
		return storage.NextIterOptions{SeekGE: keysutil.EncodeDataKey(nextK, buffer)}, nil
	}
	err := kv.base.ScanInViewWithOptions(view, from, to, func(key, value []byte) (storage.NextIterOptions, error) {
		buffer.MarkWrite()
		defer buffer.ResetWrite()

		k, kt, v := keysutil.DecodeTxnKey(key)

		// seek to next key in the tree
		if !tree.Contains(k) {
			return seekToNextKeyInTree(k, tree.Seek)
		}

		switch kt {
		case keysutil.TxnOriginKeyType:
			meta := txnpb.TxnUncommittedMVCCMetadata{}
			protoc.MustUnmarshal(&meta, value)
			// conflict with uncommitted
			conflicts = append(conflicts, txnpb.TxnConflictData{
				OriginKey:       k,
				WithUncommitted: meta,
			})
			return seekToNextKeyInTree(k, tree.SeekGT)
		case keysutil.TxnMVCCKeyType:
			ts := keysutil.DecodeTimestamp(v)
			if ts.Less(timestamp) {
				return storage.NextIterOptions{SeekGE: keysutil.EncodeTxnMVCCKey(k, timestamp, buffer, true)}, nil
			}
			// conflict with committed
			conflicts = append(conflicts, txnpb.TxnConflictData{
				OriginKey:     k,
				WithCommitted: ts,
			})
			return seekToNextKeyInTree(k, tree.SeekGT)
		case keysutil.TxnRecordKeyType:
			return seekToNextKeyInTree(k, tree.SeekGT)
		}
		return storage.NextIterOptions{}, nil
	})
	if err != nil {
		return nil, err
	}

	return conflicts, nil
}

func (kv *kvDataStorage) doGetUncommittedMVCCMetadata(originDataKey []byte) (bool, txnpb.TxnUncommittedMVCCMetadata, error) {
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
	oldMVCCKey := keysutil.EncodeTxnMVCCKey(originKey, timestamp, buffer, true)
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
