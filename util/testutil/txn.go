package testutil

import (
	"fmt"
	"testing"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/hlcpb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util/buf"
	keysutil "github.com/matrixorigin/matrixcube/util/keys"
	"github.com/stretchr/testify/assert"
)

// AddTestUncommittedMVCCRecord add test uncommitted record
func AddTestUncommittedMVCCRecord(t *testing.T, base storage.KVBaseStorage, key []byte, timestamp int64) {
	ts := hlcpb.Timestamp{PhysicalTime: timestamp}
	assert.NoError(t, base.Set(keysutil.EncodeDataKey(key, nil), protoc.MustMarshal(&txnpb.TxnUncommittedMVCCMetadata{
		Timestamp: ts,
	}), false))
	assert.NoError(t, base.Set(keysutil.EncodeTxnMVCCKey(key, ts, buf.NewByteBuf(32), true),
		[]byte(fmt.Sprintf("%s-%d(u)", key, timestamp)), false))
}

// AddTestCommittedMVCCRecord add test uncommitted record
func AddTestCommittedMVCCRecord(t *testing.T, base storage.KVBaseStorage, key []byte, timestamp int64) {
	assert.NoError(t, base.Set(keysutil.EncodeTxnMVCCKey(key, hlcpb.Timestamp{PhysicalTime: timestamp},
		buf.NewByteBuf(32), true), []byte(fmt.Sprintf("%s-%d(c)", key, timestamp)), false))
}

// AddTestCommittedMVCCRecordWithPrefix add test uncommitted record
func AddTestCommittedMVCCRecordWithPrefix(t *testing.T, base storage.KVBaseStorage, key, prefix []byte, timestamp int64) {
	assert.NoError(t, base.Set(keysutil.EncodeTxnMVCCKey(key, hlcpb.Timestamp{PhysicalTime: timestamp},
		buf.NewByteBuf(32), true), keysutil.Join(prefix, []byte(fmt.Sprintf("%s-%d(c)", key, timestamp))), false))
}

// AddTestTxnRecord add test txn record
func AddTestTxnRecord(t *testing.T, base storage.KVBaseStorage, txnRecordRouteKey, txnID []byte) {
	assert.NoError(t, base.Set(keysutil.EncodeTxnRecordKey(txnRecordRouteKey, txnID, buf.NewByteBuf(32), true), protoc.MustMarshal(&txnpb.TxnRecord{
		TxnMeta: txnpb.TxnMeta{ID: txnID, TxnRecordRouteKey: txnRecordRouteKey},
	}), false))
}

// GetTestTxnOpMeta returns test txn op meta
func GetTestTxnOpMeta(seq, epoch uint32, ts int64) txnpb.TxnOpMeta {
	v := txnpb.TxnOpMeta{}
	v.Epoch = epoch
	v.Sequence = seq
	v.ReadTimestamp = hlcpb.Timestamp{PhysicalTime: ts}
	v.WriteTimestamp = hlcpb.Timestamp{PhysicalTime: ts}
	return v
}

// GetUncommitted returns uncommitted mvccmetadata
func GetUncommitted(seq, epoch uint32, ts int64) txnpb.TxnUncommittedMVCCMetadata {
	v := txnpb.TxnUncommittedMVCCMetadata{}
	v.Sequence = seq
	v.Epoch = epoch
	v.Timestamp = hlcpb.Timestamp{PhysicalTime: ts}
	v.WriteTimestamp = hlcpb.Timestamp{PhysicalTime: ts}
	v.ReadTimestamp = hlcpb.Timestamp{PhysicalTime: ts}
	return v
}

// GetUncommittedWithTxnOp returns uncommitted mvccmetadata
func GetUncommittedWithTxnOp(txn txnpb.TxnOpMeta) txnpb.TxnUncommittedMVCCMetadata {
	v := txnpb.TxnUncommittedMVCCMetadata{}
	v.TxnMeta = txn.TxnMeta
	v.Sequence = txn.Sequence
	v.Epoch = txn.Epoch
	v.Timestamp = txn.WriteTimestamp
	return v
}
