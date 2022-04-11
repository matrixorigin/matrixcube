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
	"testing"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/hlcpb"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor"
	"github.com/matrixorigin/matrixcube/util/buf"
	keysutil "github.com/matrixorigin/matrixcube/util/keys"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateAndDeleteTxnRecord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	ts := s.(storage.TransactionalDataStorage)
	ctx := storage.NewSimpleWriteContext(0, base, storage.Batch{Index: 1})
	record := txnpb.TxnRecord{TxnMeta: txnpb.TxnMeta{ID: []byte("txn-id"), TxnRecordRouteKey: []byte("key")}}

	ok, v, err := ts.GetTxnRecord(record.TxnRecordRouteKey, record.ID)
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, txnpb.TxnRecord{}, v)

	assert.NoError(t, ts.UpdateTxnRecord(record, ctx))
	assert.NoError(t, ts.Write(ctx))

	ok, v, err = ts.GetTxnRecord(record.TxnRecordRouteKey, record.ID)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, record, v)

	record.Name = "txn"
	assert.NoError(t, ts.UpdateTxnRecord(record, ctx))
	assert.NoError(t, ts.Write(ctx))
	ok, v, err = ts.GetTxnRecord(record.TxnRecordRouteKey, record.ID)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, record, v)

	assert.NoError(t, ts.DeleteTxnRecord(record.TxnRecordRouteKey, record.ID, ctx))
	assert.NoError(t, ts.Write(ctx))
	ok, v, err = ts.GetTxnRecord(record.TxnRecordRouteKey, record.ID)
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, txnpb.TxnRecord{}, v)
}

func TestCommitWrittenDataWithNoUncommittedData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	ts := s.(storage.TransactionalDataStorage)
	ctx := storage.NewSimpleWriteContext(0, base, storage.Batch{Index: 1})
	originKey := []byte("key")
	assert.NoError(t, ts.CommitWrittenData(originKey, hlcpb.Timestamp{PhysicalTime: 10}, ctx))
	assert.NoError(t, ts.Write(ctx))

	checkTxnKeysCount(t, 0, base)
}

func TestCommitWrittenDataWithSameTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	ts := s.(storage.TransactionalDataStorage)
	ctx := storage.NewSimpleWriteContext(0, base, storage.Batch{Index: 1})
	originKey := []byte("key")

	addTestUncommittedMVCCRecord(t, base, originKey, []byte{1}, 10)
	assert.NoError(t, ts.CommitWrittenData(originKey, getTestTimestamp(10), ctx))
	assert.NoError(t, ts.Write(ctx))

	n := 0
	var mvccKey []byte
	assert.NoError(t, base.Scan(keysutil.EncodeShardStart(nil, nil), keysutil.EncodeShardEnd(nil, nil), func(key, value []byte) (bool, error) {
		n++
		mvccKey = key
		return true, nil
	}, true))
	assert.Equal(t, 1, n)

	k, kt, v := keysutil.DecodeTxnKey(mvccKey)
	assert.Equal(t, originKey, k)
	assert.Equal(t, keysutil.TxnMVCCKeyType, kt)
	assert.Equal(t, getTestTimestamp(10), keysutil.DecodeTimestamp(v))
}

func TestCommitWrittenDataWithHighTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	ts := s.(storage.TransactionalDataStorage)
	ctx := storage.NewSimpleWriteContext(0, base, storage.Batch{Index: 1})
	originKey := []byte("key")

	addTestUncommittedMVCCRecord(t, base, originKey, []byte{1}, 10)
	assert.NoError(t, ts.CommitWrittenData(originKey, getTestTimestamp(11), ctx))
	assert.NoError(t, ts.Write(ctx))

	n := 0
	var mvccKey []byte
	assert.NoError(t, base.Scan(keysutil.EncodeShardStart(nil, nil), keysutil.EncodeShardEnd(nil, nil), func(key, value []byte) (bool, error) {
		n++
		mvccKey = key
		return true, nil
	}, true))
	assert.Equal(t, 1, n)

	k, kt, v := keysutil.DecodeTxnKey(mvccKey)
	assert.Equal(t, originKey, k)
	assert.Equal(t, keysutil.TxnMVCCKeyType, kt)
	assert.Equal(t, getTestTimestamp(11), keysutil.DecodeTimestamp(v))
}

func TestCommitWrittenDataWithLowTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	ts := s.(storage.TransactionalDataStorage)
	ctx := storage.NewSimpleWriteContext(0, base, storage.Batch{Index: 1})
	originKey := []byte("key")

	addTestUncommittedMVCCRecord(t, base, originKey, []byte{1}, 10)
	assert.Error(t, ts.CommitWrittenData(originKey, getTestTimestamp(9), ctx))
}

func TestRollbackWrittenDataWithNoProvisionalData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	ts := s.(storage.TransactionalDataStorage)
	ctx := storage.NewSimpleWriteContext(0, base, storage.Batch{Index: 1})
	originKey := []byte("key")
	assert.NoError(t, ts.RollbackWrittenData(originKey, hlcpb.Timestamp{PhysicalTime: 10}, ctx))
	assert.NoError(t, ts.Write(ctx))

	checkTxnKeysCount(t, 0, base)
}

func TestRollbackWrittenData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	ts := s.(storage.TransactionalDataStorage)
	ctx := storage.NewSimpleWriteContext(0, base, storage.Batch{Index: 1})
	originKey := []byte("key")

	addTestUncommittedMVCCRecord(t, base, originKey, []byte{1}, 10)
	assert.NoError(t, ts.RollbackWrittenData(originKey, getTestTimestamp(10), ctx))
	assert.NoError(t, ts.Write(ctx))

	checkTxnKeysCount(t, 0, base)
}

func TestRollbackWrittenDataWithOtherTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	ts := s.(storage.TransactionalDataStorage)
	originKey := []byte("key")

	addTestUncommittedMVCCRecord(t, base, originKey, []byte{1}, 10)

	ctx := storage.NewSimpleWriteContext(0, base, storage.Batch{Index: 1})
	assert.NoError(t, ts.RollbackWrittenData(originKey, getTestTimestamp(11), ctx))
	assert.NoError(t, ts.Write(ctx))

	checkTxnKeysCount(t, 2, base)
}

func TestCleanMVCCData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	ts := s.(storage.TransactionalDataStorage)
	for i := int64(1); i < 10; i++ {
		addTestCommittedMVCCRecord(t, base, []byte("k1"), []byte{1}, i)
	}

	ctx := storage.NewSimpleWriteContext(0, base, storage.Batch{Index: 1})
	assert.NoError(t, ts.CleanMVCCData(metapb.Shard{}, getTestTimestamp(8), ctx))
	assert.NoError(t, ts.Write(ctx))
	checkTxnKeysCount(t, 2, base)

	ctx.WriteBatch().Reset()
	assert.NoError(t, ts.CleanMVCCData(metapb.Shard{}, getTestTimestamp(10), ctx))
	assert.NoError(t, ts.Write(ctx))
	checkTxnKeysCount(t, 0, base)
}

func TestCleanMVCCDataWithUncommittedData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	ts := s.(storage.TransactionalDataStorage)
	addTestUncommittedMVCCRecord(t, base, []byte("k1"), []byte{1}, 1)
	for i := int64(2); i < 10; i++ {
		addTestCommittedMVCCRecord(t, base, []byte("k1"), []byte{1}, i)
	}

	ctx := storage.NewSimpleWriteContext(0, base, storage.Batch{Index: 1})
	assert.NoError(t, ts.CleanMVCCData(metapb.Shard{}, getTestTimestamp(10), ctx))
	assert.NoError(t, ts.Write(ctx))
	checkTxnKeysCount(t, 2, base)
}

func TestCleanMVCCDataWithMultiKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	ts := s.(storage.TransactionalDataStorage)
	for i := int64(1); i < 10; i++ {
		addTestCommittedMVCCRecord(t, base, []byte("k1"), []byte{1}, i)
	}
	for i := int64(1); i < 10; i++ {
		addTestCommittedMVCCRecord(t, base, []byte("k2"), []byte{1}, i)
	}
	checkTxnKeysCount(t, 18, base)

	ctx := storage.NewSimpleWriteContext(0, base, storage.Batch{Index: 1})
	assert.NoError(t, ts.CleanMVCCData(metapb.Shard{}, getTestTimestamp(10), ctx))
	assert.NoError(t, ts.Write(ctx))
	checkTxnKeysCount(t, 0, base)
}

func TestCleanMVCCDataWithMultiKeysAndShard(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	ts := s.(storage.TransactionalDataStorage)
	for i := int64(1); i < 10; i++ {
		addTestCommittedMVCCRecord(t, base, []byte("k1"), []byte{1}, i)
	}
	for i := int64(1); i < 10; i++ {
		addTestCommittedMVCCRecord(t, base, []byte("k2"), []byte{1}, i)
	}
	checkTxnKeysCount(t, 18, base)

	ctx := storage.NewSimpleWriteContext(0, base, storage.Batch{Index: 1})
	assert.NoError(t, ts.CleanMVCCData(metapb.Shard{Start: []byte("k1"), End: []byte("k2")}, getTestTimestamp(10), ctx))
	assert.NoError(t, ts.Write(ctx))
	checkTxnKeysCount(t, 9, base)
}

func TestGetCommitted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	ts := s.(storage.TransactionalDataStorage)
	ok, v, err := ts.GetCommitted([]byte("k1"), getTestTimestamp(10))
	assert.False(t, ok)
	assert.Empty(t, v)
	assert.NoError(t, err)

	for i := int64(1); i < 10; i++ {
		addTestCommittedMVCCRecord(t, base, []byte("k1"), []byte{byte(i)}, i)
	}

	ok, v, err = ts.GetCommitted([]byte("k1"), getTestTimestamp(10))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, []byte{9}, v)
}

func TestGetUncommittedOrAnyHighCommittedWithNoConflict(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	originKey := []byte("k1")
	ts := s.(storage.TransactionalDataStorage)

	// no any records
	v, err := ts.GetUncommittedOrAnyHighCommitted(originKey, getTestTimestamp(8))
	assert.NoError(t, err)
	assert.True(t, v.IsEmpty())

	addTestUncommittedMVCCRecord(t, base, []byte("k2"), []byte("k2"), 11)
	v, err = ts.GetUncommittedOrAnyHighCommitted(originKey, getTestTimestamp(8))
	assert.NoError(t, err)
	assert.True(t, v.IsEmpty())

	addTestCommittedMVCCRecord(t, base, originKey, originKey, 10)
	v, err = ts.GetUncommittedOrAnyHighCommitted(originKey, getTestTimestamp(11))
	assert.NoError(t, err)
	assert.True(t, v.IsEmpty())
}

func TestGetUncommittedOrAnyHighCommittedWithUncommitted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	originKey := []byte("k1")
	ts := s.(storage.TransactionalDataStorage)
	addTestUncommittedMVCCRecord(t, base, originKey, originKey, 11)
	addTestCommittedMVCCRecord(t, base, originKey, originKey, 10)

	v, err := ts.GetUncommittedOrAnyHighCommitted(originKey, getTestTimestamp(8))
	assert.NoError(t, err)
	assert.True(t, !v.IsEmpty())
	assert.False(t, v.WithUncommitted.IsEmpty())
	assert.Equal(t, getTestTimestamp(11), v.WithUncommitted.Timestamp)
	assert.True(t, v.WithCommitted.IsEmpty())
}

func TestGetUncommittedOrAnyHighCommittedWithCommitted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base), WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	originKey := []byte("k1")
	ts := s.(storage.TransactionalDataStorage)
	addTestCommittedMVCCRecord(t, base, originKey, originKey, 10)

	v, err := ts.GetUncommittedOrAnyHighCommitted(originKey, getTestTimestamp(8))
	assert.NoError(t, err)
	assert.True(t, !v.IsEmpty())
	assert.True(t, v.WithUncommitted.IsEmpty())
	assert.Equal(t, getTestTimestamp(10), v.WithCommitted)
	assert.False(t, v.WithCommitted.IsEmpty())
}

func TestGetUncommittedOrAnyHighCommittedByRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)

	k1 := []byte("k1")
	k2 := []byte("k2")
	k3 := []byte("k3")
	k4 := []byte("k4")

	resolver := newMockTxnOperationResolver(nil)
	s := NewKVDataStorage(base, executor.NewKVExecutor(base),
		WithFeature(storage.Feature{SupportTransaction: true}),
		WithTxnOperationKeysResolver(resolver))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	addTestTxnRecord(t, base, k1, k1)
	addTestUncommittedMVCCRecord(t, base, k2, k2, 10)
	addTestCommittedMVCCRecord(t, base, k2, k2, 11)
	addTestCommittedMVCCRecord(t, base, k3, k3, 9)
	addTestCommittedMVCCRecord(t, base, k3, k3, 10)
	addTestCommittedMVCCRecord(t, base, k4, k4, 9)
	addTestCommittedMVCCRecord(t, base, k4, k4, 10)

	ts := s.(storage.TransactionalDataStorage)

	resolver.keys = [][]byte{k1}
	conflicts, err := ts.GetUncommittedOrAnyHighCommittedByRange(txnpb.TxnOperation{}, getTestTimestamp(11))
	assert.NoError(t, err)
	assert.Equal(t, 0, len(conflicts))

	resolver.keys = [][]byte{k2}
	conflicts, err = ts.GetUncommittedOrAnyHighCommittedByRange(txnpb.TxnOperation{}, getTestTimestamp(11))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(conflicts))
	assert.True(t, conflicts[0].ConflictWithUncommitted())

	resolver.keys = [][]byte{k3}
	conflicts, err = ts.GetUncommittedOrAnyHighCommittedByRange(txnpb.TxnOperation{}, getTestTimestamp(8))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(conflicts))
	assert.True(t, conflicts[0].ConflictWithCommitted())
	resolver.keys = [][]byte{k3}
	conflicts, err = ts.GetUncommittedOrAnyHighCommittedByRange(txnpb.TxnOperation{}, getTestTimestamp(11))
	assert.NoError(t, err)
	assert.Equal(t, 0, len(conflicts))

	resolver.keys = [][]byte{k1, k2}
	conflicts, err = ts.GetUncommittedOrAnyHighCommittedByRange(txnpb.TxnOperation{}, getTestTimestamp(11))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(conflicts))
	assert.True(t, conflicts[0].ConflictWithUncommitted())

	resolver.keys = [][]byte{k1, k2, k3}
	conflicts, err = ts.GetUncommittedOrAnyHighCommittedByRange(txnpb.TxnOperation{}, getTestTimestamp(8))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(conflicts))
	assert.True(t, conflicts[0].ConflictWithUncommitted())
	assert.True(t, conflicts[1].ConflictWithCommitted())

	resolver.keys = [][]byte{k1, k2, k4}
	conflicts, err = ts.GetUncommittedOrAnyHighCommittedByRange(txnpb.TxnOperation{}, getTestTimestamp(8))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(conflicts))
	assert.True(t, conflicts[0].ConflictWithUncommitted())
	assert.True(t, conflicts[1].ConflictWithCommitted())
}

func addTestUncommittedMVCCRecord(t *testing.T, base storage.KVBaseStorage, key, value []byte, timestamp int64) {
	ts := hlcpb.Timestamp{PhysicalTime: timestamp}
	assert.NoError(t, base.Set(keysutil.EncodeDataKey(key, nil), protoc.MustMarshal(&txnpb.TxnUncommittedMVCCMetadata{
		Timestamp: ts,
	}), false))
	assert.NoError(t, base.Set(keysutil.EncodeTxnMVCCKey(key, ts, buf.NewByteBuf(32)), value, false))
}

func addTestCommittedMVCCRecord(t *testing.T, base storage.KVBaseStorage, key, value []byte, timestamp int64) {
	assert.NoError(t, base.Set(keysutil.EncodeTxnMVCCKey(key, hlcpb.Timestamp{PhysicalTime: timestamp}, buf.NewByteBuf(32)), value, false))
}

func addTestTxnRecord(t *testing.T, base storage.KVBaseStorage, txnRecordRouteKey, txnID []byte) {
	assert.NoError(t, base.Set(keysutil.EncodeTxnRecordKey(txnRecordRouteKey, txnID, buf.NewByteBuf(32)), protoc.MustMarshal(&txnpb.TxnRecord{
		TxnMeta: txnpb.TxnMeta{ID: txnID, TxnRecordRouteKey: txnRecordRouteKey},
	}), false))
}

func checkTxnKeysCount(t *testing.T, expect int, base storage.KVBaseStorage) {
	n := 0
	assert.NoError(t, base.Scan(keysutil.EncodeShardStart(nil, nil), keysutil.EncodeShardEnd(nil, nil), func(key, value []byte) (bool, error) {
		n++
		return true, nil
	}, true))
	assert.Equal(t, expect, n)
}

func getTestTimestamp(v int64) hlcpb.Timestamp {
	return hlcpb.Timestamp{PhysicalTime: v}
}

type mockTxnOperationResolver struct {
	keys [][]byte
}

func newMockTxnOperationResolver(keys [][]byte) *mockTxnOperationResolver {
	return &mockTxnOperationResolver{
		keys: keys,
	}
}

func (m *mockTxnOperationResolver) Resolve(txnpb.TxnOperation) [][]byte {
	return m.keys
}
