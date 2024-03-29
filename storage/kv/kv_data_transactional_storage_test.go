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
	"testing"

	"github.com/matrixorigin/matrixcube/pb/hlcpb"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor"
	keysutil "github.com/matrixorigin/matrixcube/util/keys"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/testutil"
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

	testutil.AddTestUncommittedMVCCRecord(t, base, originKey, 10)
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

	testutil.AddTestUncommittedMVCCRecord(t, base, originKey, 10)
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

	testutil.AddTestUncommittedMVCCRecord(t, base, originKey, 10)
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

	testutil.AddTestUncommittedMVCCRecord(t, base, originKey, 10)
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

	testutil.AddTestUncommittedMVCCRecord(t, base, originKey, 10)

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
		testutil.AddTestCommittedMVCCRecord(t, base, []byte("k1"), i)
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
	testutil.AddTestUncommittedMVCCRecord(t, base, []byte("k1"), 1)
	for i := int64(2); i < 10; i++ {
		testutil.AddTestCommittedMVCCRecord(t, base, []byte("k1"), i)
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
		testutil.AddTestCommittedMVCCRecord(t, base, []byte("k1"), i)
	}
	for i := int64(1); i < 10; i++ {
		testutil.AddTestCommittedMVCCRecord(t, base, []byte("k2"), i)
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
		testutil.AddTestCommittedMVCCRecord(t, base, []byte("k1"), i)
	}
	for i := int64(1); i < 10; i++ {
		testutil.AddTestCommittedMVCCRecord(t, base, []byte("k2"), i)
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
		testutil.AddTestCommittedMVCCRecord(t, base, []byte("k1"), i)
	}

	ok, v, err = ts.GetCommitted([]byte("k1"), getTestTimestamp(10))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, []byte("k1-9(c)"), v)
}

func TestGetCommittedWithNoCommitted(t *testing.T) {
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
	testutil.AddTestCommittedMVCCRecord(t, base, []byte("k1"), 2)

	ok, v, err := ts.GetCommitted([]byte("k2"), getTestTimestamp(3))
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Empty(t, v)
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

	testutil.AddTestUncommittedMVCCRecord(t, base, []byte("k2"), 11)
	v, err = ts.GetUncommittedOrAnyHighCommitted(originKey, getTestTimestamp(8))
	assert.NoError(t, err)
	assert.True(t, v.IsEmpty())

	testutil.AddTestCommittedMVCCRecord(t, base, originKey, 10)
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
	testutil.AddTestUncommittedMVCCRecord(t, base, originKey, 11)
	testutil.AddTestCommittedMVCCRecord(t, base, originKey, 10)

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
	testutil.AddTestCommittedMVCCRecord(t, base, originKey, 10)

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

	s := NewKVDataStorage(base, executor.NewKVExecutor(base),
		WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	testutil.AddTestTxnRecord(t, base, k1, k1)
	testutil.AddTestUncommittedMVCCRecord(t, base, k2, 10)
	testutil.AddTestCommittedMVCCRecord(t, base, k2, 11)
	testutil.AddTestCommittedMVCCRecord(t, base, k3, 9)
	testutil.AddTestCommittedMVCCRecord(t, base, k3, 10)
	testutil.AddTestCommittedMVCCRecord(t, base, k4, 9)
	testutil.AddTestCommittedMVCCRecord(t, base, k4, 10)

	ts := s.(storage.TransactionalDataStorage)

	conflicts, err := ts.GetUncommittedOrAnyHighCommittedByRange(getTestImpactedKeys(k1), getTestTimestamp(11))
	assert.NoError(t, err)
	assert.Equal(t, 0, len(conflicts))

	conflicts, err = ts.GetUncommittedOrAnyHighCommittedByRange(getTestImpactedKeys(k2), getTestTimestamp(11))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(conflicts))
	assert.True(t, conflicts[0].ConflictWithUncommitted())

	conflicts, err = ts.GetUncommittedOrAnyHighCommittedByRange(getTestImpactedKeys(k3), getTestTimestamp(8))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(conflicts))
	assert.True(t, conflicts[0].ConflictWithCommitted())

	conflicts, err = ts.GetUncommittedOrAnyHighCommittedByRange(getTestImpactedKeys(k3), getTestTimestamp(11))
	assert.NoError(t, err)
	assert.Equal(t, 0, len(conflicts))

	conflicts, err = ts.GetUncommittedOrAnyHighCommittedByRange(getTestImpactedKeys(k1, k2), getTestTimestamp(11))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(conflicts))
	assert.True(t, conflicts[0].ConflictWithUncommitted())

	conflicts, err = ts.GetUncommittedOrAnyHighCommittedByRange(getTestImpactedKeys(k1, k2, k3), getTestTimestamp(8))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(conflicts))
	assert.True(t, conflicts[0].ConflictWithUncommitted())
	assert.True(t, conflicts[1].ConflictWithCommitted())

	conflicts, err = ts.GetUncommittedOrAnyHighCommittedByRange(getTestImpactedKeys(k1, k2, k4), getTestTimestamp(8))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(conflicts))
	assert.True(t, conflicts[0].ConflictWithUncommitted())
	assert.True(t, conflicts[1].ConflictWithCommitted())
}

func TestScanTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)

	k1 := []byte("k1")
	k2 := []byte("k2")
	k3 := []byte("k3")
	k4 := []byte("k4")

	s := NewKVDataStorage(base, executor.NewKVExecutor(base),
		WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()
	ts := s.(storage.TransactionalDataStorage)

	// k1 only has txn record
	testutil.AddTestTxnRecord(t, base, k1, k1)

	// k2 has uncommitted and committed record
	testutil.AddTestCommittedMVCCRecord(t, base, k2, 1)
	testutil.AddTestUncommittedMVCCRecord(t, base, k2, 2)

	// k3 only has committed record
	testutil.AddTestCommittedMVCCRecord(t, base, k3, 1)
	testutil.AddTestCommittedMVCCRecord(t, base, k3, 2)

	cases := []struct {
		timestamp              hlcpb.Timestamp
		canReadUncommittedKeys [][]byte
		expectKeys             [][]byte
		expectValues           [][]byte
	}{
		{
			timestamp:              getTestTimestamp(2),
			canReadUncommittedKeys: [][]byte{k1, k2, k3},
			expectKeys:             [][]byte{k2, k3},
			expectValues:           [][]byte{[]byte("k2-2(u)"), []byte("k3-1(c)")},
		},
		{
			timestamp:              getTestTimestamp(2),
			canReadUncommittedKeys: [][]byte{},
			expectKeys:             [][]byte{k2, k3},
			expectValues:           [][]byte{[]byte("k2-1(c)"), []byte("k3-1(c)")},
		},
		{
			timestamp:              getTestTimestamp(1),
			canReadUncommittedKeys: [][]byte{},
			expectKeys:             nil,
			expectValues:           nil,
		},
	}

	for _, c := range cases {
		var keys [][]byte
		var values [][]byte
		assert.NoError(t, ts.Scan(k1, k4, c.timestamp,
			func(key []byte, uncommitted txnpb.TxnUncommittedMVCCMetadata) bool {
				for _, k := range c.canReadUncommittedKeys {
					if bytes.Equal(k, key) {
						return true
					}
				}
				return false
			},
			func(key, value []byte) (bool, error) {
				keys = append(keys, keysutil.Clone(key))
				values = append(values, keysutil.Clone(value))
				return true, nil
			}))
		assert.Equal(t, c.expectKeys, keys)
		assert.Equal(t, c.expectValues, values)
	}
}

func TestGetUncommittedMVCCMetadata(t *testing.T) {
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
	testutil.AddTestUncommittedMVCCRecord(t, base, originKey, 11)

	ok, v, err := ts.GetUncommittedMVCCMetadata(originKey)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, txnpb.TxnConflictData{
		OriginKey: originKey,
		WithUncommitted: txnpb.TxnUncommittedMVCCMetadata{
			Timestamp: hlcpb.Timestamp{PhysicalTime: 11},
		},
	}, v)

	ok, v, err = ts.GetUncommittedMVCCMetadata([]byte("k2"))
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, txnpb.TxnConflictData{}, v)
}

func TestGetUncommittedMVCCMetadataByRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kv := getTestPebbleStorage(t, fs)
	base := NewBaseStorage(kv, fs)

	k1 := []byte("k1")
	k2 := []byte("k2")
	k3 := []byte("k3")
	k4 := []byte("k4")

	s := NewKVDataStorage(base, executor.NewKVExecutor(base),
		WithFeature(storage.Feature{SupportTransaction: true}))
	defer func() {
		require.NoError(t, fs.RemoveAll(testDir))
	}()
	defer s.Close()

	testutil.AddTestTxnRecord(t, base, k1, k1)
	testutil.AddTestUncommittedMVCCRecord(t, base, k2, 12)
	testutil.AddTestCommittedMVCCRecord(t, base, k2, 11)
	testutil.AddTestCommittedMVCCRecord(t, base, k3, 9)
	testutil.AddTestCommittedMVCCRecord(t, base, k3, 10)
	testutil.AddTestUncommittedMVCCRecord(t, base, k3, 11)
	testutil.AddTestCommittedMVCCRecord(t, base, k4, 9)
	testutil.AddTestCommittedMVCCRecord(t, base, k4, 10)

	ts := s.(storage.TransactionalDataStorage)

	conflicts, err := ts.GetUncommittedMVCCMetadataByRange(getTestImpactedKeys(k1))
	assert.NoError(t, err)
	assert.Equal(t, 0, len(conflicts))

	conflicts, err = ts.GetUncommittedMVCCMetadataByRange(getTestImpactedKeys(k2))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(conflicts))
	assert.Equal(t, k2, conflicts[0].OriginKey)

	conflicts, err = ts.GetUncommittedMVCCMetadataByRange(getTestImpactedKeys(k1, k2, k3, k4))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(conflicts))
	assert.True(t, conflicts[0].ConflictWithUncommitted())
	assert.Equal(t, k2, conflicts[0].OriginKey)
	assert.True(t, conflicts[1].ConflictWithUncommitted())
	assert.Equal(t, k3, conflicts[1].OriginKey)

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

func getTestImpactedKeys(keys ...[]byte) txnpb.TxnOperation {
	return txnpb.TxnOperation{Impacted: txnpb.KeySet{PointKeys: keys}}
}
