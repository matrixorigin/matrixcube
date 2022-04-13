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

package txn

import (
	"testing"

	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/txn/util"
	keysutil "github.com/matrixorigin/matrixcube/util/keys"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/testutil"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

func TestReadWithCommittedBecauseNoUncommitted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	k1 := []byte("k1")
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 10)

	reader := NewTransactionDataReader(ts)
	v, err := reader.Read(k1, testutil.GetTestTxnOpMeta(0, 0, 11), nil)
	assert.NoError(t, err)
	assert.Equal(t, "k1-10(c)", string(v))

	v, err = reader.Read(k1, testutil.GetTestTxnOpMeta(0, 0, 10), nil)
	assert.NoError(t, err)
	assert.Empty(t, v)
}

func TestReadWithCommittedBecauseNoUncommittedByKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	k1 := []byte("k1")
	k2 := []byte("k2")
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 10)
	testutil.AddTestCommittedMVCCRecord(t, base, k2, 10)

	uncommittedTree := util.NewUncommittedTree()
	uncommittedTree.Add(k2, testutil.GetUncommitted(0, 0, 11))

	reader := NewTransactionDataReader(ts)
	v, err := reader.Read(k1, testutil.GetTestTxnOpMeta(0, 0, 11), uncommittedTree)
	assert.NoError(t, err)
	assert.Equal(t, "k1-10(c)", string(v))
}

func TestReadWithCanReadUncommitted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	k1 := []byte("k1")
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 10)
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 11)

	uncommittedTree := util.NewUncommittedTree()
	uncommittedTree.Add(k1, testutil.GetUncommitted(0, 0, 11))

	reader := NewTransactionDataReader(ts)
	v, err := reader.Read(k1, testutil.GetTestTxnOpMeta(0, 0, 11), uncommittedTree)
	assert.NoError(t, err)
	assert.Equal(t, "k1-11(c)", string(v))
}

func TestReadWithCommittedBecaseLowerEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	k1 := []byte("k1")
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 10)
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 11)

	uncommittedTree := util.NewUncommittedTree()
	uncommittedTree.Add(k1, testutil.GetUncommitted(0, 1, 11))

	reader := NewTransactionDataReader(ts)
	v, err := reader.Read(k1, testutil.GetTestTxnOpMeta(0, 0, 11), uncommittedTree)
	assert.NoError(t, err)
	assert.Equal(t, "k1-10(c)", string(v))
}

func TestReadWithCommittedBecaseHigherEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	k1 := []byte("k1")
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 10)
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 11)

	uncommittedTree := util.NewUncommittedTree()
	uncommittedTree.Add(k1, testutil.GetUncommitted(0, 1, 11))

	reader := NewTransactionDataReader(ts)
	v, err := reader.Read(k1, testutil.GetTestTxnOpMeta(0, 2, 11), uncommittedTree)
	assert.NoError(t, err)
	assert.Equal(t, "k1-10(c)", string(v))
}

func TestReadWithCommittedBecaseLowerSeq(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	k1 := []byte("k1")
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 10)
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 11)

	uncommittedTree := util.NewUncommittedTree()
	uncommittedTree.Add(k1, testutil.GetUncommitted(1, 1, 11))

	reader := NewTransactionDataReader(ts)
	v, err := reader.Read(k1, testutil.GetTestTxnOpMeta(0, 1, 11), uncommittedTree)
	assert.NoError(t, err)
	assert.Equal(t, "k1-10(c)", string(v))
}

func TestReadWithUncommittedBecaseHigherSeq(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	k1 := []byte("k1")
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 10)
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 11)

	uncommittedTree := util.NewUncommittedTree()
	uncommittedTree.Add(k1, testutil.GetUncommitted(1, 1, 11))

	reader := NewTransactionDataReader(ts)
	v, err := reader.Read(k1, testutil.GetTestTxnOpMeta(2, 1, 11), uncommittedTree)
	assert.NoError(t, err)
	assert.Equal(t, "k1-11(c)", string(v))
}

func TestReadWithUncommittedBecaseSameSeq(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	k1 := []byte("k1")
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 10)
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 11)

	uncommittedTree := util.NewUncommittedTree()
	uncommittedTree.Add(k1, testutil.GetUncommitted(1, 1, 11))

	reader := NewTransactionDataReader(ts)
	v, err := reader.Read(k1, testutil.GetTestTxnOpMeta(1, 1, 11), uncommittedTree)
	assert.NoError(t, err)
	assert.Equal(t, "k1-11(c)", string(v))
}

func TestReadRangeWithCommittedBecauseNoUncommitted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	k1 := []byte("k1")
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 1)
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 2)
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 3)

	k2 := []byte("k2")
	testutil.AddTestCommittedMVCCRecord(t, base, k2, 1)
	testutil.AddTestCommittedMVCCRecord(t, base, k2, 2)
	testutil.AddTestCommittedMVCCRecord(t, base, k2, 3)

	k3 := []byte("k3")
	testutil.AddTestCommittedMVCCRecord(t, base, k3, 1)
	testutil.AddTestCommittedMVCCRecord(t, base, k3, 2)
	testutil.AddTestCommittedMVCCRecord(t, base, k3, 3)

	reader := NewTransactionDataReader(ts)
	var keys, values [][]byte
	assert.NoError(t, reader.ReadRange(k1, []byte("k4"), testutil.GetTestTxnOpMeta(0, 0, 2), func(originKey, data []byte) (bool, error) {
		keys = append(keys, keysutil.Clone(originKey))
		values = append(values, keysutil.Clone(data))
		return true, nil
	}, nil))
	keysutil.Sort(keys)
	assert.Equal(t, [][]byte{k1, k2, k3}, keys)
	assert.Equal(t, [][]byte{[]byte("k1-1(c)"), []byte("k2-1(c)"), []byte("k3-1(c)")}, values)
}

func TestReadRangeWithUncommitted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	uncommittedTree := util.NewUncommittedTree()

	k1 := []byte("k1")
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 1)
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 2)
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 3)
	testutil.AddTestUncommittedMVCCRecord(t, base, k1, 4)
	uncommittedTree.Add(k1, testutil.GetUncommitted(0, 0, 4))

	k2 := []byte("k2")
	testutil.AddTestCommittedMVCCRecord(t, base, k2, 1)
	testutil.AddTestCommittedMVCCRecord(t, base, k2, 2)
	testutil.AddTestCommittedMVCCRecord(t, base, k2, 3)
	testutil.AddTestUncommittedMVCCRecord(t, base, k2, 4)
	uncommittedTree.Add(k2, testutil.GetUncommitted(0, 0, 4))

	k3 := []byte("k3")
	testutil.AddTestCommittedMVCCRecord(t, base, k3, 1)
	testutil.AddTestCommittedMVCCRecord(t, base, k3, 2)
	testutil.AddTestCommittedMVCCRecord(t, base, k3, 3)
	testutil.AddTestUncommittedMVCCRecord(t, base, k3, 4)
	uncommittedTree.Add(k3, testutil.GetUncommitted(0, 0, 4))

	reader := NewTransactionDataReader(ts)
	var keys, values [][]byte
	assert.NoError(t, reader.ReadRange(k1, []byte("k4"), testutil.GetTestTxnOpMeta(0, 0, 4), func(originKey, data []byte) (bool, error) {
		keys = append(keys, keysutil.Clone(originKey))
		values = append(values, keysutil.Clone(data))
		return true, nil
	}, uncommittedTree))
	keysutil.Sort(keys)
	assert.Equal(t, [][]byte{k1, k2, k3}, keys)
	assert.Equal(t, [][]byte{[]byte("k1-4(u)"), []byte("k2-4(u)"), []byte("k3-4(u)")}, values)
}

func TestReadRangeWithCommittedAndUncommitted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts, base, closer := getTestTransactionDataStorage(t)
	defer closer()

	uncommittedTree := util.NewUncommittedTree()

	k1 := []byte("k1")
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 1)
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 2)
	testutil.AddTestCommittedMVCCRecord(t, base, k1, 3)
	testutil.AddTestUncommittedMVCCRecord(t, base, k1, 4)
	uncommittedTree.Add(k1, testutil.GetUncommitted(0, 0, 4))

	k2 := []byte("k2")
	testutil.AddTestCommittedMVCCRecord(t, base, k2, 1)
	testutil.AddTestCommittedMVCCRecord(t, base, k2, 2)
	testutil.AddTestCommittedMVCCRecord(t, base, k2, 3)

	k3 := []byte("k3")
	testutil.AddTestCommittedMVCCRecord(t, base, k3, 4)

	reader := NewTransactionDataReader(ts)
	var keys, values [][]byte
	assert.NoError(t, reader.ReadRange(k1, []byte("k4"), testutil.GetTestTxnOpMeta(0, 0, 4), func(originKey, data []byte) (bool, error) {
		keys = append(keys, keysutil.Clone(originKey))
		values = append(values, keysutil.Clone(data))
		return true, nil
	}, uncommittedTree))
	assert.Equal(t, [][]byte{k1, k2}, keys)
	assert.Equal(t, [][]byte{[]byte("k1-4(u)"), []byte("k2-3(c)")}, values)
}

func getTestTransactionDataStorage(t *testing.T) (storage.TransactionalDataStorage, storage.KVBaseStorage, func()) {
	fs := vfs.GetTestFS()
	kvStore := mem.NewStorage()
	base := kv.NewBaseStorage(kvStore, fs)
	s := kv.NewKVDataStorage(base, executor.NewKVExecutor(base),
		kv.WithFeature(storage.Feature{SupportTransaction: true}))
	return s.(storage.TransactionalDataStorage), base, func() {
		assert.NoError(t, s.Close())
		vfs.ReportLeakedFD(fs, t)
	}
}
