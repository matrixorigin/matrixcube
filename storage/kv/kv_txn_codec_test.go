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
	"time"

	"github.com/matrixorigin/matrixcube/pb/hlcpb"
	"github.com/matrixorigin/matrixcube/util/buf"
	"github.com/matrixorigin/matrixcube/util/uuid"
	"github.com/stretchr/testify/assert"
)

func TestEncodeTxnRecordKey(t *testing.T) {
	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	originKey := []byte("key")
	txnID := uuid.NewV4().Bytes()
	k, kt, v := decodeTxnKey(encodeTxnRecordKey(originKey, txnID, buffer))
	assert.Equal(t, originKey, k)
	assert.Equal(t, txnRecordKeyType, kt)
	assert.Equal(t, txnID, v)
}

func TestEncodeTxnMVCCKey(t *testing.T) {
	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	originKey := []byte("key")
	ts := hlcpb.Timestamp{PhysicalTime: time.Now().Unix(), LogicalTime: 100}
	k, kt, v := decodeTxnKey(encodeTxnMVCCKey(originKey, ts, buffer))
	assert.Equal(t, originKey, k)
	assert.Equal(t, txnMVCCKeyType, kt)
	assert.Equal(t, 12, len(v))
	assert.Equal(t, buf.Byte2Int64(v[:8]), ts.PhysicalTime)
	assert.Equal(t, buf.Byte2UInt32(v[8:]), ts.LogicalTime)
}

func TestDecodeKeyWithOriginKey(t *testing.T) {
	k, kt, v := decodeTxnKey([]byte("\x00key"))
	assert.Equal(t, []byte("key"), k)
	assert.Equal(t, txnOriginKeyType, kt)
	assert.Empty(t, v)
}

func TestDecodeTimestamp(t *testing.T) {
	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	ts := hlcpb.Timestamp{PhysicalTime: time.Now().Unix(), LogicalTime: 100}
	_, _, v := decodeTxnKey(encodeTxnMVCCKey([]byte("key"), ts, buffer))
	assert.True(t, ts.Equal(decodeTimestamp(v)))
}

func TestTxnRecordKeyLen(t *testing.T) {
	assert.Equal(t, 1+3+1+2+1, txnRecordKeyLen([]byte("key"), []byte("id")))
}

func TestTxnMVCCKeyLen(t *testing.T) {
	assert.Equal(t, 1+3+1+12+1, txnMVCCKeyLen([]byte("key")))
}
