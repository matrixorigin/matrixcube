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
	"github.com/matrixorigin/matrixcube/pb/hlcpb"
	"github.com/matrixorigin/matrixcube/util/buf"
)

type keyType byte

var (
	// key + 0 + timestamp + len(timestamp)
	txnMVCCKeyType = keyType(0)
	// key + 1 + txnid + len(txnid)
	txnRecordKeyType = keyType(1)
	// txnNextScanKeyType next scanKey
	txnNextScanKeyType = keyType(5)
	// txnOriginKeyType origin key type
	txnOriginKeyType = keyType(255)
)

// encodeTxnRecordKey encode TxnRecordKey = dataPrefix + txnRecordRouteKey + txnRecordKeyType + txnID + len(txnID)
func encodeTxnRecordKey(txnRecordRouteKey []byte, txnID []byte, buffer *buf.ByteBuf) []byte {
	buffer.MarkWrite()
	mustWriteByte(buffer, dataPrefix)
	mustWrite(buffer, txnRecordRouteKey)
	mustWriteByte(buffer, byte(txnRecordKeyType))
	mustWrite(buffer, txnID)
	mustWriteByte(buffer, byte(len(txnID)))
	return buffer.WrittenDataAfterMark().Data()
}

// txnNextScanKey next scan key
func txnNextScanKey(originKey []byte, buffer *buf.ByteBuf) []byte {
	buffer.MarkWrite()
	mustWriteByte(buffer, dataPrefix)
	mustWrite(buffer, originKey)
	mustWriteByte(buffer, byte(txnNextScanKeyType))
	return buffer.WrittenDataAfterMark().Data()
}

// encodeTxnMVCCKey encode TxnMVCCKey = originKey + txnMVCCKeyType + timestamp + len(timestamp)
func encodeTxnMVCCKey(originKey []byte, timestamp hlcpb.Timestamp, buffer *buf.ByteBuf) []byte {
	buffer.MarkWrite()
	mustWriteByte(buffer, dataPrefix)
	mustWrite(buffer, originKey)
	mustWriteByte(buffer, byte(txnMVCCKeyType))
	mustWriteInt64(buffer, timestamp.PhysicalTime)
	mustWriteUInt32(buffer, timestamp.LogicalTime)
	mustWriteByte(buffer, 12)
	return buffer.WrittenDataAfterMark().Data()
}

// decodeTxnKey decode key, format: originKey + keyType + value + len(value)
func decodeTxnKey(key []byte) ([]byte, keyType, []byte) {
	key = DecodeDataKey(key)
	n := len(key)
	kLen := int(key[len(key)-1])
	kTypePos := n - kLen - 2
	if kTypePos < 0 {
		return key, txnOriginKeyType, nil
	}

	switch keyType(key[kTypePos]) {
	case txnMVCCKeyType:
		return key[:kTypePos], txnMVCCKeyType, key[kTypePos+1 : n-1]
	case txnRecordKeyType:
		return key[:kTypePos], txnRecordKeyType, key[kTypePos+1 : n-1]
	}
	return key, txnOriginKeyType, nil
}

func decodeTimestamp(v []byte) hlcpb.Timestamp {
	return hlcpb.Timestamp{
		PhysicalTime: buf.Byte2Int64(v[:8]),
		LogicalTime:  buf.Byte2UInt32(v[8:]),
	}
}

func txnRecordKeyLen(txnRecordRouteKey []byte, txnID []byte) int {
	return prefixLen + len(txnRecordRouteKey) + 1 + len(txnID) + 1
}

func txnMVCCKeyLen(originKey []byte) int {
	return prefixLen + len(originKey) + 14
}

func mustWriteByte(buffer *buf.ByteBuf, value byte) {
	if err := buffer.WriteByte(value); err != nil {
		panic(err)
	}
}

func mustWriteInt64(buffer *buf.ByteBuf, value int64) {
	if _, err := buffer.WriteInt64(value); err != nil {
		panic(err)
	}
}

func mustWriteUInt32(buffer *buf.ByteBuf, value uint32) {
	if _, err := buffer.WriteUInt32(value); err != nil {
		panic(err)
	}
}

func mustWrite(buffer *buf.ByteBuf, value []byte) {
	if _, err := buffer.Write(value); err != nil {
		panic(err)
	}
}
