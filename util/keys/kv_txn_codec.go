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

package keys

import (
	"github.com/matrixorigin/matrixcube/pb/hlcpb"
	"github.com/matrixorigin/matrixcube/util/buf"
)

// TxnKeyType txn key type
type TxnKeyType byte

var (
	// key + 0 + timestamp + len(timestamp)
	TxnMVCCKeyType = TxnKeyType(0)
	// key + 1 + txnid + len(txnid)
	TxnRecordKeyType = TxnKeyType(1)
	// TxnNextScanKeyType next scanKey
	TxnNextScanKeyType = TxnKeyType(5)
	// TxnOriginKeyType origin key type
	TxnOriginKeyType = TxnKeyType(255)
)

// EncodeTxnRecordKey encode TxnRecordKey = dataPrefix + txnRecordRouteKey + txnRecordKeyType + txnID + len(txnID)
func EncodeTxnRecordKey(txnRecordRouteKey []byte, txnID []byte, buffer *buf.ByteBuf) []byte {
	buffer.MarkWrite()
	mustWriteByte(buffer, dataPrefix)
	mustWrite(buffer, txnRecordRouteKey)
	mustWriteByte(buffer, byte(TxnRecordKeyType))
	mustWrite(buffer, txnID)
	mustWriteByte(buffer, byte(len(txnID)))
	return buffer.WrittenDataAfterMark().Data()
}

// TxnNextScanKey next scan key
func TxnNextScanKey(originKey []byte, buffer *buf.ByteBuf) []byte {
	buffer.MarkWrite()
	mustWriteByte(buffer, dataPrefix)
	mustWrite(buffer, originKey)
	mustWriteByte(buffer, byte(TxnNextScanKeyType))
	return buffer.WrittenDataAfterMark().Data()
}

// EncodeTxnMVCCKey encode TxnMVCCKey = originKey + txnMVCCKeyType + timestamp + len(timestamp)
func EncodeTxnMVCCKey(originKey []byte, timestamp hlcpb.Timestamp, buffer *buf.ByteBuf) []byte {
	buffer.MarkWrite()
	mustWriteByte(buffer, dataPrefix)
	mustWrite(buffer, originKey)
	mustWriteByte(buffer, byte(TxnMVCCKeyType))
	mustWriteInt64(buffer, timestamp.PhysicalTime)
	mustWriteUInt32(buffer, timestamp.LogicalTime)
	mustWriteByte(buffer, 12)
	return buffer.WrittenDataAfterMark().Data()
}

// DecodeTxnKey decode key, format: originKey + keyType + value + len(value)
func DecodeTxnKey(key []byte) ([]byte, TxnKeyType, []byte) {
	key = DecodeDataKey(key)
	n := len(key)
	kLen := int(key[len(key)-1])
	kTypePos := n - kLen - 2
	if kTypePos < 0 {
		return key, TxnOriginKeyType, nil
	}

	switch TxnKeyType(key[kTypePos]) {
	case TxnMVCCKeyType:
		return key[:kTypePos], TxnMVCCKeyType, key[kTypePos+1 : n-1]
	case TxnRecordKeyType:
		return key[:kTypePos], TxnRecordKeyType, key[kTypePos+1 : n-1]
	}
	return key, TxnOriginKeyType, nil
}

// DecodeTimestamp decode timestamp
func DecodeTimestamp(v []byte) hlcpb.Timestamp {
	return hlcpb.Timestamp{
		PhysicalTime: buf.Byte2Int64(v[:8]),
		LogicalTime:  buf.Byte2UInt32(v[8:]),
	}
}

// TxnRecordKeyLen txn record key len
func TxnRecordKeyLen(txnRecordRouteKey []byte, txnID []byte) int {
	return prefixLen + len(txnRecordRouteKey) + 1 + len(txnID) + 1
}

// TxnMVCCKeyLen txn mvcc key len
func TxnMVCCKeyLen(originKey []byte) int {
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
