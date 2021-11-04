// Copyright 2020 MatrixOrigin.
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
// limitations under the License

package kv

import (
	"github.com/matrixorigin/matrixcube/util/buf"
)

var (
	metaPrefix byte = 0x00
	dataPrefix byte = 0x01

	minStartKey = []byte{dataPrefix}
	maxEndKey   = []byte{dataPrefix + 1}
)

// EncodeDataKey encode data key with data key prefix
func EncodeDataKey(keys []byte, buffer *buf.ByteBuf) []byte {
	return doAppendPrefix(keys, dataPrefix, buffer)
}

// EncodeShardStart encode shard start key with data prefix
func EncodeShardStart(value []byte, buffer *buf.ByteBuf) []byte {
	if len(value) == 0 {
		return minStartKey
	}
	return doAppendPrefix(value, dataPrefix, buffer)
}

// EncodeShardEnd encode shard start key with data prefix
func EncodeShardEnd(value []byte, buffer *buf.ByteBuf) []byte {
	if len(value) == 0 {
		return maxEndKey
	}
	return doAppendPrefix(value, dataPrefix, buffer)
}

// EncodeShardMetadataKey encode shard metadata key with metadata prefix
func EncodeShardMetadataKey(key []byte, buffer *buf.ByteBuf) []byte {
	return doAppendPrefix(key, metaPrefix, buffer)
}

func doAppendPrefix(key []byte, prefix byte, buffer *buf.ByteBuf) []byte {
	if buffer == nil {
		v := make([]byte, 1+len(key))
		v[0] = prefix
		copy(v[1:], key)
		return v
	}

	buffer.MarkWrite()
	buffer.WriteByte(prefix)
	buffer.Write(key)
	return buffer.WrittenDataAfterMark().Data()
}
