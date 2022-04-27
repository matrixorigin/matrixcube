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

package keys

import (
	"github.com/matrixorigin/matrixcube/util/buf"
)

var (
	prefixLen       = 1
	metaPrefix byte = 0x00
	dataPrefix byte = 0x01

	minStartKey = []byte{dataPrefix}
	maxEndKey   = []byte{dataPrefix + 1}
)

// DataKeyLen return data key length
func DataKeyLen(originKey []byte) int {
	return len(originKey) + prefixLen
}

// EncodeDataKeyTo encode data key with data key prefix
func EncodeDataKeyTo(originKey, dst []byte) []byte {
	n := DataKeyLen(originKey)
	if cap(dst) < n {
		dst = make([]byte, n)
	}

	dst[0] = dataPrefix
	copy(dst[dataPrefix:], originKey)
	return dst
}

// EncodeDataKey encode data key with data key prefix
func EncodeDataKey(originKey []byte, buffer *buf.ByteBuf) []byte {
	return doAppendPrefix(originKey, dataPrefix, buffer)
}

// DecodeDataKey returns the origin data key.
// Note that no data copy is generated here, only a slice of the key is returned
func DecodeDataKey(key []byte) []byte {
	return key[prefixLen:]
}

// EncodeShardStart encode shard start key with data prefix
func EncodeShardStart(value []byte, buffer *buf.ByteBuf) []byte {
	if len(value) == 0 {
		return minStartKey
	}
	return doAppendPrefix(value, dataPrefix, buffer)
}

// EncodeShardStartTo encode shard start key with data prefix
func EncodeShardStartTo(value []byte, dst []byte) []byte {
	n := DataKeyLen(value)
	if cap(dst) < n {
		dst = make([]byte, n)
	}

	if len(value) == 0 {
		copy(dst, minStartKey)
		return dst
	}

	dst[0] = dataPrefix
	copy(dst[dataPrefix:], value)
	return dst
}

// EncodeShardEnd encode shard end key with data prefix
func EncodeShardEnd(value []byte, buffer *buf.ByteBuf) []byte {
	if len(value) == 0 {
		return maxEndKey
	}
	return doAppendPrefix(value, dataPrefix, buffer)
}

// EncodeShardEndTo encode shard end key with data prefix
func EncodeShardEndTo(value []byte, dst []byte) []byte {
	n := DataKeyLen(value)
	if cap(dst) < n {
		dst = make([]byte, n)
	}

	if len(value) == 0 {
		copy(dst, maxEndKey)
		return dst
	}

	dst[0] = dataPrefix
	copy(dst[dataPrefix:], value)
	return dst
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
	if err := buffer.WriteByte(prefix); err != nil {
		panic(err)
	}
	if _, err := buffer.Write(key); err != nil {
		panic(err)
	}
	return buffer.WrittenDataAfterMark().Data()
}
