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
// limitations under the License.

package keys

import (
	"encoding/binary"
	"fmt"

	"github.com/fagongzi/goetty/buf"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
)

// suffix for local metadata
const (
	stateSuffix = 0x01

	// Following are the suffix after the local prefix.
	// For shard id
	raftLogSuffix                 = 0x01
	maxIndexSuffix                = 0x04
	hardStateSuffix               = 0x06
	dataStorageAppliedIndexSuffix = 0x07
	dataStorageMetadataSuffix     = 0x08
)

// local is in (0x01, 0x02);
var (
	localPrefix byte = 0x01
)

// data is in (z, z+1)
var (
	dataPrefix        byte = 'z'
	dataPrefixKey          = []byte{dataPrefix}
	dataPrefixKeySize      = len(dataPrefixKey)

	// DataPrefixSize data prefix size
	DataPrefixSize = dataPrefixKeySize + 8
)

var storeIdentKey = []byte{localPrefix, 0x01}

var (
	// We save two types shard data in DB, for raft and other meta data.
	// When the store starts, we should iterate all shard meta data to
	// construct peer, no need to travel large raft data, so we separate them
	// with different prefixes.
	raftPrefix            byte = 0x02
	raftPrefixKey              = []byte{localPrefix, raftPrefix}
	baseRaftSuffixKeySize      = len(raftPrefixKey) + 9
)

func IsStateSuffix(suffix byte) bool {
	return suffix == stateSuffix
}

// GetStoreIdentKey return key of StoreIdent
func GetStoreIdentKey() []byte {
	return storeIdentKey
}

// GetHardStateKey returns key that used to store `raftpb.HardState`
func GetHardStateKey(shardID uint64, peerID uint64) []byte {
	return getIDKey(shardID, hardStateSuffix, 8, peerID)
}

// GetDataStorageAppliedIndexKey returns key that used to store `applied log index` for `storage.DataStorage`
func GetDataStorageAppliedIndexKey(shardID uint64) []byte {
	return getIDKey(shardID, dataStorageAppliedIndexSuffix, 0, 0)
}

// DecodeDataStorageAppliedIndexKey returns shard id
func DecodeDataStorageAppliedIndexKey(key []byte) uint64 {
	return buf.Byte2UInt64(key[raftPrefix:])
}

// GetDataStorageMetadataKey returns key that used to store `shard metadata` for `storage.DataStorage`
func GetDataStorageMetadataKey(shardID uint64) []byte {
	return getIDKey(shardID, dataStorageMetadataSuffix, 0, 0)
}

func isRaftSuffixKey(key []byte, size int, suffix byte) bool {
	if len(key) != size {
		return false
	}
	return key[baseRaftSuffixKeySize-1] == suffix
}

func IsDataStorageMetadataKey(key []byte) bool {
	return isRaftSuffixKey(key, baseRaftSuffixKeySize+8, dataStorageMetadataSuffix)
}

func IsDataStorageAppliedIndexKey(key []byte) bool {
	return isRaftSuffixKey(key, baseRaftSuffixKeySize, dataStorageAppliedIndexSuffix)
}

func IsRaftLogKey(key []byte) bool {
	return isRaftSuffixKey(key, baseRaftSuffixKeySize+8, raftLogSuffix)
}

func GetRaftPrefix(shardID uint64) []byte {
	buf := acquireBuf()
	buf.Write(raftPrefixKey)
	buf.WriteInt64(int64(shardID))
	_, data, _ := buf.ReadBytes(buf.Readable())

	releaseBuf(buf)
	return data
}

// GetMaxIndexKey returns key that used to max applied log index
func GetMaxIndexKey(shardID uint64) []byte {
	return getIDKey(shardID, maxIndexSuffix, 0, 0)
}

// GetRaftLogKey returns key that used to store `raftpb.Entry`
func GetRaftLogKey(shardID uint64, logIndex uint64) []byte {
	return getIDKey(shardID, raftLogSuffix, 8, logIndex)
}

func GetRaftLogIndex(key []byte) (uint64, error) {
	expectKeyLen := len(raftPrefixKey) + 8*2 + 1
	if len(key) != expectKeyLen {
		return 0, fmt.Errorf("key<%v> is not a valid raft log key", key)
	}

	return binary.BigEndian.Uint64(key[len(raftPrefixKey)+9:]), nil
}

func getIDKey(shardID uint64, suffix byte, extraCap int, extra uint64) []byte {
	buf := acquireBuf()
	buf.Write(raftPrefixKey)
	buf.WriteInt64(int64(shardID))
	buf.WriteByte(suffix)
	if extraCap > 0 {
		buf.WriteInt64(int64(extra))
	}
	_, data, _ := buf.ReadBytes(buf.Readable())

	releaseBuf(buf)
	return data
}

func GetDataKeyWithBuf(group uint64, key []byte, buf *buf.ByteBuf) []byte {
	buf.Write(dataPrefixKey)
	buf.WriteUInt64(group)
	if len(key) > 0 {
		buf.Write(key)
	}

	_, data, _ := buf.ReadBytes(buf.Readable())
	return data
}

// WriteGroupPrefix write group prefix
func WriteGroupPrefix(group uint64, key []byte) {
	copy(key, dataPrefixKey)
	buf.Uint64ToBytesTo(group, key[dataPrefixKeySize:])
}

// EncodeDataKey encode data key
func EncodeDataKey(group uint64, key []byte) []byte {
	buf := acquireBuf()
	data := GetDataKeyWithBuf(group, key, buf)
	releaseBuf(buf)
	return data
}

// DecodeDataKey decode data key
func DecodeDataKey(key []byte) []byte {
	return key[DataPrefixSize:]
}

func getDataMaxKey(group uint64) []byte {
	buf := acquireBuf()
	buf.WriteByte(dataPrefix)
	buf.WriteUint64(group + 1)
	_, data, _ := buf.ReadBytes(buf.Readable())
	releaseBuf(buf)
	return data
}

func GetDataEndKey(group uint64, endKey []byte) []byte {
	if len(endKey) == 0 {
		return getDataMaxKey(group)
	}
	return EncodeDataKey(group, endKey)
}

func EncStartKey(shard *bhmetapb.Shard) []byte {
	// only initialized shard's startKey can be encoded, otherwise there must be bugs
	// somewhere.
	if len(shard.Peers) == 0 {
		panic("bug: shard peers len is empty")
	}

	return EncodeDataKey(shard.Group, shard.Start)
}

func EncEndKey(shard *bhmetapb.Shard) []byte {
	// only initialized shard's end_key can be encoded, otherwise there must be bugs
	// somewhere.
	if len(shard.Peers) == 0 {
		panic("bug: shard peers len is empty")
	}
	return GetDataEndKey(shard.Group, shard.End)
}
