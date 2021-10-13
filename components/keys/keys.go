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

	"github.com/matrixorigin/matrixcube/pb/meta"
)

const (
	raftLogSuffix      = 0x01
	maxIndexSuffix     = 0x04
	hardStateSuffix    = 0x06
	appliedIndexSuffix = 0x07
	metadataSuffix     = 0x08
)

// data is in (z, z+1)
var (
	dataPrefix        byte = 'z'
	dataPrefixKey          = []byte{dataPrefix}
	dataPrefixKeySize      = len(dataPrefixKey)

	// DataPrefixSize data prefix size
	DataPrefixSize = dataPrefixKeySize + 8
)

var (
	localPrefix   byte = 0x01
	storeIdentKey      = []byte{localPrefix, 0x01}
	// We save two types shard data in the KVStore, they are raft and other meta
	// data. When the store starts, we should iterate all shard meta data to
	// launch replicas, to avoid iterating large volume of data, we separate them
	// with different prefixes.
	raftPrefix            byte = 0x02
	raftPrefixKey              = []byte{localPrefix, raftPrefix}
	baseRaftSuffixKeySize      = len(raftPrefixKey) + 9
)

var (
	// we use this fixed key to write a dummy record into the KVStore with sync=true
	// to force a sync of the WAL of the KVStore.
	ForcedSyncKey = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
)

const (
	idKeyLength        = 11
	indexedIDKeyLength = 19
)

// GetStoreIdentKey return key of StoreIdent
func GetStoreIdentKey() []byte {
	return storeIdentKey
}

// GetHardStateKey returns key that used to store `raftpb.HardState`
func GetHardStateKey(shardID uint64, replicaID uint64, key []byte) []byte {
	key = getKeySlice(key, indexedIDKeyLength)
	return getIndexedIDKey(hardStateSuffix, shardID, replicaID, key)
}

// GetAppliedIndexKey returns key that used to store `applied log index` for `storage.DataStorage`
func GetAppliedIndexKey(shardID uint64, key []byte) []byte {
	key = getKeySlice(key, idKeyLength)
	return getIDKey(appliedIndexSuffix, shardID, key)
}

// GetShardIDFromAppliedIndexKey returns shard id
func GetShardIDFromAppliedIndexKey(key []byte) (uint64, error) {
	if !IsAppliedIndexKey(key) {
		return 0, fmt.Errorf("key<%v> is not a valid applied index key", key)
	}
	return parseUint64(key[len(raftPrefixKey):]), nil
}

// GetMetadataKey returns key that used to store `shard metadata` for `storage.DataStorage`
func GetMetadataKey(shardID uint64, index uint64, key []byte) []byte {
	key = getKeySlice(key, indexedIDKeyLength)
	return getIndexedIDKey(metadataSuffix, shardID, index, key)
}

func GetMetadataIndex(key []byte) (uint64, error) {
	if !IsMetadataKey(key) {
		return 0, fmt.Errorf("key<%v> is not a valid metadata key", key)
	}
	return parseUint64(key[idKeyLength:]), nil
}

func IsMetadataKey(key []byte) bool {
	return isRaftSuffixKey(key, metadataSuffix) && len(key) == indexedIDKeyLength
}

func IsAppliedIndexKey(key []byte) bool {
	return isRaftSuffixKey(key, appliedIndexSuffix) && len(key) == idKeyLength
}

func IsRaftLogKey(key []byte) bool {
	return isRaftSuffixKey(key, raftLogSuffix) && len(key) == indexedIDKeyLength
}

func GetRaftPrefix(shardID uint64) []byte {
	key := make([]byte, 10)
	key[0] = raftPrefixKey[0]
	key[1] = raftPrefixKey[1]
	writeUint64(shardID, key[2:])
	return key
}

// GetMaxIndexKey returns key that used to max applied log index
func GetMaxIndexKey(shardID uint64, key []byte) []byte {
	key = getKeySlice(key, idKeyLength)
	return getIDKey(maxIndexSuffix, shardID, key)
}

// GetRaftLogKey returns key that used to store `raftpb.Entry`
func GetRaftLogKey(shardID uint64, index uint64, key []byte) []byte {
	key = getKeySlice(key, indexedIDKeyLength)
	return getIndexedIDKey(raftLogSuffix, shardID, index, key)
}

func GetRaftLogIndex(key []byte) (uint64, error) {
	if !IsRaftLogKey(key) {
		return 0, fmt.Errorf("key<%v> is not a valid raft log key", key)
	}
	return parseUint64(key[idKeyLength:]), nil
}

func getKeySlice(key []byte, length int) []byte {
	if length == 0 {
		panic("invalid key length")
	}
	if len(key) < length {
		key = make([]byte, length)
	}
	return key
}

func getIDKeySlice() []byte {
	return make([]byte, idKeyLength)
}

func getIndexedIDKeySlice() []byte {
	return make([]byte, indexedIDKeyLength)
}

func parseUint64(source []byte) uint64 {
	return binary.BigEndian.Uint64(source)
}

func writeUint64(value uint64, target []byte) {
	binary.BigEndian.PutUint64(target, value)
}

func isRaftSuffixKey(key []byte, suffix byte) bool {
	return isRaftPrefixKey(key) && isMatchedSuffix(key, suffix)
}

func isRaftPrefixKey(key []byte) bool {
	if len(key) < len(raftPrefixKey) {
		return false
	}
	return key[0] == raftPrefixKey[0] && key[1] == raftPrefixKey[1]
}

func isMatchedSuffix(key []byte, suffix byte) bool {
	if len(key) < idKeyLength {
		return false
	}
	return key[idKeyLength-1] == suffix
}

func getIDKey(suffix byte, shardID uint64, key []byte) []byte {
	if len(key) < idKeyLength {
		panic("key slice is too short")
	}
	if len(raftPrefixKey) != 2 {
		panic("unexpected raftPrefixKey length")
	}
	key[0] = raftPrefixKey[0]
	key[1] = raftPrefixKey[1]
	writeUint64(shardID, key[2:])
	key[10] = suffix
	return key[:idKeyLength]
}

func getIndexedIDKey(suffix byte, shardID uint64, index uint64, key []byte) []byte {
	if len(key) < indexedIDKeyLength {
		panic("key slice is too short")
	}
	if len(raftPrefixKey) != 2 {
		panic("unexpected raftPrefixKey length")
	}
	key[0] = raftPrefixKey[0]
	key[1] = raftPrefixKey[1]
	writeUint64(shardID, key[2:])
	key[10] = suffix
	writeUint64(index, key[11:])
	return key[:indexedIDKeyLength]
}

func getDataKey(group uint64, key []byte, output []byte) []byte {
	output = getKeySlice(output, len(dataPrefixKey)+8+len(key))
	if len(dataPrefixKey) != 1 {
		panic("unexpected dataPrefixKey length")
	}
	output[0] = dataPrefixKey[0]
	writeUint64(group, output[1:])
	if len(key) > 0 {
		copy(output[9:], key)
	}
	return output[:9+len(key)]
}

func EncodeDataKey(group uint64, dataKey []byte, output []byte) []byte {
	return getDataKey(group, dataKey, output)
}

func DecodeDataKey(key []byte) []byte {
	return key[DataPrefixSize:]
}

func getDataMaxKey(group uint64, key []byte) []byte {
	key = getKeySlice(key, 9)
	key[0] = dataPrefix
	writeUint64(group+1, key[1:])
	return key[:9]
}

func GetDataEndKey(group uint64, endKey []byte, output []byte) []byte {
	if len(endKey) == 0 {
		output = getKeySlice(output, 9)
		return getDataMaxKey(group, output)
	}
	output = getKeySlice(output, 9+len(endKey))
	return EncodeDataKey(group, endKey, output)
}

func EncodeStartKey(shard meta.Shard, key []byte) []byte {
	// only initialized shard's startKey can be encoded
	if len(shard.Replicas) == 0 {
		panic("shard replicas len is empty")
	}
	key = getKeySlice(key, 9+len(shard.Start))
	return EncodeDataKey(shard.Group, shard.Start, key)
}

func EncodeEndKey(shard meta.Shard, key []byte) []byte {
	// only initialized shard's endKey can be encoded
	if len(shard.Replicas) == 0 {
		panic("shard peers len is empty")
	}
	key = getKeySlice(key, 9+len(shard.End))
	return GetDataEndKey(shard.Group, shard.End, key)
}
