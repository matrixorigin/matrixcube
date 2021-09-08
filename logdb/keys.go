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

package logdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/fagongzi/goetty/buf"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
)

// suffix for local metadata
const (
	stateSuffix = 0x01

	// Following are the suffix after the local prefix.
	// For shard id
	raftLogSuffix       = 0x01
	raftStateSuffix     = 0x02
	applyStateSuffix    = 0x03
	maxIndexSuffix      = 0x04
	bootstrapInfoSuffix = 0x05
	hardStateSuffix     = 0x06
)

// local is in (0x01, 0x02);
var (
	localPrefix byte = 0x01

	maxKey = []byte{}
	minKey = []byte{0xff}
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
	raftPrefix    byte = 0x02
	raftPrefixKey      = []byte{localPrefix, raftPrefix}
	metaPrefix    byte = 0x03
	metaPrefixKey      = []byte{localPrefix, metaPrefix}
	metaMinKey         = []byte{localPrefix, metaPrefix}
	metaMaxKey         = []byte{localPrefix, metaPrefix + 1}
)

var (
	bufPool sync.Pool
)

func acquireBuf() *buf.ByteBuf {
	value := bufPool.Get()
	if value == nil {
		return buf.NewByteBuf(64)
	}

	buf := value.(*buf.ByteBuf)
	buf.Resume(64)

	return buf
}

func releaseBuf(value *buf.ByteBuf) {
	value.Clear()
	value.Release()
	bufPool.Put(value)
}

// GetStoreIdentKey return key of StoreIdent
func GetStoreIdentKey() []byte {
	return storeIdentKey
}

// GetMaxKey return max key
func GetMaxKey() []byte {
	return maxKey
}

// GetMinKey return min key
func GetMinKey() []byte {
	return minKey
}

func decodeMetaKey(key []byte) (uint64, byte, error) {
	prefixLen := len(metaPrefixKey)
	keyLen := len(key)

	if prefixLen+9 != len(key) {
		return 0, 0, fmt.Errorf("invalid shard meta key length for key %v", key)
	}

	if !bytes.HasPrefix(key, metaPrefixKey) {
		return 0, 0, fmt.Errorf("invalid shard meta prefix for key %v", key)
	}

	return binary.BigEndian.Uint64(key[prefixLen:keyLen]), key[keyLen-1], nil
}

func getMetaKey(shardID uint64, suffix byte) []byte {
	buf := acquireBuf()
	buf.Write(metaPrefixKey)
	buf.WriteInt64(int64(shardID))
	buf.WriteByte(suffix)
	_, data, _ := buf.ReadBytes(buf.Readable())

	releaseBuf(buf)
	return data
}

func getMetaPrefix(shardID uint64) []byte {
	buf := acquireBuf()
	buf.Write(metaPrefixKey)
	buf.WriteInt64(int64(shardID))
	_, data, _ := buf.ReadBytes(buf.Readable())

	releaseBuf(buf)
	return data
}

func getBootstrapInfoKey(shardID uint64, peerID uint64) []byte {
	return getIDKey(shardID, bootstrapInfoSuffix, 8, peerID)
}

func getShardLocalStateKey(shardID uint64) []byte {
	return getMetaKey(shardID, stateSuffix)
}

func getHardStateKey(shardID uint64, peerID uint64) []byte {
	return getIDKey(shardID, hardStateSuffix, 8, peerID)
}

func getRaftLocalStateKey(shardID uint64) []byte {
	return getIDKey(shardID, raftStateSuffix, 0, 0)
}

func getRaftApplyStateKey(shardID uint64) []byte {
	return getIDKey(shardID, applyStateSuffix, 0, 0)
}

func getMaxIndexKey(shardID uint64) []byte {
	return getIDKey(shardID, maxIndexSuffix, 0, 0)
}

func getRaftPrefix(shardID uint64) []byte {
	buf := acquireBuf()
	buf.Write(raftPrefixKey)
	buf.WriteInt64(int64(shardID))
	_, data, _ := buf.ReadBytes(buf.Readable())

	releaseBuf(buf)
	return data
}

func getRaftLogKey(shardID uint64, logIndex uint64) []byte {
	return getIDKey(shardID, raftLogSuffix, 8, logIndex)
}

func isRaftLogKey(key []byte) bool {
	if len(key) != len(raftPrefixKey)+8*2+1 {
		return false
	}
	return key[len(raftPrefixKey)+8] == raftLogSuffix
}

func getRaftLogIndex(key []byte) (uint64, error) {
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

func getDataKey0(group uint64, key []byte, buf *buf.ByteBuf) []byte {
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
	data := getDataKey0(group, key, buf)
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

func getDataEndKey(group uint64, endKey []byte) []byte {
	if len(endKey) == 0 {
		return getDataMaxKey(group)
	}
	return EncodeDataKey(group, endKey)
}

func encStartKey(shard *bhmetapb.Shard) []byte {
	// only initialized shard's startKey can be encoded, otherwise there must be bugs
	// somewhere.
	if len(shard.Peers) == 0 {
		panic("bug: shard peers len is empty")
	}

	return EncodeDataKey(shard.Group, shard.Start)
}

func encEndKey(shard *bhmetapb.Shard) []byte {
	// only initialized shard's end_key can be encoded, otherwise there must be bugs
	// somewhere.
	if len(shard.Peers) == 0 {
		panic("bug: shard peers len is empty")
	}
	return getDataEndKey(shard.Group, shard.End)
}
