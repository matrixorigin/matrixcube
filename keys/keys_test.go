// Copyright 2021 MatrixOrigin.
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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRaftLogKey(t *testing.T) {
	tests := []struct {
		key    []byte
		result bool
	}{
		{GetRaftLogKey(1, 0, nil), true},
		{GetRaftLogKey(1, 1, nil), true},
		{GetRaftLogKey(1, math.MaxUint64, nil), true},
		{GetHardStateKey(1, 1, nil), false},
		{GetMaxIndexKey(1, nil), false},
		{GetAppliedIndexKey(1, nil), false},
		{GetMetadataKey(1, 1, nil), false},
	}

	for idx, tt := range tests {
		if v := IsRaftLogKey(tt.key); v != tt.result {
			t.Errorf("%d, got %t, want %t", idx, v, tt.result)
		}
	}
}

func TestGetRaftLogIndex(t *testing.T) {
	tests := []struct {
		key     []byte
		index   uint64
		noError bool
	}{
		{GetRaftLogKey(1, 10, nil), 10, true},
		{GetRaftLogKey(10, 1, nil), 1, true},
		{GetStoreIdentKey(), 0, false},
		{GetHardStateKey(1, 1, nil), 0, false},
		{GetMaxIndexKey(1, nil), 0, false},
		{GetMetadataKey(100, 1, nil), 0, false},
		{GetAppliedIndexKey(11, nil), 0, false},
	}

	for _, tt := range tests {
		result, err := GetRaftLogIndex(tt.key)
		if tt.noError {
			assert.NoError(t, err)
			assert.Equal(t, tt.index, result)
		} else {
			assert.Error(t, err)
		}
	}
}

func TestGetShardIDFromAppliedIndexKey(t *testing.T) {
	tests := []struct {
		key     []byte
		result  uint64
		noError bool
	}{
		{GetAppliedIndexKey(0, nil), 0, true},
		{GetAppliedIndexKey(1, nil), 1, true},
		{GetAppliedIndexKey(math.MaxUint64, nil), math.MaxUint64, true},
		{GetStoreIdentKey(), 0, false},
		{GetHardStateKey(1, 1, nil), 0, false},
		{GetMaxIndexKey(1, nil), 0, false},
		{GetRaftLogKey(1, 1, nil), 0, false},
		{GetMetadataKey(100, 1, nil), 0, false},
	}

	for idx, ct := range tests {
		shardID, err := GetShardIDFromAppliedIndexKey(ct.key)
		if ct.noError {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
		assert.Equal(t, ct.result, shardID, "index %d", idx)
	}
}

func TestGetMetadataShradID(t *testing.T) {
	tests := []struct {
		key     []byte
		shardID uint64
		noError bool
	}{
		{GetMetadataKey(100, 1, nil), 100, true},
		{GetMetadataKey(200, 11, nil), 200, true},
		{GetAppliedIndexKey(0, nil), 0, false},
		{GetStoreIdentKey(), 0, false},
		{GetHardStateKey(1, 1, nil), 0, false},
		{GetMaxIndexKey(1, nil), 0, false},
		{GetRaftLogKey(1, 1, nil), 0, false},
	}

	for _, tt := range tests {
		v, err := GetShardIDFromMetadataKey(tt.key)
		if tt.noError {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
		assert.Equal(t, tt.shardID, v)
	}
}

func TestGetMetadataIndex(t *testing.T) {
	tests := []struct {
		key     []byte
		index   uint64
		noError bool
	}{
		{GetMetadataKey(100, 1, nil), 1, true},
		{GetMetadataKey(100, 11, nil), 11, true},
		{GetMetadataKey(100, math.MaxUint64, nil), math.MaxUint64, true},
		{GetAppliedIndexKey(0, nil), 0, false},
		{GetStoreIdentKey(), 0, false},
		{GetHardStateKey(1, 1, nil), 0, false},
		{GetMaxIndexKey(1, nil), 0, false},
		{GetRaftLogKey(1, 1, nil), 0, false},
	}

	for _, tt := range tests {
		v, err := GetMetadataIndex(tt.key)
		if tt.noError {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
		assert.Equal(t, tt.index, v)
	}
}

func TestGetKeySlice(t *testing.T) {
	tests := []struct {
		key    []byte
		length int
		result int
		panic  bool
	}{
		{nil, 10, 10, false},
		{make([]byte, 10), 15, 15, false},
		{make([]byte, 10), 5, 10, false},
		{make([]byte, 10), 10, 10, false},
		{make([]byte, 10), 0, 0, true},
	}

	for _, tt := range tests {
		shouldPanic := tt.panic
		{
			defer func() {
				if r := recover(); r == nil && shouldPanic {
					t.Errorf("failed to trigger panic")
				}
			}()
			result := getKeySlice(tt.key, tt.length)
			assert.Equal(t, tt.result, len(result))
			assert.Equal(t, tt.result, cap(result))
		}
	}
}

func TestWriteParseUint64(t *testing.T) {
	buf := make([]byte, 8)
	v := uint64(1234567890)
	writeUint64(v, buf)
	assert.Equal(t, v, parseUint64(buf))
}

func TestGetMetadataKey(t *testing.T) {
	keyL := make([]byte, indexedIDKeyLength*2)
	keyI := make([]byte, indexedIDKeyLength)
	key := make([]byte, idKeyLength)
	key1 := GetMetadataKey(10, 10, keyI)
	key2 := GetMetadataKey(10, 10, keyL)
	key3 := GetMetadataKey(10, 10, key)
	key4 := GetMetadataKey(10, 10, nil)
	assert.Equal(t, key1, key2)
	assert.Equal(t, key1, key3)
	assert.Equal(t, key1, key4)
	assert.True(t, IsMetadataKey(key1))
	assert.True(t, IsMetadataKey(key2))
	assert.True(t, IsMetadataKey(key3))
	assert.True(t, IsMetadataKey(key4))
}

func TestGetHardStateKey(t *testing.T) {
	keyL := make([]byte, indexedIDKeyLength*2)
	keyI := make([]byte, indexedIDKeyLength)
	key := make([]byte, idKeyLength)
	key1 := GetHardStateKey(10, 10, keyI)
	key2 := GetHardStateKey(10, 10, keyL)
	key3 := GetHardStateKey(10, 10, key)
	key4 := GetHardStateKey(10, 10, nil)
	assert.Equal(t, key1, key2)
	assert.Equal(t, key1, key3)
	assert.Equal(t, key1, key4)
}

func TestGetAppliedIndexKey(t *testing.T) {
	keyL := make([]byte, indexedIDKeyLength*2)
	keyI := make([]byte, indexedIDKeyLength)
	key := make([]byte, idKeyLength)
	key1 := GetAppliedIndexKey(10, keyI)
	key2 := GetAppliedIndexKey(10, keyL)
	key3 := GetAppliedIndexKey(10, key)
	key4 := GetAppliedIndexKey(10, nil)
	assert.Equal(t, key1, key2)
	assert.Equal(t, key1, key3)
	assert.Equal(t, key1, key4)
	assert.True(t, IsAppliedIndexKey(key1))
	assert.True(t, IsAppliedIndexKey(key2))
	assert.True(t, IsAppliedIndexKey(key3))
	assert.True(t, IsAppliedIndexKey(key4))
}

func TestGetMaxIndexKey(t *testing.T) {
	keyL := make([]byte, indexedIDKeyLength*2)
	keyI := make([]byte, indexedIDKeyLength)
	key := make([]byte, idKeyLength)
	key1 := GetMaxIndexKey(10, keyI)
	key2 := GetMaxIndexKey(10, keyL)
	key3 := GetMaxIndexKey(10, key)
	key4 := GetMaxIndexKey(10, nil)
	assert.Equal(t, key1, key2)
	assert.Equal(t, key1, key3)
	assert.Equal(t, key1, key4)
}

func TestGetRaftLogKey(t *testing.T) {
	keyL := make([]byte, indexedIDKeyLength*2)
	keyI := make([]byte, indexedIDKeyLength)
	key := make([]byte, idKeyLength)
	key1 := GetRaftLogKey(10, 10, keyI)
	key2 := GetRaftLogKey(10, 10, keyL)
	key3 := GetRaftLogKey(10, 10, key)
	key4 := GetRaftLogKey(10, 10, nil)
	assert.Equal(t, key1, key2)
	assert.Equal(t, key1, key3)
	assert.Equal(t, key1, key4)
	assert.True(t, IsRaftLogKey(key1))
	assert.True(t, IsRaftLogKey(key2))
	assert.True(t, IsRaftLogKey(key3))
	assert.True(t, IsRaftLogKey(key4))
}
