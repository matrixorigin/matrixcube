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
		{GetRaftLogKey(1, 0), true},
		{GetRaftLogKey(1, 1), true},
		{GetRaftLogKey(1, math.MaxUint64), true},
		{GetHardStateKey(1, 1), false},
		{GetMaxIndexKey(1), false},
		{GetAppliedIndexKey(1), false},
		{GetMetadataKey(1, 1), false},
	}

	for idx, tt := range tests {
		if v := IsRaftLogKey(tt.key); v != tt.result {
			t.Errorf("%d, got %t, want %t", idx, v, tt.result)
		}
	}
}

func TestDecodeAppliedIndexKey(t *testing.T) {
	tests := []struct {
		id     uint64
		result uint64
	}{
		{DecodeAppliedIndexKey(GetAppliedIndexKey(0)), 0},
		{DecodeAppliedIndexKey(GetAppliedIndexKey(1)), 1},
		{DecodeAppliedIndexKey(GetAppliedIndexKey(math.MaxUint64)), math.MaxUint64},
	}

	for idx, ct := range tests {
		assert.Equal(t, ct.id, ct.result, "index %d", idx)
	}
}

func TestGetMetadataIndex(t *testing.T) {
	tests := []struct {
		key   []byte
		index uint64
	}{
		{GetMetadataKey(100, 1), 1},
		{GetMetadataKey(100, 11), 11},
		{GetMetadataKey(100, math.MaxUint64), math.MaxUint64},
	}

	for _, tt := range tests {
		v, err := GetMetadataIndex(tt.key)
		assert.NoError(t, err)
		assert.Equal(t, tt.index, v)
	}
}
