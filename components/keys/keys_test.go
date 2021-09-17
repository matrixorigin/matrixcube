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
		{GetDataStorageAppliedIndexKey(1), false},
		{GetDataStorageMetadataKey(1, 1), false},
	}

	for idx, tt := range tests {
		if v := IsRaftLogKey(tt.key); v != tt.result {
			t.Errorf("%d, got %t, want %t", idx, v, tt.result)
		}
	}
}

func TestDecodeDataStorageAppliedIndexKey(t *testing.T) {
	tests := []struct {
		id     uint64
		result uint64
	}{
		{DecodeDataStorageAppliedIndexKey(GetDataStorageAppliedIndexKey(0)), 0},
		{DecodeDataStorageAppliedIndexKey(GetDataStorageAppliedIndexKey(1)), 1},
		{DecodeDataStorageAppliedIndexKey(GetDataStorageAppliedIndexKey(math.MaxUint64)), math.MaxUint64},
	}

	for idx, ct := range tests {
		assert.Equal(t, ct.id, ct.result, "index %d", idx)
	}
}
