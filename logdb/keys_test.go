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

package logdb

import (
	"math"
	"testing"
)

func TestIsRaftLogKey(t *testing.T) {
	tests := []struct {
		key    []byte
		result bool
	}{
		{getRaftLogKey(1, 0), true},
		{getRaftLogKey(1, 1), true},
		{getRaftLogKey(1, math.MaxUint64), true},
		{getBootstrapInfoKey(1, 1), false},
		{getShardLocalStateKey(1), false},
		{getHardStateKey(1, 1), false},
		{getMaxIndexKey(1), false},
	}

	for idx, tt := range tests {
		if v := isRaftLogKey(tt.key); v != tt.result {
			t.Errorf("%d, got %t, want %t", idx, v, tt.result)
		}
	}
}
