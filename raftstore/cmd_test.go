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

package raftstore

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/stretchr/testify/assert"
)

func TestEpochMatch(t *testing.T) {
	tests := []struct {
		confVer1 uint64
		version1 uint64
		confVer2 uint64
		version2 uint64
		match    bool
	}{
		{1, 1, 1, 1, true},
		{1, 1, 1, 2, false},
		{1, 1, 2, 1, false},
		{1, 1, 2, 2, false},
	}

	for _, tt := range tests {
		e1 := metapb.ResourceEpoch{
			ConfVer: tt.confVer1,
			Version: tt.version1,
		}
		e2 := metapb.ResourceEpoch{
			ConfVer: tt.confVer2,
			Version: tt.version2,
		}
		assert.Equal(t, tt.match, epochMatch(e1, e2))
	}
}

func TestCanAppendCmd(t *testing.T) {
	tests := []struct {
		ignored1  bool
		confVer1  uint64
		version1  uint64
		ignored2  bool
		confVer2  uint64
		version2  uint64
		canAppend bool
	}{
		{true, 1, 1, true, 1, 1, true},
		{true, 1, 1, true, 2, 2, true},
		{true, 1, 1, false, 1, 1, false},
		{true, 1, 1, false, 2, 2, false},
		{false, 1, 1, false, 1, 1, true},
		{false, 1, 1, false, 1, 2, false},
		{false, 1, 1, false, 2, 1, false},
		{false, 1, 1, false, 2, 2, false},
	}

	for _, tt := range tests {
		cmd := &cmd{
			req: &rpc.RequestBatch{
				Header: &rpc.RequestBatchHeader{
					Epoch: metapb.ResourceEpoch{
						ConfVer: tt.confVer1,
						Version: tt.version1,
					},
					IgnoreEpochCheck: tt.ignored1,
				},
			},
		}
		req := &rpc.Request{
			IgnoreEpochCheck: tt.ignored2,
		}
		epoch := metapb.ResourceEpoch{
			ConfVer: tt.confVer2,
			Version: tt.version2,
		}
		assert.Equal(t, tt.canAppend, cmd.canAppend(epoch, req))
	}
}

// TODO: add more tests for cmd.go
