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

package util

import (
	"testing"

	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestEncodeGroupKey(t *testing.T) {
	assert.Equal(t, string([]byte{0, 0, 0, 0, 0, 0, 0, 0}), EncodeGroupKey(0, nil, nil))
	assert.Equal(t, string([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), EncodeGroupKey(0,
		[]metapb.ScheduleGroupRule{{ID: 1, GroupByLabel: "l1"}, {ID: 2, GroupByLabel: "l2"}}, nil))
	assert.Equal(t, string([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0}),
		EncodeGroupKey(0, []metapb.ScheduleGroupRule{{ID: 2, GroupByLabel: "l2"}, {ID: 1, GroupByLabel: "l1"}},
			[]metapb.Label{{Key: "l1", Value: string([]byte{1})}}))
	assert.Equal(t, string([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 2}),
		EncodeGroupKey(0, []metapb.ScheduleGroupRule{{ID: 2, GroupByLabel: "l2"}, {ID: 1, GroupByLabel: "l1"}},
			[]metapb.Label{{Key: "l1", Value: string([]byte{1})}, {Key: "l2", Value: string([]byte{2})}}))
}

func TestDecodeGroupKey(t *testing.T) {
	assert.Equal(t, uint64(0), DecodeGroupKey(""))
	assert.Equal(t, uint64(1), DecodeGroupKey(string([]byte{0, 0, 0, 0, 0, 0, 0, 1})))
	assert.Equal(t, uint64(1), DecodeGroupKey(string([]byte{0, 0, 0, 0, 0, 0, 0, 1, 2, 3})))
}
