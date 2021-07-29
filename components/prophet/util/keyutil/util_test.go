// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
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

package keyutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyUtil(t *testing.T) {
	startKey := []byte("a")
	endKey := []byte("b")
	key := BuildKeyRangeKey(1, startKey, endKey)
	assert.Equal(t, "1-61-62", key)

	assert.Equal(t, uint64(1), GetGroupFromRangeKey(key))
}
