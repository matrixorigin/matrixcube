// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless assertd by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCopy(t *testing.T) {
	stats := Stats{
		WrittenKeys:  1,
		WrittenBytes: 2,
		ReadKeys:     3,
		ReadBytes:    4,
		SyncCount:    5,
	}
	actual := stats.Copy()

	// check corresponding fields
	assert.Equal(t, stats.WrittenKeys, actual.WrittenKeys)
	assert.Equal(t, stats.WrittenBytes, actual.WrittenBytes)
	assert.Equal(t, stats.ReadKeys, actual.ReadKeys)
	assert.Equal(t, stats.ReadBytes, actual.ReadBytes)
	assert.Equal(t, stats.SyncCount, actual.SyncCount)
}
