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

package util

import (
	"testing"

	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestTree(t *testing.T) {
	tree := NewShardTree()

	tree.Update(metapb.Shard{
		ID:    1,
		Start: []byte{0},
		End:   []byte{1},
	})

	tree.Update(metapb.Shard{
		ID:    2,
		Start: []byte{2},
		End:   []byte{3},
	})

	tree.Update(metapb.Shard{
		ID:    3,
		Start: []byte{4},
		End:   []byte{5},
	})

	if tree.tree.Len() != 3 {
		t.Errorf("tree failed, insert 3 elements, but only %d", tree.tree.Len())
	}

	expect := []byte{0, 2, 4}
	count := 0
	tree.Ascend(func(Shard *metapb.Shard) bool {
		if expect[count] != Shard.Start[0] {
			t.Error("tree failed, asc order is error")
		}
		count++

		return true
	})

	Shard := tree.Search([]byte{2})
	if len(Shard.Start) == 0 || Shard.Start[0] != 2 {
		t.Error("tree failed, search failed")
	}

	c := tree.NextShard(nil)
	if c == nil || len(c.Start) == 0 || c.Start[0] != 0 {
		t.Error("tree failed, search next failed")
	}

	count = 0
	tree.AscendRange(nil, []byte{4}, func(Shard *metapb.Shard) bool {
		count++
		return true
	})

	if count != 2 {
		t.Error("tree failed, asc range failed")
	}

	count = 0
	tree.AscendRange(nil, []byte{5}, func(Shard *metapb.Shard) bool {
		count++
		return true
	})

	if count != 3 {
		t.Error("tree failed, asc range failed")
	}

	// it will replace with 0,1 Shard
	tree.Update(metapb.Shard{
		ID:    10,
		Start: nil,
		End:   []byte{1},
	})
	Shard = tree.Search([]byte{0})
	if len(Shard.Start) != 0 && Shard.Start[0] == 0 {
		t.Error("tree failed, update overlaps failed")
	}

	tree.Remove(metapb.Shard{
		ID:    2,
		Start: []byte{2},
		End:   []byte{3},
	})
	if tree.length() != 2 {
		t.Error("tree failed, Remove failed")
	}
}

func TestTreeOverlap(t *testing.T) {
	tree := NewShardTree()
	tree.Update(metapb.Shard{
		ID:    1,
		Start: []byte{1},
		End:   []byte{10},
	})
	tree.Update(metapb.Shard{
		ID:    2,
		Start: []byte{5},
		End:   []byte{10},
	})
	tree.Update(metapb.Shard{
		ID:    1,
		Start: []byte{1},
		End:   []byte{5},
	})
	s := tree.Search([]byte{5})
	assert.Equal(t, uint64(2), s.ID)
	s = tree.Search([]byte{1})
	assert.Equal(t, uint64(1), s.ID)
}

func TestAddDestroyShard(t *testing.T) {
	tree := NewShardTree()
	tree.Update(
		metapb.Shard{
			ID:    1,
			Start: []byte{1},
			End:   []byte{10},
			State: metapb.ShardState_Destroyed,
		},
		metapb.Shard{
			ID:    1,
			Start: []byte{10},
			End:   []byte{20},
			State: metapb.ShardState_Destroying,
		},
	)

	assert.Equal(t, metapb.Shard{}, tree.Search([]byte{1}))
	assert.Equal(t, metapb.Shard{}, tree.Search([]byte{10}))
}

func TestAscendRange(t *testing.T) {
	values := []metapb.Shard{
		{
			ID:    1,
			Start: nil,
			End:   []byte{5},
		},
		{
			ID:    2,
			Start: []byte{5},
			End:   []byte{10},
		},
		{
			ID:    3,
			Start: []byte{10},
			End:   nil,
		},
	}
	tree := NewShardTree()
	tree.Update(values...)

	cases := []struct {
		start        []byte
		end          []byte
		expectShards []metapb.Shard
	}{
		{
			start:        nil,
			end:          nil,
			expectShards: values,
		},
		{
			start:        nil,
			end:          []byte{5},
			expectShards: values[:1],
		},
		{
			start:        nil,
			end:          []byte{6},
			expectShards: values[:2],
		},
		{
			start:        nil,
			end:          []byte{10},
			expectShards: values[:2],
		},
		{
			start:        []byte{5},
			end:          []byte{6},
			expectShards: values[1:2],
		},
		{
			start:        []byte{5},
			end:          []byte{10},
			expectShards: values[1:2],
		},
		{
			start:        []byte{5},
			end:          nil,
			expectShards: values[1:],
		},
		{
			start:        []byte{10},
			end:          nil,
			expectShards: values[2:],
		},
	}

	for _, c := range cases {
		var shards []metapb.Shard
		tree.AscendRange(c.start, c.end, func(shard *metapb.Shard) bool {
			shards = append(shards, *shard)
			return true
		})
		assert.Equal(t, c.expectShards, shards)
	}
}
