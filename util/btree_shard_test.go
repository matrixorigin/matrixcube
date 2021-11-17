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

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/stretchr/testify/assert"
)

func TestTree(t *testing.T) {
	tree := NewShardTree()

	tree.Update(meta.Shard{
		ID:    1,
		Start: []byte{0},
		End:   []byte{1},
	})

	tree.Update(meta.Shard{
		ID:    2,
		Start: []byte{2},
		End:   []byte{3},
	})

	tree.Update(meta.Shard{
		ID:    3,
		Start: []byte{4},
		End:   []byte{5},
	})

	if tree.tree.Len() != 3 {
		t.Errorf("tree failed, insert 3 elements, but only %d", tree.tree.Len())
	}

	expect := []byte{0, 2, 4}
	count := 0
	tree.Ascend(func(Shard *meta.Shard) bool {
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
	tree.AscendRange(nil, []byte{4}, func(Shard *meta.Shard) bool {
		count++
		return true
	})

	if count != 2 {
		t.Error("tree failed, asc range failed")
	}

	count = 0
	tree.AscendRange(nil, []byte{5}, func(Shard *meta.Shard) bool {
		count++
		return true
	})

	if count != 3 {
		t.Error("tree failed, asc range failed")
	}

	// it will replace with 0,1 Shard
	tree.Update(meta.Shard{
		ID:    10,
		Start: nil,
		End:   []byte{1},
	})
	Shard = tree.Search([]byte{0})
	if len(Shard.Start) != 0 && Shard.Start[0] == 0 {
		t.Error("tree failed, update overlaps failed")
	}

	tree.Remove(meta.Shard{
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
	tree.Update(meta.Shard{
		ID:    1,
		Start: []byte{1},
		End:   []byte{10},
	})
	tree.Update(meta.Shard{
		ID:    2,
		Start: []byte{5},
		End:   []byte{10},
	})
	tree.Update(meta.Shard{
		ID:    1,
		Start: []byte{1},
		End:   []byte{5},
	})
	s := tree.Search([]byte{5})
	assert.Equal(t, uint64(2), s.ID)
	s = tree.Search([]byte{1})
	assert.Equal(t, uint64(1), s.ID)
}

func TestAddDestoryShard(t *testing.T) {
	tree := NewShardTree()
	tree.Update(
		meta.Shard{
			ID:    1,
			Start: []byte{1},
			End:   []byte{10},
			State: metapb.ResourceState_Destroyed,
		},
		meta.Shard{
			ID:    1,
			Start: []byte{10},
			End:   []byte{20},
			State: metapb.ResourceState_Destroying,
		},
	)

	assert.Equal(t, meta.Shard{}, tree.Search([]byte{1}))
	assert.Equal(t, meta.Shard{}, tree.Search([]byte{10}))
}
