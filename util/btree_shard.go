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
	"bytes"
	"sync"

	"github.com/google/btree"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

const (
	defaultBTreeDegree = 64
)

var (
	emptyShard metapb.Shard
)

type shardItem struct {
	shard metapb.Shard
}

// ShardTree is the btree for Shard
type ShardTree struct {
	sync.RWMutex
	tree *btree.BTree
}

// NewShardTree returns a default Shard btree
func NewShardTree() *ShardTree {
	return &ShardTree{
		tree: btree.New(defaultBTreeDegree),
	}
}

// Less returns true r < other
func (r shardItem) Less(other btree.Item) bool {
	left := r.shard.Start
	right := other.(shardItem).shard.Start
	return bytes.Compare(left, right) < 0
}

// Contains returns the item contains the key
func (r shardItem) Contains(key []byte) bool {
	start, end := r.shard.Start, r.shard.End
	return (len(start) == 0 || bytes.Compare(key, start) >= 0) &&
		(len(end) == 0 || bytes.Compare(key, end) < 0)
}

func (t *ShardTree) length() int {
	return t.tree.Len()
}

// Update updates the tree with the Shard.
// It finds and deletes all the overlapped Shards first, and then
// insert the Shard.
func (t *ShardTree) Update(shards ...metapb.Shard) {
	t.Lock()
	defer t.Unlock()

	for _, shard := range shards {
		if shard.State == metapb.ShardState_Destroyed ||
			shard.State == metapb.ShardState_Destroying {
			continue
		}

		item := shardItem{shard: shard}
		result, ok := t.findLocked(shard.Start)
		if !ok {
			result = item
		}

		var overlaps []shardItem
		// between [Shard, Last], so is iterator all.min >= Shard.min' Shard
		// until all.min > Shard.max
		t.tree.AscendGreaterOrEqual(result, func(i btree.Item) bool {
			over := i.(shardItem)
			// Shard.max <= i.start, so Shard and i has no overlaps,
			// otherwise Shard and i has overlaps
			if len(shard.End) > 0 && bytes.Compare(shard.End, over.shard.Start) <= 0 {
				return false
			}
			overlaps = append(overlaps, over)
			return true
		})

		for _, overlap := range overlaps {
			t.tree.Delete(overlap)
		}
		t.tree.ReplaceOrInsert(item)
	}
}

// Remove removes a Shard if the Shard is in the tree.
// It will do nothing if it cannot find the Shard or the found Shard
// is not the same with the Shard.
func (t *ShardTree) Remove(shard metapb.Shard) bool {
	t.Lock()
	defer t.Unlock()

	result, ok := t.findLocked(shard.Start)
	if !ok || result.shard.ID != shard.ID {
		return false
	}

	t.tree.Delete(result)
	return true
}

// Ascend asc iterator the tree until fn returns false
func (t *ShardTree) Ascend(fn func(Shard *metapb.Shard) bool) {
	t.RLock()
	defer t.RUnlock()

	t.tree.Ascend(func(item btree.Item) bool {
		shard := item.(shardItem).shard
		return fn(&shard)
	})
}

// NextShard return the next bigger key range Shard
func (t *ShardTree) NextShard(start []byte) *metapb.Shard {
	t.RLock()
	defer t.RUnlock()

	var value shardItem
	var ok bool
	t.tree.AscendGreaterOrEqual(shardItem{
		shard: metapb.Shard{Start: start},
	}, func(item btree.Item) bool {
		if bytes.Compare(item.(shardItem).shard.Start, start) > 0 {
			value = item.(shardItem)
			ok = true
			return false
		}

		return true
	})
	if !ok {
		return nil
	}

	return &value.shard
}

// AscendRange asc iterator the tree in the range [start, end) until fn returns false
func (t *ShardTree) AscendRange(start, end []byte, handler func(shard *metapb.Shard) bool) {
	t.RLock()
	defer t.RUnlock()
	fn := func(item btree.Item) bool {
		i := item.(shardItem)
		if len(end) > 0 && bytes.Compare(i.shard.Start, end) >= 0 {
			return false
		}

		return handler(&i.shard)
	}

	if len(start) == 0 {
		t.tree.Ascend(fn)
		return
	}

	item, ok := t.findLocked(start)
	if !ok {
		return
	}

	t.tree.AscendGreaterOrEqual(item, fn)
}

// Search returns a Shard that contains the key.
func (t *ShardTree) Search(key []byte) metapb.Shard {
	t.RLock()
	defer t.RUnlock()

	result, ok := t.findLocked(key)
	if !ok {
		return emptyShard
	}
	return result.shard
}

func (t *ShardTree) findLocked(key []byte) (shardItem, bool) {
	var result shardItem
	var found bool
	t.tree.DescendLessOrEqual(shardItem{shard: metapb.Shard{Start: key}}, func(i btree.Item) bool {
		result = i.(shardItem)
		found = true
		return false
	})

	if !found || !result.Contains(key) {
		return result, false
	}
	return result, true
}
