// Copyright 2022 MatrixOrigin.
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

	"github.com/google/btree"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
)

type treeItem struct {
	key  []byte
	data txnpb.TxnUncommittedMVCCMetadata
}

// Less returns true if the item key is less than the other.
func (item treeItem) Less(other btree.Item) bool {
	left := item.key
	right := other.(treeItem).key
	return bytes.Compare(right, left) > 0
}

// UncommittedTree uncommitted data btree
type UncommittedTree struct {
	tree *btree.BTree
	tmp  treeItem
}

// NewUncommittedTree return a uncommitted btree
func NewUncommittedTree() *UncommittedTree {
	return &UncommittedTree{
		tree: btree.New(64),
	}
}

// Add adds a key
func (k *UncommittedTree) Add(key []byte, data txnpb.TxnUncommittedMVCCMetadata) {
	k.tree.ReplaceOrInsert(treeItem{
		key:  key,
		data: data,
	})
}

// Len return the count of keys
func (k *UncommittedTree) Len() int {
	return k.tree.Len()
}

// AscendRange iter in [start, end)
func (k *UncommittedTree) AscendRange(start, end []byte, fn func(key []byte, data txnpb.TxnUncommittedMVCCMetadata) (bool, error)) error {
	k.tmp.key = start
	var err error
	var ok bool
	k.tree.AscendGreaterOrEqual(k.tmp, func(i btree.Item) bool {
		target := i.(treeItem)
		if bytes.Compare(target.key, end) < 0 {
			ok, err = fn(target.key, target.data)
			if ok {
				return ok
			}
		}

		return false
	})
	return err
}

// Get get value, return nil if not the key is not exists
func (k *UncommittedTree) Get(key []byte) (txnpb.TxnUncommittedMVCCMetadata, bool) {
	k.tmp.key = key
	var result treeItem
	k.tree.AscendGreaterOrEqual(k.tmp, func(i btree.Item) bool {
		result = i.(treeItem)
		return false
	})

	return result.data, bytes.Equal(result.key, key)
}
