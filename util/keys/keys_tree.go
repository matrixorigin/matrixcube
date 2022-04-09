package keys

import (
	"bytes"

	"github.com/google/btree"
)

type treeItem struct {
	key []byte
}

// Less returns true if the item key is less than the other.
func (item treeItem) Less(other btree.Item) bool {
	left := item.key
	right := other.(treeItem).key
	return bytes.Compare(right, left) > 0
}

// KeyTree key btree
type KeyTree struct {
	tree *btree.BTree
	tmp  treeItem
	size int
}

// NewKeyTree return a kv btree
func NewKeyTree(btreeDegree int) *KeyTree {
	return &KeyTree{
		tree: btree.New(btreeDegree),
	}
}

// Min returns the min keys in the tree
func (k *KeyTree) Min() []byte {
	if k.Len() == 0 {
		return nil
	}
	return k.tree.Min().(treeItem).key
}

// Max returns the max keys in the tree
func (k *KeyTree) Max() []byte {
	if k.Len() == 0 {
		return nil
	}
	return k.tree.Max().(treeItem).key
}

// Add adds a key
func (k *KeyTree) Add(key []byte) {
	k.size += len(key)
	item := k.tree.ReplaceOrInsert(treeItem{
		key: key,
	})
	if item != nil {
		k.size -= len(item.(treeItem).key)
	}
}

// AddMany adds many key
func (k *KeyTree) AddMany(keys [][]byte) {
	for _, key := range keys {
		k.Add(key)
	}
}

// Bytes returns keys bytes
func (k *KeyTree) Bytes() int {
	return k.size
}

// Contains returns true if the key is in the tree
func (k *KeyTree) Contains(key []byte) bool {
	k.tmp.key = key
	return nil != k.tree.Get(k.tmp)
}

// Len return the count of keys
func (k *KeyTree) Len() int {
	return k.tree.Len()
}

// Delete deletes a key
func (k *KeyTree) Delete(key []byte) {
	k.tmp.key = key
	item := k.tree.Delete(k.tmp)
	if item != nil {
		k.size -= len(item.(treeItem).key)
	}
}

// DeleteMany deletes many keys
func (k *KeyTree) DeleteMany(keys [][]byte) {
	for _, key := range keys {
		k.Delete(key)
	}
}

// AscendRange iter in [start, end)
func (k *KeyTree) AscendRange(start, end []byte, fn func(key []byte) bool) {
	k.tmp.key = start
	k.tree.AscendGreaterOrEqual(k.tmp, func(i btree.Item) bool {
		target := i.(treeItem)
		if bytes.Compare(target.key, end) < 0 {
			return fn(target.key)
		}

		return false
	})
}

// Ascend iter all tree
func (k *KeyTree) Ascend(fn func(key []byte) bool) {
	k.tmp.key = nil
	k.tree.AscendGreaterOrEqual(k.tmp, func(i btree.Item) bool {
		target := i.(treeItem)
		return fn(target.key)
	})
}

// Seek returns the next key which key >= spec key
func (k *KeyTree) Seek(key []byte) []byte {
	k.tmp.key = key
	var result []byte
	k.tree.AscendGreaterOrEqual(k.tmp, func(i btree.Item) bool {
		result = i.(treeItem).key
		return false
	})
	return result
}

// SeekGT returns the next key which key > spec key
func (k *KeyTree) SeekGT(key []byte) []byte {
	k.tmp.key = key
	var result []byte
	k.tree.AscendGreaterOrEqual(k.tmp, func(i btree.Item) bool {
		ck := i.(treeItem).key
		if bytes.Equal(ck, key) {
			return true
		}
		result = i.(treeItem).key
		return false
	})
	return result
}
