package util

import (
	"bytes"
	"sync"

	"github.com/google/btree"
)

type treeItem struct {
	key   []byte
	value []byte
}

// Less returns true if the item key is less than the other.
func (item *treeItem) Less(other btree.Item) bool {
	left := item.key
	right := other.(*treeItem).key
	return bytes.Compare(right, left) > 0
}

// Equals returns true if the item key is equals the other.
func (item *treeItem) Equals(other btree.Item) bool {
	left := item.key
	right := other.(*treeItem).key
	return bytes.Equal(right, left)
}

// KVTree kv btree
type KVTree struct {
	sync.RWMutex
	tree *btree.BTree
}

// NewKVTree return a kv btree
func NewKVTree() *KVTree {
	return &KVTree{
		tree: btree.New(defaultBTreeDegree),
	}
}

// Put puts a key, value to the tree
func (kv *KVTree) Put(key, value []byte) {
	kv.Lock()

	kv.tree.ReplaceOrInsert(&treeItem{
		key:   key,
		value: value,
	})

	kv.Unlock()
}

// Delete deletes a key, return false if not the key is not exists
func (kv *KVTree) Delete(key []byte) bool {
	kv.Lock()
	item := &treeItem{key: key}
	ok := nil != kv.tree.Delete(item)
	kv.Unlock()

	return ok
}

// RangeDelete deletes key in [start, end)
func (kv *KVTree) RangeDelete(start, end []byte) {
	kv.Lock()

	var items []btree.Item
	item := &treeItem{key: start}
	kv.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		target := i.(*treeItem)
		if bytes.Compare(target.key, end) < 0 {
			items = append(items, i)
			return true
		}

		return false
	})

	for _, target := range items {
		kv.tree.Delete(target)
	}

	kv.Unlock()
}

// Get get value, return nil if not the key is not exists
func (kv *KVTree) Get(key []byte) []byte {
	kv.RLock()

	item := &treeItem{key: key}

	var result *treeItem
	kv.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*treeItem)
		return false
	})

	if result == nil || !result.Equals(item) {
		kv.RUnlock()
		return nil
	}

	value := result.value
	kv.RUnlock()
	return value
}

// Seek returns the next key and value which key >= spec key
func (kv *KVTree) Seek(key []byte) ([]byte, []byte) {
	kv.RLock()
	defer kv.RUnlock()

	item := &treeItem{key: key}

	var result *treeItem
	kv.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*treeItem)
		return false
	})

	if result == nil {
		return nil, nil
	}

	return result.key, result.value
}

// Scan scans in [start, end)
func (kv *KVTree) Scan(start, end []byte, handler func(key, value []byte) (bool, error)) error {
	kv.RLock()
	var items []*treeItem
	item := &treeItem{key: start}
	kv.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		target := i.(*treeItem)
		if bytes.Compare(target.key, end) < 0 {
			items = append(items, target)
			return true
		}

		return false
	})
	kv.RUnlock()

	for _, target := range items {
		c, err := handler(target.key, target.value)
		if err != nil || !c {
			return err
		}
	}

	return nil
}
