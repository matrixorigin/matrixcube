package keys

import (
	"bytes"

	"github.com/google/btree"
)

var (
	pointType byte = 1
	startType byte = 2
	endType   byte = 3
)

type mixedTreeItem struct {
	key     []byte
	keyType byte
}

// Less returns true if item < other
func (item mixedTreeItem) Less(other btree.Item) bool {
	left := item.key
	right := other.(mixedTreeItem).key
	return bytes.Compare(right, left) > 0
}

// MixedKeysTree is a mixed point and key range btree. Used to merge scan with other keys set
type MixedKeysTree struct {
	tree     *btree.BTree
	hasRange bool
	tmp      mixedTreeItem
}

// NewMixedKeysTree return a mixed point and key range btree.
func NewMixedKeysTree(keys [][]byte) *MixedKeysTree {
	k := &MixedKeysTree{
		tree: btree.New(32),
	}
	for _, key := range keys {
		k.tree.ReplaceOrInsert(mixedTreeItem{key: key, keyType: pointType})
	}
	return k
}

// AddKeyRange add key range [start, end) into tree
func (k *MixedKeysTree) AddKeyRange(start []byte, end []byte) {
	if bytes.Compare(start, end) >= 0 {
		panic("invalid key range")
	}

	if k.len() == 0 {
		k.tree.ReplaceOrInsert(mixedTreeItem{key: start, keyType: startType})
		k.tree.ReplaceOrInsert(mixedTreeItem{key: end, keyType: endType})
		k.hasRange = true
		return
	}

	k.addStartKey(start)
	k.addEndKey(start, end)
	k.hasRange = true
}

// Contains returns true if the key is in the tree
func (k *MixedKeysTree) Contains(key []byte) bool {
	k.tmp.key = key
	var contains bool
	k.tree.AscendGreaterOrEqual(k.tmp, func(i btree.Item) bool {
		item := i.(mixedTreeItem)
		if bytes.Equal(item.key, key) {
			if item.keyType != endType {
				contains = true
			}
			return false
		}

		// item.key > key
		switch item.keyType {
		case pointType:
			return true
		case startType:
			return false
		case endType:
			contains = true
			return false
		}
		return false
	})
	return contains
}

// Seek returns min[key, +inf)
func (k *MixedKeysTree) Seek(key []byte) []byte {
	k.tmp.key = key
	var result []byte
	k.tree.AscendGreaterOrEqual(k.tmp, func(i btree.Item) bool {
		item := i.(mixedTreeItem)
		equal := bytes.Equal(item.key, key)
		if item.keyType == endType {
			if equal {
				return true
			}
			result = key
			return false
		} else if item.keyType == startType {
			result = item.key
		}

		result = item.key
		return false
	})
	return result
}

// SeekGT returns min(key, +inf)
func (k *MixedKeysTree) SeekGT(key []byte) []byte {
	k.tmp.key = key
	var result []byte
	k.tree.AscendGreaterOrEqual(k.tmp, func(i btree.Item) bool {
		item := i.(mixedTreeItem)
		if bytes.Equal(item.key, key) {
			return true
		}

		if item.keyType == endType {
			nextKey, ok := getNextKeyByEndKey(key, item.key)
			if !ok {
				return true
			}
			result = nextKey
		} else {
			result = item.key
		}
		return false
	})
	return result
}

func (k *MixedKeysTree) addStartKey(start []byte) {
	key, kt := k.getPrevRange(start, nil, 0)
	// [1, 5) + [3, ?) => [1, max(end))
	if len(key) > 0 {
		switch kt {
		case startType:
			// prev.start <= start
		case endType:
			// [1, 5) + [5, ?)
			if bytes.Equal(key, start) {
				k.delete(key)
				return
			}
			// [1, 5) + [6, ?)
			k.tree.ReplaceOrInsert(mixedTreeItem{key: start, keyType: startType})
		}

		return
	}
	// no prev start and end
	key, kt = k.getNextRange(start)
	if len(key) > 0 {
		switch kt {
		case startType:
			// next.start >= start, e.g. [2, 4) + [1, 3)
			k.delete(key)
		case endType:
			if k.hasRange {
				panic("unexpect key type, missing start key")
			}
		}
	}
	k.tree.ReplaceOrInsert(mixedTreeItem{key: start, keyType: startType})
}

func (k *MixedKeysTree) addEndKey(start, end []byte) {
	key, kt := k.getNextRange(end)
	if len(key) > 0 {
		switch kt {
		case startType:
			// [?, 5) + [5, 10)
			if bytes.Equal(key, end) {
				k.delete(key)
			}

			// [?, 5) + [6, 10),
		case endType:
			// end >= next.end
		}
		return
	}

	// no next start and end
	// [0, 1), [2, 3)
	key, kt = k.getPrevRange(end, start, startType)
	if len(key) > 0 {
		switch kt {
		case startType:
			if k.hasRange {
				panic("unexpect key type, missing end key")
			}
		case endType:
			if bytes.Compare(key, start) >= 0 {
				k.delete(key)
			}
		}
	}

	added := end
	if ok, kt := k.get(end); ok && kt == pointType {
		added = NextKey(end, nil)
		k.delete(end)
	}
	k.tree.ReplaceOrInsert(mixedTreeItem{key: added, keyType: endType})
}

func (k *MixedKeysTree) delete(key []byte) {
	k.tmp.key = key
	k.tree.Delete(k.tmp)
}

func (k *MixedKeysTree) len() int {
	return k.tree.Len()
}

func (k *MixedKeysTree) get(key []byte) (bool, byte) {
	k.tmp.key = key
	item := k.tree.Get(k.tmp)
	if item == nil {
		return false, 0
	}
	return true, item.(mixedTreeItem).keyType
}

func (k *MixedKeysTree) getPrevRange(key []byte, exclude []byte, excludeKeyType byte) ([]byte, byte) {
	k.tmp.key = key
	var result []byte
	var kt byte
	k.tree.DescendLessOrEqual(k.tmp, func(i btree.Item) bool {
		item := i.(mixedTreeItem)
		if bytes.Equal(item.key, exclude) && item.keyType == excludeKeyType {
			return true
		}

		if item.keyType != pointType {
			result = item.key
			kt = item.keyType
			return false
		}
		return true
	})
	return result, kt
}

func (k *MixedKeysTree) getNextRange(key []byte) ([]byte, byte) {
	k.tmp.key = key
	var result []byte
	var kt byte
	k.tree.AscendGreaterOrEqual(k.tmp, func(i btree.Item) bool {
		item := i.(mixedTreeItem)
		if item.keyType != pointType {
			result = item.key
			kt = item.keyType
			return false
		}

		return true
	})
	return result, kt
}

func getNextKeyByEndKey(key []byte, endKey []byte) ([]byte, bool) {
	if bytes.Equal(key, endKey) {
		return nil, false
	}

	// check key.nextKey in [start, end)
	nextKey := NextKey(key, nil)
	if bytes.Compare(nextKey, endKey) >= 0 {
		return nil, false
	}
	return nextKey, true
}
