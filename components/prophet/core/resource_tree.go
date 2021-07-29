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

package core

import (
	"bytes"
	"math/rand"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/btree"
)

var _ btree.Item = &resourceItem{}

type resourceItem struct {
	res *CachedResource
}

// Less returns true if the resource start key is less than the other.
func (r *resourceItem) Less(other btree.Item) bool {
	left := r.res.GetStartKey()
	right := other.(*resourceItem).res.GetStartKey()
	return bytes.Compare(left, right) < 0
}

func (r *resourceItem) Contains(key []byte) bool {
	start, end := r.res.GetStartKey(), r.res.GetEndKey()
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

const (
	defaultBTreeDegree = 64
)

type resourceTree struct {
	tree    *btree.BTree
	factory func() metadata.Resource
}

func newResourceTree(factory func() metadata.Resource) *resourceTree {
	return &resourceTree{
		factory: factory,
		tree:    btree.New(defaultBTreeDegree),
	}
}

func (t *resourceTree) newSearchRes(key []byte) *CachedResource {
	meta := t.factory()
	meta.SetStartKey(key)
	return &CachedResource{Meta: meta}
}

func (t *resourceTree) length() int {
	return t.tree.Len()
}

// getOverlaps gets the resources which are overlapped with the specified resource range.
func (t *resourceTree) getOverlaps(res *CachedResource) []*CachedResource {
	item := &resourceItem{res: res}

	// note that find() gets the last item that is less or equal than the resource.
	// in the case: |_______a_______|_____b_____|___c___|
	// new resource is   |______d______|
	// find() will return resourceItem of resource_a
	// and both startKey of resource_a and resource_b are less than endKey of resource_d,
	// thus they are regarded as overlapped resources.
	result := t.find(res)
	if result == nil {
		result = item
	}

	var overlaps []*CachedResource
	t.tree.AscendGreaterOrEqual(result, func(i btree.Item) bool {
		over := i.(*resourceItem)
		if len(res.GetEndKey()) > 0 && bytes.Compare(res.GetEndKey(), over.res.GetStartKey()) <= 0 {
			return false
		}
		overlaps = append(overlaps, over.res)
		return true
	})
	return overlaps
}

// update updates the tree with the resource.
// It finds and deletes all the overlapped resources first, and then
// insert the resource.
func (t *resourceTree) update(res *CachedResource) []*CachedResource {
	overlaps := t.getOverlaps(res)
	for _, item := range overlaps {
		t.tree.Delete(&resourceItem{item})
	}

	t.tree.ReplaceOrInsert(&resourceItem{res: res})

	return overlaps
}

// remove removes a resource if the resource is in the tree.
// It will do nothing if it cannot find the resource or the found resource
// is not the same with the resource.
func (t *resourceTree) remove(res *CachedResource) btree.Item {
	if t.length() == 0 {
		return nil
	}
	result := t.find(res)
	if result == nil || result.res.Meta.ID() != res.Meta.ID() {
		return nil
	}

	return t.tree.Delete(result)
}

// search returns a resource that contains the key.
func (t *resourceTree) search(resKey []byte) *CachedResource {
	res := t.newSearchRes(resKey)
	result := t.find(res)
	if result == nil {
		return nil
	}
	return result.res
}

// searchPrev returns the previous resource of the resource where the resourceKey is located.
func (t *resourceTree) searchPrev(resKey []byte) *CachedResource {
	curRes := t.newSearchRes(resKey)
	curResItem := t.find(curRes)
	if curResItem == nil {
		return nil
	}
	prevResourceItem, _ := t.getAdjacentResources(curResItem.res)
	if prevResourceItem == nil {
		return nil
	}
	if !bytes.Equal(prevResourceItem.res.GetEndKey(), curResItem.res.GetStartKey()) {
		return nil
	}
	return prevResourceItem.res
}

// find is a helper function to find an item that contains the resources start
// key.
func (t *resourceTree) find(res *CachedResource) *resourceItem {
	item := &resourceItem{res: res}

	var result *resourceItem
	t.tree.DescendLessOrEqual(item, func(i btree.Item) bool {
		result = i.(*resourceItem)
		return false
	})

	if result == nil || !result.Contains(res.GetStartKey()) {
		return nil
	}

	return result
}

// scanRage scans from the first resource containing or behind the start key
// until f return false
func (t *resourceTree) scanRange(startKey []byte, f func(*CachedResource) bool) {
	res := t.newSearchRes(startKey)
	// find if there is a resource with key range [s, d), s < startKey < d
	startItem := t.find(res)
	if startItem == nil {
		startItem = &resourceItem{res: t.newSearchRes(startKey)}
	}
	t.tree.AscendGreaterOrEqual(startItem, func(item btree.Item) bool {
		return f(item.(*resourceItem).res)
	})
}

func (t *resourceTree) getAdjacentResources(res *CachedResource) (*resourceItem, *resourceItem) {
	item := &resourceItem{res: t.newSearchRes(res.GetStartKey())}
	var prev, next *resourceItem
	t.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		if bytes.Equal(item.res.GetStartKey(), i.(*resourceItem).res.GetStartKey()) {
			return true
		}
		next = i.(*resourceItem)
		return false
	})
	t.tree.DescendLessOrEqual(item, func(i btree.Item) bool {
		if bytes.Equal(item.res.GetStartKey(), i.(*resourceItem).res.GetStartKey()) {
			return true
		}
		prev = i.(*resourceItem)
		return false
	})
	return prev, next
}

// RandomResource is used to get a random resource within ranges.
func (t *resourceTree) RandomResource(ranges []KeyRange) *CachedResource {
	if t.length() == 0 {
		return nil
	}

	if len(ranges) == 0 {
		ranges = []KeyRange{NewKeyRange("", "")}
	}

	for _, i := range rand.Perm(len(ranges)) {
		var endIndex int
		startKey, endKey := ranges[i].StartKey, ranges[i].EndKey
		startResource, startIndex := t.tree.GetWithIndex(&resourceItem{res: t.newSearchRes(startKey)})

		if len(endKey) != 0 {
			_, endIndex = t.tree.GetWithIndex(&resourceItem{res: t.newSearchRes(endKey)})
		} else {
			endIndex = t.tree.Len()
		}

		// Consider that the item in the tree may not be continuous,
		// we need to check if the previous item contains the key.
		if startIndex != 0 && startResource == nil && t.tree.GetAt(startIndex-1).(*resourceItem).Contains(startKey) {
			startIndex--
		}

		if endIndex <= startIndex {
			if len(endKey) > 0 && bytes.Compare(startKey, endKey) > 0 {
				util.GetLogger().Errorf("wrong range keys, start %+v, end %+v",
					string(HexResourceKey(startKey)),
					string(HexResourceKey(endKey)))
			}
			continue
		}
		index := rand.Intn(endIndex-startIndex) + startIndex
		res := t.tree.GetAt(index).(*resourceItem).res
		if isInvolved(res, startKey, endKey) {
			return res
		}
	}

	return nil
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
