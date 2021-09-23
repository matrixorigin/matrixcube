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
	"reflect"
	"regexp"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestCachedResource(t *testing.T) {
	n := uint64(3)

	peers := make([]metapb.Replica, 0, n)
	for i := uint64(0); i < n; i++ {
		p := metapb.Replica{
			ID:          i,
			ContainerID: i,
		}
		peers = append(peers, p)
	}
	res := &metadata.TestResource{
		ResPeers: peers,
	}
	downPeer, pendingPeer := peers[0], peers[1]

	info := NewCachedResource(
		res,
		&peers[0],
		WithDownPeers([]metapb.ReplicaStats{{Replica: downPeer}}),
		WithPendingPeers([]metapb.Replica{pendingPeer}))

	r := info.Clone()
	assert.True(t, reflect.DeepEqual(r, info))

	for i := uint64(0); i < n; i++ {
		p, ok := r.GetPeer(i)
		assert.True(t, ok)
		assert.True(t, reflect.DeepEqual(p, r.Meta.Peers()[i]))
	}

	_, ok := r.GetPeer(n)
	assert.False(t, ok)

	_, ok = r.GetDownPeer(n)
	assert.False(t, ok)

	p, _ := r.GetDownPeer(downPeer.ID)
	assert.True(t, reflect.DeepEqual(p, downPeer))

	_, ok = r.GetPendingPeer(n)
	assert.False(t, ok)

	p, _ = r.GetPendingPeer(pendingPeer.ID)
	assert.True(t, reflect.DeepEqual(p, pendingPeer))

	for i := uint64(0); i < n; i++ {
		p, _ := r.GetContainerPeer(i)
		assert.Equal(t, i, p.ContainerID)
	}

	_, ok = r.GetContainerPeer(n)
	assert.False(t, ok)

	removePeer := metapb.Replica{
		ID:          n,
		ContainerID: n,
	}
	r = r.Clone(SetPeers(append(r.Meta.Peers(), removePeer)))

	assert.True(t, regexp.MustCompile("Add peer.*").MatchString(DiffResourcePeersInfo(info, r)))
	assert.True(t, regexp.MustCompile("Remove peer.*").MatchString(DiffResourcePeersInfo(r, info)))
	p, _ = r.GetContainerPeer(n)
	assert.True(t, reflect.DeepEqual(p, removePeer))

	r = r.Clone(WithRemoveContainerPeer(n))
	assert.Empty(t, DiffResourcePeersInfo(r, info))
	_, ok = r.GetContainerPeer(n)
	assert.False(t, ok)

	r = r.Clone(WithStartKey([]byte{0}))
	assert.True(t, regexp.MustCompile("StartKey Changed.*").MatchString(DiffResourceKeyInfo(r, info)))

	r = r.Clone(WithEndKey([]byte{1}))
	assert.True(t, regexp.MustCompile("EndKey Changed.*").MatchString(DiffResourceKeyInfo(r, info)))

	containers := r.GetContainerIDs()
	assert.Equal(t, int(n), len(containers))
	for i := uint64(0); i < n; i++ {
		_, ok := containers[i]
		assert.True(t, ok)
	}

	followers := r.GetFollowers()
	assert.Equal(t, int(n-1), len(followers))
	for i := uint64(1); i < n; i++ {
		assert.True(t, reflect.DeepEqual(followers[peers[i].ContainerID], peers[i]))
	}
}

func TestResourceItem(t *testing.T) {
	item := newTestResourceItem([]byte("b"), []byte{})

	assert.False(t, item.Less(newTestResourceItem([]byte("a"), []byte{})))
	assert.False(t, item.Less(newTestResourceItem([]byte("b"), []byte{})))
	assert.True(t, item.Less(newTestResourceItem([]byte("c"), []byte{})))

	assert.False(t, item.Contains([]byte("a")))
	assert.True(t, item.Contains([]byte("b")))
	assert.True(t, item.Contains([]byte("c")))

	item = newTestResourceItem([]byte("b"), []byte("d"))
	assert.False(t, item.Contains([]byte("a")))
	assert.True(t, item.Contains([]byte("b")))
	assert.True(t, item.Contains([]byte("c")))
	assert.False(t, item.Contains([]byte("d")))
}

func TestResourceSubTree(t *testing.T) {
	tree := newResourceSubTree(testResourceFactory)
	assert.Equal(t, int64(0), tree.totalSize)
	assert.Equal(t, int64(0), tree.totalKeys)

	tree.update(newResourceWithStat("a", "b", 1, 2))
	assert.Equal(t, int64(1), tree.totalSize)
	assert.Equal(t, int64(2), tree.totalKeys)

	tree.update(newResourceWithStat("b", "c", 3, 4))
	assert.Equal(t, int64(4), tree.totalSize)
	assert.Equal(t, int64(6), tree.totalKeys)

	tree.update(newResourceWithStat("b", "e", 5, 6))
	assert.Equal(t, int64(6), tree.totalSize)
	assert.Equal(t, int64(8), tree.totalKeys)

	tree.remove(newResourceWithStat("a", "b", 1, 2))
	assert.Equal(t, int64(5), tree.totalSize)
	assert.Equal(t, int64(6), tree.totalKeys)

	tree.remove(newResourceWithStat("f", "g", 1, 2))
	assert.Equal(t, int64(5), tree.totalSize)
	assert.Equal(t, int64(6), tree.totalKeys)
}

func TestResourceSubTreeMerge(t *testing.T) {
	tree := newResourceSubTree(testResourceFactory)
	tree.update(newResourceWithStat("a", "b", 1, 2))
	tree.update(newResourceWithStat("b", "c", 3, 4))
	assert.Equal(t, int64(4), tree.totalSize)
	assert.Equal(t, int64(6), tree.totalKeys)

	tree.update(newResourceWithStat("a", "c", 5, 5))
	assert.Equal(t, int64(5), tree.totalSize)
	assert.Equal(t, int64(5), tree.totalKeys)
}

func TestResourceTree(t *testing.T) {
	tree := newResourceSubTree(testResourceFactory)
	assert.Nil(t, tree.search([]byte("a")))

	resA := NewTestCachedResource([]byte("a"), []byte("b"))
	resB := NewTestCachedResource([]byte("b"), []byte("c"))
	resC := NewTestCachedResource([]byte("c"), []byte("d"))
	resD := NewTestCachedResource([]byte("d"), []byte{})

	tree.update(resA)
	tree.update(resC)
	assert.Nil(t, tree.search([]byte{}))
	assert.Equal(t, resA, tree.search([]byte("a")))
	assert.Nil(t, tree.search([]byte("b")))
	assert.Equal(t, resC, tree.search([]byte("c")))
	assert.Nil(t, tree.search([]byte("d")))

	// search previous resource
	assert.Nil(t, tree.searchPrev([]byte("a")))
	assert.Nil(t, tree.searchPrev([]byte("b")))
	assert.Nil(t, tree.searchPrev([]byte("c")))

	tree.update(resB)
	// search previous resource
	assert.Equal(t, resB, tree.searchPrev([]byte("c")))
	assert.Equal(t, resA, tree.searchPrev([]byte("b")))

	tree.remove(resC)
	tree.update(resD)
	assert.Nil(t, tree.search([]byte{}))
	assert.Equal(t, resA, tree.search([]byte("a")))
	assert.Equal(t, resB, tree.search([]byte("b")))
	assert.Nil(t, tree.search([]byte("c")))
	assert.Equal(t, resD, tree.search([]byte("d")))

	// check get adjacent resources
	prev, next := tree.getAdjacentResources(resA)
	assert.Nil(t, prev)
	assert.Equal(t, resB, next.res)

	prev, next = tree.getAdjacentResources(resB)
	assert.Equal(t, resA, prev.res)
	assert.Equal(t, resD, next.res)

	prev, next = tree.getAdjacentResources(resC)
	assert.Equal(t, resB, prev.res)
	assert.Equal(t, resD, next.res)

	prev, next = tree.getAdjacentResources(resD)
	assert.Equal(t, resB, prev.res)
	assert.Nil(t, next)

	// resource with the same range and different resource id will not be delete.
	res0 := newTestResourceItem([]byte{}, []byte("a")).res
	tree.update(res0)
	assert.Equal(t, res0, tree.search([]byte{}))

	anotherRes0 := newTestResourceItem([]byte{}, []byte("a")).res
	anotherRes0.Meta.SetID(123)
	tree.remove(anotherRes0)
	assert.Equal(t, res0, tree.search([]byte{}))

	// overlaps with 0, A, B, C.
	res0D := newTestResourceItem([]byte(""), []byte("d")).res
	tree.update(res0D)
	assert.Equal(t, res0D, tree.search([]byte{}))
	assert.Equal(t, res0D, tree.search([]byte("a")))
	assert.Equal(t, res0D, tree.search([]byte("b")))
	assert.Equal(t, res0D, tree.search([]byte("c")))
	assert.Equal(t, resD, tree.search([]byte("d")))

	// overlaps with D.
	resE := newTestResourceItem([]byte("e"), []byte{}).res
	tree.update(resE)
	assert.Equal(t, res0D, tree.search([]byte{}))
	assert.Equal(t, res0D, tree.search([]byte("a")))
	assert.Equal(t, res0D, tree.search([]byte("b")))
	assert.Equal(t, res0D, tree.search([]byte("c")))
	assert.Nil(t, tree.search([]byte("d")))
	assert.Equal(t, resE, tree.search([]byte("e")))
}

func TestResourceTreeSplitAndMerge(t *testing.T) {
	tree := newResourceTree(testResourceFactory)
	resources := []*CachedResource{newTestResourceItem([]byte{}, []byte{}).res}

	// Byte will underflow/overflow if n > 7.
	n := 7

	// Split.
	for i := 0; i < n; i++ {
		resources = SplitTestResources(resources)
		updateTestResources(t, tree, resources)
	}

	// Merge.
	for i := 0; i < n; i++ {
		resources = MergeTestResources(resources)
		updateTestResources(t, tree, resources)
	}

	// Split twice and merge once.
	for i := 0; i < n*2; i++ {
		if (i+1)%3 == 0 {
			resources = MergeTestResources(resources)
		} else {
			resources = SplitTestResources(resources)
		}
		updateTestResources(t, tree, resources)
	}
}

func TestRandomResource(t *testing.T) {
	tree := newResourceTree(testResourceFactory)
	r := tree.RandomResource(nil)
	assert.Nil(t, r)

	resA := NewTestCachedResource([]byte(""), []byte("g"))
	tree.update(resA)
	ra := tree.RandomResource([]KeyRange{NewKeyRange("", "")})
	assert.True(t, reflect.DeepEqual(ra, resA))

	resB := NewTestCachedResource([]byte("g"), []byte("n"))
	resC := NewTestCachedResource([]byte("n"), []byte("t"))
	resD := NewTestCachedResource([]byte("t"), []byte(""))
	tree.update(resB)
	tree.update(resC)
	tree.update(resD)

	rb := tree.RandomResource([]KeyRange{NewKeyRange("g", "n")})
	assert.True(t, reflect.DeepEqual(rb, resB))

	rc := tree.RandomResource([]KeyRange{NewKeyRange("n", "t")})
	assert.True(t, reflect.DeepEqual(rc, resC))

	rd := tree.RandomResource([]KeyRange{NewKeyRange("t", "")})
	assert.True(t, reflect.DeepEqual(rd, resD))

	re := tree.RandomResource([]KeyRange{NewKeyRange("", "a")})
	assert.Nil(t, re)
	re = tree.RandomResource([]KeyRange{NewKeyRange("o", "s")})
	assert.Nil(t, re)
	re = tree.RandomResource([]KeyRange{NewKeyRange("", "a")})
	assert.Nil(t, re)
	re = tree.RandomResource([]KeyRange{NewKeyRange("z", "")})
	assert.Nil(t, re)

	checkRandomResource(t, tree, []*CachedResource{resA, resB, resC, resD}, []KeyRange{NewKeyRange("", "")})
	checkRandomResource(t, tree, []*CachedResource{resA, resB}, []KeyRange{NewKeyRange("", "n")})
	checkRandomResource(t, tree, []*CachedResource{resC, resD}, []KeyRange{NewKeyRange("n", "")})
	checkRandomResource(t, tree, []*CachedResource{}, []KeyRange{NewKeyRange("h", "s")})
	checkRandomResource(t, tree, []*CachedResource{resB, resC}, []KeyRange{NewKeyRange("a", "z")})
}

func TestRandomResourceDiscontinuous(t *testing.T) {
	tree := newResourceTree(testResourceFactory)
	r := tree.RandomResource([]KeyRange{NewKeyRange("c", "f")})
	assert.Nil(t, r)

	// test for single resource
	resA := NewTestCachedResource([]byte("c"), []byte("f"))
	tree.update(resA)
	ra := tree.RandomResource([]KeyRange{NewKeyRange("c", "e")})
	assert.Nil(t, ra)

	ra = tree.RandomResource([]KeyRange{NewKeyRange("c", "f")})
	assert.True(t, reflect.DeepEqual(ra, resA))

	ra = tree.RandomResource([]KeyRange{NewKeyRange("c", "g")})
	assert.True(t, reflect.DeepEqual(ra, resA))

	ra = tree.RandomResource([]KeyRange{NewKeyRange("a", "e")})
	assert.Nil(t, ra)

	ra = tree.RandomResource([]KeyRange{NewKeyRange("a", "f")})
	assert.True(t, reflect.DeepEqual(ra, resA))

	ra = tree.RandomResource([]KeyRange{NewKeyRange("a", "g")})
	assert.True(t, reflect.DeepEqual(ra, resA))

	resB := NewTestCachedResource([]byte("n"), []byte("x"))
	tree.update(resB)
	rb := tree.RandomResource([]KeyRange{NewKeyRange("g", "x")})
	assert.True(t, reflect.DeepEqual(rb, resB))

	rb = tree.RandomResource([]KeyRange{NewKeyRange("g", "y")})
	assert.True(t, reflect.DeepEqual(rb, resB))

	rb = tree.RandomResource([]KeyRange{NewKeyRange("n", "y")})
	assert.True(t, reflect.DeepEqual(rb, resB))

	rb = tree.RandomResource([]KeyRange{NewKeyRange("o", "y")})
	assert.Nil(t, rb)

	resC := NewTestCachedResource([]byte("z"), []byte(""))
	tree.update(resC)

	rc := tree.RandomResource([]KeyRange{NewKeyRange("y", "")})
	assert.True(t, reflect.DeepEqual(rc, resC))

	resD := NewTestCachedResource([]byte(""), []byte("a"))
	tree.update(resD)

	rd := tree.RandomResource([]KeyRange{NewKeyRange("", "b")})
	assert.True(t, reflect.DeepEqual(rd, resD))

	checkRandomResource(t, tree, []*CachedResource{resA, resB, resC, resD}, []KeyRange{NewKeyRange("", "")})
}

func updateTestResources(t *testing.T, tree *resourceTree, resources []*CachedResource) {
	for _, res := range resources {
		startKey, endKey := res.Meta.Range()
		tree.update(res)
		assert.Equal(t, res, tree.search(startKey))
		if len(endKey) > 0 {
			end := endKey[0]
			assert.Equal(t, res, tree.search([]byte{end - 1}))
			assert.NotEqual(t, res, tree.search([]byte{end + 1}))
		}
	}
}

func checkRandomResource(t *testing.T, tree *resourceTree, resources []*CachedResource, ranges []KeyRange) {
	keys := make(map[string]struct{})
	for i := 0; i < 10000 && len(keys) < len(resources); i++ {
		re := tree.RandomResource(ranges)
		if re == nil {
			continue
		}
		k := string(re.GetStartKey())
		if _, ok := keys[k]; !ok {
			keys[k] = struct{}{}
		}
	}
	for _, res := range resources {
		_, ok := keys[string(res.GetStartKey())]
		assert.True(t, ok)
	}
	assert.Equal(t, len(resources), len(keys))
}
