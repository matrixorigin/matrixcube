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
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestCachedShard(t *testing.T) {
	n := uint64(3)

	peers := make([]metapb.Replica, 0, n)
	for i := uint64(0); i < n; i++ {
		p := metapb.Replica{
			ID:      i,
			StoreID: i,
		}
		peers = append(peers, p)
	}
	res := &metadata.ShardWithRWLock{
		Shard: metapb.Shard{
			Replicas: peers,
		},
	}
	downPeer, pendingPeer := peers[0], peers[1]

	info := NewCachedShard(
		res,
		&peers[0],
		WithDownPeers([]metapb.ReplicaStats{{Replica: downPeer}}),
		WithPendingPeers([]metapb.Replica{pendingPeer}))

	r := info.Clone()
	assert.True(t, reflect.DeepEqual(r, info))

	for i := uint64(0); i < n; i++ {
		p, ok := r.GetPeer(i)
		assert.True(t, ok)
		assert.True(t, reflect.DeepEqual(p, r.Meta.Replicas()[i]))
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
		p, _ := r.GetStorePeer(i)
		assert.Equal(t, i, p.StoreID)
	}

	_, ok = r.GetStorePeer(n)
	assert.False(t, ok)

	removePeer := metapb.Replica{
		ID:      n,
		StoreID: n,
	}
	r = r.Clone(SetPeers(append(r.Meta.Replicas(), removePeer)))

	assert.True(t, regexp.MustCompile("Add peer.*").MatchString(DiffShardPeersInfo(info, r)))
	assert.True(t, regexp.MustCompile("Remove peer.*").MatchString(DiffShardPeersInfo(r, info)))
	p, _ = r.GetStorePeer(n)
	assert.True(t, reflect.DeepEqual(p, removePeer))

	r = r.Clone(WithRemoveStorePeer(n))
	assert.Empty(t, DiffShardPeersInfo(r, info))
	_, ok = r.GetStorePeer(n)
	assert.False(t, ok)

	r = r.Clone(WithStartKey([]byte{0}))
	assert.True(t, regexp.MustCompile("StartKey Changed.*").MatchString(DiffShardKeyInfo(r, info)))

	r = r.Clone(WithEndKey([]byte{1}))
	assert.True(t, regexp.MustCompile("EndKey Changed.*").MatchString(DiffShardKeyInfo(r, info)))

	containers := r.GetStoreIDs()
	assert.Equal(t, int(n), len(containers))
	for i := uint64(0); i < n; i++ {
		_, ok := containers[i]
		assert.True(t, ok)
	}

	followers := r.GetFollowers()
	assert.Equal(t, int(n-1), len(followers))
	for i := uint64(1); i < n; i++ {
		assert.True(t, reflect.DeepEqual(followers[peers[i].StoreID], peers[i]))
	}
}

func TestShardItem(t *testing.T) {
	item := newTestShardItem([]byte("b"), []byte{})

	assert.False(t, item.Less(newTestShardItem([]byte("a"), []byte{})))
	assert.False(t, item.Less(newTestShardItem([]byte("b"), []byte{})))
	assert.True(t, item.Less(newTestShardItem([]byte("c"), []byte{})))

	assert.False(t, item.Contains([]byte("a")))
	assert.True(t, item.Contains([]byte("b")))
	assert.True(t, item.Contains([]byte("c")))

	item = newTestShardItem([]byte("b"), []byte("d"))
	assert.False(t, item.Contains([]byte("a")))
	assert.True(t, item.Contains([]byte("b")))
	assert.True(t, item.Contains([]byte("c")))
	assert.False(t, item.Contains([]byte("d")))
}

func TestShardSubTree(t *testing.T) {
	tree := newShardSubTree()
	assert.Equal(t, int64(0), tree.totalSize)
	assert.Equal(t, int64(0), tree.totalKeys)

	tree.update(newShardWithStat("a", "b", 1, 2))
	assert.Equal(t, int64(1), tree.totalSize)
	assert.Equal(t, int64(2), tree.totalKeys)

	tree.update(newShardWithStat("b", "c", 3, 4))
	assert.Equal(t, int64(4), tree.totalSize)
	assert.Equal(t, int64(6), tree.totalKeys)

	tree.update(newShardWithStat("b", "e", 5, 6))
	assert.Equal(t, int64(6), tree.totalSize)
	assert.Equal(t, int64(8), tree.totalKeys)

	tree.remove(newShardWithStat("a", "b", 1, 2))
	assert.Equal(t, int64(5), tree.totalSize)
	assert.Equal(t, int64(6), tree.totalKeys)

	tree.remove(newShardWithStat("f", "g", 1, 2))
	assert.Equal(t, int64(5), tree.totalSize)
	assert.Equal(t, int64(6), tree.totalKeys)
}

func TestShardSubTreeMerge(t *testing.T) {
	tree := newShardSubTree()
	tree.update(newShardWithStat("a", "b", 1, 2))
	tree.update(newShardWithStat("b", "c", 3, 4))
	assert.Equal(t, int64(4), tree.totalSize)
	assert.Equal(t, int64(6), tree.totalKeys)

	tree.update(newShardWithStat("a", "c", 5, 5))
	assert.Equal(t, int64(5), tree.totalSize)
	assert.Equal(t, int64(5), tree.totalKeys)
}

func TestShardTree(t *testing.T) {
	tree := newShardSubTree()
	assert.Nil(t, tree.search([]byte("a")))

	resA := NewTestCachedShard([]byte("a"), []byte("b"))
	resB := NewTestCachedShard([]byte("b"), []byte("c"))
	resC := NewTestCachedShard([]byte("c"), []byte("d"))
	resD := NewTestCachedShard([]byte("d"), []byte{})

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
	prev, next := tree.getAdjacentShards(resA)
	assert.Nil(t, prev)
	assert.Equal(t, resB, next.res)

	prev, next = tree.getAdjacentShards(resB)
	assert.Equal(t, resA, prev.res)
	assert.Equal(t, resD, next.res)

	prev, next = tree.getAdjacentShards(resC)
	assert.Equal(t, resB, prev.res)
	assert.Equal(t, resD, next.res)

	prev, next = tree.getAdjacentShards(resD)
	assert.Equal(t, resB, prev.res)
	assert.Nil(t, next)

	// resource with the same range and different resource id will not be delete.
	res0 := newTestShardItem([]byte{}, []byte("a")).res
	tree.update(res0)
	assert.Equal(t, res0, tree.search([]byte{}))

	anotherRes0 := newTestShardItem([]byte{}, []byte("a")).res
	anotherRes0.Meta.SetID(123)
	tree.remove(anotherRes0)
	assert.Equal(t, res0, tree.search([]byte{}))

	// overlaps with 0, A, B, C.
	res0D := newTestShardItem([]byte(""), []byte("d")).res
	tree.update(res0D)
	assert.Equal(t, res0D, tree.search([]byte{}))
	assert.Equal(t, res0D, tree.search([]byte("a")))
	assert.Equal(t, res0D, tree.search([]byte("b")))
	assert.Equal(t, res0D, tree.search([]byte("c")))
	assert.Equal(t, resD, tree.search([]byte("d")))

	// overlaps with D.
	resE := newTestShardItem([]byte("e"), []byte{}).res
	tree.update(resE)
	assert.Equal(t, res0D, tree.search([]byte{}))
	assert.Equal(t, res0D, tree.search([]byte("a")))
	assert.Equal(t, res0D, tree.search([]byte("b")))
	assert.Equal(t, res0D, tree.search([]byte("c")))
	assert.Nil(t, tree.search([]byte("d")))
	assert.Equal(t, resE, tree.search([]byte("e")))
}

func TestShardTreeSplitAndMerge(t *testing.T) {
	tree := newShardTree()
	resources := []*CachedShard{newTestShardItem([]byte{}, []byte{}).res}

	// Byte will underflow/overflow if n > 7.
	n := 7

	// Split.
	for i := 0; i < n; i++ {
		resources = SplitTestShards(resources)
		updateTestShards(t, tree, resources)
	}

	// Merge.
	for i := 0; i < n; i++ {
		resources = MergeTestShards(resources)
		updateTestShards(t, tree, resources)
	}

	// Split twice and merge once.
	for i := 0; i < n*2; i++ {
		if (i+1)%3 == 0 {
			resources = MergeTestShards(resources)
		} else {
			resources = SplitTestShards(resources)
		}
		updateTestShards(t, tree, resources)
	}
}

func TestRandomShard(t *testing.T) {
	tree := newShardTree()
	r := tree.RandomShard(nil)
	assert.Nil(t, r)

	resA := NewTestCachedShard([]byte(""), []byte("g"))
	tree.update(resA)
	ra := tree.RandomShard([]KeyRange{NewKeyRange(0, "", "")})
	assert.True(t, reflect.DeepEqual(ra, resA))

	resB := NewTestCachedShard([]byte("g"), []byte("n"))
	resC := NewTestCachedShard([]byte("n"), []byte("t"))
	resD := NewTestCachedShard([]byte("t"), []byte(""))
	tree.update(resB)
	tree.update(resC)
	tree.update(resD)

	rb := tree.RandomShard([]KeyRange{NewKeyRange(0, "g", "n")})
	assert.True(t, reflect.DeepEqual(rb, resB))

	rc := tree.RandomShard([]KeyRange{NewKeyRange(0, "n", "t")})
	assert.True(t, reflect.DeepEqual(rc, resC))

	rd := tree.RandomShard([]KeyRange{NewKeyRange(0, "t", "")})
	assert.True(t, reflect.DeepEqual(rd, resD))

	re := tree.RandomShard([]KeyRange{NewKeyRange(0, "", "a")})
	assert.Nil(t, re)
	re = tree.RandomShard([]KeyRange{NewKeyRange(0, "o", "s")})
	assert.Nil(t, re)
	re = tree.RandomShard([]KeyRange{NewKeyRange(0, "", "a")})
	assert.Nil(t, re)
	re = tree.RandomShard([]KeyRange{NewKeyRange(0, "z", "")})
	assert.Nil(t, re)

	checkRandomShard(t, tree, []*CachedShard{resA, resB, resC, resD}, []KeyRange{NewKeyRange(0, "", "")})
	checkRandomShard(t, tree, []*CachedShard{resA, resB}, []KeyRange{NewKeyRange(0, "", "n")})
	checkRandomShard(t, tree, []*CachedShard{resC, resD}, []KeyRange{NewKeyRange(0, "n", "")})
	checkRandomShard(t, tree, []*CachedShard{}, []KeyRange{NewKeyRange(0, "h", "s")})
	checkRandomShard(t, tree, []*CachedShard{resB, resC}, []KeyRange{NewKeyRange(0, "a", "z")})
}

func TestRandomShardDiscontinuous(t *testing.T) {
	tree := newShardTree()
	r := tree.RandomShard([]KeyRange{NewKeyRange(0, "c", "f")})
	assert.Nil(t, r)

	// test for single resource
	resA := NewTestCachedShard([]byte("c"), []byte("f"))
	tree.update(resA)
	ra := tree.RandomShard([]KeyRange{NewKeyRange(0, "c", "e")})
	assert.Nil(t, ra)

	ra = tree.RandomShard([]KeyRange{NewKeyRange(0, "c", "f")})
	assert.True(t, reflect.DeepEqual(ra, resA))

	ra = tree.RandomShard([]KeyRange{NewKeyRange(0, "c", "g")})
	assert.True(t, reflect.DeepEqual(ra, resA))

	ra = tree.RandomShard([]KeyRange{NewKeyRange(0, "a", "e")})
	assert.Nil(t, ra)

	ra = tree.RandomShard([]KeyRange{NewKeyRange(0, "a", "f")})
	assert.True(t, reflect.DeepEqual(ra, resA))

	ra = tree.RandomShard([]KeyRange{NewKeyRange(0, "a", "g")})
	assert.True(t, reflect.DeepEqual(ra, resA))

	resB := NewTestCachedShard([]byte("n"), []byte("x"))
	tree.update(resB)
	rb := tree.RandomShard([]KeyRange{NewKeyRange(0, "g", "x")})
	assert.True(t, reflect.DeepEqual(rb, resB))

	rb = tree.RandomShard([]KeyRange{NewKeyRange(0, "g", "y")})
	assert.True(t, reflect.DeepEqual(rb, resB))

	rb = tree.RandomShard([]KeyRange{NewKeyRange(0, "n", "y")})
	assert.True(t, reflect.DeepEqual(rb, resB))

	rb = tree.RandomShard([]KeyRange{NewKeyRange(0, "o", "y")})
	assert.Nil(t, rb)

	resC := NewTestCachedShard([]byte("z"), []byte(""))
	tree.update(resC)

	rc := tree.RandomShard([]KeyRange{NewKeyRange(0, "y", "")})
	assert.True(t, reflect.DeepEqual(rc, resC))

	resD := NewTestCachedShard([]byte(""), []byte("a"))
	tree.update(resD)

	rd := tree.RandomShard([]KeyRange{NewKeyRange(0, "", "b")})
	assert.True(t, reflect.DeepEqual(rd, resD))

	checkRandomShard(t, tree, []*CachedShard{resA, resB, resC, resD}, []KeyRange{NewKeyRange(0, "", "")})
}

func updateTestShards(t *testing.T, tree *resourceTree, resources []*CachedShard) {
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

func checkRandomShard(t *testing.T, tree *resourceTree, resources []*CachedShard, ranges []KeyRange) {
	keys := make(map[string]struct{})
	for i := 0; i < 10000 && len(keys) < len(resources); i++ {
		re := tree.RandomShard(ranges)
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
