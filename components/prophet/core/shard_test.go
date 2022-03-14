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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func newTestCachedShardWithID(id uint64) *CachedShard {
	res := &CachedShard{Meta: metapb.Shard{ID: id}}
	res.stats.ApproximateSize = id
	res.stats.ApproximateKeys = id
	return res
}

func TestSortedEqual(t *testing.T) {
	testcases := []struct {
		idsA    []uint64
		idsB    []uint64
		isEqual bool
	}{
		{
			[]uint64{},
			[]uint64{},
			true,
		},
		{
			[]uint64{},
			[]uint64{1, 2},
			false,
		},
		{
			[]uint64{1, 2},
			[]uint64{1, 2},
			true,
		},
		{
			[]uint64{1, 2},
			[]uint64{2, 1},
			true,
		},
		{
			[]uint64{1, 2},
			[]uint64{1, 2, 3},
			false,
		},
		{
			[]uint64{1, 2, 3},
			[]uint64{2, 3, 1},
			true,
		},
		{
			[]uint64{1, 3},
			[]uint64{1, 2},
			false,
		},
	}

	meta := metapb.Shard{
		ID: 100,
		Replicas: []metapb.Replica{
			{ID: 1, StoreID: 10},
			{ID: 3, StoreID: 30},
			{ID: 2, StoreID: 20},
			{ID: 4, StoreID: 40},
		},
	}

	res := NewCachedShard(meta, &meta.GetReplicas()[0])

	for _, tc := range testcases {
		downPeersA := make([]metapb.ReplicaStats, 0)
		downPeersB := make([]metapb.ReplicaStats, 0)
		pendingPeersA := make([]metapb.Replica, 0)
		pendingPeersB := make([]metapb.Replica, 0)
		for _, i := range tc.idsA {
			downPeersA = append(downPeersA, metapb.ReplicaStats{Replica: meta.GetReplicas()[i]})
			pendingPeersA = append(pendingPeersA, meta.GetReplicas()[i])
		}
		for _, i := range tc.idsB {
			downPeersB = append(downPeersB, metapb.ReplicaStats{Replica: meta.GetReplicas()[i]})
			pendingPeersB = append(pendingPeersB, meta.GetReplicas()[i])
		}

		resA := res.Clone(WithDownPeers(downPeersA), WithPendingPeers(pendingPeersA))
		resB := res.Clone(WithDownPeers(downPeersB), WithPendingPeers(pendingPeersB))
		assert.Equal(t, tc.isEqual, SortedPeersStatsEqual(resA.GetDownPeers(), resB.GetDownPeers()))
		assert.Equal(t, tc.isEqual, SortedPeersEqual(resA.GetPendingPeers(), resB.GetPendingPeers()))
	}
}

func TestShardMap(t *testing.T) {
	var empty *shardMap
	assert.Equal(t, 0, empty.Len(), "TestShardMap failed")
	assert.Nil(t, empty.Get(1), "TestShardMap failed")

	rm := newShardMap()
	checkShardMap(t, "TestShardMap failed", rm)
	rm.Put(newTestCachedShardWithID(1))
	checkShardMap(t, "TestShardMap failed", rm, 1)

	rm.Put(newTestCachedShardWithID(2))
	rm.Put(newTestCachedShardWithID(3))
	checkShardMap(t, "TestShardMap failed", rm, 1, 2, 3)

	rm.Put(newTestCachedShardWithID(3))
	rm.Delete(4)
	checkShardMap(t, "TestShardMap failed", rm, 1, 2, 3)

	rm.Delete(3)
	rm.Delete(1)
	checkShardMap(t, "TestShardMap failed", rm, 2)

	rm.Put(newTestCachedShardWithID(3))
	checkShardMap(t, "TestShardMap failed", rm, 2, 3)
}

func TestShardKey(t *testing.T) {
	cases := []struct {
		key    string
		expect string
	}{
		{`"t\x80\x00\x00\x00\x00\x00\x00\xff!_r\x80\x00\x00\x00\x00\xff\x02\u007fY\x00\x00\x00\x00\x00\xfa"`,
			`7480000000000000FF215F728000000000FF027F590000000000FA`},
		{"\"\\x80\\x00\\x00\\x00\\x00\\x00\\x00\\xff\\x05\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\xf8\"",
			`80000000000000FF0500000000000000F8`},
	}
	for _, c := range cases {
		got, err := strconv.Unquote(c.key)
		assert.NoError(t, err, "TestShardKey failed")

		// start key changed
		origin := NewCachedShard(metapb.Shard{End: []byte(got)}, nil)
		res := NewCachedShard(metapb.Shard{Start: []byte(got), End: []byte(got)}, nil)

		s := DiffShardKeyInfo(origin, res)
		assert.True(t, regexp.MustCompile("^.*StartKey Changed.*$").MatchString(s), "TestShardKey failed")
		assert.True(t, strings.Contains(s, c.expect), "TestShardKey failed")

		// end key changed
		origin = NewCachedShard(metapb.Shard{Start: []byte(got)}, nil)
		res = NewCachedShard(metapb.Shard{Start: []byte(got), End: []byte(got)}, nil)
		s = DiffShardKeyInfo(origin, res)
		assert.True(t, regexp.MustCompile(".*EndKey Changed.*").MatchString(s), "TestShardKey failed")
		assert.True(t, strings.Contains(s, c.expect), "TestShardKey failed")
	}
}

func TestSetShard(t *testing.T) {
	resources := NewCachedShards()
	for i := 0; i < 100; i++ {
		peer1 := metapb.Replica{StoreID: uint64(i%5 + 1), ID: uint64(i*5 + 1)}
		peer2 := metapb.Replica{StoreID: uint64((i+1)%5 + 1), ID: uint64(i*5 + 2)}
		peer3 := metapb.Replica{StoreID: uint64((i+2)%5 + 1), ID: uint64(i*5 + 3)}
		res := NewCachedShard(metapb.Shard{
			ID:       uint64(i + 1),
			Replicas: []metapb.Replica{peer1, peer2, peer3},
			Start:    []byte(fmt.Sprintf("%20d", i*10)),
			End:      []byte(fmt.Sprintf("%20d", (i+1)*10)),
		}, &peer1)
		resources.SetShard(res)
	}

	peer1 := metapb.Replica{StoreID: uint64(4), ID: uint64(101)}
	peer2 := metapb.Replica{StoreID: uint64(5), ID: uint64(102)}
	peer3 := metapb.Replica{StoreID: uint64(1), ID: uint64(103)}
	res := NewCachedShard(metapb.Shard{
		ID:       uint64(21),
		Replicas: []metapb.Replica{peer1, peer2, peer3},
		Start:    []byte(fmt.Sprintf("%20d", 184)),
		End:      []byte(fmt.Sprintf("%20d", 211)),
	}, &peer1)
	res.learners = append(res.learners, peer2)
	res.pendingReplicas = append(res.pendingReplicas, peer3)

	resources.SetShard(res)
	checkShards(t, resources, "TestSetShard failed")
	assert.Equal(t, 97, resources.trees[0].length(), "TestSetShard failed")
	assert.Equal(t, 97, len(resources.GetShards()), "TestSetShard failed")

	resources.SetShard(res)
	peer1 = metapb.Replica{StoreID: uint64(2), ID: uint64(101)}
	peer2 = metapb.Replica{StoreID: uint64(3), ID: uint64(102)}
	peer3 = metapb.Replica{StoreID: uint64(1), ID: uint64(103)}
	res = NewCachedShard(metapb.Shard{
		ID:       uint64(21),
		Replicas: []metapb.Replica{peer1, peer2, peer3},
		Start:    []byte(fmt.Sprintf("%20d", 184)),
		End:      []byte(fmt.Sprintf("%20d", 211)),
	}, &peer1)
	res.learners = append(res.learners, peer2)
	res.pendingReplicas = append(res.pendingReplicas, peer3)

	resources.SetShard(res)
	checkShards(t, resources, "TestSetShard failed")
	assert.Equal(t, 97, resources.trees[0].length(), "TestSetShard failed")
	assert.Equal(t, 97, len(resources.GetShards()), "TestSetShard failed")

	// Test remove overlaps.
	res = res.Clone(WithStartKey([]byte(fmt.Sprintf("%20d", 175))), WithNewShardID(201))
	assert.NotNil(t, resources.GetShard(21), "TestSetShard failed")
	assert.NotNil(t, resources.GetShard(18), "TestSetShard failed")
	resources.SetShard(res)
	checkShards(t, resources, "TestSetShard failed")
	assert.Equal(t, 96, resources.trees[0].length(), "TestSetShard failed")
	assert.Equal(t, 96, len(resources.GetShards()), "TestSetShard failed")
	assert.NotNil(t, resources.GetShard(201), "TestSetShard failed")
	assert.Nil(t, resources.GetShard(21), "TestSetShard failed")
	assert.Nil(t, resources.GetShard(18), "TestSetShard failed")

	// Test update keys and size of resource.
	res = res.Clone()
	res.stats.ApproximateKeys = 20
	res.stats.ApproximateSize = 30
	resources.SetShard(res)
	checkShards(t, resources, "TestSetShard failed")
	assert.Equal(t, 96, resources.trees[0].length(), "TestSetShard failed")
	assert.Equal(t, 96, len(resources.GetShards()), "TestSetShard failed")
	assert.NotNil(t, resources.GetShard(201), "TestSetShard failed")
	assert.Equal(t, int64(20), resources.shards.totalKeys, "TestSetShard failed")
	assert.Equal(t, int64(30), resources.shards.totalSize, "TestSetShard failed")
}

func TestShouldRemoveFromSubTree(t *testing.T) {
	resources := NewCachedShards()
	peer1 := metapb.Replica{StoreID: uint64(1), ID: uint64(1)}
	peer2 := metapb.Replica{StoreID: uint64(2), ID: uint64(2)}
	peer3 := metapb.Replica{StoreID: uint64(3), ID: uint64(3)}
	peer4 := metapb.Replica{StoreID: uint64(3), ID: uint64(3)}
	res := NewCachedShard(metapb.Shard{
		ID:       uint64(1),
		Replicas: []metapb.Replica{peer1, peer2, peer4},
		Start:    []byte(fmt.Sprintf("%20d", 10)),
		End:      []byte(fmt.Sprintf("%20d", 20)),
	}, &peer1)

	origin := NewCachedShard(metapb.Shard{
		ID:       uint64(2),
		Replicas: []metapb.Replica{peer1, peer2, peer3},
		Start:    []byte(fmt.Sprintf("%20d", 10)),
		End:      []byte(fmt.Sprintf("%20d", 20)),
	}, &peer1)
	assert.False(t, resources.shouldRemoveFromSubTree(res, origin))

	res.leader = &peer2
	assert.True(t, resources.shouldRemoveFromSubTree(res, origin))

	res.leader = &peer1
	res.pendingReplicas = append(res.pendingReplicas, peer4)
	assert.True(t, resources.shouldRemoveFromSubTree(res, origin))

	res.pendingReplicas = nil
	res.learners = append(res.learners, peer2)
	assert.True(t, resources.shouldRemoveFromSubTree(res, origin))

	origin.learners = append(origin.learners, peer3, peer2)
	res.learners = append(res.learners, peer4)
	assert.False(t, resources.shouldRemoveFromSubTree(res, origin))

	res.voters[2].StoreID = 4
	assert.True(t, resources.shouldRemoveFromSubTree(res, origin))
}

func checkShardMap(t *testing.T, msg string, rm *shardMap, ids ...uint64) {
	// Check Get.
	for _, id := range ids {
		assert.Equal(t, id, rm.Get(id).Meta.GetID(), msg)
	}

	// Check Len.
	assert.Equal(t, len(ids), rm.Len(), msg)

	// Check id set.
	set1 := make(map[uint64]struct{})
	for _, r := range rm.m {
		set1[r.Meta.GetID()] = struct{}{}
	}
	for _, id := range ids {
		_, ok := set1[id]
		assert.True(t, ok, msg)
	}

	// Check resource size.
	var total int64
	for _, id := range ids {
		total += int64(id)
	}
	assert.Equal(t, rm.totalSize, total, msg)
}

func checkShards(t *testing.T, resources *ShardsContainer, msg string) {
	leaderMap := make(map[uint64]uint64)
	followerMap := make(map[uint64]uint64)
	learnerMap := make(map[uint64]uint64)
	pendingPeerMap := make(map[uint64]uint64)
	for _, item := range resources.GetShards() {
		if leaderCount, ok := leaderMap[item.leader.StoreID]; ok {
			leaderMap[item.leader.StoreID] = leaderCount + 1
		} else {
			leaderMap[item.leader.StoreID] = 1
		}
		for _, follower := range item.GetFollowers() {
			if followerCount, ok := followerMap[follower.StoreID]; ok {
				followerMap[follower.StoreID] = followerCount + 1
			} else {
				followerMap[follower.StoreID] = 1
			}
		}
		for _, learner := range item.GetLearners() {
			if learnerCount, ok := learnerMap[learner.StoreID]; ok {
				learnerMap[learner.StoreID] = learnerCount + 1
			} else {
				learnerMap[learner.StoreID] = 1
			}
		}
		for _, pendingPeer := range item.GetPendingPeers() {
			if pendingPeerCount, ok := pendingPeerMap[pendingPeer.StoreID]; ok {
				pendingPeerMap[pendingPeer.StoreID] = pendingPeerCount + 1
			} else {
				pendingPeerMap[pendingPeer.StoreID] = 1
			}
		}
	}
	resources.maybeInitWithGroup("")
	for key, value := range resources.leaders[""] {
		assert.Equal(t, int(leaderMap[key]), value.length(), msg)
	}
	for key, value := range resources.followers[""] {
		assert.Equal(t, int(followerMap[key]), value.length(), msg)
	}
	for key, value := range resources.learners[""] {
		assert.Equal(t, int(learnerMap[key]), value.length(), msg)
	}
	for key, value := range resources.pendingReplicas[""] {
		assert.Equal(t, int(pendingPeerMap[key]), value.length(), msg)
	}
}
