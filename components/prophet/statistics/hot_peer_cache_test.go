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

package statistics

import (
	"math/rand"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestContainerTimeUnsync(t *testing.T) {
	cache := newHotContainersStats(WriteFlow)
	peers := newPeers(3,
		func(i int) uint64 { return uint64(10000 + i) },
		func(i int) uint64 { return uint64(i) })
	meta := &metadata.TestResource{
		ResID:    1000,
		ResPeers: peers,
		Start:    []byte(""),
		End:      []byte(""),
		ResEpoch: metapb.ResourceEpoch{ConfVer: 6, Version: 6},
	}
	intervals := []uint64{120, 60}
	for _, interval := range intervals {
		resource := core.NewCachedResource(meta, &peers[0],
			// interval is [0, interval]
			core.SetReportInterval(interval),
			core.SetWrittenBytes(interval*100*1024))

		checkAndUpdate(t, cache, resource, 3)
		{
			stats := cache.ResourceStats(0)
			assert.Equal(t, 3, len(stats))
			for _, s := range stats {
				assert.Equal(t, 1, len(s))
			}
		}
	}
}

type operator int

const (
	transferLeader operator = iota
	movePeer
	addReplica
)

type testCacheCase struct {
	kind     FlowKind
	operator operator
	expect   int
}

func TestCache(t *testing.T) {
	tests := []*testCacheCase{
		{ReadFlow, transferLeader, 2},
		{ReadFlow, movePeer, 1},
		{ReadFlow, addReplica, 1},
		{WriteFlow, transferLeader, 3},
		{WriteFlow, movePeer, 4},
		{WriteFlow, addReplica, 4},
	}
	for _, c := range tests {
		testCache(t, c)
	}
}

func testCache(t *testing.T, c *testCacheCase) {
	defaultSize := map[FlowKind]int{
		ReadFlow:  1, // only leader
		WriteFlow: 3, // all peers
	}
	cache := newHotContainersStats(c.kind)
	resource := buildresource(nil, nil, c.kind)
	checkAndUpdate(t, cache, resource, defaultSize[c.kind])
	checkHit(t, cache, resource, c.kind, false) // all peers are new

	srcContainer, resource := schedule(c.operator, resource, c.kind)
	res := checkAndUpdate(t, cache, resource, c.expect)
	checkHit(t, cache, resource, c.kind, true) // hit cache
	if c.expect != defaultSize[c.kind] {
		checkNeedDelete(t, res, srcContainer)
	}
}

func checkAndUpdate(t *testing.T, cache *hotPeerCache, resource *core.CachedResource, expect int) []*HotPeerStat {
	res := cache.CheckResourceFlow(resource)
	assert.Equal(t, expect, len(res))
	for _, p := range res {
		cache.Update(p)
	}
	return res
}

func checkHit(t *testing.T, cache *hotPeerCache, resource *core.CachedResource, kind FlowKind, isHit bool) {
	var peers []metapb.Peer
	if kind == ReadFlow {
		peers = []metapb.Peer{*resource.GetLeader()}
	} else {
		peers = resource.Meta.Peers()
	}
	for _, peer := range peers {
		item := cache.getOldHotPeerStat(resource.Meta.ID(), peer.ContainerID)
		assert.NotNil(t, item)
		assert.Equal(t, !isHit, item.isNew)
	}
}

func checkNeedDelete(t *testing.T, ret []*HotPeerStat, ContainerID uint64) {
	for _, item := range ret {
		if item.ContainerID == ContainerID {
			assert.True(t, item.needDelete)
			return
		}
	}
}

func schedule(operator operator, resource *core.CachedResource, kind FlowKind) (srcContainer uint64, _ *core.CachedResource) {
	switch operator {
	case transferLeader:
		_, newLeader := pickFollower(resource)
		return resource.GetLeader().ContainerID, buildresource(resource.Meta, &newLeader, kind)
	case movePeer:
		index, _ := pickFollower(resource)
		meta := resource.Meta.(*metadata.TestResource)
		srcContainer := meta.ResPeers[index].ContainerID
		meta.ResPeers[index] = metapb.Peer{ID: 4, ContainerID: 4}
		return srcContainer, buildresource(meta, resource.GetLeader(), kind)
	case addReplica:
		meta := resource.Meta.(*metadata.TestResource)
		meta.ResPeers = append(meta.ResPeers, metapb.Peer{ID: 4, ContainerID: 4})
		return 0, buildresource(meta, resource.GetLeader(), kind)
	default:
		return 0, nil
	}
}

func pickFollower(resource *core.CachedResource) (index int, peer metapb.Peer) {
	var dst int
	meta := resource.Meta.(*metadata.TestResource)

	for index, peer := range meta.ResPeers {
		if peer.ContainerID == resource.GetLeader().ContainerID {
			continue
		}
		dst = index
		if rand.Intn(2) == 0 {
			break
		}
	}
	return dst, meta.ResPeers[dst]
}

func buildresource(meta metadata.Resource, leader *metapb.Peer, kind FlowKind) *core.CachedResource {
	const interval = uint64(60)
	if meta == nil {
		peer1 := metapb.Peer{ID: 1, ContainerID: 1}
		peer2 := metapb.Peer{ID: 2, ContainerID: 2}
		peer3 := metapb.Peer{ID: 3, ContainerID: 3}

		meta = &metadata.TestResource{
			ResID:    1000,
			ResPeers: []metapb.Peer{peer1, peer2, peer3},
			Start:    []byte(""),
			End:      []byte(""),
			ResEpoch: metapb.ResourceEpoch{ConfVer: 6, Version: 6},
		}
		leader = &meta.Peers()[rand.Intn(3)]
	}

	switch kind {
	case ReadFlow:
		return core.NewCachedResource(meta, leader, core.SetReportInterval(interval),
			core.SetReadBytes(interval*100*1024))
	case WriteFlow:
		return core.NewCachedResource(meta, leader, core.SetReportInterval(interval),
			core.SetWrittenBytes(interval*100*1024))
	default:
		return nil
	}
}

type genID func(i int) uint64

func newPeers(n int, pid genID, sid genID) []metapb.Peer {
	peers := make([]metapb.Peer, 0, n)
	for i := 1; i <= n; i++ {
		peer := metapb.Peer{
			ID: pid(i),
		}
		peer.ContainerID = sid(i)
		peers = append(peers, peer)
	}
	return peers
}

func TestUpdateHotPeerStat(t *testing.T) {
	cache := newHotContainersStats(ReadFlow)

	// skip interval=0
	newItem := &HotPeerStat{needDelete: false, thresholds: [2]float64{0.0, 0.0}}
	newItem = cache.updateHotPeerStat(newItem, nil, 0, 0, 0)
	assert.Nil(t, newItem)

	// new peer, interval is larger than report interval, but no hot
	newItem = &HotPeerStat{needDelete: false, thresholds: [2]float64{1.0, 1.0}}
	newItem = cache.updateHotPeerStat(newItem, nil, 0, 0, 60*time.Second)
	assert.Nil(t, newItem)

	// new peer, interval is less than report interval
	newItem = &HotPeerStat{needDelete: false, thresholds: [2]float64{0.0, 0.0}}
	newItem = cache.updateHotPeerStat(newItem, nil, 60, 60, 30*time.Second)
	assert.NotNil(t, newItem)
	assert.Equal(t, 0, newItem.HotDegree)
	assert.Equal(t, 0, newItem.AntiCount)
	// sum of interval is less than report interval
	oldItem := newItem
	newItem = cache.updateHotPeerStat(newItem, oldItem, 60, 60, 10*time.Second)
	assert.Equal(t, 0, newItem.HotDegree)
	assert.Equal(t, 0, newItem.AntiCount)
	// sum of interval is larger than report interval, and hot
	oldItem = newItem
	newItem = cache.updateHotPeerStat(newItem, oldItem, 60, 60, 30*time.Second)
	assert.Equal(t, 1, newItem.HotDegree)
	assert.Equal(t, 2, newItem.AntiCount)
	// sum of interval is less than report interval
	oldItem = newItem
	newItem = cache.updateHotPeerStat(newItem, oldItem, 60, 60, 10*time.Second)
	assert.Equal(t, 1, newItem.HotDegree)
	assert.Equal(t, 2, newItem.AntiCount)
	// sum of interval is larger than report interval, and hot
	oldItem = newItem
	newItem = cache.updateHotPeerStat(newItem, oldItem, 60, 60, 50*time.Second)
	assert.Equal(t, 2, newItem.HotDegree)
	assert.Equal(t, 2, newItem.AntiCount)
	// sum of interval is larger than report interval, and cold
	oldItem = newItem
	newItem.thresholds = [2]float64{10.0, 10.0}
	newItem = cache.updateHotPeerStat(newItem, oldItem, 60, 60, 60*time.Second)
	assert.Equal(t, 1, newItem.HotDegree)
	assert.Equal(t, 1, newItem.AntiCount)
	// sum of interval is larger than report interval, and cold
	oldItem = newItem
	newItem = cache.updateHotPeerStat(newItem, oldItem, 60, 60, 60*time.Second)
	assert.Equal(t, 0, newItem.HotDegree)
	assert.Equal(t, 0, newItem.AntiCount)
	assert.True(t, newItem.needDelete)
}

func TestThresholdWithUpdateHotPeerStat(t *testing.T) {
	byteRate := minHotThresholds[ReadFlow][byteDim] * 2
	expectThreshold := byteRate * HotThresholdRatio
	testMetrics(t, 120., byteRate, expectThreshold)
	testMetrics(t, 60., byteRate, expectThreshold)
	testMetrics(t, 30., byteRate, expectThreshold)
	testMetrics(t, 17., byteRate, expectThreshold)
	testMetrics(t, 1., byteRate, expectThreshold)
}
func testMetrics(t *testing.T, interval, byteRate, expectThreshold float64) {
	cache := newHotContainersStats(ReadFlow)
	minThresholds := minHotThresholds[cache.kind]
	containerID := uint64(1)
	assert.True(t, byteRate >= minThresholds[byteDim])
	for i := uint64(1); i < TopNN+10; i++ {
		var oldItem *HotPeerStat
		for {
			thresholds := cache.calcHotThresholds(containerID)
			newItem := &HotPeerStat{
				ContainerID: containerID,
				ResourceID:  i,
				needDelete:  false,
				thresholds:  thresholds,
				ByteRate:    byteRate,
				KeyRate:     0,
			}
			oldItem = cache.getOldHotPeerStat(i, containerID)
			if oldItem != nil && oldItem.rollingByteRate.isHot(thresholds) == true {
				break
			}
			item := cache.updateHotPeerStat(newItem, oldItem, byteRate*interval, 0, time.Duration(interval)*time.Second)
			cache.Update(item)
		}
		thresholds := cache.calcHotThresholds(containerID)
		if i < TopNN {
			assert.Equal(t, minThresholds[byteDim], thresholds[byteDim])
		} else {
			assert.Equal(t, expectThreshold, thresholds[byteDim])
		}
	}
}
