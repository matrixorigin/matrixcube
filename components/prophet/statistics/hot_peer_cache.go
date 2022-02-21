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
	"math"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/util/movingaverage"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

const (
	// TopNN is the threshold which means we can get hot threshold from store.
	TopNN = 60
	// HotThresholdRatio is used to calculate hot thresholds
	HotThresholdRatio = 0.8
	topNTTL           = 3 * ShardHeartBeatReportInterval * time.Second

	rollingWindowsSize = 5

	// HotShardReportMinInterval is used for the simulator and test
	HotShardReportMinInterval = 3

	hotShardAntiCount = 2
)

var (
	minHotThresholds = [2][dimLen]float64{
		WriteFlow: {
			byteDim: 1 * 1024,
			keyDim:  32,
		},
		ReadFlow: {
			byteDim: 8 * 1024,
			keyDim:  128,
		},
	}
)

// hotPeerCache saves the hot peer's statistics.
type hotPeerCache struct {
	kind                 FlowKind
	peersOfStore     map[uint64]*TopN               // containerID -> hot peers
	containersOfShard map[uint64]map[uint64]struct{} // resourceID -> containerIDs
}

func newHotStoresStats(kind FlowKind) *hotPeerCache {
	return &hotPeerCache{
		kind:                 kind,
		peersOfStore:     make(map[uint64]*TopN),
		containersOfShard: make(map[uint64]map[uint64]struct{}),
	}
}

// ShardStats returns hot items
func (f *hotPeerCache) ShardStats(minHotDegree int) map[uint64][]*HotPeerStat {
	res := make(map[uint64][]*HotPeerStat)
	for storeID, peers := range f.peersOfStore {
		values := peers.GetAll()
		stat := make([]*HotPeerStat, 0, len(values))
		for _, v := range values {
			if peer := v.(*HotPeerStat); peer.HotDegree >= minHotDegree {
				stat = append(stat, peer)
			}
		}
		res[storeID] = stat
	}
	return res
}

// Update updates the items in statistics.
func (f *hotPeerCache) Update(item *HotPeerStat) {
	if item.IsNeedDelete() {
		if peers, ok := f.peersOfStore[item.StoreID]; ok {
			peers.Remove(item.ShardID)
		}

		if containers, ok := f.containersOfShard[item.ShardID]; ok {
			delete(containers, item.StoreID)
		}
	} else {
		peers, ok := f.peersOfStore[item.StoreID]
		if !ok {
			peers = NewTopN(dimLen, TopNN, topNTTL)
			f.peersOfStore[item.StoreID] = peers
		}
		peers.Put(item)

		containers, ok := f.containersOfShard[item.ShardID]
		if !ok {
			containers = make(map[uint64]struct{})
			f.containersOfShard[item.ShardID] = containers
		}
		containers[item.StoreID] = struct{}{}
	}
}

func (f *hotPeerCache) collectShardMetrics(byteRate, keyRate float64, interval uint64) {
	resourceHeartbeatIntervalHist.Observe(float64(interval))
	if interval == 0 {
		return
	}
	if f.kind == ReadFlow {
		readByteHist.Observe(byteRate)
		readKeyHist.Observe(keyRate)
	}
	if f.kind == WriteFlow {
		writeByteHist.Observe(byteRate)
		writeKeyHist.Observe(keyRate)
	}
}

// CheckShardFlow checks the flow information of resource.
func (f *hotPeerCache) CheckShardFlow(res *core.CachedShard) (ret []*HotPeerStat) {
	bytes := float64(f.getShardBytes(res))
	keys := float64(f.getShardKeys(res))

	reportInterval := res.GetInterval()
	interval := reportInterval.GetEnd() - reportInterval.GetStart()

	byteRate := bytes / float64(interval)
	keyRate := keys / float64(interval)

	f.collectShardMetrics(byteRate, keyRate, interval)

	// old resource is in the front and new resource is in the back
	// which ensures it will hit the cache if moving peer or transfer leader occurs with the same replica number

	var peers []uint64
	for _, peer := range res.Meta.Peers() {
		peers = append(peers, peer.StoreID)
	}

	var tmpItem *HotPeerStat
	containerIDs := f.getAllStoreIDs(res)
	justTransferLeader := f.justTransferLeader(res)
	for _, containerID := range containerIDs {
		isExpired := f.isShardExpired(res, containerID) // transfer read leader or remove write peer
		oldItem := f.getOldHotPeerStat(res.Meta.ID(), containerID)
		if isExpired && oldItem != nil { // it may has been moved to other container, we save it to tmpItem
			tmpItem = oldItem
		}

		// This is used for the simulator and test. Ignore if report too fast.
		if !isExpired && Denoising && interval < HotShardReportMinInterval {
			continue
		}

		thresholds := f.calcHotThresholds(containerID)

		newItem := &HotPeerStat{
			StoreID:        containerID,
			ShardID:         res.Meta.ID(),
			Kind:               f.kind,
			ByteRate:           byteRate,
			KeyRate:            keyRate,
			LastUpdateTime:     time.Now(),
			needDelete:         isExpired,
			isLeader:           res.GetLeader().GetStoreID() == containerID,
			justTransferLeader: justTransferLeader,
			interval:           interval,
			peers:              peers,
			thresholds:         thresholds,
		}

		if oldItem == nil {
			if tmpItem != nil { // use the tmpItem cached from the container where this resource was in before
				oldItem = tmpItem
			} else { // new item is new peer after adding replica
				for _, containerID := range containerIDs {
					oldItem = f.getOldHotPeerStat(res.Meta.ID(), containerID)
					if oldItem != nil {
						break
					}
				}
			}
		}

		newItem = f.updateHotPeerStat(newItem, oldItem, bytes, keys, time.Duration(interval)*time.Second)
		if newItem != nil {
			ret = append(ret, newItem)
		}
	}

	return ret
}

func (f *hotPeerCache) IsShardHot(res *core.CachedShard, hotDegree int) bool {
	switch f.kind {
	case WriteFlow:
		return f.isShardHotWithAnyPeers(res, hotDegree)
	case ReadFlow:
		return f.isShardHotWithPeer(res, res.GetLeader(), hotDegree)
	}
	return false
}

func (f *hotPeerCache) CollectMetrics(typ string) {
	for containerID, peers := range f.peersOfStore {
		container := containerTag(containerID)
		thresholds := f.calcHotThresholds(containerID)
		hotCacheStatusGauge.WithLabelValues("total_length", container, typ).Set(float64(peers.Len()))
		hotCacheStatusGauge.WithLabelValues("byte-rate-threshold", container, typ).Set(thresholds[byteDim])
		hotCacheStatusGauge.WithLabelValues("key-rate-threshold", container, typ).Set(thresholds[keyDim])
		// for compatibility
		hotCacheStatusGauge.WithLabelValues("hotThreshold", container, typ).Set(thresholds[byteDim])
	}
}

func (f *hotPeerCache) getShardBytes(res *core.CachedShard) uint64 {
	switch f.kind {
	case WriteFlow:
		return res.GetBytesWritten()
	case ReadFlow:
		return res.GetBytesRead()
	}
	return 0
}

func (f *hotPeerCache) getShardKeys(res *core.CachedShard) uint64 {
	switch f.kind {
	case WriteFlow:
		return res.GetKeysWritten()
	case ReadFlow:
		return res.GetKeysRead()
	}
	return 0
}

func (f *hotPeerCache) getOldHotPeerStat(resID, containerID uint64) *HotPeerStat {
	if hotPeers, ok := f.peersOfStore[containerID]; ok {
		if v := hotPeers.Get(resID); v != nil {
			return v.(*HotPeerStat)
		}
	}
	return nil
}

func (f *hotPeerCache) isShardExpired(res *core.CachedShard, containerID uint64) bool {
	switch f.kind {
	case WriteFlow:
		_, ok := res.GetStorePeer(containerID)
		return !ok
	case ReadFlow:
		return res.GetLeader().GetStoreID() != containerID
	}
	return false
}

func (f *hotPeerCache) calcHotThresholds(containerID uint64) [dimLen]float64 {
	minThresholds := minHotThresholds[f.kind]
	tn, ok := f.peersOfStore[containerID]
	if !ok || tn.Len() < TopNN {
		return minThresholds
	}
	ret := [dimLen]float64{
		byteDim: tn.GetTopNMin(byteDim).(*HotPeerStat).GetByteRate(),
		keyDim:  tn.GetTopNMin(keyDim).(*HotPeerStat).GetKeyRate(),
	}
	for k := 0; k < dimLen; k++ {
		ret[k] = math.Max(ret[k]*HotThresholdRatio, minThresholds[k])
	}
	return ret
}

// gets the containerIDs, including old resource and new resource
func (f *hotPeerCache) getAllStoreIDs(res *core.CachedShard) []uint64 {
	containerIDs := make(map[uint64]struct{})
	ret := make([]uint64, 0, len(res.Meta.Peers()))
	// old containers
	ids, ok := f.containersOfShard[res.Meta.ID()]
	if ok {
		for containerID := range ids {
			containerIDs[containerID] = struct{}{}
			ret = append(ret, containerID)
		}
	}

	// new containers
	for _, peer := range res.Meta.Peers() {
		// ReadFlow no need consider the followers.
		if f.kind == ReadFlow && peer.StoreID != res.GetLeader().GetStoreID() {
			continue
		}
		if _, ok := containerIDs[peer.StoreID]; !ok {
			containerIDs[peer.StoreID] = struct{}{}
			ret = append(ret, peer.StoreID)
		}
	}

	return ret
}

func (f *hotPeerCache) isOldColdPeer(oldItem *HotPeerStat, storeID uint64) bool {
	isOldPeer := func() bool {
		for _, id := range oldItem.peers {
			if id == storeID {
				return true
			}
		}
		return false
	}
	noInCache := func() bool {
		ids, ok := f.containersOfShard[oldItem.ShardID]
		if ok {
			for id := range ids {
				if id == storeID {
					return false
				}
			}
		}
		return true
	}
	return isOldPeer() && noInCache()
}

func (f *hotPeerCache) justTransferLeader(res *core.CachedShard) bool {
	ids, ok := f.containersOfShard[res.Meta.ID()]
	if ok {
		for containerID := range ids {
			oldItem := f.getOldHotPeerStat(res.Meta.ID(), containerID)
			if oldItem == nil {
				continue
			}
			if oldItem.isLeader {
				return oldItem.StoreID != res.GetLeader().GetStoreID()
			}
		}
	}
	return false
}

func (f *hotPeerCache) isShardHotWithAnyPeers(res *core.CachedShard, hotDegree int) bool {
	for _, peer := range res.Meta.Peers() {
		if f.isShardHotWithPeer(res, &peer, hotDegree) {
			return true
		}
	}
	return false
}

func (f *hotPeerCache) isShardHotWithPeer(res *core.CachedShard, peer *metapb.Replica, hotDegree int) bool {
	if peer == nil {
		return false
	}
	containerID := peer.GetStoreID()
	if peers, ok := f.peersOfStore[containerID]; ok {
		if stat := peers.Get(res.Meta.ID()); stat != nil {
			return stat.(*HotPeerStat).HotDegree >= hotDegree
		}
	}
	return false
}

func (f *hotPeerCache) getDefaultTimeMedian() *movingaverage.TimeMedian {
	return movingaverage.NewTimeMedian(DefaultAotSize, rollingWindowsSize, ShardHeartBeatReportInterval*time.Second)
}

func (f *hotPeerCache) updateHotPeerStat(newItem, oldItem *HotPeerStat, bytes, keys float64, interval time.Duration) *HotPeerStat {
	if newItem.needDelete {
		return newItem
	}

	if oldItem == nil {
		if interval == 0 {
			return nil
		}
		isHot := bytes/interval.Seconds() >= newItem.thresholds[byteDim] || keys/interval.Seconds() >= newItem.thresholds[keyDim]
		if !isHot {
			return nil
		}
		if interval.Seconds() >= ShardHeartBeatReportInterval {
			newItem.HotDegree = 1
			newItem.AntiCount = hotShardAntiCount
		}
		newItem.isNew = true
		newItem.rollingByteRate = newDimStat(byteDim)
		newItem.rollingKeyRate = newDimStat(keyDim)
		newItem.rollingByteRate.Add(bytes, interval)
		newItem.rollingKeyRate.Add(keys, interval)
		if newItem.rollingKeyRate.isFull() {
			newItem.clearLastAverage()
		}
		return newItem
	}

	newItem.rollingByteRate = oldItem.rollingByteRate
	newItem.rollingKeyRate = oldItem.rollingKeyRate

	if newItem.justTransferLeader {
		// skip the first heartbeat flow statistic after transfer leader, because its statistics are calculated by the last leader in this store and are inaccurate
		// maintain anticount and hotdegree to avoid store threshold and hot peer are unstable.
		newItem.HotDegree = oldItem.HotDegree
		newItem.AntiCount = oldItem.AntiCount
		newItem.lastTransferLeaderTime = time.Now()
		return newItem
	}

	newItem.lastTransferLeaderTime = oldItem.lastTransferLeaderTime
	newItem.rollingByteRate.Add(bytes, interval)
	newItem.rollingKeyRate.Add(keys, interval)

	if !newItem.rollingKeyRate.isFull() {
		// not update hot degree and anti count
		newItem.HotDegree = oldItem.HotDegree
		newItem.AntiCount = oldItem.AntiCount
	} else {
		if f.isOldColdPeer(oldItem, newItem.StoreID) {
			if newItem.isFullAndHot() {
				newItem.HotDegree = 1
				newItem.AntiCount = hotShardAntiCount
			} else {
				newItem.needDelete = true
			}
		} else {
			if newItem.isFullAndHot() {
				newItem.HotDegree = oldItem.HotDegree + 1
				newItem.AntiCount = hotShardAntiCount
			} else {
				newItem.HotDegree = oldItem.HotDegree - 1
				newItem.AntiCount = oldItem.AntiCount - 1
				if newItem.AntiCount <= 0 {
					newItem.needDelete = true
				}
			}
		}
		newItem.clearLastAverage()
	}
	return newItem
}
