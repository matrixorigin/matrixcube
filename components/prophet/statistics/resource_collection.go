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
	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
)

// ShardStatisticType represents the type of the resource's status.
type ShardStatisticType uint32

// resource status type
const (
	MissPeer ShardStatisticType = 1 << iota
	ExtraPeer
	DownPeer
	PendingPeer
	OfflinePeer
	LearnerPeer
	EmptyShard
)

const nonIsolation = "none"

// ShardStatistics is used to record the status of resources.
type ShardStatistics struct {
	opt          *config.PersistOptions
	stats        map[ShardStatisticType]map[uint64]*core.CachedShard
	offlineStats map[ShardStatisticType]map[uint64]*core.CachedShard
	index        map[uint64]ShardStatisticType
	offlineIndex map[uint64]ShardStatisticType
	ruleManager  *placement.RuleManager
}

// NewShardStatistics creates a new ShardStatistics.
func NewShardStatistics(opt *config.PersistOptions, ruleManager *placement.RuleManager) *ShardStatistics {
	r := &ShardStatistics{
		opt:          opt,
		stats:        make(map[ShardStatisticType]map[uint64]*core.CachedShard),
		offlineStats: make(map[ShardStatisticType]map[uint64]*core.CachedShard),
		index:        make(map[uint64]ShardStatisticType),
		offlineIndex: make(map[uint64]ShardStatisticType),
	}
	r.stats[MissPeer] = make(map[uint64]*core.CachedShard)
	r.stats[ExtraPeer] = make(map[uint64]*core.CachedShard)
	r.stats[DownPeer] = make(map[uint64]*core.CachedShard)
	r.stats[PendingPeer] = make(map[uint64]*core.CachedShard)
	r.stats[OfflinePeer] = make(map[uint64]*core.CachedShard)
	r.stats[LearnerPeer] = make(map[uint64]*core.CachedShard)
	r.stats[EmptyShard] = make(map[uint64]*core.CachedShard)

	r.offlineStats[MissPeer] = make(map[uint64]*core.CachedShard)
	r.offlineStats[ExtraPeer] = make(map[uint64]*core.CachedShard)
	r.offlineStats[DownPeer] = make(map[uint64]*core.CachedShard)
	r.offlineStats[PendingPeer] = make(map[uint64]*core.CachedShard)
	r.offlineStats[LearnerPeer] = make(map[uint64]*core.CachedShard)
	r.offlineStats[EmptyShard] = make(map[uint64]*core.CachedShard)
	r.offlineStats[OfflinePeer] = make(map[uint64]*core.CachedShard)
	r.ruleManager = ruleManager
	return r
}

// GetShardStatsByType gets the status of the resource by types.
func (r *ShardStatistics) GetShardStatsByType(typ ShardStatisticType) []*core.CachedShard {
	res := make([]*core.CachedShard, 0, len(r.stats[typ]))
	for _, r := range r.stats[typ] {
		res = append(res, r)
	}
	return res
}

// GetOfflineShardStatsByType gets the status of the offline region by types.
func (r *ShardStatistics) GetOfflineShardStatsByType(typ ShardStatisticType) []*core.CachedShard {
	res := make([]*core.CachedShard, 0, len(r.stats[typ]))
	for _, r := range r.offlineStats[typ] {
		res = append(res, r)
	}
	return res
}

func (r *ShardStatistics) deleteEntry(deleteIndex ShardStatisticType, resID uint64) {
	for typ := ShardStatisticType(1); typ <= deleteIndex; typ <<= 1 {
		if deleteIndex&typ != 0 {
			delete(r.stats[typ], resID)
		}
	}
}

func (r *ShardStatistics) deleteOfflineEntry(deleteIndex ShardStatisticType, resID uint64) {
	for typ := ShardStatisticType(1); typ <= deleteIndex; typ <<= 1 {
		if deleteIndex&typ != 0 {
			delete(r.offlineStats[typ], resID)
		}
	}
}

// Observe records the current resources' status.
func (r *ShardStatistics) Observe(res *core.CachedShard, containers []*core.CachedStore) {
	// Shard state.
	resID := res.Meta.ID()
	var (
		peerTypeIndex        ShardStatisticType
		offlinePeerTypeIndex ShardStatisticType
		deleteIndex          ShardStatisticType
	)
	desiredReplicas := r.opt.GetMaxReplicas()
	if r.opt.IsPlacementRulesEnabled() {
		if !r.ruleManager.IsInitialized() {
			return
		}
		desiredReplicas = 0
		rules := r.ruleManager.GetRulesForApplyShard(res)
		for _, rule := range rules {
			desiredReplicas += rule.Count
		}
	}

	var isOffline bool

	for _, store := range containers {
		if store.IsOffline() {
			_, ok := res.GetStorePeer(store.Meta.ID())
			if ok {
				isOffline = true
				break
			}
		}
	}

	conditions := map[ShardStatisticType]bool{
		MissPeer:      len(res.Meta.Peers()) < desiredReplicas,
		ExtraPeer:     len(res.Meta.Peers()) > desiredReplicas,
		DownPeer:      len(res.GetDownPeers()) > 0,
		PendingPeer:   len(res.GetPendingPeers()) > 0,
		LearnerPeer:   len(res.GetLearners()) > 0,
		EmptyShard: res.GetApproximateSize() <= core.EmptyShardApproximateSize,
	}

	for typ, c := range conditions {
		if c {
			if isOffline {
				r.offlineStats[typ][resID] = res
				offlinePeerTypeIndex |= typ
			}
			r.stats[typ][resID] = res
			peerTypeIndex |= typ
		}
	}

	if isOffline {
		r.offlineStats[OfflinePeer][resID] = res
		offlinePeerTypeIndex |= OfflinePeer
	}

	if oldIndex, ok := r.offlineIndex[resID]; ok {
		deleteIndex = oldIndex &^ offlinePeerTypeIndex
	}
	r.deleteOfflineEntry(deleteIndex, resID)
	r.offlineIndex[resID] = offlinePeerTypeIndex

	if oldIndex, ok := r.index[resID]; ok {
		deleteIndex = oldIndex &^ peerTypeIndex
	}
	r.deleteEntry(deleteIndex, resID)
	r.index[resID] = peerTypeIndex
}

// ClearDefunctShard is used to handle the overlap resource.
func (r *ShardStatistics) ClearDefunctShard(resID uint64) {
	if oldIndex, ok := r.index[resID]; ok {
		r.deleteEntry(oldIndex, resID)
	}
}

// Collect collects the metrics of the resources' status.
func (r *ShardStatistics) Collect() {
	resourceStatusGauge.WithLabelValues("miss-peer-resource-count").Set(float64(len(r.stats[MissPeer])))
	resourceStatusGauge.WithLabelValues("extra-peer-resource-count").Set(float64(len(r.stats[ExtraPeer])))
	resourceStatusGauge.WithLabelValues("down-peer-resource-count").Set(float64(len(r.stats[DownPeer])))
	resourceStatusGauge.WithLabelValues("pending-peer-resource-count").Set(float64(len(r.stats[PendingPeer])))
	resourceStatusGauge.WithLabelValues("learner-peer-resource-count").Set(float64(len(r.stats[LearnerPeer])))
	resourceStatusGauge.WithLabelValues("empty-resource-count").Set(float64(len(r.stats[EmptyShard])))

	offlineShardStatusGauge.WithLabelValues("miss-peer-resource-count").Set(float64(len(r.offlineStats[MissPeer])))
	offlineShardStatusGauge.WithLabelValues("extra-peer-resource-count").Set(float64(len(r.offlineStats[ExtraPeer])))
	offlineShardStatusGauge.WithLabelValues("down-peer-resource-count").Set(float64(len(r.offlineStats[DownPeer])))
	offlineShardStatusGauge.WithLabelValues("pending-peer-resource-count").Set(float64(len(r.offlineStats[PendingPeer])))
	offlineShardStatusGauge.WithLabelValues("learner-peer-resource-count").Set(float64(len(r.offlineStats[LearnerPeer])))
	offlineShardStatusGauge.WithLabelValues("empty-resource-count").Set(float64(len(r.offlineStats[EmptyShard])))
	offlineShardStatusGauge.WithLabelValues("offline-peer-resource-count").Set(float64(len(r.offlineStats[OfflinePeer])))
}

// Reset resets the metrics of the resources' status.
func (r *ShardStatistics) Reset() {
	resourceStatusGauge.Reset()
	offlineShardStatusGauge.Reset()
}

// LabelStatistics is the statistics of the level of labels.
type LabelStatistics struct {
	resourceLabelStats map[uint64]string
	labelCounter       map[string]int
}

// NewLabelStatistics creates a new LabelStatistics.
func NewLabelStatistics() *LabelStatistics {
	return &LabelStatistics{
		resourceLabelStats: make(map[uint64]string),
		labelCounter:       make(map[string]int),
	}
}

// Observe records the current label status.
func (l *LabelStatistics) Observe(res *core.CachedShard, containers []*core.CachedStore, labels []string) {
	resID := res.Meta.ID()
	resourceIsolation := getShardLabelIsolation(containers, labels)
	if label, ok := l.resourceLabelStats[resID]; ok {
		if label == resourceIsolation {
			return
		}
		l.labelCounter[label]--
	}
	l.resourceLabelStats[resID] = resourceIsolation
	l.labelCounter[resourceIsolation]++
}

// Collect collects the metrics of the label status.
func (l *LabelStatistics) Collect() {
	for level, count := range l.labelCounter {
		resourceLabelLevelGauge.WithLabelValues(level).Set(float64(count))
	}
}

// Reset resets the metrics of the label status.
func (l *LabelStatistics) Reset() {
	resourceLabelLevelGauge.Reset()
}

// ClearDefunctShard is used to handle the overlap resource.
func (l *LabelStatistics) ClearDefunctShard(resID uint64) {
	if label, ok := l.resourceLabelStats[resID]; ok {
		l.labelCounter[label]--
		delete(l.resourceLabelStats, resID)
	}
}

func getShardLabelIsolation(containers []*core.CachedStore, labels []string) string {
	if len(containers) == 0 || len(labels) == 0 {
		return nonIsolation
	}
	queueStores := [][]*core.CachedStore{containers}
	for level, label := range labels {
		newQueueStores := make([][]*core.CachedStore, 0, len(containers))
		for _, containers := range queueStores {
			notIsolatedStores := notIsolatedStoresWithLabel(containers, label)
			if len(notIsolatedStores) > 0 {
				newQueueStores = append(newQueueStores, notIsolatedStores...)
			}
		}
		queueStores = newQueueStores
		if len(queueStores) == 0 {
			return labels[level]
		}
	}
	return nonIsolation
}

func notIsolatedStoresWithLabel(containers []*core.CachedStore, label string) [][]*core.CachedStore {
	var emptyValueStores []*core.CachedStore
	valueStoresMap := make(map[string][]*core.CachedStore)

	for _, s := range containers {
		labelValue := s.GetLabelValue(label)
		if labelValue == "" {
			emptyValueStores = append(emptyValueStores, s)
		} else {
			valueStoresMap[labelValue] = append(valueStoresMap[labelValue], s)
		}
	}

	if len(valueStoresMap) == 0 {
		// Usually it is because all TiKVs lack this label.
		if len(emptyValueStores) > 1 {
			return [][]*core.CachedStore{emptyValueStores}
		}
		return nil
	}

	var res [][]*core.CachedStore
	if len(emptyValueStores) == 0 {
		// No TiKV lacks this label.
		for _, stores := range valueStoresMap {
			if len(stores) > 1 {
				res = append(res, stores)
			}
		}
	} else {
		// Usually it is because some TiKVs lack this label.
		// The TiKVs in each label and the TiKVs without label form a group.
		for _, stores := range valueStoresMap {
			stores = append(stores, emptyValueStores...)
			res = append(res, stores)
		}
	}
	return res
}
