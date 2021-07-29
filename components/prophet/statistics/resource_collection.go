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
	"github.com/matrixorigin/matrixcube/components/prophet/util"
)

// ResourceStatisticType represents the type of the resource's status.
type ResourceStatisticType uint32

// resource status type
const (
	MissPeer ResourceStatisticType = 1 << iota
	ExtraPeer
	DownPeer
	PendingPeer
	OfflinePeer
	LearnerPeer
	EmptyResource
)

const nonIsolation = "none"

// ResourceStatistics is used to record the status of resources.
type ResourceStatistics struct {
	opt          *config.PersistOptions
	stats        map[ResourceStatisticType]map[uint64]*core.CachedResource
	offlineStats map[ResourceStatisticType]map[uint64]*core.CachedResource
	index        map[uint64]ResourceStatisticType
	offlineIndex map[uint64]ResourceStatisticType
	ruleManager  *placement.RuleManager
}

// NewResourceStatistics creates a new ResourceStatistics.
func NewResourceStatistics(opt *config.PersistOptions, ruleManager *placement.RuleManager) *ResourceStatistics {
	r := &ResourceStatistics{
		opt:          opt,
		stats:        make(map[ResourceStatisticType]map[uint64]*core.CachedResource),
		offlineStats: make(map[ResourceStatisticType]map[uint64]*core.CachedResource),
		index:        make(map[uint64]ResourceStatisticType),
		offlineIndex: make(map[uint64]ResourceStatisticType),
	}
	r.stats[MissPeer] = make(map[uint64]*core.CachedResource)
	r.stats[ExtraPeer] = make(map[uint64]*core.CachedResource)
	r.stats[DownPeer] = make(map[uint64]*core.CachedResource)
	r.stats[PendingPeer] = make(map[uint64]*core.CachedResource)
	r.stats[OfflinePeer] = make(map[uint64]*core.CachedResource)
	r.stats[LearnerPeer] = make(map[uint64]*core.CachedResource)
	r.stats[EmptyResource] = make(map[uint64]*core.CachedResource)

	r.offlineStats[MissPeer] = make(map[uint64]*core.CachedResource)
	r.offlineStats[ExtraPeer] = make(map[uint64]*core.CachedResource)
	r.offlineStats[DownPeer] = make(map[uint64]*core.CachedResource)
	r.offlineStats[PendingPeer] = make(map[uint64]*core.CachedResource)
	r.offlineStats[LearnerPeer] = make(map[uint64]*core.CachedResource)
	r.offlineStats[EmptyResource] = make(map[uint64]*core.CachedResource)
	r.offlineStats[OfflinePeer] = make(map[uint64]*core.CachedResource)
	r.ruleManager = ruleManager
	return r
}

// GetResourceStatsByType gets the status of the resource by types.
func (r *ResourceStatistics) GetResourceStatsByType(typ ResourceStatisticType) []*core.CachedResource {
	res := make([]*core.CachedResource, 0, len(r.stats[typ]))
	for _, r := range r.stats[typ] {
		res = append(res, r)
	}
	return res
}

// GetOfflineResourceStatsByType gets the status of the offline region by types.
func (r *ResourceStatistics) GetOfflineResourceStatsByType(typ ResourceStatisticType) []*core.CachedResource {
	res := make([]*core.CachedResource, 0, len(r.stats[typ]))
	for _, r := range r.offlineStats[typ] {
		res = append(res, r)
	}
	return res
}

func (r *ResourceStatistics) deleteEntry(deleteIndex ResourceStatisticType, resID uint64) {
	for typ := ResourceStatisticType(1); typ <= deleteIndex; typ <<= 1 {
		if deleteIndex&typ != 0 {
			delete(r.stats[typ], resID)
		}
	}
}

func (r *ResourceStatistics) deleteOfflineEntry(deleteIndex ResourceStatisticType, resID uint64) {
	for typ := ResourceStatisticType(1); typ <= deleteIndex; typ <<= 1 {
		if deleteIndex&typ != 0 {
			delete(r.offlineStats[typ], resID)
		}
	}
}

// Observe records the current resources' status.
func (r *ResourceStatistics) Observe(res *core.CachedResource, containers []*core.CachedContainer) {
	// Resource state.
	resID := res.Meta.ID()
	var (
		peerTypeIndex        ResourceStatisticType
		offlinePeerTypeIndex ResourceStatisticType
		deleteIndex          ResourceStatisticType
	)
	desiredReplicas := r.opt.GetMaxReplicas()
	if r.opt.IsPlacementRulesEnabled() {
		if !r.ruleManager.IsInitialized() {
			util.GetLogger().Warning("ruleManager haven't been initialized")
			return
		}
		desiredReplicas = 0
		rules := r.ruleManager.GetRulesForApplyResource(res)
		for _, rule := range rules {
			desiredReplicas += rule.Count
		}
	}

	var isOffline bool

	for _, store := range containers {
		if store.IsOffline() {
			_, ok := res.GetContainerPeer(store.Meta.ID())
			if ok {
				isOffline = true
				break
			}
		}
	}

	conditions := map[ResourceStatisticType]bool{
		MissPeer:      len(res.Meta.Peers()) < desiredReplicas,
		ExtraPeer:     len(res.Meta.Peers()) > desiredReplicas,
		DownPeer:      len(res.GetDownPeers()) > 0,
		PendingPeer:   len(res.GetPendingPeers()) > 0,
		LearnerPeer:   len(res.GetLearners()) > 0,
		EmptyResource: res.GetApproximateSize() <= core.EmptyResourceApproximateSize,
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

// ClearDefunctResource is used to handle the overlap resource.
func (r *ResourceStatistics) ClearDefunctResource(resID uint64) {
	if oldIndex, ok := r.index[resID]; ok {
		r.deleteEntry(oldIndex, resID)
	}
}

// Collect collects the metrics of the resources' status.
func (r *ResourceStatistics) Collect() {
	resourceStatusGauge.WithLabelValues("miss-peer-resource-count").Set(float64(len(r.stats[MissPeer])))
	resourceStatusGauge.WithLabelValues("extra-peer-resource-count").Set(float64(len(r.stats[ExtraPeer])))
	resourceStatusGauge.WithLabelValues("down-peer-resource-count").Set(float64(len(r.stats[DownPeer])))
	resourceStatusGauge.WithLabelValues("pending-peer-resource-count").Set(float64(len(r.stats[PendingPeer])))
	resourceStatusGauge.WithLabelValues("learner-peer-resource-count").Set(float64(len(r.stats[LearnerPeer])))
	resourceStatusGauge.WithLabelValues("empty-resource-count").Set(float64(len(r.stats[EmptyResource])))

	offlineResourceStatusGauge.WithLabelValues("miss-peer-resource-count").Set(float64(len(r.offlineStats[MissPeer])))
	offlineResourceStatusGauge.WithLabelValues("extra-peer-resource-count").Set(float64(len(r.offlineStats[ExtraPeer])))
	offlineResourceStatusGauge.WithLabelValues("down-peer-resource-count").Set(float64(len(r.offlineStats[DownPeer])))
	offlineResourceStatusGauge.WithLabelValues("pending-peer-resource-count").Set(float64(len(r.offlineStats[PendingPeer])))
	offlineResourceStatusGauge.WithLabelValues("learner-peer-resource-count").Set(float64(len(r.offlineStats[LearnerPeer])))
	offlineResourceStatusGauge.WithLabelValues("empty-resource-count").Set(float64(len(r.offlineStats[EmptyResource])))
	offlineResourceStatusGauge.WithLabelValues("offline-peer-resource-count").Set(float64(len(r.offlineStats[OfflinePeer])))
}

// Reset resets the metrics of the resources' status.
func (r *ResourceStatistics) Reset() {
	resourceStatusGauge.Reset()
	offlineResourceStatusGauge.Reset()
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
func (l *LabelStatistics) Observe(res *core.CachedResource, containers []*core.CachedContainer, labels []string) {
	resID := res.Meta.ID()
	resourceIsolation := getResourceLabelIsolation(containers, labels)
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

// ClearDefunctResource is used to handle the overlap resource.
func (l *LabelStatistics) ClearDefunctResource(resID uint64) {
	if label, ok := l.resourceLabelStats[resID]; ok {
		l.labelCounter[label]--
		delete(l.resourceLabelStats, resID)
	}
}

func getResourceLabelIsolation(containers []*core.CachedContainer, labels []string) string {
	if len(containers) == 0 || len(labels) == 0 {
		return nonIsolation
	}
	queueContainers := [][]*core.CachedContainer{containers}
	for level, label := range labels {
		newQueueContainers := make([][]*core.CachedContainer, 0, len(containers))
		for _, containers := range queueContainers {
			notIsolatedContainers := notIsolatedContainersWithLabel(containers, label)
			if len(notIsolatedContainers) > 0 {
				newQueueContainers = append(newQueueContainers, notIsolatedContainers...)
			}
		}
		queueContainers = newQueueContainers
		if len(queueContainers) == 0 {
			return labels[level]
		}
	}
	return nonIsolation
}

func notIsolatedContainersWithLabel(containers []*core.CachedContainer, label string) [][]*core.CachedContainer {
	var emptyValueStores []*core.CachedContainer
	valueStoresMap := make(map[string][]*core.CachedContainer)

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
			return [][]*core.CachedContainer{emptyValueStores}
		}
		return nil
	}

	var res [][]*core.CachedContainer
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
