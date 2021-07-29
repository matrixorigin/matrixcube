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

	"github.com/matrixorigin/matrixcube/components/prophet/core"
)

// Denoising is an option to calculate flow base on the real heartbeats. Should
// only turned off by the simulator and the test.
var Denoising = true

// HotCache is a cache hold hot resources.
type HotCache struct {
	writeFlow *hotPeerCache
	readFlow  *hotPeerCache
}

// NewHotCache creates a new hot spot cache.
func NewHotCache() *HotCache {
	return &HotCache{
		writeFlow: newHotContainersStats(WriteFlow),
		readFlow:  newHotContainersStats(ReadFlow),
	}
}

// CheckWrite checks the write status, returns update items.
func (w *HotCache) CheckWrite(res *core.CachedResource) []*HotPeerStat {
	return w.writeFlow.CheckResourceFlow(res)
}

// CheckRead checks the read status, returns update items.
func (w *HotCache) CheckRead(res *core.CachedResource) []*HotPeerStat {
	return w.readFlow.CheckResourceFlow(res)
}

// Update updates the cache.
func (w *HotCache) Update(item *HotPeerStat) {
	switch item.Kind {
	case WriteFlow:
		w.writeFlow.Update(item)
	case ReadFlow:
		w.readFlow.Update(item)
	}

	if item.IsNeedDelete() {
		w.incMetrics("remove_item", item.ContainerID, item.Kind)
	} else if item.IsNew() {
		w.incMetrics("add_item", item.ContainerID, item.Kind)
	} else {
		w.incMetrics("update_item", item.ContainerID, item.Kind)
	}
}

// ResourceStats returns hot items according to kind
func (w *HotCache) ResourceStats(kind FlowKind, minHotDegree int) map[uint64][]*HotPeerStat {
	switch kind {
	case WriteFlow:
		return w.writeFlow.ResourceStats(minHotDegree)
	case ReadFlow:
		return w.readFlow.ResourceStats(minHotDegree)
	}
	return nil
}

// RandHotResourceFromContainer random picks a hot resource in specify store.
func (w *HotCache) RandHotResourceFromContainer(storeID uint64, kind FlowKind, minHotDegree int) *HotPeerStat {
	if stats, ok := w.ResourceStats(kind, minHotDegree)[storeID]; ok && len(stats) > 0 {
		return stats[rand.Intn(len(stats))]
	}
	return nil
}

// IsResourceHot checks if the resource is hot.
func (w *HotCache) IsResourceHot(res *core.CachedResource, minHotDegree int) bool {
	return w.writeFlow.IsResourceHot(res, minHotDegree) ||
		w.readFlow.IsResourceHot(res, minHotDegree)
}

// CollectMetrics collects the hot cache metrics.
func (w *HotCache) CollectMetrics() {
	w.writeFlow.CollectMetrics("write")
	w.readFlow.CollectMetrics("read")
}

// ResetMetrics resets the hot cache metrics.
func (w *HotCache) ResetMetrics() {
	hotCacheStatusGauge.Reset()
}

func (w *HotCache) incMetrics(name string, containerID uint64, kind FlowKind) {
	container := containerTag(containerID)
	switch kind {
	case WriteFlow:
		hotCacheStatusGauge.WithLabelValues(name, container, "write").Inc()
	case ReadFlow:
		hotCacheStatusGauge.WithLabelValues(name, container, "read").Inc()
	}
}

// GetFilledPeriod returns filled period.
func (w *HotCache) GetFilledPeriod(kind FlowKind) int {
	switch kind {
	case WriteFlow:
		return w.writeFlow.getDefaultTimeMedian().GetFilledPeriod()
	case ReadFlow:
		return w.readFlow.getDefaultTimeMedian().GetFilledPeriod()
	}
	return 0
}
