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
	opt         *config.PersistOptions
	stats       map[ResourceStatisticType]map[uint64]*core.CachedResource
	index       map[uint64]ResourceStatisticType
	ruleManager *placement.RuleManager
}

// NewResourceStatistics creates a new ResourceStatistics.
func NewResourceStatistics(opt *config.PersistOptions, ruleManager *placement.RuleManager) *ResourceStatistics {
	r := &ResourceStatistics{
		opt:   opt,
		stats: make(map[ResourceStatisticType]map[uint64]*core.CachedResource),
		index: make(map[uint64]ResourceStatisticType),
	}
	r.stats[MissPeer] = make(map[uint64]*core.CachedResource)
	r.stats[ExtraPeer] = make(map[uint64]*core.CachedResource)
	r.stats[DownPeer] = make(map[uint64]*core.CachedResource)
	r.stats[PendingPeer] = make(map[uint64]*core.CachedResource)
	r.stats[OfflinePeer] = make(map[uint64]*core.CachedResource)
	r.stats[LearnerPeer] = make(map[uint64]*core.CachedResource)
	r.stats[EmptyResource] = make(map[uint64]*core.CachedResource)
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

func (r *ResourceStatistics) deleteEntry(deleteIndex ResourceStatisticType, resID uint64) {
	for typ := ResourceStatisticType(1); typ <= deleteIndex; typ <<= 1 {
		if deleteIndex&typ != 0 {
			delete(r.stats[typ], resID)
		}
	}
}

// Observe records the current resources' status.
func (r *ResourceStatistics) Observe(res *core.CachedResource, containers []*core.CachedContainer) {
	// Resource state.
	resID := res.Meta.ID()
	var (
		peerTypeIndex ResourceStatisticType
		deleteIndex   ResourceStatisticType
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

	if len(res.Meta.Peers()) < desiredReplicas {
		r.stats[MissPeer][resID] = res
		peerTypeIndex |= MissPeer
	} else if len(res.Meta.Peers()) > desiredReplicas {
		r.stats[ExtraPeer][resID] = res
		peerTypeIndex |= ExtraPeer
	}

	if len(res.GetDownPeers()) > 0 {
		r.stats[DownPeer][resID] = res
		peerTypeIndex |= DownPeer
	}

	if len(res.GetPendingPeers()) > 0 {
		r.stats[PendingPeer][resID] = res
		peerTypeIndex |= PendingPeer
	}

	if len(res.GetLearners()) > 0 {
		r.stats[LearnerPeer][resID] = res
		peerTypeIndex |= LearnerPeer
	}

	if res.GetApproximateSize() <= core.EmptyResourceApproximateSize {
		r.stats[EmptyResource][resID] = res
		peerTypeIndex |= EmptyResource
	}

	for _, container := range containers {
		if container.IsOffline() {
			if _, ok := res.GetContainerPeer(container.Meta.ID()); ok {
				r.stats[OfflinePeer][resID] = res
				peerTypeIndex |= OfflinePeer
			}
		}
	}

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
	resourceStatusGauge.WithLabelValues("offline-peer-resource-count").Set(float64(len(r.stats[OfflinePeer])))
	resourceStatusGauge.WithLabelValues("learner-peer-resource-count").Set(float64(len(r.stats[LearnerPeer])))
	resourceStatusGauge.WithLabelValues("empty-resource-count").Set(float64(len(r.stats[EmptyResource])))
}

// Reset resets the metrics of the resources' status.
func (r *ResourceStatistics) Reset() {
	resourceStatusGauge.Reset()
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
		l.counterDec(label)
	}
	l.resourceLabelStats[resID] = resourceIsolation
	l.counterInc(resourceIsolation)
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
func (l *LabelStatistics) ClearDefunctResource(resID uint64, labels []string) {
	if label, ok := l.resourceLabelStats[resID]; ok {
		l.counterDec(label)
		delete(l.resourceLabelStats, resID)
	}
}

func (l *LabelStatistics) counterInc(label string) {
	if label == nonIsolation {
		l.labelCounter[nonIsolation]++
	} else {
		l.labelCounter[label]++
	}
}

func (l *LabelStatistics) counterDec(label string) {
	if label == nonIsolation {
		l.labelCounter[nonIsolation]--
	} else {
		l.labelCounter[label]--
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
	m := make(map[string][]*core.CachedContainer)
	for _, s := range containers {
		labelValue := s.GetLabelValue(label)
		if labelValue == "" {
			continue
		}
		m[labelValue] = append(m[labelValue], s)
	}
	var res [][]*core.CachedContainer
	for _, caches := range m {
		if len(caches) > 1 {
			res = append(res, caches)
		}
	}
	return res
}
