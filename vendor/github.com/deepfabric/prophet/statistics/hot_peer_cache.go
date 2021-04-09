package statistics

import (
	"math"
	"time"

	"github.com/deepfabric/prophet/core"
	"github.com/deepfabric/prophet/pb/metapb"
	"github.com/deepfabric/prophet/util/movingaverage"
)

const (
	topNN                        = 60
	topNTTL                      = 3 * ResourceHeartBeatReportInterval * time.Second
	hotThresholdRatio            = 0.8
	rollingWindowsSize           = 5
	hotResourceReportMinInterval = 3
	hotResourceAntiCount         = 2
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
	peersOfContainer     map[uint64]*TopN               // containerID -> hot peers
	containersOfResource map[uint64]map[uint64]struct{} // resourceID -> containerIDs
}

func newHotContainersStats(kind FlowKind) *hotPeerCache {
	return &hotPeerCache{
		kind:                 kind,
		peersOfContainer:     make(map[uint64]*TopN),
		containersOfResource: make(map[uint64]map[uint64]struct{}),
	}
}

// ResourceStats returns hot items
func (f *hotPeerCache) ResourceStats() map[uint64][]*HotPeerStat {
	res := make(map[uint64][]*HotPeerStat)
	for containerID, peers := range f.peersOfContainer {
		values := peers.GetAll()
		stat := make([]*HotPeerStat, len(values))
		res[containerID] = stat
		for i := range values {
			stat[i] = values[i].(*HotPeerStat)
		}
	}
	return res
}

// Update updates the items in statistics.
func (f *hotPeerCache) Update(item *HotPeerStat) {
	if item.IsNeedDelete() {
		if peers, ok := f.peersOfContainer[item.ContainerID]; ok {
			peers.Remove(item.ResourceID)
		}

		if containers, ok := f.containersOfResource[item.ResourceID]; ok {
			delete(containers, item.ContainerID)
		}
	} else {
		peers, ok := f.peersOfContainer[item.ContainerID]
		if !ok {
			peers = NewTopN(dimLen, topNN, topNTTL)
			f.peersOfContainer[item.ContainerID] = peers
		}
		peers.Put(item)

		containers, ok := f.containersOfResource[item.ResourceID]
		if !ok {
			containers = make(map[uint64]struct{})
			f.containersOfResource[item.ResourceID] = containers
		}
		containers[item.ContainerID] = struct{}{}
	}
}

func (f *hotPeerCache) collectResourceMetrics(byteRate, keyRate float64, interval uint64) {
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

// CheckResourceFlow checks the flow information of resource.
func (f *hotPeerCache) CheckResourceFlow(res *core.CachedResource) (ret []*HotPeerStat) {
	bytes := float64(f.getResourceBytes(res))
	keys := float64(f.getResourceKeys(res))

	reportInterval := res.GetInterval()
	interval := reportInterval.GetEnd() - reportInterval.GetStart()

	byteRate := bytes / float64(interval)
	keyRate := keys / float64(interval)

	f.collectResourceMetrics(byteRate, keyRate, interval)

	// old resource is in the front and new resource is in the back
	// which ensures it will hit the cache if moving peer or transfer leader occurs with the same replica number

	var tmpItem *HotPeerStat
	containerIDs := f.getAllContainerIDs(res)
	for _, containerID := range containerIDs {
		isExpired := f.isResourceExpired(res, containerID) // transfer read leader or remove write peer
		oldItem := f.getOldHotPeerStat(res.Meta.ID(), containerID)
		if isExpired && oldItem != nil { // it may has been moved to other container, we save it to tmpItem
			tmpItem = oldItem
		}

		// This is used for the simulator. Ignore if report too fast.
		if !isExpired && Denoising && interval < hotResourceReportMinInterval {
			continue
		}

		newItem := &HotPeerStat{
			ContainerID:    containerID,
			ResourceID:     res.Meta.ID(),
			Kind:           f.kind,
			ByteRate:       byteRate,
			KeyRate:        keyRate,
			LastUpdateTime: time.Now(),
			needDelete:     isExpired,
			isLeader:       res.GetLeader().GetContainerID() == containerID,
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

		newItem = f.updateHotPeerStat(newItem, oldItem, bytes, keys, time.Duration(interval))
		if newItem != nil {
			ret = append(ret, newItem)
		}
	}

	return ret
}

func (f *hotPeerCache) IsResourceHot(res *core.CachedResource, hotDegree int) bool {
	switch f.kind {
	case WriteFlow:
		return f.isResourceHotWithAnyPeers(res, hotDegree)
	case ReadFlow:
		return f.isResourceHotWithPeer(res, res.GetLeader(), hotDegree)
	}
	return false
}

func (f *hotPeerCache) CollectMetrics(typ string) {
	for containerID, peers := range f.peersOfContainer {
		container := containerTag(containerID)
		thresholds := f.calcHotThresholds(containerID)
		hotCacheStatusGauge.WithLabelValues("total_length", container, typ).Set(float64(peers.Len()))
		hotCacheStatusGauge.WithLabelValues("byte-rate-threshold", container, typ).Set(thresholds[byteDim])
		hotCacheStatusGauge.WithLabelValues("key-rate-threshold", container, typ).Set(thresholds[keyDim])
		// for compatibility
		hotCacheStatusGauge.WithLabelValues("hotThreshold", container, typ).Set(thresholds[byteDim])
	}
}

func (f *hotPeerCache) getResourceBytes(res *core.CachedResource) uint64 {
	switch f.kind {
	case WriteFlow:
		return res.GetBytesWritten()
	case ReadFlow:
		return res.GetBytesRead()
	}
	return 0
}

func (f *hotPeerCache) getResourceKeys(res *core.CachedResource) uint64 {
	switch f.kind {
	case WriteFlow:
		return res.GetKeysWritten()
	case ReadFlow:
		return res.GetKeysRead()
	}
	return 0
}

func (f *hotPeerCache) getOldHotPeerStat(resID, containerID uint64) *HotPeerStat {
	if hotPeers, ok := f.peersOfContainer[containerID]; ok {
		if v := hotPeers.Get(resID); v != nil {
			return v.(*HotPeerStat)
		}
	}
	return nil
}

func (f *hotPeerCache) isResourceExpired(res *core.CachedResource, containerID uint64) bool {
	switch f.kind {
	case WriteFlow:
		_, ok := res.GetContainerPeer(containerID)
		return !ok
	case ReadFlow:
		return res.GetLeader().GetContainerID() != containerID
	}
	return false
}

func (f *hotPeerCache) calcHotThresholds(containerID uint64) [dimLen]float64 {
	minThresholds := minHotThresholds[f.kind]
	tn, ok := f.peersOfContainer[containerID]
	if !ok || tn.Len() < topNN {
		return minThresholds
	}
	ret := [dimLen]float64{
		byteDim: tn.GetTopNMin(byteDim).(*HotPeerStat).GetByteRate(),
		keyDim:  tn.GetTopNMin(keyDim).(*HotPeerStat).GetKeyRate(),
	}
	for k := 0; k < dimLen; k++ {
		ret[k] = math.Max(ret[k]*hotThresholdRatio, minThresholds[k])
	}
	return ret
}

// gets the containerIDs, including old resource and new resource
func (f *hotPeerCache) getAllContainerIDs(res *core.CachedResource) []uint64 {
	containerIDs := make(map[uint64]struct{})
	ret := make([]uint64, 0, len(res.Meta.Peers()))
	// old containers
	ids, ok := f.containersOfResource[res.Meta.ID()]
	if ok {
		for containerID := range ids {
			containerIDs[containerID] = struct{}{}
			ret = append(ret, containerID)
		}
	}

	// new containers
	for _, peer := range res.Meta.Peers() {
		// ReadFlow no need consider the followers.
		if f.kind == ReadFlow && peer.ContainerID != res.GetLeader().GetContainerID() {
			continue
		}
		if _, ok := containerIDs[peer.ContainerID]; !ok {
			containerIDs[peer.ContainerID] = struct{}{}
			ret = append(ret, peer.ContainerID)
		}
	}

	return ret
}

func (f *hotPeerCache) isResourceHotWithAnyPeers(res *core.CachedResource, hotDegree int) bool {
	for _, peer := range res.Meta.Peers() {
		if f.isResourceHotWithPeer(res, &peer, hotDegree) {
			return true
		}
	}
	return false
}

func (f *hotPeerCache) isResourceHotWithPeer(res *core.CachedResource, peer *metapb.Peer, hotDegree int) bool {
	if peer == nil {
		return false
	}
	containerID := peer.GetContainerID()
	if peers, ok := f.peersOfContainer[containerID]; ok {
		if stat := peers.Get(res.Meta.ID()); stat != nil {
			return stat.(*HotPeerStat).HotDegree >= hotDegree
		}
	}
	return false
}

func (f *hotPeerCache) getDefaultTimeMedian() *movingaverage.TimeMedian {
	return movingaverage.NewTimeMedian(DefaultAotSize, rollingWindowsSize, ResourceHeartBeatReportInterval)
}

func (f *hotPeerCache) updateHotPeerStat(newItem, oldItem *HotPeerStat, bytes, keys float64, interval time.Duration) *HotPeerStat {
	thresholds := f.calcHotThresholds(newItem.ContainerID)
	isHot := newItem.ByteRate >= thresholds[byteDim] || // if interval is zero, rate will be NaN, isHot will be false
		newItem.KeyRate >= thresholds[keyDim]

	if newItem.needDelete {
		return newItem
	}

	if oldItem != nil {
		newItem.rollingByteRate = oldItem.rollingByteRate
		newItem.rollingKeyRate = oldItem.rollingKeyRate
		if isHot {
			newItem.HotDegree = oldItem.HotDegree + 1
			newItem.AntiCount = hotResourceAntiCount
		} else if interval != 0 {
			newItem.HotDegree = oldItem.HotDegree - 1
			newItem.AntiCount = oldItem.AntiCount - 1
			if newItem.AntiCount <= 0 {
				newItem.needDelete = true
			}
		}
	} else {
		if !isHot {
			return nil
		}
		newItem.rollingByteRate = f.getDefaultTimeMedian()
		newItem.rollingKeyRate = f.getDefaultTimeMedian()
		newItem.AntiCount = hotResourceAntiCount
		newItem.isNew = true
	}
	newItem.rollingByteRate.Add(bytes, interval*time.Second)
	newItem.rollingKeyRate.Add(keys, interval*time.Second)

	return newItem
}
