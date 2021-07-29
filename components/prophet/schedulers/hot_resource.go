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

package schedulers

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/filter"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/statistics"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(HotResourceType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler(HotResourceType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := initHotResourceScheduleConfig()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.storage = storage
		return newHotScheduler(opController, conf), nil
	})

	// FIXME: remove this two schedule after the balance test move in schedulers package
	{
		schedule.RegisterScheduler(HotWriteResourceType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
			return newHotWriteScheduler(opController, initHotResourceScheduleConfig()), nil
		})
		schedule.RegisterScheduler(HotReadResourceType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
			return newHotReadScheduler(opController, initHotResourceScheduleConfig()), nil
		})

	}
}

const (
	// HotResourceName is balance hot resource scheduler name.
	HotResourceName = "balance-hot-resource-scheduler"
	// HotResourceType is balance hot resource scheduler type.
	HotResourceType = "hot-resource"
	// HotReadResourceType is hot read resource scheduler type.
	HotReadResourceType = "hot-read-resource"
	// HotWriteResourceType is hot write resource scheduler type.
	HotWriteResourceType = "hot-write-resource"

	minHotScheduleInterval = time.Second
	maxHotScheduleInterval = 20 * time.Second
)

// schedulePeerPr the probability of schedule the hot peer.
var schedulePeerPr = 0.66

type hotScheduler struct {
	name string
	*BaseScheduler
	sync.RWMutex
	leaderLimit uint64
	peerLimit   uint64
	types       []rwType
	r           *rand.Rand

	// states across multiple `Schedule` calls
	pendings [resourceTypeLen]map[*pendingInfluence]struct{}
	// resourcePendings containers resourceID -> [opType]Operator
	// this records resourceID which have pending Operator by operation type. During filterHotPeers, the hot peers won't
	// be selected if its owner resource is tracked in this attribute.
	resourcePendings map[uint64][2]*operator.Operator

	// temporary states but exported to API or metrics
	stLoadInfos [resourceTypeLen]map[uint64]*containerLoadDetail
	// pendingSums indicates the [resourceType] containerID -> pending Influence
	// This containers the pending Influence for each container by resource type.
	pendingSums [resourceTypeLen]map[uint64]Influence
	// config of hot scheduler
	conf *hotResourceSchedulerConfig
}

func newHotScheduler(opController *schedule.OperatorController, conf *hotResourceSchedulerConfig) *hotScheduler {
	base := NewBaseScheduler(opController)
	ret := &hotScheduler{
		name:             HotResourceName,
		BaseScheduler:    base,
		leaderLimit:      1,
		peerLimit:        1,
		types:            []rwType{write, read},
		r:                rand.New(rand.NewSource(time.Now().UnixNano())),
		resourcePendings: make(map[uint64][2]*operator.Operator),
		conf:             conf,
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.pendings[ty] = map[*pendingInfluence]struct{}{}
		ret.stLoadInfos[ty] = map[uint64]*containerLoadDetail{}
	}
	return ret
}

func newHotReadScheduler(opController *schedule.OperatorController, conf *hotResourceSchedulerConfig) *hotScheduler {
	ret := newHotScheduler(opController, conf)
	ret.name = ""
	ret.types = []rwType{read}
	return ret
}

func newHotWriteScheduler(opController *schedule.OperatorController, conf *hotResourceSchedulerConfig) *hotScheduler {
	ret := newHotScheduler(opController, conf)
	ret.name = ""
	ret.types = []rwType{write}
	return ret
}

func (h *hotScheduler) GetName() string {
	return h.name
}

func (h *hotScheduler) GetType() string {
	return HotResourceType
}

func (h *hotScheduler) GetMinInterval() time.Duration {
	return minHotScheduleInterval
}
func (h *hotScheduler) GetNextInterval(interval time.Duration) time.Duration {
	return intervalGrow(h.GetMinInterval(), maxHotScheduleInterval, exponentialGrowth)
}

func (h *hotScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return h.allowBalanceLeader(cluster) || h.allowBalanceResource(cluster)
}

func (h *hotScheduler) allowBalanceLeader(cluster opt.Cluster) bool {
	hotResourceAllowed := h.OpController.OperatorCount(operator.OpHotResource) < cluster.GetOpts().GetHotResourceScheduleLimit()
	leaderAllowed := h.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !hotResourceAllowed {
		operator.OperatorLimitCounter.WithLabelValues(h.GetType(), operator.OpHotResource.String()).Inc()
	}
	if !leaderAllowed {
		operator.OperatorLimitCounter.WithLabelValues(h.GetType(), operator.OpLeader.String()).Inc()
	}
	return hotResourceAllowed && leaderAllowed
}

func (h *hotScheduler) allowBalanceResource(cluster opt.Cluster) bool {
	allowed := h.OpController.OperatorCount(operator.OpHotResource) < cluster.GetOpts().GetHotResourceScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(h.GetType(), operator.OpHotResource.String()).Inc()
	}
	return allowed
}

func (h *hotScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(h.GetName(), "schedule").Inc()
	return h.dispatch(h.types[h.r.Int()%len(h.types)], cluster)
}

func (h *hotScheduler) dispatch(typ rwType, cluster opt.Cluster) []*operator.Operator {
	h.Lock()
	defer h.Unlock()

	h.prepareForBalance(cluster)

	switch typ {
	case read:
		return h.balanceHotReadResources(cluster)
	case write:
		return h.balanceHotWriteResources(cluster)
	}
	return nil
}

// prepareForBalance calculate the summary of pending Influence for each container and prepare the load detail for
// each container
func (h *hotScheduler) prepareForBalance(cluster opt.Cluster) {
	h.summaryPendingInfluence()

	containersLoads := cluster.GetContainersLoads()

	{ // update read statistics
		resourceRead := cluster.ResourceReadStats()
		h.stLoadInfos[readLeader] = summaryContainersLoad(
			containersLoads,
			h.pendingSums[readLeader],
			resourceRead,
			read, metapb.ResourceKind_LeaderKind)
	}

	{ // update write statistics
		resourceWrite := cluster.ResourceWriteStats()
		h.stLoadInfos[writeLeader] = summaryContainersLoad(
			containersLoads,
			h.pendingSums[writeLeader],
			resourceWrite,
			write, metapb.ResourceKind_LeaderKind)

		h.stLoadInfos[writePeer] = summaryContainersLoad(
			containersLoads,
			h.pendingSums[writePeer],
			resourceWrite,
			write, metapb.ResourceKind_ReplicaKind)
	}
}

// summaryPendingInfluence calculate the summary of pending Influence for each container
// and clean the resource from resourceInfluence if they have ended operator.
func (h *hotScheduler) summaryPendingInfluence() {
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		h.pendingSums[ty] = summaryPendingInfluence(h.pendings[ty], h.calcPendingWeight)
	}
	h.gcResourcePendings()
}

// gcResourcePendings check the resource whether it need to be deleted from resourcePendings depended on whether it have
// ended operator
func (h *hotScheduler) gcResourcePendings() {
	for resID, pendings := range h.resourcePendings {
		empty := true
		for ty, op := range pendings {
			if op != nil && op.IsEnd() {
				if time.Now().After(op.GetCreateTime().Add(h.conf.GetMaxZombieDuration())) {
					schedulerStatus.WithLabelValues(h.GetName(), "pending_op_infos").Dec()
					pendings[ty] = nil
				}
			}
			if pendings[ty] != nil {
				empty = false
			}
		}
		if empty {
			delete(h.resourcePendings, resID)
		} else {
			h.resourcePendings[resID] = pendings
		}
	}
}

// summaryContainersLoad Load information of all available containers.
// it will filtered the hot peer and calculate the current and future stat(byte/key rate,count) for each container
func summaryContainersLoad(
	containersLoads map[uint64][]float64,
	containerPendings map[uint64]Influence,
	containerHotPeers map[uint64][]*statistics.HotPeerStat,
	rwTy rwType,
	kind metapb.ResourceKind,
) map[uint64]*containerLoadDetail {
	// loadDetail stores the storeID -> hotPeers stat and its current and future stat(key/byte rate,count)
	loadDetail := make(map[uint64]*containerLoadDetail, len(containersLoads))
	allByteSum := 0.0
	allKeySum := 0.0
	allCount := 0.0

	for id, loads := range containersLoads {
		var byteRate, keyRate float64
		switch rwTy {
		case read:
			byteRate, keyRate = loads[statistics.ContainerReadBytes], loads[statistics.ContainerReadKeys]
		case write:
			byteRate, keyRate = loads[statistics.ContainerWriteBytes], loads[statistics.ContainerWriteKeys]
		}

		// Find all hot peers first
		var hotPeers []*statistics.HotPeerStat
		{
			byteSum := 0.0
			keySum := 0.0
			for _, peer := range filterHotPeers(kind, containerHotPeers[id]) {
				byteSum += peer.GetByteRate()
				keySum += peer.GetKeyRate()
				hotPeers = append(hotPeers, peer.Clone())
			}
			// Use sum of hot peers to estimate leader-only byte rate.
			// For write requests, Write{Bytes, Keys} is applied to all Peers at the same time, while the Leader and Follower are under different loads (usually the Leader consumes more CPU).
			// But none of the current dimension reflect this difference, so we create a new dimension to reflect it.
			if kind == metapb.ResourceKind_LeaderKind && rwTy == write {
				byteRate = byteSum
				keyRate = keySum
			}

			// Metric for debug.
			{
				ty := "byte-rate-" + rwTy.String() + "-" + kind.String()
				hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(byteSum)
			}
			{
				ty := "key-rate-" + rwTy.String() + "-" + kind.String()
				hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(keySum)
			}
		}
		allByteSum += byteRate
		allKeySum += keyRate
		allCount += float64(len(hotPeers))

		// Build store load prediction from current load and pending influence.
		stLoadPred := (&containerLoad{
			ByteRate: byteRate,
			KeyRate:  keyRate,
			Count:    float64(len(hotPeers)),
		}).ToLoadPred(containerPendings[id])

		// Construct store load info.
		loadDetail[id] = &containerLoadDetail{
			LoadPred: stLoadPred,
			HotPeers: hotPeers,
		}
	}

	containerLen := float64(len(containersLoads))
	// store expectation byte/key rate and count for each store-load detail.
	for id, detail := range loadDetail {
		byteExp := allByteSum / containerLen
		keyExp := allKeySum / containerLen
		countExp := allCount / containerLen
		detail.LoadPred.Expect.ByteRate = byteExp
		detail.LoadPred.Expect.KeyRate = keyExp
		detail.LoadPred.Expect.Count = countExp
		// Debug
		{
			ty := "exp-byte-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(byteExp)
		}
		{
			ty := "exp-key-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(keyExp)
		}
		{
			ty := "exp-count-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(countExp)
		}
	}
	return loadDetail
}

// filterHotPeers filter the peer whose hot degree is less than minHotDegress
func filterHotPeers(
	kind metapb.ResourceKind,
	peers []*statistics.HotPeerStat,
) []*statistics.HotPeerStat {
	var ret []*statistics.HotPeerStat
	for _, peer := range peers {
		if kind == metapb.ResourceKind_LeaderKind && !peer.IsLeader() {
			continue
		}
		ret = append(ret, peer)
	}
	return ret
}

func (h *hotScheduler) addPendingInfluence(op *operator.Operator, srcContainer, dstContainer uint64, infl Influence, rwTy rwType, opTy opType) bool {
	resID := op.ResourceID()
	_, ok := h.resourcePendings[resID]
	if ok {
		schedulerStatus.WithLabelValues(h.GetName(), "pending_op_fails").Inc()
		return false
	}

	influence := newPendingInfluence(op, srcContainer, dstContainer, infl)
	rcTy := toResourceType(rwTy, opTy)
	h.pendings[rcTy][influence] = struct{}{}

	h.resourcePendings[resID] = [2]*operator.Operator{nil, nil}
	{ // h.pendingOpInfos[resourceID][ty] = influence
		tmp := h.resourcePendings[resID]
		tmp[opTy] = op
		h.resourcePendings[resID] = tmp
	}

	schedulerStatus.WithLabelValues(h.GetName(), "pending_op_infos").Inc()
	return true
}

func (h *hotScheduler) balanceHotReadResources(cluster opt.Cluster) []*operator.Operator {
	// prefer to balance by leader
	leaderSolver := newBalanceSolver(h, cluster, read, transferLeader)
	ops := leaderSolver.solve()
	if len(ops) > 0 {
		return ops
	}

	peerSolver := newBalanceSolver(h, cluster, read, movePeer)
	ops = peerSolver.solve()
	if len(ops) > 0 {
		return ops
	}

	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

func (h *hotScheduler) balanceHotWriteResources(cluster opt.Cluster) []*operator.Operator {
	// prefer to balance by peer
	s := h.r.Intn(100)
	switch {
	case s < int(schedulePeerPr*100):
		peerSolver := newBalanceSolver(h, cluster, write, movePeer)
		ops := peerSolver.solve()
		if len(ops) > 0 {
			return ops
		}
	default:
	}

	leaderSolver := newBalanceSolver(h, cluster, write, transferLeader)
	ops := leaderSolver.solve()
	if len(ops) > 0 {
		return ops
	}

	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

type balanceSolver struct {
	sche         *hotScheduler
	cluster      opt.Cluster
	stLoadDetail map[uint64]*containerLoadDetail
	rwTy         rwType
	opTy         opType

	cur *solution

	maxSrc   *containerLoad
	minDst   *containerLoad
	rankStep *containerLoad
}

type solution struct {
	srcContainerID uint64
	srcPeerStat    *statistics.HotPeerStat
	resource       *core.CachedResource
	dstContainerID uint64

	// progressiveRank measures the contribution for balance.
	// The smaller the rank, the better this solution is.
	// If rank < 0, this solution makes thing better.
	progressiveRank int64
}

func (bs *balanceSolver) init() {
	switch toResourceType(bs.rwTy, bs.opTy) {
	case writePeer:
		bs.stLoadDetail = bs.sche.stLoadInfos[writePeer]
	case writeLeader:
		bs.stLoadDetail = bs.sche.stLoadInfos[writeLeader]
	case readLeader:
		bs.stLoadDetail = bs.sche.stLoadInfos[readLeader]
	}
	// And it will be unnecessary to filter unhealthy container, because it has been solved in process heartbeat

	bs.maxSrc = &containerLoad{}
	bs.minDst = &containerLoad{
		ByteRate: math.MaxFloat64,
		KeyRate:  math.MaxFloat64,
		Count:    math.MaxFloat64,
	}
	maxCur := &containerLoad{}

	for _, detail := range bs.stLoadDetail {
		bs.maxSrc = maxLoad(bs.maxSrc, detail.LoadPred.min())
		bs.minDst = minLoad(bs.minDst, detail.LoadPred.max())
		maxCur = maxLoad(maxCur, &detail.LoadPred.Current)
	}

	bs.rankStep = &containerLoad{
		ByteRate: maxCur.ByteRate * bs.sche.conf.GetByteRankStepRatio(),
		KeyRate:  maxCur.KeyRate * bs.sche.conf.GetKeyRankStepRatio(),
		Count:    maxCur.Count * bs.sche.conf.GetCountRankStepRatio(),
	}
}

func newBalanceSolver(sche *hotScheduler, cluster opt.Cluster, rwTy rwType, opTy opType) *balanceSolver {
	solver := &balanceSolver{
		sche:    sche,
		cluster: cluster,
		rwTy:    rwTy,
		opTy:    opTy,
	}
	solver.init()
	return solver
}

func (bs *balanceSolver) isValid() bool {
	if bs.cluster == nil || bs.sche == nil || bs.stLoadDetail == nil {
		return false
	}
	switch bs.rwTy {
	case write, read:
	default:
		return false
	}
	switch bs.opTy {
	case movePeer, transferLeader:
	default:
		return false
	}
	return true
}

// solve travels all the src containers, hot peers, dst containers and select each one of them to make a best scheduling solution.
// The comparing between solutions is based on calcProgressiveRank.
func (bs *balanceSolver) solve() []*operator.Operator {
	if !bs.isValid() || !bs.allowBalance() {
		return nil
	}
	bs.cur = &solution{}
	var (
		best  *solution
		ops   []*operator.Operator
		infls []Influence
	)

	for srcContainerID := range bs.filterSrcContainers() {
		bs.cur.srcContainerID = srcContainerID

		for _, srcPeerStat := range bs.filterHotPeers() {
			bs.cur.srcPeerStat = srcPeerStat
			bs.cur.resource = bs.getResource()
			if bs.cur.resource == nil {
				continue
			}
			for dstContainerID := range bs.filterDstContainers() {
				bs.cur.dstContainerID = dstContainerID
				bs.calcProgressiveRank()
				if bs.cur.progressiveRank < 0 && bs.betterThan(best) {
					if newOps, newInfls := bs.buildOperators(); len(newOps) > 0 {
						ops = newOps
						infls = newInfls
						clone := *bs.cur
						best = &clone
					}
				}
			}
		}
	}

	for i := 0; i < len(ops); i++ {
		// TODO: multiple operators need to be atomic.
		if !bs.sche.addPendingInfluence(ops[i], best.srcContainerID, best.dstContainerID, infls[i], bs.rwTy, bs.opTy) {
			return nil
		}
	}
	return ops
}

// allowBalance check whether the operator count have exceed the hot resource limit by type
func (bs *balanceSolver) allowBalance() bool {
	switch bs.opTy {
	case movePeer:
		return bs.sche.allowBalanceResource(bs.cluster)
	case transferLeader:
		return bs.sche.allowBalanceLeader(bs.cluster)
	default:
		return false
	}
}

// filterSrcContainers compare the min rate and the ratio * expectation rate, if both key and byte rate is greater than
// its expectation * ratio, the container would be selected as hot source container
func (bs *balanceSolver) filterSrcContainers() map[uint64]*containerLoadDetail {
	ret := make(map[uint64]*containerLoadDetail)
	for id, detail := range bs.stLoadDetail {
		if bs.cluster.GetContainer(id) == nil {
			util.GetLogger().Errorf("get the source container %d failed with not found",
				id)
			continue
		}
		if len(detail.HotPeers) == 0 {
			continue
		}
		if detail.LoadPred.min().ByteRate > bs.sche.conf.GetSrcToleranceRatio()*detail.LoadPred.Expect.ByteRate &&
			detail.LoadPred.min().KeyRate > bs.sche.conf.GetSrcToleranceRatio()*detail.LoadPred.Expect.KeyRate {
			ret[id] = detail
			hotSchedulerResultCounter.WithLabelValues("src-container-succ", strconv.FormatUint(id, 10)).Inc()
		}
		hotSchedulerResultCounter.WithLabelValues("src-container-failed", strconv.FormatUint(id, 10)).Inc()
	}
	return ret
}

// filterHotPeers filtered hot peers from statistics.HotPeerStat and deleted the peer if its resource is in pending status.
// The returned hotPeer count in controlled by `max-peer-number`.
func (bs *balanceSolver) filterHotPeers() []*statistics.HotPeerStat {
	ret := bs.stLoadDetail[bs.cur.srcContainerID].HotPeers
	// Return at most MaxPeerNum peers, to prevent balanceSolver.solve() too slow.
	maxPeerNum := bs.sche.conf.GetMaxPeerNumber()

	// filter pending resource
	appendItem := func(items []*statistics.HotPeerStat, item *statistics.HotPeerStat) []*statistics.HotPeerStat {
		minHotDegree := bs.cluster.GetOpts().GetHotResourceCacheHitsThreshold()
		if _, ok := bs.sche.resourcePendings[item.ID()]; !ok && !item.IsNeedCoolDownTransferLeader(minHotDegree) {
			// no in pending operator and no need cool down after transfer leader
			items = append(items, item)
		}
		return items
	}
	if len(ret) <= maxPeerNum {
		nret := make([]*statistics.HotPeerStat, 0, len(ret))
		for _, peer := range ret {
			nret = appendItem(nret, peer)
		}
		return nret
	}

	byteSort := make([]*statistics.HotPeerStat, len(ret))
	copy(byteSort, ret)
	sort.Slice(byteSort, func(i, j int) bool {
		return byteSort[i].GetByteRate() > byteSort[j].GetByteRate()
	})
	keySort := make([]*statistics.HotPeerStat, len(ret))
	copy(keySort, ret)
	sort.Slice(keySort, func(i, j int) bool {
		return keySort[i].GetKeyRate() > keySort[j].GetKeyRate()
	})

	union := make(map[*statistics.HotPeerStat]struct{}, maxPeerNum)
	for len(union) < maxPeerNum {
		for len(byteSort) > 0 {
			peer := byteSort[0]
			byteSort = byteSort[1:]
			if _, ok := union[peer]; !ok {
				union[peer] = struct{}{}
				break
			}
		}
		for len(keySort) > 0 {
			peer := keySort[0]
			keySort = keySort[1:]
			if _, ok := union[peer]; !ok {
				union[peer] = struct{}{}
				break
			}
		}
	}
	ret = make([]*statistics.HotPeerStat, 0, len(union))
	for peer := range union {
		ret = appendItem(ret, peer)
	}
	return ret
}

// isResourceAvailable checks whether the given resource is not available to schedule.
func (bs *balanceSolver) isResourceAvailable(res *core.CachedResource) bool {
	if res == nil {
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "no-resource").Inc()
		return false
	}

	if pendings, ok := bs.sche.resourcePendings[res.Meta.ID()]; ok {
		if bs.opTy == transferLeader {
			return false
		}
		if pendings[movePeer] != nil ||
			(pendings[transferLeader] != nil && !pendings[transferLeader].IsEnd()) {
			return false
		}
	}

	if !opt.IsHealthyAllowPending(bs.cluster, res) {
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "unhealthy-replica").Inc()
		return false
	}

	if !opt.IsResourceReplicated(bs.cluster, res) {
		util.GetLogger().Debugf("resource %d has abnormal replica count, scheduler %s",
			res.Meta.ID(),
			res.Meta.ID())
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "abnormal-replica").Inc()
		return false
	}

	return true
}

func (bs *balanceSolver) getResource() *core.CachedResource {
	res := bs.cluster.GetResource(bs.cur.srcPeerStat.ID())
	if !bs.isResourceAvailable(res) {
		return nil
	}

	switch bs.opTy {
	case movePeer:
		_, ok := res.GetContainerPeer(bs.cur.srcContainerID)
		if !ok {
			util.GetLogger().Debugf("resource %d does not have a peer on source container, maybe stat out of date",
				bs.cur.srcPeerStat.ID())
			return nil
		}
	case transferLeader:
		if res.GetLeader().GetContainerID() != bs.cur.srcContainerID {
			util.GetLogger().Debugf("resource %d leader is not on source container, maybe stat out of date",
				bs.cur.srcPeerStat.ID())
			return nil
		}
	default:
		return nil
	}

	return res
}

// filterDstContainers select the candidate container by filters
func (bs *balanceSolver) filterDstContainers() map[uint64]*containerLoadDetail {
	var (
		filters    []filter.Filter
		candidates []*core.CachedContainer
	)
	srcContainer := bs.cluster.GetContainer(bs.cur.srcContainerID)
	if srcContainer == nil {
		return nil
	}

	switch bs.opTy {
	case movePeer:
		filters = []filter.Filter{
			&filter.ContainerStateFilter{ActionScope: bs.sche.GetName(), MoveResource: true},
			filter.NewExcludedFilter(bs.sche.GetName(), bs.cur.resource.GetContainerIDs(), bs.cur.resource.GetContainerIDs()),
			filter.NewSpecialUseFilter(bs.sche.GetName(), filter.SpecialUseHotResource),
			filter.NewPlacementSafeguard(bs.sche.GetName(), bs.cluster, bs.cur.resource, srcContainer, bs.cluster.GetResourceFactory()),
		}

		for containerID := range bs.stLoadDetail {
			candidates = append(candidates, bs.cluster.GetContainer(containerID))
		}

	case transferLeader:
		filters = []filter.Filter{
			&filter.ContainerStateFilter{ActionScope: bs.sche.GetName(), TransferLeader: true},
			filter.NewSpecialUseFilter(bs.sche.GetName(), filter.SpecialUseHotResource),
		}
		if leaderFilter := filter.NewPlacementLeaderSafeguard(bs.sche.GetName(), bs.cluster, bs.cur.resource, srcContainer, bs.cluster.GetResourceFactory()); leaderFilter != nil {
			filters = append(filters, leaderFilter)
		}

		for _, container := range bs.cluster.GetFollowerContainers(bs.cur.resource) {
			if _, ok := bs.stLoadDetail[container.Meta.ID()]; ok {
				candidates = append(candidates, container)
			}
		}

	default:
		return nil
	}
	return bs.pickDstContainers(filters, candidates)
}

func (bs *balanceSolver) pickDstContainers(filters []filter.Filter, candidates []*core.CachedContainer) map[uint64]*containerLoadDetail {
	ret := make(map[uint64]*containerLoadDetail, len(candidates))
	dstToleranceRatio := bs.sche.conf.GetDstToleranceRatio()
	for _, container := range candidates {
		if filter.Target(bs.cluster.GetOpts(), container, filters) {
			detail := bs.stLoadDetail[container.Meta.ID()]
			if detail.LoadPred.max().ByteRate*dstToleranceRatio < detail.LoadPred.Expect.ByteRate &&
				detail.LoadPred.max().KeyRate*dstToleranceRatio < detail.LoadPred.Expect.KeyRate {
				ret[container.Meta.ID()] = bs.stLoadDetail[container.Meta.ID()]
				hotSchedulerResultCounter.WithLabelValues("dst-container-succ", strconv.FormatUint(container.Meta.ID(), 10)).Inc()
			}
			hotSchedulerResultCounter.WithLabelValues("dst-container-fail", strconv.FormatUint(container.Meta.ID(), 10)).Inc()
		}
	}
	return ret
}

// calcProgressiveRank calculates `bs.cur.progressiveRank`.
// See the comments of `solution.progressiveRank` for more about progressive rank.
func (bs *balanceSolver) calcProgressiveRank() {
	srcLd := bs.stLoadDetail[bs.cur.srcContainerID].LoadPred.min()
	dstLd := bs.stLoadDetail[bs.cur.dstContainerID].LoadPred.max()
	peer := bs.cur.srcPeerStat
	rank := int64(0)
	if bs.rwTy == write && bs.opTy == transferLeader {
		// In this condition, CPU usage is the matter.
		// Only consider about key rate.
		if srcLd.KeyRate-peer.GetKeyRate() >= dstLd.KeyRate+peer.GetKeyRate() {
			rank = -1
		}
	} else {
		getSrcDecRate := func(a, b float64) float64 {
			if a-b <= 0 {
				return 1
			}
			return a - b
		}
		// we use DecRatio(Decline Ratio) to expect that the dst container's (key/byte) rate should still be less
		// than the src container's (key/byte) rate after scheduling one peer.
		keyDecRatio := (dstLd.KeyRate + peer.GetKeyRate()) / getSrcDecRate(srcLd.KeyRate, peer.GetKeyRate())
		keyHot := peer.GetKeyRate() >= bs.sche.conf.GetMinHotKeyRate()
		byteDecRatio := (dstLd.ByteRate + peer.GetByteRate()) / getSrcDecRate(srcLd.ByteRate, peer.GetByteRate())
		byteHot := peer.GetByteRate() > bs.sche.conf.GetMinHotByteRate()
		greatDecRatio, minorDecRatio := bs.sche.conf.GetGreatDecRatio(), bs.sche.conf.GetMinorGreatDecRatio()
		switch {
		case byteHot && byteDecRatio <= greatDecRatio && keyHot && keyDecRatio <= greatDecRatio:
			// If belong to the case, both byte rate and key rate will be more balanced, the best choice.
			rank = -3
		case byteDecRatio <= minorDecRatio && keyHot && keyDecRatio <= greatDecRatio:
			// If belong to the case, byte rate will be not worsened, key rate will be more balanced.
			rank = -2
		case byteHot && byteDecRatio <= greatDecRatio:
			// If belong to the case, byte rate will be more balanced, ignore the key rate.
			rank = -1
		}
	}
	bs.cur.progressiveRank = rank
}

// betterThan checks if `bs.cur` is a better solution than `old`.
func (bs *balanceSolver) betterThan(old *solution) bool {
	if old == nil {
		return true
	}

	switch {
	case bs.cur.progressiveRank < old.progressiveRank:
		return true
	case bs.cur.progressiveRank > old.progressiveRank:
		return false
	}

	if r := bs.compareSrcContainer(bs.cur.srcContainerID, old.srcContainerID); r < 0 {
		return true
	} else if r > 0 {
		return false
	}

	if r := bs.compareDstContainer(bs.cur.dstContainerID, old.dstContainerID); r < 0 {
		return true
	} else if r > 0 {
		return false
	}

	if bs.cur.srcPeerStat != old.srcPeerStat {
		// compare resource

		if bs.rwTy == write && bs.opTy == transferLeader {
			switch {
			case bs.cur.srcPeerStat.GetKeyRate() > old.srcPeerStat.GetKeyRate():
				return true
			case bs.cur.srcPeerStat.GetKeyRate() < old.srcPeerStat.GetKeyRate():
				return false
			}
		} else {
			byteRkCmp := rankCmp(bs.cur.srcPeerStat.GetByteRate(), old.srcPeerStat.GetByteRate(), stepRank(0, 100))
			keyRkCmp := rankCmp(bs.cur.srcPeerStat.GetKeyRate(), old.srcPeerStat.GetKeyRate(), stepRank(0, 10))

			switch bs.cur.progressiveRank {
			case -2: // greatDecRatio < byteDecRatio <= minorDecRatio && keyDecRatio <= greatDecRatio
				if keyRkCmp != 0 {
					return keyRkCmp > 0
				}
				if byteRkCmp != 0 {
					// prefer smaller byte rate, to reduce oscillation
					return byteRkCmp < 0
				}
			case -3: // byteDecRatio <= greatDecRatio && keyDecRatio <= greatDecRatio
				if keyRkCmp != 0 {
					return keyRkCmp > 0
				}
				fallthrough
			case -1: // byteDecRatio <= greatDecRatio
				if byteRkCmp != 0 {
					// prefer resource with larger byte rate, to converge faster
					return byteRkCmp > 0
				}
			}
		}
	}

	return false
}

// smaller is better
func (bs *balanceSolver) compareSrcContainer(st1, st2 uint64) int {
	if st1 != st2 {
		// compare source container
		var lpCmp containerLPCmp
		if bs.rwTy == write && bs.opTy == transferLeader {
			lpCmp = sliceLPCmp(
				minLPCmp(negLoadCmp(sliceLoadCmp(
					stLdRankCmp(stLdKeyRate, stepRank(bs.maxSrc.KeyRate, bs.rankStep.KeyRate)),
					stLdRankCmp(stLdByteRate, stepRank(bs.maxSrc.ByteRate, bs.rankStep.ByteRate)),
				))),
				diffCmp(sliceLoadCmp(
					stLdRankCmp(stLdCount, stepRank(0, bs.rankStep.Count)),
					stLdRankCmp(stLdKeyRate, stepRank(0, bs.rankStep.KeyRate)),
					stLdRankCmp(stLdByteRate, stepRank(0, bs.rankStep.ByteRate)),
				)),
			)
		} else {
			lpCmp = sliceLPCmp(
				minLPCmp(negLoadCmp(sliceLoadCmp(
					stLdRankCmp(stLdByteRate, stepRank(bs.maxSrc.ByteRate, bs.rankStep.ByteRate)),
					stLdRankCmp(stLdKeyRate, stepRank(bs.maxSrc.KeyRate, bs.rankStep.KeyRate)),
				))),
				diffCmp(
					stLdRankCmp(stLdByteRate, stepRank(0, bs.rankStep.ByteRate)),
				),
			)
		}

		lp1 := bs.stLoadDetail[st1].LoadPred
		lp2 := bs.stLoadDetail[st2].LoadPred
		return lpCmp(lp1, lp2)
	}
	return 0
}

// smaller is better
func (bs *balanceSolver) compareDstContainer(st1, st2 uint64) int {
	if st1 != st2 {
		// compare destination container
		var lpCmp containerLPCmp
		if bs.rwTy == write && bs.opTy == transferLeader {
			lpCmp = sliceLPCmp(
				maxLPCmp(sliceLoadCmp(
					stLdRankCmp(stLdKeyRate, stepRank(bs.minDst.KeyRate, bs.rankStep.KeyRate)),
					stLdRankCmp(stLdByteRate, stepRank(bs.minDst.ByteRate, bs.rankStep.ByteRate)),
				)),
				diffCmp(sliceLoadCmp(
					stLdRankCmp(stLdCount, stepRank(0, bs.rankStep.Count)),
					stLdRankCmp(stLdKeyRate, stepRank(0, bs.rankStep.KeyRate)),
					stLdRankCmp(stLdByteRate, stepRank(0, bs.rankStep.ByteRate)),
				)))
		} else {
			lpCmp = sliceLPCmp(
				maxLPCmp(sliceLoadCmp(
					stLdRankCmp(stLdByteRate, stepRank(bs.minDst.ByteRate, bs.rankStep.ByteRate)),
					stLdRankCmp(stLdKeyRate, stepRank(bs.minDst.KeyRate, bs.rankStep.KeyRate)),
				)),
				diffCmp(
					stLdRankCmp(stLdByteRate, stepRank(0, bs.rankStep.ByteRate)),
				),
			)
		}

		lp1 := bs.stLoadDetail[st1].LoadPred
		lp2 := bs.stLoadDetail[st2].LoadPred
		return lpCmp(lp1, lp2)
	}
	return 0
}

func stepRank(rk0 float64, step float64) func(float64) int64 {
	return func(rate float64) int64 {
		return int64((rate - rk0) / step)
	}
}

func (bs *balanceSolver) isReadyToBuild() bool {
	if bs.cur.srcContainerID == 0 || bs.cur.dstContainerID == 0 ||
		bs.cur.srcPeerStat == nil || bs.cur.resource == nil {
		return false
	}
	if bs.cur.srcContainerID != bs.cur.srcPeerStat.ContainerID ||
		bs.cur.resource.Meta.ID() != bs.cur.srcPeerStat.ID() {
		return false
	}
	return true
}

func (bs *balanceSolver) buildOperators() ([]*operator.Operator, []Influence) {
	if !bs.isReadyToBuild() {
		return nil, nil
	}
	var (
		op       *operator.Operator
		counters []prometheus.Counter
		err      error
	)

	switch bs.opTy {
	case movePeer:
		srcPeer, _ := bs.cur.resource.GetContainerPeer(bs.cur.srcContainerID) // checked in getResourceAndSrcPeer
		dstPeer := metapb.Peer{ContainerID: bs.cur.dstContainerID, Role: srcPeer.Role}
		desc := "move-hot-" + bs.rwTy.String() + "-peer"
		op, err = operator.CreateMovePeerOperator(
			desc,
			bs.cluster,
			bs.cur.resource,
			operator.OpHotResource,
			bs.cur.srcContainerID,
			dstPeer)

		counters = append(counters,
			hotDirectionCounter.WithLabelValues("move-peer", bs.rwTy.String(), strconv.FormatUint(bs.cur.srcContainerID, 10), "out"),
			hotDirectionCounter.WithLabelValues("move-peer", bs.rwTy.String(), strconv.FormatUint(dstPeer.GetContainerID(), 10), "in"))
	case transferLeader:
		if _, ok := bs.cur.resource.GetContainerVoter(bs.cur.dstContainerID); !ok {
			return nil, nil
		}
		desc := "transfer-hot-" + bs.rwTy.String() + "-leader"
		op, err = operator.CreateTransferLeaderOperator(
			desc,
			bs.cluster,
			bs.cur.resource,
			bs.cur.srcContainerID,
			bs.cur.dstContainerID,
			operator.OpHotResource)
		counters = append(counters,
			hotDirectionCounter.WithLabelValues("transfer-leader", bs.rwTy.String(), strconv.FormatUint(bs.cur.srcContainerID, 10), "out"),
			hotDirectionCounter.WithLabelValues("transfer-leader", bs.rwTy.String(), strconv.FormatUint(bs.cur.dstContainerID, 10), "in"))
	}

	if err != nil {
		util.GetLogger().Debugf("create operator type %s failed with %+v",
			bs.rwTy,
			err)
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "create-operator-fail").Inc()
		return nil, nil
	}

	op.SetPriorityLevel(core.HighPriority)
	op.Counters = append(op.Counters, counters...)
	op.Counters = append(op.Counters,
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "new-operator"),
		schedulerCounter.WithLabelValues(bs.sche.GetName(), bs.opTy.String()))

	infl := Influence{
		ByteRate: bs.cur.srcPeerStat.GetByteRate(),
		KeyRate:  bs.cur.srcPeerStat.GetKeyRate(),
		Count:    1,
	}

	return []*operator.Operator{op}, []Influence{infl}
}

func (h *hotScheduler) GetHotReadStatus() *statistics.ContainerHotPeersInfos {
	h.RLock()
	defer h.RUnlock()
	asLeader := make(statistics.ContainerHotPeersStat, len(h.stLoadInfos[readLeader]))
	for id, detail := range h.stLoadInfos[readLeader] {
		asLeader[id] = detail.toHotPeersStat()
	}
	return &statistics.ContainerHotPeersInfos{
		AsLeader: asLeader,
	}
}

func (h *hotScheduler) GetHotWriteStatus() *statistics.ContainerHotPeersInfos {
	h.RLock()
	defer h.RUnlock()
	asLeader := make(statistics.ContainerHotPeersStat, len(h.stLoadInfos[writeLeader]))
	asPeer := make(statistics.ContainerHotPeersStat, len(h.stLoadInfos[writePeer]))
	for id, detail := range h.stLoadInfos[writeLeader] {
		asLeader[id] = detail.toHotPeersStat()
	}
	for id, detail := range h.stLoadInfos[writePeer] {
		asPeer[id] = detail.toHotPeersStat()
	}
	return &statistics.ContainerHotPeersInfos{
		AsLeader: asLeader,
		AsPeer:   asPeer,
	}
}

func (h *hotScheduler) GetWritePendingInfluence() map[uint64]Influence {
	return h.copyPendingInfluence(writePeer)
}

func (h *hotScheduler) GetReadPendingInfluence() map[uint64]Influence {
	return h.copyPendingInfluence(readLeader)
}

func (h *hotScheduler) copyPendingInfluence(ty resourceType) map[uint64]Influence {
	h.RLock()
	defer h.RUnlock()
	pendingSum := h.pendingSums[ty]
	ret := make(map[uint64]Influence, len(pendingSum))
	for id, infl := range pendingSum {
		ret[id] = infl
	}
	return ret
}

// calcPendingWeight return the calculate weight of one Operator, the value will between [0,1]
func (h *hotScheduler) calcPendingWeight(op *operator.Operator) float64 {
	if op.CheckExpired() || op.CheckTimeout() {
		return 0
	}
	status := op.Status()
	if !operator.IsEndStatus(status) {
		return 1
	}
	switch status {
	case operator.SUCCESS:
		zombieDur := time.Since(op.GetReachTimeOf(status))
		maxZombieDur := h.conf.GetMaxZombieDuration()
		if zombieDur >= maxZombieDur {
			return 0
		}
		// TODO: use container statistics update time to make a more accurate estimation
		return float64(maxZombieDur-zombieDur) / float64(maxZombieDur)
	default:
		return 0
	}
}

func (h *hotScheduler) clearPendingInfluence() {
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		h.pendings[ty] = map[*pendingInfluence]struct{}{}
		h.pendingSums[ty] = nil
	}
	h.resourcePendings = make(map[uint64][2]*operator.Operator)
}

// rwType : the perspective of balance
type rwType int

const (
	write rwType = iota
	read
)

func (rw rwType) String() string {
	switch rw {
	case read:
		return "read"
	case write:
		return "write"
	default:
		return ""
	}
}

type opType int

const (
	movePeer opType = iota
	transferLeader
)

func (ty opType) String() string {
	switch ty {
	case movePeer:
		return "move-peer"
	case transferLeader:
		return "transfer-leader"
	default:
		return ""
	}
}

type resourceType int

const (
	writePeer resourceType = iota
	writeLeader
	readLeader
	resourceTypeLen
)

func toResourceType(rwTy rwType, opTy opType) resourceType {
	switch rwTy {
	case write:
		switch opTy {
		case movePeer:
			return writePeer
		case transferLeader:
			return writeLeader
		}
	case read:
		return readLeader
	}
	panic(fmt.Sprintf("invalid arguments for toResourceType: rwTy = %v, opTy = %v", rwTy, opTy))
}
