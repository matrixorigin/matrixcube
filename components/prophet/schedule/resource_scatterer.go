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

package schedule

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/filter"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/cache"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
)

const resourceScatterName = "resource-scatter"

var gcInterval = time.Minute
var gcTTL = time.Minute * 3

type selectedContainers struct {
	mu sync.RWMutex

	groupDistribution *cache.TTLString // value type: map[uint64]uint64, group -> containerID -> count
}

func newSelectedContainers(ctx context.Context) *selectedContainers {
	return &selectedContainers{
		groupDistribution: cache.NewStringTTL(ctx, gcInterval, gcTTL),
	}
}

func (s *selectedContainers) put(id uint64, group string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	distribution, ok := s.getDistributionByGroupLocked(group)
	if !ok {
		distribution = map[uint64]uint64{}
		distribution[id] = 0
	}
	distribution[id] = distribution[id] + 1
	s.groupDistribution.Put(group, distribution)
}

func (s *selectedContainers) get(id uint64, group string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	distribution, ok := s.getDistributionByGroupLocked(group)
	if !ok {
		return 0
	}
	count, ok := distribution[id]
	if !ok {
		return 0
	}
	return count
}

// GetGroupDistribution get distribution group by `group`
func (s *selectedContainers) GetGroupDistribution(group string) (map[uint64]uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getDistributionByGroupLocked(group)
}

// getDistributionByGroupLocked should be called with lock
func (s *selectedContainers) getDistributionByGroupLocked(group string) (map[uint64]uint64, bool) {
	if result, ok := s.groupDistribution.Get(group); ok {
		return result.(map[uint64]uint64), true
	}
	return nil, false
}

// ResourceScatterer scatters resources.
type ResourceScatterer struct {
	ctx            context.Context
	name           string
	cluster        opt.Cluster
	ordinaryEngine engineContext
	specialEngines map[string]engineContext
}

// NewResourceScatterer creates a resource scatterer.
// ResourceScatter is used for the `Lightning`, it will scatter the specified resources before import data.
func NewResourceScatterer(ctx context.Context, cluster opt.Cluster) *ResourceScatterer {
	return &ResourceScatterer{
		ctx:            ctx,
		name:           resourceScatterName,
		cluster:        cluster,
		ordinaryEngine: newEngineContext(ctx, filter.NewOrdinaryEngineFilter(resourceScatterName)),
		specialEngines: make(map[string]engineContext),
	}
}

type engineContext struct {
	filters        []filter.Filter
	selectedPeer   *selectedContainers
	selectedLeader *selectedContainers
}

func newEngineContext(ctx context.Context, filters ...filter.Filter) engineContext {
	filters = append(filters, &filter.ContainerStateFilter{ActionScope: resourceScatterName, MoveResource: true, ScatterResource: true})
	return engineContext{
		filters:        filters,
		selectedPeer:   newSelectedContainers(ctx),
		selectedLeader: newSelectedContainers(ctx),
	}
}

const maxSleepDuration = 1 * time.Minute
const initialSleepDuration = 100 * time.Millisecond
const maxRetryLimit = 30

// ScatterResourcesByRange directly scatter resources by ScatterResources
func (r *ResourceScatterer) ScatterResourcesByRange(resGroup uint64, startKey, endKey []byte, group string, retryLimit int) ([]*operator.Operator, map[uint64]error, error) {
	resources := r.cluster.ScanResources(resGroup, startKey, endKey, -1)
	if len(resources) < 1 {
		scatterCounter.WithLabelValues("skip", "empty-resource").Inc()
		return nil, nil, errors.New("empty resource")
	}
	failures := make(map[uint64]error, len(resources))
	resourceMap := make(map[uint64]*core.CachedResource, len(resources))
	for _, res := range resources {
		resourceMap[res.Meta.ID()] = res
	}
	// If there existed any resource failed to relocated after retry, add it into unProcessedResources
	ops, err := r.ScatterResources(resourceMap, failures, group, retryLimit)
	if err != nil {
		return nil, nil, err
	}
	return ops, failures, nil
}

// ScatterResourcesByID directly scatter resources by ScatterResources
func (r *ResourceScatterer) ScatterResourcesByID(resourceIDs []uint64, group string, retryLimit int) ([]*operator.Operator, map[uint64]error, error) {
	if len(resourceIDs) < 1 {
		scatterCounter.WithLabelValues("skip", "empty-resource").Inc()
		return nil, nil, errors.New("empty resource")
	}
	failures := make(map[uint64]error, len(resourceIDs))
	var resources []*core.CachedResource
	for _, id := range resourceIDs {
		res := r.cluster.GetResource(id)
		if res == nil {
			scatterCounter.WithLabelValues("skip", "no-resource").Inc()
			util.GetLogger().Warningf("resource %d failed to find region during scatter", id)
			failures[id] = fmt.Errorf("failed to find resource %v", id)
			continue
		}
		resources = append(resources, res)
	}
	resourceMap := make(map[uint64]*core.CachedResource, len(resources))
	for _, res := range resources {
		resourceMap[res.Meta.ID()] = res
	}
	// If there existed any resource failed to relocated after retry, add it into unProcessedResources
	ops, err := r.ScatterResources(resourceMap, failures, group, retryLimit)
	if err != nil {
		return nil, nil, err
	}
	return ops, failures, nil
}

// ScatterResources relocates the resources. If the group is defined, the resources' leader with the same group would be scattered
// in a group level instead of cluster level.
// RetryTimes indicates the retry times if any of the resources failed to relocate during scattering. There will be
// time.Sleep between each retry.
// Failures indicates the resources which are failed to be relocated, the key of the failures indicates the resID
// and the value of the failures indicates the failure error.
func (r *ResourceScatterer) ScatterResources(resources map[uint64]*core.CachedResource, failures map[uint64]error, group string, retryLimit int) ([]*operator.Operator, error) {
	if len(resources) < 1 {
		scatterCounter.WithLabelValues("skip", "empty-resource").Inc()
		return nil, errors.New("empty resource")
	}
	if retryLimit > maxRetryLimit {
		retryLimit = maxRetryLimit
	}
	ops := make([]*operator.Operator, 0, len(resources))
	for currentRetry := 0; currentRetry <= retryLimit; currentRetry++ {
		for _, res := range resources {
			op, err := r.Scatter(res, group)
			if err != nil {
				failures[res.Meta.ID()] = err
				continue
			}
			if op != nil {
				ops = append(ops, op)
			}
			delete(resources, res.Meta.ID())
			delete(failures, res.Meta.ID())
		}
		// all resources have been relocated, break the loop.
		if len(resources) < 1 {
			break
		}
		// Wait for a while if there are some resources failed to be relocated
		time.Sleep(typeutil.MinDuration(maxSleepDuration, time.Duration(math.Pow(2, float64(currentRetry)))*initialSleepDuration))
	}
	return ops, nil
}

// Scatter relocates the resource. If the group is defined, the resources' leader with the same group would be scattered
// in a group level instead of cluster level.
func (r *ResourceScatterer) Scatter(res *core.CachedResource, group string) (*operator.Operator, error) {
	if !opt.IsResourceReplicated(r.cluster, res) {
		r.cluster.AddSuspectResources(res.Meta.ID())
		scatterCounter.WithLabelValues("skip", "not-replicated").Inc()
		util.GetLogger().Warningf("resource %d not replicated during scatter", res.Meta.ID())
		return nil, fmt.Errorf("resource %d is not fully replicated", res.Meta.ID())
	}

	if res.GetLeader() == nil {
		scatterCounter.WithLabelValues("skip", "no-leader").Inc()
		util.GetLogger().Warningf("resource %d no leader during scatter", res.Meta.ID())
		return nil, fmt.Errorf("resource %d has no leader", res.Meta.ID())
	}

	if r.cluster.IsResourceHot(res) {
		scatterCounter.WithLabelValues("skip", "hot").Inc()
		util.GetLogger().Warningf("resource %d  too hot during scatter", res.Meta.ID())
		return nil, fmt.Errorf("resource %d is hot", res.Meta.ID())
	}

	return r.scatterResource(res, group), nil
}

func (r *ResourceScatterer) scatterResource(res *core.CachedResource, group string) *operator.Operator {
	ordinaryFilter := filter.NewOrdinaryEngineFilter(r.name)
	ordinaryPeers := make(map[uint64]metapb.Peer)
	specialPeers := make(map[string]map[uint64]metapb.Peer)
	// Group peers by the engine of their stores
	for _, peer := range res.Meta.Peers() {
		store := r.cluster.GetContainer(peer.GetContainerID())
		if ordinaryFilter.Target(r.cluster.GetOpts(), store) {
			ordinaryPeers[peer.ID] = peer
		} else {
			engine := store.GetLabelValue(filter.EngineKey)
			if _, ok := specialPeers[engine]; !ok {
				specialPeers[engine] = make(map[uint64]metapb.Peer)
			}
			specialPeers[engine][peer.ID] = peer
		}
	}

	targetPeers := make(map[uint64]metapb.Peer)
	selectedStores := make(map[uint64]struct{})
	scatterWithSameEngine := func(peers map[uint64]metapb.Peer, context engineContext) {
		for _, peer := range peers {
			candidates := r.selectCandidates(res, peer.GetContainerID(), selectedStores, context)
			newPeer := r.selectContainer(group, &peer, peer.GetContainerID(), candidates, context)
			targetPeers[newPeer.GetContainerID()] = *newPeer
			selectedStores[newPeer.GetContainerID()] = struct{}{}
		}
	}

	scatterWithSameEngine(ordinaryPeers, r.ordinaryEngine)
	// FIXME: target leader only considers the ordinary stores，maybe we need to consider the
	// special engine stores if the engine supports to become a leader. But now there is only
	// one engine, tiflash, which does not support the leader, so don't consider it for now.
	targetLeader := r.selectAvailableLeaderContainers(group, targetPeers, r.ordinaryEngine)

	for engine, peers := range specialPeers {
		ctx, ok := r.specialEngines[engine]
		if !ok {
			ctx = newEngineContext(r.ctx, filter.NewEngineFilter(r.name, engine))
			r.specialEngines[engine] = ctx
		}
		scatterWithSameEngine(peers, ctx)
	}

	op, err := operator.CreateScatterResourceOperator("scatter-resource", r.cluster, res, targetPeers, targetLeader)
	if err != nil {
		scatterCounter.WithLabelValues("fail", "").Inc()
		for _, peer := range res.Meta.Peers() {
			targetPeers[peer.GetContainerID()] = peer
		}
		r.Put(targetPeers, res.GetLeader().GetContainerID(), group)
		util.GetLogger().Debugf("create scatter resource operator failed with %+v", err)
		return nil
	}
	if op != nil {
		scatterCounter.WithLabelValues("success", "").Inc()
		r.Put(targetPeers, targetLeader, group)
		op.SetPriorityLevel(core.HighPriority)
	}
	return op
}

func (r *ResourceScatterer) selectCandidates(res *core.CachedResource, sourceContainerID uint64, selectedContainers map[uint64]struct{}, context engineContext) []uint64 {
	sourceStore := r.cluster.GetContainer(sourceContainerID)
	if sourceStore == nil {
		util.GetLogger().Errorf("failed to get the container %d", sourceContainerID)
		return nil
	}
	filters := []filter.Filter{
		filter.NewExcludedFilter(r.name, nil, selectedContainers),
	}
	scoreGuard := filter.NewPlacementSafeguard(r.name, r.cluster, res, sourceStore, r.cluster.GetResourceFactory())
	filters = append(filters, context.filters...)
	filters = append(filters, scoreGuard)
	stores := r.cluster.GetContainers()
	candidates := make([]uint64, 0)
	for _, store := range stores {
		if filter.Target(r.cluster.GetOpts(), store, filters) {
			candidates = append(candidates, store.Meta.ID())
		}
	}
	return candidates
}

func (r *ResourceScatterer) selectContainer(group string, peer *metapb.Peer, sourceContainerID uint64, candidates []uint64, context engineContext) *metapb.Peer {
	if len(candidates) < 1 {
		return peer
	}
	var newPeer *metapb.Peer
	minCount := uint64(math.MaxUint64)
	for _, storeID := range candidates {
		count := context.selectedPeer.get(storeID, group)
		if count < minCount {
			minCount = count
			newPeer = &metapb.Peer{
				ContainerID: storeID,
				Role:        peer.GetRole(),
			}
		}
	}
	// if the source store have the least count, we don't need to scatter this peer
	for _, containerID := range candidates {
		if containerID == sourceContainerID && context.selectedPeer.get(sourceContainerID, group) <= minCount {
			return peer
		}
	}
	if newPeer == nil {
		return peer
	}
	return newPeer
}

// selectAvailableLeaderContainers select the target leader container from the candidates. The candidates would be collected by
// the existed peers container depended on the leader counts in the group level.
func (r *ResourceScatterer) selectAvailableLeaderContainers(group string, peers map[uint64]metapb.Peer, context engineContext) uint64 {
	minContainerGroupLeader := uint64(math.MaxUint64)
	id := uint64(0)
	for storeID := range peers {
		if id == 0 {
			id = storeID
		}
		storeGroupLeaderCount := context.selectedLeader.get(storeID, group)
		if minContainerGroupLeader > storeGroupLeaderCount {
			minContainerGroupLeader = storeGroupLeaderCount
			id = storeID
		}
	}
	return id
}

// Put put the final distribution in the context no matter the operator was created
func (r *ResourceScatterer) Put(peers map[uint64]metapb.Peer, leaderContainerID uint64, group string) {
	ordinaryFilter := filter.NewOrdinaryEngineFilter(r.name)
	// Group peers by the engine of their stores
	for _, peer := range peers {
		containerID := peer.GetContainerID()
		container := r.cluster.GetContainer(containerID)
		if ordinaryFilter.Target(r.cluster.GetOpts(), container) {
			r.ordinaryEngine.selectedPeer.put(containerID, group)
			scatterDistributionCounter.WithLabelValues(
				fmt.Sprintf("%v", containerID),
				strconv.FormatBool(false),
				filter.EngineTiKV).Inc()
		} else {
			engine := container.GetLabelValue(filter.EngineKey)
			r.specialEngines[engine].selectedPeer.put(containerID, group)
			scatterDistributionCounter.WithLabelValues(
				fmt.Sprintf("%v", containerID),
				strconv.FormatBool(false),
				engine).Inc()
		}
	}
	r.ordinaryEngine.selectedLeader.put(leaderContainerID, group)
	scatterDistributionCounter.WithLabelValues(
		fmt.Sprintf("%v", leaderContainerID),
		strconv.FormatBool(true),
		filter.EngineTiKV).Inc()
}
