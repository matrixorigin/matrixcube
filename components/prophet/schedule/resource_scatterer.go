package schedule

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
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
	mu sync.Mutex
	// If checkExist is true, after each putting operation, an entry with the key constructed by group and containerID would be put
	// into "containers" map. And the entry with the same key (containerID, group) couldn't be put before "containers" being reset
	checkExist bool

	containers        *cache.TTLString // value type: map[uint64]struct{}, group -> containerID -> struct{}
	groupDistribution *cache.TTLString // value type: map[uint64]uint64, group -> containerID -> count
}

func newSelectedContainers(ctx context.Context, checkExist bool) *selectedContainers {
	return &selectedContainers{
		checkExist:        checkExist,
		containers:        cache.NewStringTTL(ctx, gcInterval, gcTTL),
		groupDistribution: cache.NewStringTTL(ctx, gcInterval, gcTTL),
	}
}

func (s *selectedContainers) getContainer(group string) (map[uint64]struct{}, bool) {
	if result, ok := s.containers.Get(group); ok {
		return result.(map[uint64]struct{}), true
	}
	return nil, false
}

func (s *selectedContainers) getGroupDistribution(group string) (map[uint64]uint64, bool) {
	if result, ok := s.groupDistribution.Get(group); ok {
		return result.(map[uint64]uint64), true
	}
	return nil, false
}

func (s *selectedContainers) getContainerOrDefault(group string) map[uint64]struct{} {
	if result, ok := s.getContainer(group); ok {
		return result
	}
	return make(map[uint64]struct{})
}

func (s *selectedContainers) getGroupDistributionOrDefault(group string) map[uint64]uint64 {
	if result, ok := s.getGroupDistribution(group); ok {
		return result
	}
	return make(map[uint64]uint64)
}

func (s *selectedContainers) put(id uint64, group string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checkExist {
		placed := s.getContainerOrDefault(group)
		if _, ok := placed[id]; ok {
			return false
		}
		placed[id] = struct{}{}
		s.containers.Put(group, placed)
	}
	distribution := s.getGroupDistributionOrDefault(group)
	distribution[id] = distribution[id] + 1
	s.groupDistribution.Put(group, distribution)
	return true
}

func (s *selectedContainers) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.checkExist {
		return
	}
	s.containers.Clear()
}

func (s *selectedContainers) get(id uint64, group string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	distribution, ok := s.getGroupDistribution(group)
	if !ok {
		return 0
	}
	count, ok := distribution[id]
	if !ok {
		return 0
	}
	return count
}

func (s *selectedContainers) newFilters(scope, group string) []filter.Filter {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.checkExist {
		return nil
	}
	cloned := make(map[uint64]struct{})
	if groupPlaced, ok := s.getContainer(group); ok {
		for id := range groupPlaced {
			cloned[id] = struct{}{}
		}
	}
	return []filter.Filter{filter.NewExcludedFilter(scope, nil, cloned)}
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
	filters = append(filters, &filter.ContainerStateFilter{ActionScope: resourceScatterName})
	return engineContext{
		filters:        filters,
		selectedPeer:   newSelectedContainers(ctx, true),
		selectedLeader: newSelectedContainers(ctx, false),
	}
}

const maxSleepDuration = 1 * time.Minute
const initialSleepDuration = 100 * time.Millisecond
const maxRetryLimit = 30

// ScatterResourcesByRange directly scatter resources by ScatterResources
func (r *ResourceScatterer) ScatterResourcesByRange(startKey, endKey []byte, group string, retryLimit int) ([]*operator.Operator, map[uint64]error, error) {
	resources := r.cluster.ScanResources(startKey, endKey, -1)
	if len(resources) < 1 {
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
		return nil, nil, errors.New("empty resource")
	}
	failures := make(map[uint64]error, len(resourceIDs))
	var resources []*core.CachedResource
	for _, id := range resourceIDs {
		res := r.cluster.GetResource(id)
		if res == nil {
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
		return nil, fmt.Errorf("resource %d is not fully replicated", res.Meta.ID())
	}

	if res.GetLeader() == nil {
		return nil, fmt.Errorf("resource %d has no leader", res.Meta.ID())
	}

	if r.cluster.IsResourceHot(res) {
		return nil, fmt.Errorf("resource %d is hot", res.Meta.ID())
	}

	return r.scatterResource(res, group), nil
}

func (r *ResourceScatterer) scatterResource(res *core.CachedResource, group string) *operator.Operator {
	ordinaryFilter := filter.NewOrdinaryEngineFilter(r.name)
	var ordinaryPeers []metapb.Peer
	specialPeers := make(map[string][]metapb.Peer)
	// Group peers by the engine of their containers
	for _, peer := range res.Meta.Peers() {
		container := r.cluster.GetContainer(peer.ContainerID)
		if ordinaryFilter.Target(r.cluster.GetOpts(), container) {
			ordinaryPeers = append(ordinaryPeers, peer)
		} else {
			engine := container.GetLabelValue(filter.EngineKey)
			specialPeers[engine] = append(specialPeers[engine], peer)
		}
	}

	targetPeers := make(map[uint64]metapb.Peer)

	scatterWithSameEngine := func(peers []metapb.Peer, context engineContext) {
		containers := r.collectAvailableContainers(group, res, context)
		for _, peer := range peers {
			if len(containers) == 0 {
				context.selectedPeer.reset()
				containers = r.collectAvailableContainers(group, res, context)
			}
			if context.selectedPeer.put(peer.ContainerID, group) {
				delete(containers, peer.ContainerID)
				targetPeers[peer.ContainerID] = peer
				continue
			}
			newPeer, ok := r.selectPeerToReplace(group, containers, res, peer, context)
			if !ok {
				targetPeers[peer.ContainerID] = peer
				continue
			}
			// Remove it from containers and mark it as selected.
			delete(containers, newPeer.ContainerID)
			context.selectedPeer.put(newPeer.ContainerID, group)
			targetPeers[newPeer.ContainerID] = newPeer
		}
	}

	scatterWithSameEngine(ordinaryPeers, r.ordinaryEngine)
	// FIXME: target leader only considers the ordinary containers，maybe we need to consider the
	// special engine containers if the engine supports to become a leader. But now there is only
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
		util.GetLogger().Debugf("create scatter resource operator failed with %+v", err)
		return nil
	}
	op.SetPriorityLevel(core.HighPriority)
	return op
}

func (r *ResourceScatterer) selectPeerToReplace(group string, containers map[uint64]*core.CachedContainer, res *core.CachedResource, oldPeer metapb.Peer, context engineContext) (metapb.Peer, bool) {
	// scoreGuard guarantees that the distinct score will not decrease.
	containerID := oldPeer.ContainerID
	sourceContainer := r.cluster.GetContainer(containerID)
	if sourceContainer == nil {
		util.GetLogger().Errorf("get the container %d failed with container not found",
			containerID)
		return metapb.Peer{}, false
	}
	scoreGuard := filter.NewPlacementSafeguard(r.name, r.cluster, res, sourceContainer, r.cluster.GetResourceFactory())

	candidates := make([]*core.CachedContainer, 0, len(containers))
	for _, container := range containers {
		if !scoreGuard.Target(r.cluster.GetOpts(), container) {
			continue
		}
		candidates = append(candidates, container)
	}

	if len(candidates) == 0 {
		return metapb.Peer{}, false
	}

	minPeer := uint64(math.MaxUint64)
	var selectedCandidateID uint64
	for _, candidate := range candidates {
		count := context.selectedPeer.get(candidate.Meta.ID(), group)
		if count < minPeer {
			minPeer = count
			selectedCandidateID = candidate.Meta.ID()
		}
	}
	if selectedCandidateID < 1 {
		target := candidates[rand.Intn(len(candidates))]
		return metapb.Peer{
			ContainerID: target.Meta.ID(),
			Role:        oldPeer.GetRole(),
		}, true
	}

	return metapb.Peer{
		ContainerID: selectedCandidateID,
		Role:        oldPeer.GetRole(),
	}, true
}

func (r *ResourceScatterer) collectAvailableContainers(group string, res *core.CachedResource, context engineContext) map[uint64]*core.CachedContainer {
	filters := []filter.Filter{
		filter.NewExcludedFilter(r.name, nil, res.GetContainerIDs()),
		&filter.ContainerStateFilter{ActionScope: r.name, MoveResource: true},
	}
	filters = append(filters, context.filters...)
	filters = append(filters, context.selectedPeer.newFilters(r.name, group)...)

	containers := r.cluster.GetContainers()
	targets := make(map[uint64]*core.CachedContainer, len(containers))
	for _, container := range containers {
		if filter.Target(r.cluster.GetOpts(), container, filters) && !container.IsBusy() {
			targets[container.Meta.ID()] = container
		}
	}
	return targets
}

// selectAvailableLeaderContainers select the target leader container from the candidates. The candidates would be collected by
// the existed peers container depended on the leader counts in the group level.
func (r *ResourceScatterer) selectAvailableLeaderContainers(group string, peers map[uint64]metapb.Peer, context engineContext) uint64 {
	minContainerGroupLeader := uint64(math.MaxUint64)
	id := uint64(0)
	for containerID := range peers {
		containerGroupLeaderCount := context.selectedLeader.get(containerID, group)
		if minContainerGroupLeader > containerGroupLeaderCount {
			minContainerGroupLeader = containerGroupLeaderCount
			id = containerID
		}
	}
	if id != 0 {
		context.selectedLeader.put(id, group)
	}
	return id
}
