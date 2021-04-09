package checker

import (
	"github.com/deepfabric/prophet/core"
	"github.com/deepfabric/prophet/schedule/filter"
	"github.com/deepfabric/prophet/schedule/opt"
	"github.com/deepfabric/prophet/util"
)

// ReplicaStrategy collects some utilities to manipulate resource peers. It
// exists to allow replica_checker and rule_checker to reuse common logics.
type ReplicaStrategy struct {
	checkerName    string // replica-checker / rule-checker
	cluster        opt.Cluster
	locationLabels []string
	isolationLevel string
	resource       *core.CachedResource
	extraFilters   []filter.Filter
}

// SelectContainerToAdd returns the container to add a replica to a resource.
// `coLocationContainers` are the containers used to compare location with target
// container.
// `extraFilters` is used to set up more filters based on the context that
// calling this method.
//
// For example, to select a target container to replace a resource's peer, we can use
// the peer list with the peer removed as `coLocationContainers`.
// Meanwhile, we need to provide more constraints to ensure that the isolation
// level cannot be reduced after replacement.
func (s *ReplicaStrategy) SelectContainerToAdd(coLocationContainers []*core.CachedContainer, extraFilters ...filter.Filter) uint64 {
	// The selection process uses a two-stage fashion. The first stage
	// ignores the temporary state of the containers and selects the containers
	// with the highest score according to the location label. The second
	// stage considers all temporary states and capacity factors to select
	// the most suitable target.
	//
	// The reason for it is to prevent the non-optimal replica placement due
	// to the short-term state, resulting in redundant scheduling.
	filters := []filter.Filter{
		filter.NewExcludedFilter(s.checkerName, nil, s.resource.GetContainerIDs()),
		filter.NewStorageThresholdFilter(s.checkerName),
		filter.NewSpecialUseFilter(s.checkerName),
		&filter.ContainerStateFilter{ActionScope: s.checkerName, MoveResource: true, AllowTemporaryStates: true},
	}
	if len(s.locationLabels) > 0 && s.isolationLevel != "" {
		filters = append(filters, filter.NewIsolationFilter(s.checkerName, s.isolationLevel, s.locationLabels, coLocationContainers))
	}
	if len(extraFilters) > 0 {
		filters = append(filters, extraFilters...)
	}
	if len(s.extraFilters) > 0 {
		filters = append(filters, s.extraFilters...)
	}

	isolationComparer := filter.IsolationComparer(s.locationLabels, coLocationContainers)
	strictStateFilter := &filter.ContainerStateFilter{ActionScope: s.checkerName, MoveResource: true}
	target := filter.NewCandidates(s.cluster.GetContainers()).
		FilterTarget(s.cluster.GetOpts(), filters...).
		Sort(isolationComparer).Reverse().Top(isolationComparer).        // greater isolation score is better
		Sort(filter.ResourceScoreComparer(s.cluster.GetOpts())).         // less resource score is better
		FilterTarget(s.cluster.GetOpts(), strictStateFilter).PickFirst() // the filter does not ignore temp states
	if target == nil {
		return 0
	}
	return target.Meta.ID()
}

// SelectContainerToReplace returns a container to replace oldContainer. The location
// placement after scheduling should be not worse than original.
func (s *ReplicaStrategy) SelectContainerToReplace(coLocationContainers []*core.CachedContainer, old uint64) uint64 {
	// trick to avoid creating a slice with `old` removed.
	s.swapContainerToFirst(coLocationContainers, old)
	safeGuard := filter.NewLocationSafeguard(s.checkerName, s.locationLabels, coLocationContainers,
		s.cluster.GetContainer(old))
	return s.SelectContainerToAdd(coLocationContainers[1:], safeGuard)
}

// SelectContainerToImprove returns a container to replace oldContainer. The location
// placement after scheduling should be better than original.
func (s *ReplicaStrategy) SelectContainerToImprove(coLocationContainers []*core.CachedContainer, old uint64) uint64 {
	// trick to avoid creating a slice with `old` removed.
	s.swapContainerToFirst(coLocationContainers, old)
	filters := []filter.Filter{
		filter.NewLocationImprover(s.checkerName, s.locationLabels, coLocationContainers, s.cluster.GetContainer(old)),
	}
	if len(s.locationLabels) > 0 && s.isolationLevel != "" {
		filters = append(filters, filter.NewIsolationFilter(s.checkerName, s.isolationLevel, s.locationLabels, coLocationContainers[1:]))
	}
	return s.SelectContainerToAdd(coLocationContainers[1:], filters...)
}

func (s *ReplicaStrategy) swapContainerToFirst(containers []*core.CachedContainer, id uint64) {
	for i, s := range containers {
		if s.Meta.ID() == id {
			containers[0], containers[i] = containers[i], containers[0]
			return
		}
	}
}

// SelectContainerToRemove returns the best option to remove from the resource.
func (s *ReplicaStrategy) SelectContainerToRemove(coLocationContainers []*core.CachedContainer) uint64 {
	isolationComparer := filter.IsolationComparer(s.locationLabels, coLocationContainers)
	source := filter.NewCandidates(coLocationContainers).
		FilterSource(s.cluster.GetOpts(), &filter.ContainerStateFilter{ActionScope: replicaCheckerName, MoveResource: true}).
		Sort(isolationComparer).Top(isolationComparer).
		Sort(filter.ResourceScoreComparer(s.cluster.GetOpts())).Reverse().
		PickFirst()
	if source == nil {
		util.GetLogger().Debugf("resource %d no removable container", s.resource.Meta.ID())
		return 0
	}
	return source.Meta.ID()
}
