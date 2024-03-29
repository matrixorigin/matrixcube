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

package checker

import (
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/filter"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
)

// ReplicaStrategy collects some utilities to manipulate resource peers. It
// exists to allow replica_checker and rule_checker to reuse common logics.
type ReplicaStrategy struct {
	checkerName    string // replica-checker / rule-checker
	cluster        opt.Cluster
	locationLabels []string
	isolationLevel string
	resource       *core.CachedShard
	extraFilters   []filter.Filter
}

// SelectStoreToAdd returns the container to add a replica to a resource.
// `coLocationStores` are the containers used to compare location with target
// container.
// `extraFilters` is used to set up more filters based on the context that
// calling this method.
//
// For example, to select a target container to replace a resource's peer, we can use
// the peer list with the peer removed as `coLocationStores`.
// Meanwhile, we need to provide more constraints to ensure that the isolation
// level cannot be reduced after replacement.
func (s *ReplicaStrategy) SelectStoreToAdd(coLocationStores []*core.CachedStore, extraFilters ...filter.Filter) uint64 {
	// The selection process uses a two-stage fashion. The first stage
	// ignores the temporary state of the containers and selects the containers
	// with the highest score according to the location label. The second
	// stage considers all temporary states and capacity factors to select
	// the most suitable target.
	//
	// The reason for it is to prevent the non-optimal replica placement due
	// to the short-term state, resulting in redundant scheduling.
	filters := []filter.Filter{
		filter.NewExcludedFilter(s.checkerName, nil, s.resource.GetStoreIDs()),
		filter.NewStorageThresholdFilter(s.checkerName),
		filter.NewSpecialUseFilter(s.checkerName),
		&filter.StoreStateFilter{ActionScope: s.checkerName, MoveShard: true, AllowTemporaryStates: true},
	}
	if len(s.locationLabels) > 0 && s.isolationLevel != "" {
		filters = append(filters, filter.NewIsolationFilter(s.checkerName, s.isolationLevel, s.locationLabels, coLocationStores))
	}
	if len(extraFilters) > 0 {
		filters = append(filters, extraFilters...)
	}
	if len(s.extraFilters) > 0 {
		filters = append(filters, s.extraFilters...)
	}

	isolationComparer := filter.IsolationComparer(s.locationLabels, coLocationStores)
	strictStateFilter := &filter.StoreStateFilter{ActionScope: s.checkerName, MoveShard: true}
	target := filter.NewCandidates(s.cluster.GetStores()).
		FilterTarget(s.cluster.GetOpts(), filters...).
		Sort(isolationComparer).Reverse().Top(isolationComparer).                       // greater isolation score is better
		Sort(filter.ShardScoreComparer(s.resource.GetGroupKey(), s.cluster.GetOpts())). // less resource score is better
		FilterTarget(s.cluster.GetOpts(), strictStateFilter).PickFirst()                // the filter does not ignore temp states
	if target == nil {
		return 0
	}
	return target.Meta.GetID()
}

// SelectStoreToReplace returns a container to replace oldStore. The location
// placement after scheduling should be not worse than original.
func (s *ReplicaStrategy) SelectStoreToReplace(coLocationStores []*core.CachedStore, old uint64) uint64 {
	// trick to avoid creating a slice with `old` removed.
	s.swapStoreToFirst(coLocationStores, old)
	safeGuard := filter.NewLocationSafeguard(s.checkerName, s.locationLabels, coLocationStores,
		s.cluster.GetStore(old))
	return s.SelectStoreToAdd(coLocationStores[1:], safeGuard)
}

// SelectStoreToImprove returns a container to replace oldStore. The location
// placement after scheduling should be better than original.
func (s *ReplicaStrategy) SelectStoreToImprove(coLocationStores []*core.CachedStore, old uint64) uint64 {
	// trick to avoid creating a slice with `old` removed.
	s.swapStoreToFirst(coLocationStores, old)
	filters := []filter.Filter{
		filter.NewLocationImprover(s.checkerName, s.locationLabels, coLocationStores, s.cluster.GetStore(old)),
	}
	if len(s.locationLabels) > 0 && s.isolationLevel != "" {
		filters = append(filters, filter.NewIsolationFilter(s.checkerName, s.isolationLevel, s.locationLabels, coLocationStores[1:]))
	}
	return s.SelectStoreToAdd(coLocationStores[1:], filters...)
}

func (s *ReplicaStrategy) swapStoreToFirst(containers []*core.CachedStore, id uint64) {
	for i, s := range containers {
		if s.Meta.GetID() == id {
			containers[0], containers[i] = containers[i], containers[0]
			return
		}
	}
}

// SelectStoreToRemove returns the best option to remove from the resource.
func (s *ReplicaStrategy) SelectStoreToRemove(coLocationStores []*core.CachedStore) uint64 {
	isolationComparer := filter.IsolationComparer(s.locationLabels, coLocationStores)
	source := filter.NewCandidates(coLocationStores).
		FilterSource(s.cluster.GetOpts(), &filter.StoreStateFilter{ActionScope: replicaCheckerName, MoveShard: true}).
		Sort(isolationComparer).Top(isolationComparer).
		Sort(filter.ShardScoreComparer(s.resource.GetGroupKey(), s.cluster.GetOpts())).Reverse().
		PickFirst()
	if source == nil {
		s.cluster.GetLogger().Debug("resource no removable container",
			log.ResourceField(s.resource.Meta.GetID()))
		return 0
	}
	return source.Meta.GetID()
}
