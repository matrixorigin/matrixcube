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

package filter

import (
	"fmt"
	"log"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/util/slice"
)

// revive:disable:unused-parameter

var (
	// LogWhySkipped cause why the filter skip a container
	LogWhySkipped = false
)

// SelectSourceContainers selects containers that be selected as source container from the list.
func SelectSourceContainers(containers []*core.CachedContainer, filters []Filter, opt *config.PersistOptions) []*core.CachedContainer {
	return filterContainersBy(containers, func(s *core.CachedContainer) bool {
		return slice.AllOf(filters, func(i int) bool {
			if !filters[i].Source(opt, s) {
				sourceID := fmt.Sprintf("%d", s.Meta.ID())
				targetID := ""
				filterCounter.WithLabelValues("filter-source", s.Meta.Addr(),
					sourceID, filters[i].Scope(), filters[i].Type(), sourceID, targetID).Inc()
				return false
			}
			return true
		})
	})
}

// SelectTargetContainers selects containers that be selected as target container from the list.
func SelectTargetContainers(containers []*core.CachedContainer, filters []Filter, opt *config.PersistOptions) []*core.CachedContainer {
	return filterContainersBy(containers, func(s *core.CachedContainer) bool {
		return slice.AllOf(filters, func(i int) bool {
			filter := filters[i]
			if !filter.Target(opt, s) {
				cfilter, ok := filter.(comparingFilter)
				targetID := fmt.Sprintf("%d", s.Meta.ID())
				sourceID := ""
				if ok {
					sourceID = fmt.Sprintf("%d", cfilter.GetSourceContainerID())
				}
				filterCounter.WithLabelValues("filter-target", s.Meta.Addr(),
					targetID, filters[i].Scope(), filters[i].Type(), sourceID, targetID).Inc()
				return false
			}
			return true
		})
	})
}

func filterContainersBy(containers []*core.CachedContainer, keepPred func(*core.CachedContainer) bool) (selected []*core.CachedContainer) {
	for _, s := range containers {
		if keepPred(s) {
			selected = append(selected, s)
		}
	}
	return
}

// Filter is an interface to filter source and target container.
type Filter interface {
	// Scope is used to indicate where the filter will act on.
	Scope() string
	Type() string
	// Return true if the container can be used as a source container.
	Source(opt *config.PersistOptions, container *core.CachedContainer) bool
	// Return true if the container can be used as a target container.
	Target(opt *config.PersistOptions, container *core.CachedContainer) bool
}

// comparingFilter is an interface to filter target store by comparing source and target stores
type comparingFilter interface {
	Filter
	// GetSourceContainerID returns the source store when comparing.
	GetSourceContainerID() uint64
}

// Source checks if container can pass all Filters as source container.
func Source(opt *config.PersistOptions, container *core.CachedContainer, filters []Filter) bool {
	containerAddress := container.Meta.Addr()
	containerID := fmt.Sprintf("%d", container.Meta.ID())
	for _, filter := range filters {
		if !filter.Source(opt, container) {
			sourceID := containerID
			targetID := ""
			filterCounter.WithLabelValues("filter-source", containerAddress,
				sourceID, filter.Scope(), filter.Type(), sourceID, targetID).Inc()
			return false
		}
	}
	return true
}

// Target checks if container can pass all Filters as target container.
func Target(opt *config.PersistOptions, container *core.CachedContainer, filters []Filter) bool {
	containerAddress := container.Meta.Addr()
	containerID := fmt.Sprintf("%d", container.Meta.ID())
	for _, filter := range filters {
		if !filter.Target(opt, container) {
			cfilter, ok := filter.(comparingFilter)
			targetID := containerID
			sourceID := ""
			if ok {
				sourceID = fmt.Sprintf("%d", cfilter.GetSourceContainerID())
			}
			filterCounter.WithLabelValues("filter-target", containerAddress,
				targetID, filter.Scope(), filter.Type(), sourceID, targetID).Inc()
			return false
		}
	}
	return true
}

type excludedFilter struct {
	scope   string
	sources map[uint64]struct{}
	targets map[uint64]struct{}
}

// NewExcludedFilter creates a Filter that filters all specified containers.
func NewExcludedFilter(scope string, sources, targets map[uint64]struct{}) Filter {
	return &excludedFilter{
		scope:   scope,
		sources: sources,
		targets: targets,
	}
}

func (f *excludedFilter) Scope() string {
	return f.scope
}

func (f *excludedFilter) Type() string {
	return "exclude-filter"
}

func (f *excludedFilter) Source(opt *config.PersistOptions, container *core.CachedContainer) bool {
	_, ok := f.sources[container.Meta.ID()]
	if ok && LogWhySkipped {
		f.maybeLogWhy(container)
	}
	return !ok
}

func (f *excludedFilter) Target(opt *config.PersistOptions, container *core.CachedContainer) bool {
	_, ok := f.targets[container.Meta.ID()]
	if ok && LogWhySkipped {
		f.maybeLogWhy(container)
	}
	return !ok
}

func (f *excludedFilter) maybeLogWhy(container *core.CachedContainer) {
	if LogWhySkipped {
		log.Printf("excludedFilter skip container %d, excluded: %+v",
			container.Meta.ID(),
			f.sources)
	}
}

type storageThresholdFilter struct {
	scope string
}

// NewStorageThresholdFilter creates a Filter that filters all containers that are
// almost full.
func NewStorageThresholdFilter(scope string) Filter {
	return &storageThresholdFilter{scope: scope}
}

func (f *storageThresholdFilter) Scope() string {
	return f.scope
}

func (f *storageThresholdFilter) Type() string {
	return "storage-threshold-filter"
}

func (f *storageThresholdFilter) Source(opt *config.PersistOptions, container *core.CachedContainer) bool {
	return true
}

func (f *storageThresholdFilter) Target(opt *config.PersistOptions, container *core.CachedContainer) bool {
	v := !container.IsLowSpace(opt.GetLowSpaceRatio())
	if !v && LogWhySkipped {
		log.Printf("storageThresholdFilter skip container %d, LowSpaceRatio %+v, Stats %+v, AvailableRatio %+v",
			container.Meta.ID(),
			opt.GetLowSpaceRatio(),
			container.GetContainerStats(),
			container.AvailableRatio())
	}
	return v
}

// distinctScoreFilter ensures that distinct score will not decrease.
type distinctScoreFilter struct {
	scope        string
	labels       []string
	containers   []*core.CachedContainer
	policy       string
	safeScore    float64
	srcContainer uint64
}

const (
	// policies used by distinctScoreFilter.
	// 'safeguard' ensures replacement is NOT WORSE than before.
	// 'improve' ensures replacement is BETTER than before.
	locationSafeguard = "safeguard"
	locationImprove   = "improve"
)

// NewLocationSafeguard creates a filter that filters all containers that have
// lower distinct score than specified container.
func NewLocationSafeguard(scope string, labels []string, containers []*core.CachedContainer, source *core.CachedContainer) Filter {
	return newDistinctScoreFilter(scope, labels, containers, source, locationSafeguard)
}

// NewLocationImprover creates a filter that filters all containers that have
// lower or equal distinct score than specified container.
func NewLocationImprover(scope string, labels []string, containers []*core.CachedContainer, source *core.CachedContainer) Filter {
	return newDistinctScoreFilter(scope, labels, containers, source, locationImprove)
}

func newDistinctScoreFilter(scope string, labels []string, containers []*core.CachedContainer, source *core.CachedContainer, policy string) Filter {
	newContainers := make([]*core.CachedContainer, 0, len(containers)-1)
	for _, s := range containers {
		if s.Meta.ID() == source.Meta.ID() {
			continue
		}
		newContainers = append(newContainers, s)
	}

	return &distinctScoreFilter{
		scope:        scope,
		labels:       labels,
		containers:   newContainers,
		safeScore:    core.DistinctScore(labels, newContainers, source),
		policy:       policy,
		srcContainer: source.Meta.ID(),
	}
}

func (f *distinctScoreFilter) Scope() string {
	return f.scope
}

func (f *distinctScoreFilter) Type() string {
	return "distinct-filter"
}

func (f *distinctScoreFilter) Source(opt *config.PersistOptions, container *core.CachedContainer) bool {
	return true
}

func (f *distinctScoreFilter) Target(opt *config.PersistOptions, container *core.CachedContainer) bool {
	score := core.DistinctScore(f.labels, f.containers, container)
	switch f.policy {
	case locationSafeguard:
		return score >= f.safeScore
	case locationImprove:
		return score > f.safeScore
	default:
		return false
	}
}

// GetSourceContainerID implements the ComparingFilter
func (f *distinctScoreFilter) GetSourceContainerID() uint64 {
	return f.srcContainer
}

// ContainerStateFilter is used to determine whether a container can be selected as the
// source or target of the schedule based on the container's state.
type ContainerStateFilter struct {
	ActionScope string
	// Set true if the schedule involves any transfer leader operation.
	TransferLeader bool
	// Set true if the schedule involves any move resource operation.
	MoveResource bool
	// Set true if the scatter move the resource
	ScatterResource bool
	// Set true if allows temporary states.
	AllowTemporaryStates bool
	// Reason is used to distinguish the reason of container state filter
	Reason string
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f *ContainerStateFilter) Scope() string {
	return f.ActionScope
}

// Type returns the type of the Filter.
func (f *ContainerStateFilter) Type() string {
	return fmt.Sprintf("container-state-%s-filter", f.Reason)
}

// conditionFunc defines condition to determine a container should be selected.
// It should consider if the filter allows temporary states.
type conditionFunc func(*config.PersistOptions, *core.CachedContainer) bool

func (f *ContainerStateFilter) isTombstone(opt *config.PersistOptions, container *core.CachedContainer) bool {
	f.Reason = "tombstone"
	return container.IsTombstone()
}

func (f *ContainerStateFilter) isDown(opt *config.PersistOptions, container *core.CachedContainer) bool {
	f.Reason = "down"
	return container.DownTime() > opt.GetMaxContainerDownTime()
}

func (f *ContainerStateFilter) isOffline(opt *config.PersistOptions, container *core.CachedContainer) bool {
	f.Reason = "offline"
	return container.IsOffline()
}

func (f *ContainerStateFilter) pauseLeaderTransfer(opt *config.PersistOptions, container *core.CachedContainer) bool {
	f.Reason = "pause-leader"
	return !container.AllowLeaderTransfer()
}

func (f *ContainerStateFilter) isDisconnected(opt *config.PersistOptions, container *core.CachedContainer) bool {
	f.Reason = "disconnected"
	return !f.AllowTemporaryStates && container.IsDisconnected()
}

func (f *ContainerStateFilter) isBusy(opt *config.PersistOptions, container *core.CachedContainer) bool {
	f.Reason = "busy"
	return !f.AllowTemporaryStates && container.IsBusy()
}

func (f *ContainerStateFilter) exceedRemoveLimit(opt *config.PersistOptions, container *core.CachedContainer) bool {
	f.Reason = "exceed-remove-limit"
	return !f.AllowTemporaryStates && !container.IsAvailable(limit.RemovePeer)
}

func (f *ContainerStateFilter) exceedAddLimit(opt *config.PersistOptions, container *core.CachedContainer) bool {
	f.Reason = "exceed-add-limit"
	return !f.AllowTemporaryStates && !container.IsAvailable(limit.AddPeer)
}

func (f *ContainerStateFilter) tooManySnapshots(opt *config.PersistOptions, container *core.CachedContainer) bool {
	f.Reason = "too-many-snapshot"
	return !f.AllowTemporaryStates && (uint64(container.GetSendingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(container.GetReceivingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(container.GetApplyingSnapCount()) > opt.GetMaxSnapshotCount())
}

func (f *ContainerStateFilter) tooManyPendingPeers(opt *config.PersistOptions, container *core.CachedContainer) bool {
	f.Reason = "too-many-pending-peer"
	return !f.AllowTemporaryStates &&
		opt.GetMaxPendingPeerCount() > 0 &&
		container.GetPendingPeerCount() > int(opt.GetMaxPendingPeerCount())
}

func (f *ContainerStateFilter) hasRejectLeaderProperty(opts *config.PersistOptions, container *core.CachedContainer) bool {
	f.Reason = "reject-leader"
	return opts.CheckLabelProperty(opt.RejectLeader, container.Meta.Labels())
}

// The condition table.
// Y: the condition is temporary (expected to become false soon).
// N: the condition is expected to be true for a long time.
// X means when the condition is true, the container CANNOT be selected.
//
// Condition      Down Offline Tomb Pause Disconn Busy RmLimit AddLimit Snap Pending Reject
// IsTemporary    N    N       N    N     Y       Y    Y       Y        Y    Y       N
//
// LeaderSource   X            X    X     X
// ResourceSource                                  X    X                X
// LeaderTarget   X    X       X    X     X       X                                  X
// ResourceTarget X    X       X          X       X            X        X    X

const (
	leaderSource = iota
	resourceSource
	leaderTarget
	resourceTarget
	scatterResourceTarget
)

func (f *ContainerStateFilter) anyConditionMatch(typ int, opt *config.PersistOptions, container *core.CachedContainer) bool {
	var funcs []conditionFunc
	switch typ {
	case leaderSource:
		funcs = []conditionFunc{f.isTombstone, f.isDown, f.pauseLeaderTransfer, f.isDisconnected}
	case resourceSource:
		funcs = []conditionFunc{f.isBusy, f.exceedRemoveLimit, f.tooManySnapshots}
	case leaderTarget:
		funcs = []conditionFunc{f.isTombstone, f.isOffline, f.isDown, f.pauseLeaderTransfer,
			f.isDisconnected, f.isBusy, f.hasRejectLeaderProperty}
	case resourceTarget:
		funcs = []conditionFunc{f.isTombstone, f.isOffline, f.isDown, f.isDisconnected, f.isBusy,
			f.exceedAddLimit, f.tooManySnapshots, f.tooManyPendingPeers}
	case scatterResourceTarget:
		funcs = []conditionFunc{f.isTombstone, f.isOffline, f.isDown, f.isDisconnected, f.isBusy}

	}
	for _, cf := range funcs {
		if cf(opt, container) {
			return true
		}
	}
	return false
}

// Source returns true when the container can be selected as the schedule
// source.
func (f *ContainerStateFilter) Source(opts *config.PersistOptions, container *core.CachedContainer) bool {
	if f.TransferLeader && f.anyConditionMatch(leaderSource, opts, container) {
		return false
	}
	if f.MoveResource && f.anyConditionMatch(resourceSource, opts, container) {
		return false
	}
	return true
}

// Target returns true when the container can be selected as the schedule
// target.
func (f *ContainerStateFilter) Target(opts *config.PersistOptions, container *core.CachedContainer) bool {
	if f.TransferLeader && f.anyConditionMatch(leaderTarget, opts, container) {
		return false
	}
	if f.MoveResource && f.ScatterResource && f.anyConditionMatch(scatterResourceTarget, opts, container) {
		return false
	}
	if f.MoveResource && !f.ScatterResource && f.anyConditionMatch(resourceTarget, opts, container) {
		return false
	}
	return true
}

// labelConstraintFilter is a filter that selects containers satisfy the constraints.
type labelConstraintFilter struct {
	scope       string
	constraints []placement.LabelConstraint
}

// NewLabelConstaintFilter creates a filter that selects containers satisfy the constraints.
func NewLabelConstaintFilter(scope string, constraints []placement.LabelConstraint) Filter {
	return labelConstraintFilter{scope: scope, constraints: constraints}
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f labelConstraintFilter) Scope() string {
	return f.scope
}

// Type returns the name of the filter.
func (f labelConstraintFilter) Type() string {
	return "label-constraint-filter"
}

// Source filters containers when select them as schedule source.
func (f labelConstraintFilter) Source(opt *config.PersistOptions, container *core.CachedContainer) bool {
	return placement.MatchLabelConstraints(container, f.constraints)
}

// Target filters containers when select them as schedule target.
func (f labelConstraintFilter) Target(opt *config.PersistOptions, container *core.CachedContainer) bool {
	return placement.MatchLabelConstraints(container, f.constraints)
}

// ResourceFitter is the interface that can fit a resource against placement rules.
type ResourceFitter interface {
	FitResource(*core.CachedResource) *placement.ResourceFit
}

type ruleFitFilter struct {
	factory      func() metadata.Resource
	scope        string
	fitter       ResourceFitter
	resource     *core.CachedResource
	oldFit       *placement.ResourceFit
	srcContainer uint64
}

// newRuleFitFilter creates a filter that ensures after replace a peer with new
// one, the isolation level will not decrease. Its function is the same as
// distinctScoreFilter but used when placement rules is enabled.
func newRuleFitFilter(scope string, fitter ResourceFitter, resource *core.CachedResource, oldContainerID uint64, factory func() metadata.Resource) Filter {
	return &ruleFitFilter{
		factory:      factory,
		scope:        scope,
		fitter:       fitter,
		resource:     resource,
		oldFit:       fitter.FitResource(resource),
		srcContainer: oldContainerID,
	}
}

func (f *ruleFitFilter) Scope() string {
	return f.scope
}

func (f *ruleFitFilter) Type() string {
	return "rule-fit-filter"
}

func (f *ruleFitFilter) Source(opt *config.PersistOptions, container *core.CachedContainer) bool {
	return true
}

func (f *ruleFitFilter) Target(opt *config.PersistOptions, container *core.CachedContainer) bool {
	start, end := f.resource.Meta.Range()

	resource := createResourceForRuleFit(start, end,
		f.resource.Meta.Peers(), f.resource.GetLeader(),
		f.factory,
		core.WithReplacePeerContainer(f.srcContainer, container.Meta.ID()))
	newFit := f.fitter.FitResource(resource)
	return placement.CompareResourceFit(f.oldFit, newFit) <= 0
}

// GetSourceContainerID implements the ComparingFilter
func (f *ruleFitFilter) GetSourceContainerID() uint64 {
	return f.srcContainer
}

type ruleLeaderFitFilter struct {
	factory              func() metadata.Resource
	scope                string
	fitter               ResourceFitter
	resource             *core.CachedResource
	oldFit               *placement.ResourceFit
	srcLeaderContainerID uint64
}

// newRuleLeaderFitFilter creates a filter that ensures after transfer leader with new container,
// the isolation level will not decrease.
func newRuleLeaderFitFilter(scope string, fitter ResourceFitter, res *core.CachedResource, oldLeaderContainerID uint64, factory func() metadata.Resource) Filter {
	return &ruleLeaderFitFilter{
		factory:              factory,
		scope:                scope,
		fitter:               fitter,
		resource:             res,
		oldFit:               fitter.FitResource(res),
		srcLeaderContainerID: oldLeaderContainerID,
	}
}

func (f *ruleLeaderFitFilter) Scope() string {
	return f.scope
}

func (f *ruleLeaderFitFilter) Type() string {
	return "rule-fit-leader-filter"
}

func (f *ruleLeaderFitFilter) Source(opt *config.PersistOptions, container *core.CachedContainer) bool {
	return true
}

func (f *ruleLeaderFitFilter) Target(opt *config.PersistOptions, container *core.CachedContainer) bool {
	targetPeer, ok := f.resource.GetContainerPeer(container.Meta.ID())
	if !ok {
		return false
	}
	copyResource := createResourceForRuleFit(f.resource.GetStartKey(), f.resource.GetEndKey(),
		f.resource.Meta.Peers(), f.resource.GetLeader(),
		f.factory,
		core.WithLeader(&targetPeer))
	newFit := f.fitter.FitResource(copyResource)
	return placement.CompareResourceFit(f.oldFit, newFit) <= 0
}

func (f *ruleLeaderFitFilter) GetSourceContainerID() uint64 {
	return f.srcLeaderContainerID
}

// NewPlacementSafeguard creates a filter that ensures after replace a peer with new
// peer, the placement restriction will not become worse.
func NewPlacementSafeguard(scope string, cluster opt.Cluster, res *core.CachedResource, sourceContainer *core.CachedContainer, factory func() metadata.Resource) Filter {
	if cluster.GetOpts().IsPlacementRulesEnabled() {
		return newRuleFitFilter(scope, cluster, res, sourceContainer.Meta.ID(), factory)
	}
	return NewLocationSafeguard(scope, cluster.GetOpts().GetLocationLabels(), cluster.GetResourceContainers(res), sourceContainer)
}

// NewPlacementLeaderSafeguard creates a filter that ensures after transfer a leader with
// existed peer, the placement restriction will not become worse.
// Note that it only worked when PlacementRules enabled otherwise it will always permit the sourceContainer.
func NewPlacementLeaderSafeguard(scope string, cluster opt.Cluster, res *core.CachedResource, sourceContainer *core.CachedContainer, factory func() metadata.Resource) Filter {
	if cluster.GetOpts().IsPlacementRulesEnabled() {
		return newRuleLeaderFitFilter(scope, cluster, res, sourceContainer.Meta.ID(), factory)
	}
	return nil
}

type engineFilter struct {
	scope      string
	constraint placement.LabelConstraint
}

// NewEngineFilter creates a filter that only keeps allowedEngines.
func NewEngineFilter(scope string, allowedEngines ...string) Filter {
	return &engineFilter{
		scope:      scope,
		constraint: placement.LabelConstraint{Key: "engine", Op: "in", Values: allowedEngines},
	}
}

func (f *engineFilter) Scope() string {
	return f.scope
}

func (f *engineFilter) Type() string {
	return "engine-filter"
}

func (f *engineFilter) Source(opt *config.PersistOptions, container *core.CachedContainer) bool {
	return f.constraint.MatchContainer(container)
}

func (f *engineFilter) Target(opt *config.PersistOptions, container *core.CachedContainer) bool {
	return f.constraint.MatchContainer(container)
}

type ordinaryEngineFilter struct {
	scope      string
	constraint placement.LabelConstraint
}

// NewOrdinaryEngineFilter creates a filter that only keeps ordinary engine containers.
func NewOrdinaryEngineFilter(scope string) Filter {
	return &ordinaryEngineFilter{
		scope:      scope,
		constraint: placement.LabelConstraint{Key: "engine", Op: "notIn", Values: allSpeicalEngines},
	}
}

func (f *ordinaryEngineFilter) Scope() string {
	return f.scope
}

func (f *ordinaryEngineFilter) Type() string {
	return "ordinary-engine-filter"
}

func (f *ordinaryEngineFilter) Source(opt *config.PersistOptions, container *core.CachedContainer) bool {
	return f.constraint.MatchContainer(container)
}

func (f *ordinaryEngineFilter) Target(opt *config.PersistOptions, container *core.CachedContainer) bool {
	return f.constraint.MatchContainer(container)
}

type specialUseFilter struct {
	scope      string
	constraint placement.LabelConstraint
}

// NewSpecialUseFilter creates a filter that filters out normal containers.
// By default, all containers that are not marked with a special use will be filtered out.
// Specify the special use label if you want to include the special containers.
func NewSpecialUseFilter(scope string, allowUses ...string) Filter {
	var values []string
	for _, v := range allSpecialUses {
		if slice.NoneOf(allowUses, func(i int) bool { return allowUses[i] == v }) {
			values = append(values, v)
		}
	}
	return &specialUseFilter{
		scope:      scope,
		constraint: placement.LabelConstraint{Key: SpecialUseKey, Op: "in", Values: values},
	}
}

func (f *specialUseFilter) Scope() string {
	return f.scope
}

func (f *specialUseFilter) Type() string {
	return "special-use-filter"
}

func (f *specialUseFilter) Source(opt *config.PersistOptions, container *core.CachedContainer) bool {
	if container.IsLowSpace(opt.GetLowSpaceRatio()) {
		return true
	}
	return !f.constraint.MatchContainer(container)
}

func (f *specialUseFilter) Target(opt *config.PersistOptions, container *core.CachedContainer) bool {
	return !f.constraint.MatchContainer(container)
}

const (
	// SpecialUseKey is the label used to indicate special use storage.
	SpecialUseKey = "specialUse"
	// SpecialUseHotResource is the hot resource value of special use label
	SpecialUseHotResource = "hotResource"
	// SpecialUseReserved is the reserved value of special use label
	SpecialUseReserved = "reserved"

	// EngineKey is the label key used to indicate engine.
	EngineKey = "engine"
	// EngineTiFlash is the tiflash value of the engine label.
	EngineTiFlash = "tiflash"
	// EngineTiKV indicates the tikv engine in metrics
	EngineTiKV = "tikv"
)

var allSpecialUses = []string{SpecialUseHotResource, SpecialUseReserved}
var allSpeicalEngines = []string{EngineTiFlash}

type isolationFilter struct {
	scope          string
	locationLabels []string
	constraintSet  [][]string
}

// NewIsolationFilter creates a filter that filters out containers with isolationLevel
// For example, a resource has 3 replicas in z1, z2 and z3 individually.
// With isolationLevel = zone, if the resource on z1 is down, we need to filter out z2 and z3
// because these two zones already have one of the resource's replicas on them.
// We need to choose a container on z1 or z4 to place the new replica to meet the isolationLevel explicitly and forcibly.
func NewIsolationFilter(scope, isolationLevel string, locationLabels []string, resourceContainers []*core.CachedContainer) Filter {
	isolationFilter := &isolationFilter{
		scope:          scope,
		locationLabels: locationLabels,
		constraintSet:  make([][]string, 0),
	}
	// Get which idx this isolationLevel at according to locationLabels
	var isolationLevelIdx int
	for level, label := range locationLabels {
		if label == isolationLevel {
			isolationLevelIdx = level
			break
		}
	}
	// Collect all constraints for given isolationLevel
	for _, rc := range resourceContainers {
		var constraintList []string
		for i := 0; i <= isolationLevelIdx; i++ {
			constraintList = append(constraintList, rc.GetLabelValue(locationLabels[i]))
		}
		isolationFilter.constraintSet = append(isolationFilter.constraintSet, constraintList)
	}
	return isolationFilter
}

func (f *isolationFilter) Scope() string {
	return f.scope
}

func (f *isolationFilter) Type() string {
	return "isolation-filter"
}

func (f *isolationFilter) Source(opt *config.PersistOptions, container *core.CachedContainer) bool {
	return true
}

func (f *isolationFilter) Target(opt *config.PersistOptions, container *core.CachedContainer) bool {
	// No isolation constraint to fit
	if len(f.constraintSet) <= 0 {
		return true
	}
	for _, constrainList := range f.constraintSet {
		match := true
		for idx, constraint := range constrainList {
			// Check every constraint in constrainList
			match = container.GetLabelValue(f.locationLabels[idx]) == constraint && match
		}
		if len(constrainList) > 0 && match {
			return false
		}
	}
	return true
}

// createResourceForRuleFit is used to create a clone resource with ResourceCreateOptions which is only used for
// FitResource in filter
func createResourceForRuleFit(startKey, endKey []byte,
	peers []metapb.Replica, leader *metapb.Replica,
	factory func() metadata.Resource,
	opts ...core.ResourceCreateOption) *core.CachedResource {
	copyLeader := proto.Clone(leader).(*metapb.Replica)
	copyPeers := make([]metapb.Replica, 0, len(peers))
	for _, p := range peers {
		peer := metapb.Replica{
			ID:          p.ID,
			ContainerID: p.ContainerID,
			Role:        p.Role,
		}
		copyPeers = append(copyPeers, peer)
	}

	meta := factory()
	meta.SetStartKey(startKey)
	meta.SetEndKey(endKey)
	meta.SetPeers(copyPeers)

	cloneResource := core.NewCachedResource(meta, copyLeader, opts...)
	return cloneResource
}
