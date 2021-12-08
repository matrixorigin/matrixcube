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

package core

import (
	"bytes"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/util/slice"
	"go.uber.org/zap"
)

// BasicCluster provides basic data member and interface for a storage application cluster.
type BasicCluster struct {
	sync.RWMutex
	logger                  *zap.Logger
	factory                 func() metadata.Resource
	Containers              *CachedContainers
	Resources               *CachedResources
	DestroyedResources      *roaring64.Bitmap
	WaittingCreateResources map[uint64]metadata.Resource
	DestroyingStatuses      map[uint64]*metapb.DestroyingStatus
	ScheduleGroupRules      map[uint64][]metapb.ScheduleGroupRule // resource group id -> rules
	SchedulingResourceGroup map[uint64][][]*CachedResource        // resource group id -> shard groups
}

// NewBasicCluster creates a BasicCluster.
func NewBasicCluster(factory func() metadata.Resource, logger *zap.Logger) *BasicCluster {
	bc := &BasicCluster{
		factory:                 factory,
		logger:                  log.Adjust(logger),
		DestroyingStatuses:      make(map[uint64]*metapb.DestroyingStatus),
		SchedulingResourceGroup: make(map[uint64][][]*CachedResource),
	}
	bc.Reset()
	return bc
}

// ResetSchedulingResourceGroup reset reosurce scheduling group by shard group id
func (bc *BasicCluster) ResetSchedulingResourceGroup(group uint64) {
	bc.Lock()
	defer bc.Unlock()
	bc.resetSchedulingResourceGroupLocked(group)
}

func (bc *BasicCluster) resetSchedulingResourceGroupLocked(group uint64) {
	rules := bc.ScheduleGroupRules[group]
	if len(rules) == 0 {
		bc.SchedulingResourceGroup[group] = [][]*CachedResource{bc.Resources.GetResources()}
		return
	}

	resources := make(map[string][]*CachedResource)
	bc.Resources.ForeachCachedResources(group, func(res *CachedResource) {
		labels := res.Meta.Labels()
		m := make(map[string]string, len(labels))
		for _, l := range labels {
			m[l.Key] = l.Value
		}

		key := ""
		for _, r := range rules {
			if v, ok := m[r.GroupByLabel]; ok {
				key += v
			}
		}

		resources[key] = append(resources[key], res)
	})
	for _, v := range resources {
		bc.SchedulingResourceGroup[group] = append(bc.SchedulingResourceGroup[group], v)
	}
}

// Reset reset Basic Cluster info
func (bc *BasicCluster) Reset() {
	bc.Lock()
	defer bc.Unlock()

	bc.Containers = NewCachedContainers()
	bc.Resources = NewCachedResources(bc.factory)
	bc.DestroyedResources = roaring64.NewBitmap()
	bc.WaittingCreateResources = make(map[uint64]metadata.Resource)
	bc.ScheduleGroupRules = make(map[uint64][]metapb.ScheduleGroupRule)
}

// AddRemovedResources add removed resources
func (bc *BasicCluster) AddRemovedResources(ids ...uint64) {
	bc.Lock()
	defer bc.Unlock()
	bc.DestroyedResources.AddMany(ids)
	for _, id := range ids {
		res := bc.Resources.GetResource(id)
		if res != nil {
			bc.Resources.RemoveResource(res)
		}
	}
}

// AddWaittingCreateResources add waitting create resources
func (bc *BasicCluster) AddWaittingCreateResources(resources ...metadata.Resource) {
	bc.Lock()
	defer bc.Unlock()
	for _, res := range resources {
		bc.WaittingCreateResources[res.ID()] = res
	}
}

// ForeachWaittingCreateResources do func for every waitting create resources
func (bc *BasicCluster) ForeachWaittingCreateResources(fn func(res metadata.Resource)) {
	bc.RLock()
	defer bc.RUnlock()
	for _, res := range bc.WaittingCreateResources {
		fn(res)
	}
}

// ForeachResources foreach resources by group
func (bc *BasicCluster) ForeachResources(group uint64, fn func(res metadata.Resource)) {
	bc.RLock()
	defer bc.RUnlock()

	bc.Resources.ForeachResources(group, fn)
}

// IsWaittingCreateResource returns true means the resource is waitting create
func (bc *BasicCluster) IsWaittingCreateResource(id uint64) bool {
	bc.RLock()
	defer bc.RUnlock()

	_, ok := bc.WaittingCreateResources[id]
	return ok
}

// AlreadyRemoved returns true means resource already removed
func (bc *BasicCluster) AlreadyRemoved(id uint64) bool {
	bc.RLock()
	defer bc.RUnlock()

	return bc.DestroyedResources.Contains(id)
}

// GetDestroyResources get destroyed and destroying state resources
func (bc *BasicCluster) GetDestroyResources(bm *roaring64.Bitmap) (*roaring64.Bitmap, *roaring64.Bitmap) {
	bc.RLock()
	defer bc.RUnlock()

	// read destroyed resources
	destroyed := bc.DestroyedResources.Clone()
	destroyed.And(bm)

	// read destroying resources
	destroying := roaring64.New()
	for id, res := range bc.Resources.resources.m {
		if res.Meta.State() == metapb.ResourceState_Destroying && bm.Contains(id) {
			destroying.Add(id)
		}
	}
	return destroyed, destroying
}

// GetContainers returns all Containers in the cluster.
func (bc *BasicCluster) GetContainers() []*CachedContainer {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Containers.GetContainers()
}

// GetMetaContainers gets a complete set of metadata.Container.
func (bc *BasicCluster) GetMetaContainers() []metadata.Container {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Containers.GetMetaContainers()
}

// GetContainer searches for a container by ID.
func (bc *BasicCluster) GetContainer(containerID uint64) *CachedContainer {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Containers.GetContainer(containerID)
}

// GetResource searches for a resource by ID.
func (bc *BasicCluster) GetResource(resourceID uint64) *CachedResource {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.GetResource(resourceID)
}

// GetResources gets all CachedResource from resourceMap.
func (bc *BasicCluster) GetResources() []*CachedResource {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.GetResources()
}

// GetResourcesGroupByRule group resources according to resource label
func (bc *BasicCluster) GetResourcesGroupByRule(group uint64) [][]*CachedResource {
	bc.RLock()
	defer bc.RUnlock()

	return bc.SchedulingResourceGroup[group]
}

// GetMetaResources gets a set of metadata.Resource from resourceMap.
func (bc *BasicCluster) GetMetaResources() []metadata.Resource {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.GetMetaResources()
}

// GetContainerResources gets all CachedResource with a given containerID.
func (bc *BasicCluster) GetContainerResources(groupID, containerID uint64) []*CachedResource {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.GetContainerResources(groupID, containerID)
}

// GetResourceContainers returns all Containers that contains the resource's peer.
func (bc *BasicCluster) GetResourceContainers(res *CachedResource) []*CachedContainer {
	bc.RLock()
	defer bc.RUnlock()
	var containers []*CachedContainer
	for id := range res.GetContainerIDs() {
		if container := bc.Containers.GetContainer(id); container != nil {
			containers = append(containers, container)
		}
	}
	return containers
}

// GetFollowerContainers returns all Containers that contains the resource's follower peer.
func (bc *BasicCluster) GetFollowerContainers(res *CachedResource) []*CachedContainer {
	bc.RLock()
	defer bc.RUnlock()
	var containers []*CachedContainer
	for id := range res.GetFollowers() {
		if container := bc.Containers.GetContainer(id); container != nil {
			containers = append(containers, container)
		}
	}
	return containers
}

// GetLeaderContainer returns all Containers that contains the resource's leader peer.
func (bc *BasicCluster) GetLeaderContainer(res *CachedResource) *CachedContainer {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Containers.GetContainer(res.GetLeader().GetContainerID())
}

// GetAdjacentResources returns resource's info that is adjacent with specific resource.
func (bc *BasicCluster) GetAdjacentResources(res *CachedResource) (*CachedResource, *CachedResource) {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.GetAdjacentResources(res)
}

// PauseLeaderTransfer prevents the container from been selected as source or
// target container of TransferLeader.
func (bc *BasicCluster) PauseLeaderTransfer(containerID uint64) error {
	bc.Lock()
	defer bc.Unlock()
	return bc.Containers.PauseLeaderTransfer(containerID)
}

// ResumeLeaderTransfer cleans a container's pause state. The container can be selected
// as source or target of TransferLeader again.
func (bc *BasicCluster) ResumeLeaderTransfer(containerID uint64) {
	bc.Lock()
	defer bc.Unlock()
	bc.Containers.ResumeLeaderTransfer(containerID)
}

// AttachAvailableFunc attaches an available function to a specific container.
func (bc *BasicCluster) AttachAvailableFunc(containerID uint64, limitType limit.Type, f func() bool) {
	bc.Lock()
	defer bc.Unlock()
	bc.Containers.AttachAvailableFunc(containerID, limitType, f)
}

// UpdateContainerStatus updates the information of the container.
func (bc *BasicCluster) UpdateContainerStatus(group, containerID uint64, leaderCount int, resourceCount int, pendingPeerCount int, leaderSize int64, resourceSize int64) {
	bc.Lock()
	defer bc.Unlock()
	bc.Containers.UpdateContainerStatus(group, containerID, leaderCount, resourceCount, pendingPeerCount, leaderSize, resourceSize)
}

const randomResourceMaxRetry = 10

// RandFollowerResource returns a random resource that has a follower on the container.
func (bc *BasicCluster) RandFollowerResource(groupID, containerID uint64, ranges []KeyRange, opts ...ResourceOption) *CachedResource {
	bc.RLock()
	resources := bc.Resources.RandFollowerResources(groupID, containerID, ranges, randomResourceMaxRetry)
	bc.RUnlock()
	return bc.selectResource(resources, opts...)
}

// RandLeaderResource returns a random resource that has leader on the container.
func (bc *BasicCluster) RandLeaderResource(groupID, containerID uint64, ranges []KeyRange, opts ...ResourceOption) *CachedResource {
	bc.RLock()
	resources := bc.Resources.RandLeaderResources(groupID, containerID, ranges, randomResourceMaxRetry)
	bc.RUnlock()
	return bc.selectResource(resources, opts...)
}

// RandPendingResource returns a random resource that has a pending peer on the container.
func (bc *BasicCluster) RandPendingResource(groupID, containerID uint64, ranges []KeyRange, opts ...ResourceOption) *CachedResource {
	bc.RLock()
	resources := bc.Resources.RandPendingResources(groupID, containerID, ranges, randomResourceMaxRetry)
	bc.RUnlock()
	return bc.selectResource(resources, opts...)
}

// RandLearnerResource returns a random resource that has a learner peer on the container.
func (bc *BasicCluster) RandLearnerResource(groupID, containerID uint64, ranges []KeyRange, opts ...ResourceOption) *CachedResource {
	bc.RLock()
	resources := bc.Resources.RandLearnerResources(groupID, containerID, ranges, randomResourceMaxRetry)
	bc.RUnlock()
	return bc.selectResource(resources, opts...)
}

func (bc *BasicCluster) selectResource(resources []*CachedResource, opts ...ResourceOption) *CachedResource {
	for _, r := range resources {
		if r == nil {
			break
		}
		if slice.AllOf(opts, func(i int) bool { return opts[i](r) }) {
			return r
		}
	}
	return nil
}

// GetResourceCount gets the total count of CachedResource of resourceMap.
func (bc *BasicCluster) GetResourceCount() int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.GetResourceCount()
}

// GetContainerCount returns the total count of CachedContainers.
func (bc *BasicCluster) GetContainerCount() int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Containers.GetContainerCount()
}

// GetContainerResourceCount gets the total count of a container's leader and follower CachedResource by containerID.
func (bc *BasicCluster) GetContainerResourceCount(groupID, containerID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.GetContainerLeaderCount(groupID, containerID) +
		bc.Resources.GetContainerFollowerCount(groupID, containerID) +
		bc.Resources.GetContainerLearnerCount(groupID, containerID)
}

// GetContainerLeaderCount get the total count of a container's leader CachedResource.
func (bc *BasicCluster) GetContainerLeaderCount(groupID, containerID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.GetContainerLeaderCount(groupID, containerID)
}

// GetContainerFollowerCount get the total count of a container's follower CachedResource.
func (bc *BasicCluster) GetContainerFollowerCount(groupID, containerID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.GetContainerFollowerCount(groupID, containerID)
}

// GetContainerPendingPeerCount gets the total count of a container's resource that includes pending peer.
func (bc *BasicCluster) GetContainerPendingPeerCount(groupID, containerID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.GetContainerPendingPeerCount(groupID, containerID)
}

// GetContainerLeaderResourceSize get total size of container's leader resources.
func (bc *BasicCluster) GetContainerLeaderResourceSize(groupID, containerID uint64) int64 {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.GetContainerLeaderResourceSize(groupID, containerID)
}

// GetContainerResourceSize get total size of container's resources.
func (bc *BasicCluster) GetContainerResourceSize(groupID, containerID uint64) int64 {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.GetContainerLeaderResourceSize(groupID, containerID) +
		bc.Resources.GetContainerFollowerResourceSize(groupID, containerID) +
		bc.Resources.GetContainerLearnerResourceSize(groupID, containerID)
}

// GetAverageResourceSize returns the average resource approximate size.
func (bc *BasicCluster) GetAverageResourceSize() int64 {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.GetAverageResourceSize()
}

// PutContainer put a container.
func (bc *BasicCluster) PutContainer(container *CachedContainer) {
	bc.Lock()
	defer bc.Unlock()
	bc.Containers.SetContainer(container)
}

// DeleteContainer deletes a container.
func (bc *BasicCluster) DeleteContainer(container *CachedContainer) {
	bc.Lock()
	defer bc.Unlock()
	bc.Containers.DeleteContainer(container)
}

// TakeContainer returns the point of the origin CachedContainers with the specified containerID.
func (bc *BasicCluster) TakeContainer(containerID uint64) *CachedContainer {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Containers.TakeContainer(containerID)
}

// PreCheckPutResource checks if the resource is valid to put.
func (bc *BasicCluster) PreCheckPutResource(res *CachedResource) (*CachedResource, error) {
	bc.RLock()
	origin := bc.Resources.GetResource(res.Meta.ID())
	if origin == nil ||
		!bytes.Equal(origin.GetStartKey(), res.GetStartKey()) ||
		!bytes.Equal(origin.GetEndKey(), res.GetEndKey()) {
		for _, item := range bc.Resources.GetOverlaps(res) {
			if res.Meta.Epoch().Version < item.Meta.Epoch().Version {
				bc.RUnlock()
				return nil, errResourceIsStale(res.Meta, item.Meta)
			}
		}
	}
	bc.RUnlock()
	if origin == nil {
		return nil, nil
	}
	r := res.Meta.Epoch()
	o := origin.Meta.Epoch()

	isTermBehind := res.GetTerm() < origin.GetTerm()

	// Resource meta is stale, return an error.
	if r.GetVersion() < o.GetVersion() || r.GetConfVer() < o.GetConfVer() || isTermBehind {
		return origin, errResourceIsStale(res.Meta, origin.Meta)
	}

	return origin, nil
}

// PutResource put a resource, returns overlap resources
func (bc *BasicCluster) PutResource(res *CachedResource) []*CachedResource {
	bc.Lock()
	defer bc.Unlock()

	if _, ok := bc.WaittingCreateResources[res.Meta.ID()]; ok {
		delete(bc.WaittingCreateResources, res.Meta.ID())
		if res.Meta.State() == metapb.ResourceState_Creating {
			res.Meta.SetState(metapb.ResourceState_Running)
		}
	}
	return bc.Resources.SetResource(res)
}

// CheckAndPutResource checks if the resource is valid to put,if valid then put.
func (bc *BasicCluster) CheckAndPutResource(res *CachedResource) []*CachedResource {
	switch res.Meta.State() {
	case metapb.ResourceState_Destroyed:
		bc.AddRemovedResources(res.Meta.ID())
		return nil
	case metapb.ResourceState_Creating:
		bc.AddWaittingCreateResources(res.Meta)
		return nil
	}

	origin, err := bc.PreCheckPutResource(res)
	if err != nil {
		bc.logger.Debug("resource is stale, need to delete",
			zap.Uint64("resource", origin.Meta.ID()))
		// return the state resource to delete.
		return []*CachedResource{res}
	}
	return bc.PutResource(res)
}

// RemoveResource removes CachedResource from resourceTree and resourceMap.
func (bc *BasicCluster) RemoveResource(res *CachedResource) {
	bc.Lock()
	defer bc.Unlock()
	bc.Resources.RemoveResource(res)
	bc.resetSchedulingResourceGroupLocked(res.Meta.Group())
}

// SearchResource searches CachedResource from resourceTree.
func (bc *BasicCluster) SearchResource(group uint64, resKey []byte) *CachedResource {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.SearchResource(group, resKey)
}

// SearchPrevResource searches previous CachedResource from resourceTree.
func (bc *BasicCluster) SearchPrevResource(group uint64, resKey []byte) *CachedResource {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.SearchPrevResource(group, resKey)
}

// ScanRange scans resources intersecting [start key, end key), returns at most
// `limit` resources. limit <= 0 means no limit.
func (bc *BasicCluster) ScanRange(group uint64, startKey, endKey []byte, limit int) []*CachedResource {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.ScanRange(group, startKey, endKey, limit)
}

// GetOverlaps returns the resources which are overlapped with the specified resource range.
func (bc *BasicCluster) GetOverlaps(res *CachedResource) []*CachedResource {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Resources.GetOverlaps(res)
}

// GetDestroyingStatus returns DestroyingStatus
func (bc *BasicCluster) GetDestroyingStatus(id uint64) *metapb.DestroyingStatus {
	bc.RLock()
	defer bc.RUnlock()

	v, ok := bc.DestroyingStatuses[id]
	if !ok {
		return nil
	}

	cloneValue := &metapb.DestroyingStatus{}
	protoc.MustUnmarshal(cloneValue, protoc.MustMarshal(v))
	return cloneValue
}

// UpdateDestroyingStatus update DestroyingStatus
func (bc *BasicCluster) UpdateDestroyingStatus(id uint64, status *metapb.DestroyingStatus) {
	bc.Lock()
	defer bc.Unlock()

	bc.DestroyingStatuses[id] = status
}

func (bc *BasicCluster) AddScheduleGroupRule(rule metapb.ScheduleGroupRule, reset bool) {
	bc.Lock()
	defer bc.Unlock()
	rules, ok := bc.ScheduleGroupRules[rule.GroupID]
	if !ok {
		rules = []metapb.ScheduleGroupRule{rule}
		bc.ScheduleGroupRules[rule.GroupID] = rules
		return
	}

	for _, r := range rules {
		if r.GroupByLabel == rule.GroupByLabel {
			return
		}
	}
	bc.ScheduleGroupRules[rule.GroupID] = rules

	if reset {
		bc.resetSchedulingResourceGroupLocked(rule.GroupID)
	}
}

// GetResourceCount gets the total count of group rules
func (bc *BasicCluster) GetResourceGroupRuleCount() int {
	bc.RLock()
	defer bc.RUnlock()
	n := 0
	for _, rules := range bc.ScheduleGroupRules {
		n += len(rules)
	}
	return n
}

// ResourceSetInformer provides access to a shared informer of resources.
type ResourceSetInformer interface {
	GetResourcesGroupByRule(group uint64) [][]*CachedResource
	GetResourceCount() int
	RandFollowerResource(group, containerID uint64, ranges []KeyRange, opts ...ResourceOption) *CachedResource
	RandLeaderResource(groupID, containerID uint64, ranges []KeyRange, opts ...ResourceOption) *CachedResource
	RandLearnerResource(groupID, containerID uint64, ranges []KeyRange, opts ...ResourceOption) *CachedResource
	RandPendingResource(groupID, containerID uint64, ranges []KeyRange, opts ...ResourceOption) *CachedResource
	GetAverageResourceSize() int64
	GetContainerResourceCount(groupID, containerID uint64) int
	GetResource(id uint64) *CachedResource
	GetAdjacentResources(res *CachedResource) (*CachedResource, *CachedResource)
	ScanResources(group uint64, startKey, endKey []byte, limit int) []*CachedResource
	GetResourceByKey(group uint64, resKey []byte) *CachedResource
}

// ContainerSetInformer provides access to a shared informer of containers.
type ContainerSetInformer interface {
	GetContainers() []*CachedContainer
	GetContainer(id uint64) *CachedContainer

	GetResourceContainers(res *CachedResource) []*CachedContainer
	GetFollowerContainers(res *CachedResource) []*CachedContainer
	GetLeaderContainer(res *CachedResource) *CachedContainer
}

// ContainerSetController is used to control containers' status.
type ContainerSetController interface {
	PauseLeaderTransfer(id uint64) error
	ResumeLeaderTransfer(id uint64)

	AttachAvailableFunc(id uint64, limitType limit.Type, f func() bool)
}

// KeyRange is a key range.
type KeyRange struct {
	Group    uint64 `json:"group"`
	StartKey []byte `json:"start-key"`
	EndKey   []byte `json:"end-key"`
}

// NewKeyRange create a KeyRange with the given start key and end key.
func NewKeyRange(groupID uint64, startKey, endKey string) KeyRange {
	return KeyRange{
		Group:    groupID,
		StartKey: []byte(startKey),
		EndKey:   []byte(endKey),
	}
}
