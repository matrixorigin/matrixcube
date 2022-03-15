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
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/slice"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"go.uber.org/zap"
)

// BasicCluster provides basic data member and interface for a storage application cluster.
type BasicCluster struct {
	sync.RWMutex
	logger              *zap.Logger
	Stores              *StoresContainer
	Shards              *ShardsContainer
	DestroyedShards     *roaring64.Bitmap
	WaitingCreateShards map[uint64]metapb.Shard
	DestroyingStatuses  map[uint64]*metapb.DestroyingStatus
	ScheduleGroupRules  ScheduleGroupRuleCache
	ScheduleGroupKeys   map[string]struct{}
}

// NewBasicCluster creates a BasicCluster.
func NewBasicCluster(logger *zap.Logger) *BasicCluster {
	bc := &BasicCluster{
		logger:             log.Adjust(logger),
		DestroyingStatuses: make(map[uint64]*metapb.DestroyingStatus),
		ScheduleGroupKeys:  make(map[string]struct{}),
		ScheduleGroupRules: NewScheduleGroupRuleCache(),
	}
	bc.Reset()
	return bc
}

// Reset reset Basic Cluster info
func (bc *BasicCluster) Reset() {
	bc.Lock()
	defer bc.Unlock()

	bc.Stores = NewCachedStores()
	bc.Shards = NewCachedShards()
	bc.DestroyedShards = roaring64.NewBitmap()
	bc.WaitingCreateShards = make(map[uint64]metapb.Shard)
	bc.ScheduleGroupRules.Clear()
}

// AddRemovedShards add removed shards
func (bc *BasicCluster) AddRemovedShards(ids ...uint64) {
	bc.Lock()
	defer bc.Unlock()
	bc.DestroyedShards.AddMany(ids)
	for _, id := range ids {
		res := bc.Shards.GetShard(id)
		if res != nil {
			bc.Shards.RemoveShard(res)
		}
	}
}

// AddWaitingCreateShards add waiting create shards
func (bc *BasicCluster) AddWaitingCreateShards(resources ...metapb.Shard) {
	bc.Lock()
	defer bc.Unlock()
	for _, res := range resources {
		bc.WaitingCreateShards[res.GetID()] = res
	}
}

// ForeachWaitingCreateShards do func for every waiting create shards
func (bc *BasicCluster) ForeachWaitingCreateShards(fn func(res metapb.Shard)) {
	bc.RLock()
	defer bc.RUnlock()
	for _, res := range bc.WaitingCreateShards {
		fn(res)
	}
}

// ForeachShards foreach shards by group
func (bc *BasicCluster) ForeachShards(group uint64, fn func(res metapb.Shard)) {
	bc.RLock()
	defer bc.RUnlock()

	bc.Shards.ForeachShards(group, fn)
}

// IsWaitingCreateShard returns true means the resource is waiting create
func (bc *BasicCluster) IsWaitingCreateShard(id uint64) bool {
	bc.RLock()
	defer bc.RUnlock()

	_, ok := bc.WaitingCreateShards[id]
	return ok
}

// AlreadyRemoved returns true means resource already removed
func (bc *BasicCluster) AlreadyRemoved(id uint64) bool {
	bc.RLock()
	defer bc.RUnlock()

	return bc.DestroyedShards.Contains(id)
}

// GetDestroyShards get destroyed and destroying state shards
func (bc *BasicCluster) GetDestroyShards(bm *roaring64.Bitmap) (*roaring64.Bitmap, *roaring64.Bitmap) {
	bc.RLock()
	defer bc.RUnlock()

	// read destroyed shards
	destroyed := bc.DestroyedShards.Clone()
	destroyed.And(bm)

	// read destroying shards
	destroying := roaring64.New()
	for id, res := range bc.Shards.shards.m {
		if res.Meta.GetState() == metapb.ShardState_Destroying && bm.Contains(id) {
			destroying.Add(id)
		}
	}
	return destroyed, destroying
}

// GetStores returns all Stores in the cluster.
func (bc *BasicCluster) GetStores() []*CachedStore {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.GetStores()
}

// GetMetaStores gets a complete set of *metapb.Store.
func (bc *BasicCluster) GetMetaStores() []metapb.Store {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.GetMetaStores()
}

// GetStore searches for a container by ID.
func (bc *BasicCluster) GetStore(containerID uint64) *CachedStore {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.GetStore(containerID)
}

// GetShard searches for a resource by ID.
func (bc *BasicCluster) GetShard(resourceID uint64) *CachedShard {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.GetShard(resourceID)
}

// GetShards gets all CachedShard from shardMap.
func (bc *BasicCluster) GetShards() []*CachedShard {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.GetShards()
}

// GetMetaShards gets a set of *metapb.Shard from shardMap.
func (bc *BasicCluster) GetMetaShards() []metapb.Shard {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.GetMetaShards()
}

// GetStoreShards gets all CachedShard with a given containerID.
func (bc *BasicCluster) GetStoreShards(groupKey string, containerID uint64) []*CachedShard {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.GetStoreShards(groupKey, containerID)
}

// GetShardStores returns all Stores that contains the resource's peer.
func (bc *BasicCluster) GetShardStores(res *CachedShard) []*CachedStore {
	bc.RLock()
	defer bc.RUnlock()
	var containers []*CachedStore
	for id := range res.GetStoreIDs() {
		if container := bc.Stores.GetStore(id); container != nil {
			containers = append(containers, container)
		}
	}
	return containers
}

// GetFollowerStores returns all Stores that contains the resource's follower peer.
func (bc *BasicCluster) GetFollowerStores(res *CachedShard) []*CachedStore {
	bc.RLock()
	defer bc.RUnlock()
	var containers []*CachedStore
	for id := range res.GetFollowers() {
		if container := bc.Stores.GetStore(id); container != nil {
			containers = append(containers, container)
		}
	}
	return containers
}

// GetLeaderStore returns all Stores that contains the resource's leader peer.
func (bc *BasicCluster) GetLeaderStore(res *CachedShard) *CachedStore {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.GetStore(res.GetLeader().GetStoreID())
}

// GetAdjacentShards returns resource's info that is adjacent with specific resource.
func (bc *BasicCluster) GetAdjacentShards(res *CachedShard) (*CachedShard, *CachedShard) {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.GetAdjacentShards(res)
}

// PauseLeaderTransfer prevents the container from been selected as source or
// target container of TransferLeader.
func (bc *BasicCluster) PauseLeaderTransfer(containerID uint64) error {
	bc.Lock()
	defer bc.Unlock()
	return bc.Stores.PauseLeaderTransfer(containerID)
}

// ResumeLeaderTransfer cleans a container's pause state. The container can be selected
// as source or target of TransferLeader again.
func (bc *BasicCluster) ResumeLeaderTransfer(containerID uint64) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stores.ResumeLeaderTransfer(containerID)
}

// AttachAvailableFunc attaches an available function to a specific container.
func (bc *BasicCluster) AttachAvailableFunc(containerID uint64, limitType limit.Type, f func() bool) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stores.AttachAvailableFunc(containerID, limitType, f)
}

// GetScheduleGroupKeys returns Schedule group keys
func (bc *BasicCluster) GetScheduleGroupKeys() []string {
	bc.RLock()
	defer bc.RUnlock()
	var keys []string
	for k := range bc.ScheduleGroupKeys {
		keys = append(keys, k)
	}
	return keys
}

func (bc *BasicCluster) GetScheduleGroupKeysWithPrefix(prefix string) []string {
	bc.RLock()
	defer bc.RUnlock()
	var keys []string
	for k := range bc.ScheduleGroupKeys {
		if strings.HasPrefix(k, prefix) ||
			(k == "" && prefix == util.EncodeGroupKey(0, nil, nil)) {
			keys = append(keys, k)
		}
	}
	return keys
}

// UpdateStoreStatus updates the information of the container.
func (bc *BasicCluster) UpdateStoreStatus(groupKey string, containerID uint64, leaderCount int, resourceCount int, pendingPeerCount int, leaderSize int64, resourceSize int64) {
	bc.Lock()
	defer bc.Unlock()
	bc.ScheduleGroupKeys[groupKey] = struct{}{}
	bc.Stores.UpdateStoreStatus(groupKey, containerID, leaderCount, resourceCount, pendingPeerCount, leaderSize, resourceSize)
}

const randomShardMaxRetry = 10

// RandFollowerShard returns a random resource that has a follower on the container.
func (bc *BasicCluster) RandFollowerShard(groupKey string, containerID uint64, ranges []KeyRange, opts ...ShardOption) *CachedShard {
	bc.RLock()
	resources := bc.Shards.RandFollowerShards(groupKey, containerID, ranges, randomShardMaxRetry)
	bc.RUnlock()
	return bc.selectShard(resources, opts...)
}

// RandLeaderShard returns a random resource that has leader on the container.
func (bc *BasicCluster) RandLeaderShard(groupKey string, containerID uint64, ranges []KeyRange, opts ...ShardOption) *CachedShard {
	bc.RLock()
	resources := bc.Shards.RandLeaderShards(groupKey, containerID, ranges, randomShardMaxRetry)
	bc.RUnlock()
	return bc.selectShard(resources, opts...)
}

// RandPendingShard returns a random resource that has a pending peer on the container.
func (bc *BasicCluster) RandPendingShard(groupKey string, containerID uint64, ranges []KeyRange, opts ...ShardOption) *CachedShard {
	bc.RLock()
	resources := bc.Shards.RandPendingShards(groupKey, containerID, ranges, randomShardMaxRetry)
	bc.RUnlock()
	return bc.selectShard(resources, opts...)
}

// RandLearnerShard returns a random resource that has a learner peer on the container.
func (bc *BasicCluster) RandLearnerShard(groupKey string, containerID uint64, ranges []KeyRange, opts ...ShardOption) *CachedShard {
	bc.RLock()
	resources := bc.Shards.RandLearnerShards(groupKey, containerID, ranges, randomShardMaxRetry)
	bc.RUnlock()
	return bc.selectShard(resources, opts...)
}

func (bc *BasicCluster) selectShard(resources []*CachedShard, opts ...ShardOption) *CachedShard {
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

// GetShardCount gets the total count of CachedShard of shardMap.
func (bc *BasicCluster) GetShardCount() int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.GetShardCount()
}

// GetStoreCount returns the total count of StoresContainer.
func (bc *BasicCluster) GetStoreCount() int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.GetStoreCount()
}

// GetStoreShardCount gets the total count of a container's leader and follower CachedShard by containerID.
func (bc *BasicCluster) GetStoreShardCount(groupKey string, containerID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.GetStoreLeaderCount(groupKey, containerID) +
		bc.Shards.GetStoreFollowerCount(groupKey, containerID) +
		bc.Shards.GetStoreLearnerCount(groupKey, containerID)
}

// GetStoreLeaderCount get the total count of a container's leader CachedShard.
func (bc *BasicCluster) GetStoreLeaderCount(groupKey string, containerID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.GetStoreLeaderCount(groupKey, containerID)
}

// GetStoreFollowerCount get the total count of a container's follower CachedShard.
func (bc *BasicCluster) GetStoreFollowerCount(groupKey string, containerID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.GetStoreFollowerCount(groupKey, containerID)
}

// GetStorePendingPeerCount gets the total count of a container's resource that includes pending peer.
func (bc *BasicCluster) GetStorePendingPeerCount(groupKey string, containerID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.GetStorePendingPeerCount(groupKey, containerID)
}

// GetStoreLeaderShardSize get total size of container's leader shards.
func (bc *BasicCluster) GetStoreLeaderShardSize(groupKey string, containerID uint64) int64 {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.GetStoreLeaderShardSize(groupKey, containerID)
}

// GetStoreShardSize get total size of container's shards.
func (bc *BasicCluster) GetStoreShardSize(groupKey string, containerID uint64) int64 {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.GetStoreLeaderShardSize(groupKey, containerID) +
		bc.Shards.GetStoreFollowerShardSize(groupKey, containerID) +
		bc.Shards.GetStoreLearnerShardSize(groupKey, containerID)
}

// GetAverageShardSize returns the average resource approximate size.
func (bc *BasicCluster) GetAverageShardSize() int64 {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.GetAverageShardSize()
}

// PutStore put a container.
func (bc *BasicCluster) PutStore(container *CachedStore) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stores.SetStore(container)
}

// DeleteStore deletes a container.
func (bc *BasicCluster) DeleteStore(container *CachedStore) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stores.DeleteStore(container)
}

// PreCheckPutShard checks if the resource is valid to put.
func (bc *BasicCluster) PreCheckPutShard(res *CachedShard) (*CachedShard, error) {
	bc.RLock()
	origin := bc.Shards.GetShard(res.Meta.GetID())
	if origin == nil ||
		!bytes.Equal(origin.GetStartKey(), res.GetStartKey()) ||
		!bytes.Equal(origin.GetEndKey(), res.GetEndKey()) {
		for _, item := range bc.Shards.GetOverlaps(res) {
			if res.Meta.GetEpoch().Generation < item.Meta.GetEpoch().Generation {
				bc.RUnlock()
				return nil, errShardIsStale(res.Meta, item.Meta)
			}
		}
	}
	bc.RUnlock()
	if origin == nil {
		return nil, nil
	}
	r := res.Meta.GetEpoch()
	o := origin.Meta.GetEpoch()

	isTermBehind := res.GetTerm() < origin.GetTerm()

	// Shard meta is stale, return an error.
	if r.GetGeneration() < o.GetGeneration() || r.GetConfigVer() < o.GetConfigVer() || isTermBehind {
		return origin, errShardIsStale(res.Meta, origin.Meta)
	}

	return origin, nil
}

// PutShard put a resource, returns overlap shards
func (bc *BasicCluster) PutShard(res *CachedShard) []*CachedShard {
	bc.Lock()
	defer bc.Unlock()

	if _, ok := bc.WaitingCreateShards[res.Meta.GetID()]; ok {
		delete(bc.WaitingCreateShards, res.Meta.GetID())
		if res.Meta.GetState() == metapb.ShardState_Creating {
			res.Meta.SetState(metapb.ShardState_Running)
		}
	}
	return bc.Shards.SetShard(res)
}

// CheckAndPutShard checks if the resource is valid to put,if valid then put.
func (bc *BasicCluster) CheckAndPutShard(res *CachedShard) []*CachedShard {
	switch res.Meta.GetState() {
	case metapb.ShardState_Destroyed:
		bc.AddRemovedShards(res.Meta.GetID())
		return nil
	case metapb.ShardState_Creating:
		bc.AddWaitingCreateShards(res.Meta)
		return nil
	}

	origin, err := bc.PreCheckPutShard(res)
	if err != nil {
		bc.logger.Debug("resource is stale, need to delete",
			zap.Uint64("resource", origin.Meta.GetID()))
		// return the state resource to delete.
		return []*CachedShard{res}
	}
	return bc.PutShard(res)
}

// RemoveShard removes CachedShard from shardTree and shardMap.
func (bc *BasicCluster) RemoveShard(res *CachedShard) {
	bc.Lock()
	defer bc.Unlock()
	bc.Shards.RemoveShard(res)
}

// SearchShard searches CachedShard from shardTree.
func (bc *BasicCluster) SearchShard(group uint64, resKey []byte) *CachedShard {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.SearchShard(group, resKey)
}

// ScanRange scans shards intersecting [start key, end key), returns at most
// `limit` shards. limit <= 0 means no limit.
func (bc *BasicCluster) ScanRange(group uint64, startKey, endKey []byte, limit int) []*CachedShard {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.ScanRange(group, startKey, endKey, limit)
}

// GetDestroyingShards returns all shards in destroying state
func (bc *BasicCluster) GetDestroyingShards() []*CachedShard {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.GetDestroyingShards()
}

// GetOverlaps returns the shards which are overlapped with the specified resource range.
func (bc *BasicCluster) GetOverlaps(res *CachedShard) []*CachedShard {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Shards.GetOverlaps(res)
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

func (bc *BasicCluster) AddScheduleGroupRule(rule metapb.ScheduleGroupRule) error {
	return bc.ScheduleGroupRules.AddRule(rule)
}

// GetShardGroupRuleCount gets the total count of group rules
func (bc *BasicCluster) GetShardGroupRuleCount() int {
	return bc.ScheduleGroupRules.RuleCount()
}

// ShardSetInformer provides access to a shared informer of shards.
type ShardSetInformer interface {
	GetScheduleGroupKeys() []string
	GetScheduleGroupKeysWithPrefix(prefix string) []string
	GetShardCount() int
	RandFollowerShard(groupKey string, containerID uint64, ranges []KeyRange, opts ...ShardOption) *CachedShard
	RandLeaderShard(groupKey string, containerID uint64, ranges []KeyRange, opts ...ShardOption) *CachedShard
	RandLearnerShard(groupKey string, containerID uint64, ranges []KeyRange, opts ...ShardOption) *CachedShard
	RandPendingShard(groupKey string, containerID uint64, ranges []KeyRange, opts ...ShardOption) *CachedShard
	GetAverageShardSize() int64
	GetStoreShardCount(groupKey string, containerID uint64) int
	GetShard(id uint64) *CachedShard
	GetAdjacentShards(res *CachedShard) (*CachedShard, *CachedShard)
	ScanShards(group uint64, startKey, endKey []byte, limit int) []*CachedShard
	GetShardByKey(group uint64, resKey []byte) *CachedShard
}

// StoreSetInformer provides access to a shared informer of stores.
type StoreSetInformer interface {
	GetStores() []*CachedStore
	GetStore(id uint64) *CachedStore

	GetShardStores(res *CachedShard) []*CachedStore
	GetFollowerStores(res *CachedShard) []*CachedStore
	GetLeaderStore(res *CachedShard) *CachedStore
}

// StoreSetController is used to control stores' status.
type StoreSetController interface {
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
