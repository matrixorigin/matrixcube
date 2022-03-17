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
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

const (
	// Interval to save store meta (including heartbeat ts) to etcd.
	storePersistInterval  = 5 * time.Minute
	gb                    = 1 << 30 // 1GB size
	initialMaxShardCounts = 30      // exclude storage Threshold Filter when resource less than 30
	initialMinSpace       = 1 << 33 // 2^3=8GB
)

type counterAndSize struct {
	count int
	size  int64
}

// CachedStore is the store runtime info cached in the cache
type CachedStore struct {
	Meta metapb.Store
	*storeStats
	pauseLeaderTransfer bool // not allow to be used as source or target of transfer leader
	shardInfo           map[string]counterAndSize
	leaderInfo          map[string]counterAndSize
	pendingPeerCounts   map[string]int
	lastPersistTime     time.Time
	leaderWeight        float64
	shardWeight         float64
	available           map[limit.Type]func() bool
}

// NewCachedStore creates CachedStore with metadata.
func NewCachedStore(meta metapb.Store, opts ...StoreCreateOption) *CachedStore {
	store := &CachedStore{
		Meta:              meta,
		storeStats:        newStoreStats(),
		shardInfo:         make(map[string]counterAndSize),
		leaderInfo:        make(map[string]counterAndSize),
		pendingPeerCounts: make(map[string]int),
		leaderWeight:      1.0,
		shardWeight:       1.0,
	}
	for _, opt := range opts {
		opt(store)
	}
	return store
}

// Clone creates a copy of current CachedStore.
func (cr *CachedStore) Clone(opts ...StoreCreateOption) *CachedStore {
	store := &CachedStore{
		Meta:                cr.Meta,
		storeStats:          cr.storeStats,
		pauseLeaderTransfer: cr.pauseLeaderTransfer,
		shardInfo:           make(map[string]counterAndSize),
		leaderInfo:          make(map[string]counterAndSize),
		pendingPeerCounts:   make(map[string]int),
		lastPersistTime:     cr.lastPersistTime,
		leaderWeight:        cr.leaderWeight,
		shardWeight:         cr.shardWeight,
		available:           cr.available,
	}

	for k, v := range cr.shardInfo {
		store.shardInfo[k] = v
	}
	for k, v := range cr.leaderInfo {
		store.leaderInfo[k] = v
	}
	for k, v := range cr.pendingPeerCounts {
		store.pendingPeerCounts[k] = v
	}

	for _, opt := range opts {
		opt(store)
	}
	return store
}

// ShallowClone creates a copy of current CachedStore, but not clone 'Meta'.
func (cr *CachedStore) ShallowClone(opts ...StoreCreateOption) *CachedStore {
	shard := &CachedStore{
		Meta:                cr.Meta,
		storeStats:          cr.storeStats,
		pauseLeaderTransfer: cr.pauseLeaderTransfer,
		shardInfo:           make(map[string]counterAndSize),
		leaderInfo:          make(map[string]counterAndSize),
		pendingPeerCounts:   make(map[string]int),
		lastPersistTime:     cr.lastPersistTime,
		leaderWeight:        cr.leaderWeight,
		shardWeight:         cr.shardWeight,
		available:           cr.available,
	}

	for k, v := range cr.shardInfo {
		shard.shardInfo[k] = v
	}
	for k, v := range cr.leaderInfo {
		shard.leaderInfo[k] = v
	}
	for k, v := range cr.pendingPeerCounts {
		shard.pendingPeerCounts[k] = v
	}

	for _, opt := range opts {
		opt(shard)
	}
	return shard
}

// AllowLeaderTransfer returns if the store is allowed to be selected
// as source or target of transfer leader.
func (cr *CachedStore) AllowLeaderTransfer() bool {
	return !cr.pauseLeaderTransfer
}

// IsAvailable returns if the store bucket of limitation is available
func (cr *CachedStore) IsAvailable(limitType limit.Type) bool {
	if cr.available != nil && cr.available[limitType] != nil {
		return cr.available[limitType]()
	}
	return true
}

// IsUp checks if the store's state is Up.
func (cr *CachedStore) IsUp() bool {
	return cr.GetState() == metapb.StoreState_Up
}

// IsOffline checks if the store's state is Offline.
func (cr *CachedStore) IsOffline() bool {
	return cr.GetState() == metapb.StoreState_Down
}

// IsTombstone checks if the store's state is Tombstone.
func (cr *CachedStore) IsTombstone() bool {
	return cr.GetState() == metapb.StoreState_StoreTombstone
}

// IsPhysicallyDestroyed checks if the store's physically destroyed.
func (cr *CachedStore) IsPhysicallyDestroyed() bool {
	return cr.Meta.GetDestroyed()
}

// DownTime returns the time elapsed since last heartbeat.
func (cr *CachedStore) DownTime() time.Duration {
	return time.Since(cr.GetLastHeartbeatTS())
}

// GetState returns the state of the store.
func (cr *CachedStore) GetState() metapb.StoreState {
	return cr.Meta.GetState()
}

// GetLeaderCount returns the leader count of the store.
func (cr *CachedStore) GetLeaderCount(groupKey string) int {
	return cr.leaderInfo[groupKey].count
}

// GetTotalLeaderCount returns the leader count of the store.
func (cr *CachedStore) GetTotalLeaderCount() int {
	n := 0
	for _, v := range cr.leaderInfo {
		n += v.count
	}
	return n
}

// GetShardCount returns the Shard count of the store.
func (cr *CachedStore) GetShardCount(groupKey string) int {
	return cr.shardInfo[groupKey].count
}

// GetTotalShardCount returns the Shard count of the store.
func (cr *CachedStore) GetTotalShardCount() int {
	n := 0
	for _, v := range cr.shardInfo {
		n += v.count
	}
	return n
}

// GetGroupKeys returns the Group Key.
func (cr *CachedStore) GetGroupKeys() string {
	var v bytes.Buffer
	for k, vs := range cr.shardInfo {
		v.WriteString(hex.EncodeToString([]byte(k)))
		v.WriteString(fmt.Sprintf("/%d", vs.count))
		v.WriteString(" ")
	}
	return v.String()
}

// GetLeaderSize returns the leader size of the store.
func (cr *CachedStore) GetLeaderSize(groupKey string) int64 {
	return cr.leaderInfo[groupKey].size
}

// GetTotalLeaderSize returns the leader size of the store.
func (cr *CachedStore) GetTotalLeaderSize() int64 {
	n := int64(0)
	for _, v := range cr.leaderInfo {
		n += v.size
	}
	return n
}

// GetShardSize returns the Shard size of the store.
func (cr *CachedStore) GetShardSize(groupKey string) int64 {
	return cr.shardInfo[groupKey].size
}

// GetTotalShardSize returns the Shard size of the store.
func (cr *CachedStore) GetTotalShardSize() int64 {
	n := int64(0)
	for _, v := range cr.shardInfo {
		n += v.size
	}
	return n
}

// GetPendingPeerCount returns the pending peer count of the store.
func (cr *CachedStore) GetPendingPeerCount() int {
	cnt := 0
	for _, v := range cr.pendingPeerCounts {
		cnt += v
	}
	return cnt
}

// GetLeaderWeight returns the leader weight of the store.
func (cr *CachedStore) GetLeaderWeight() float64 {
	return cr.leaderWeight
}

// GetShardWeight returns the Shard weight of the store.
func (cr *CachedStore) GetShardWeight() float64 {
	return cr.shardWeight
}

// GetLastHeartbeatTS returns the last heartbeat timestamp of the store.
func (cr *CachedStore) GetLastHeartbeatTS() time.Time {
	return time.Unix(0, cr.Meta.GetLastHeartbeatTime())
}

// NeedPersist returns if it needs to save to etcd.
func (cr *CachedStore) NeedPersist() bool {
	return cr.GetLastHeartbeatTS().Sub(cr.lastPersistTime) > storePersistInterval
}

const minWeight = 1e-6

// LeaderScore returns the store's leader score.
func (cr *CachedStore) LeaderScore(groupKey string, policy SchedulePolicy, delta int64) float64 {
	switch policy {
	case BySize:
		return float64(cr.GetLeaderSize(groupKey)+delta) / math.Max(cr.GetLeaderWeight(), minWeight)
	case ByCount:
		return float64(int64(cr.GetLeaderCount(groupKey))+delta) / math.Max(cr.GetLeaderWeight(), minWeight)
	default:
		return 0
	}
}

// ShardScore returns the store's resource score.
// Deviation It is used to control the direction of the deviation considered
// when calculating the resource score. It is set to -1 when it is the source
// store of balance, 1 when it is the target, and 0 in the rest of cases.
func (cr *CachedStore) ShardScore(groupKey string, highSpaceRatio, lowSpaceRatio float64, delta int64, deviation int) float64 {
	A := float64(float64(cr.GetAvgAvailable())-float64(deviation)*float64(cr.GetAvailableDeviation())) / gb
	C := float64(cr.GetCapacity()) / gb
	R := float64(cr.GetShardSize(groupKey) + delta)
	var (
		K, M float64 = 1, 256 // Experience value to control the weight of the available influence on score
		F    float64 = 50     // Experience value to prevent some nodes from running out of disk space prematurely.
		B            = 1e7
	)

	F = math.Max(F, C*(1-lowSpaceRatio))
	var score float64
	if A >= C || cr.GetUsedRatio() <= highSpaceRatio || C < 1 {
		score = R
	} else if A > F {
		// As the amount of data increases (available becomes smaller), the weight of resource size on total score
		// increases. Ideally, all nodes converge at the position where remaining space is F (default 20GiB).
		score = (K + M*(math.Log(C)-math.Log(A-F+1))/(C-A+F-1)) * R
	} else {
		// When remaining space is less than F, the score is mainly determined by available space.
		// store's score will increase rapidly after it has few space. and it will reach similar score when they have no space
		score = (K+M*math.Log(C)/C)*R + B*(F-A)/F
	}
	return score / math.Max(cr.GetShardWeight(), minWeight)
}

// StorageSize returns store's used storage size reported from your storage.
func (cr *CachedStore) StorageSize() uint64 {
	return cr.GetUsedSize()
}

// AvailableRatio is store's freeSpace/capacity.
func (cr *CachedStore) AvailableRatio() float64 {
	if cr.GetCapacity() == 0 {
		return 0
	}
	return float64(cr.GetAvailable()) / float64(cr.GetCapacity())
}

// IsLowSpace checks if the store is lack of space.
func (cr *CachedStore) IsLowSpace(lowSpaceRatio float64) bool {
	if cr.GetStoreStats() == nil {
		return false
	}
	// issue #3444
	if cr.GetTotalShardCount() < initialMaxShardCounts && cr.GetAvailable() > initialMinSpace {
		return false
	}
	return cr.AvailableRatio() < 1-lowSpaceRatio
}

// ShardCount returns count of leader/resource-replica in the store.
func (cr *CachedStore) ShardCount(groupKey string, kind metapb.ShardType) uint64 {
	switch kind {
	case metapb.ShardType_LeaderOnly:
		return uint64(cr.GetLeaderCount(groupKey))
	case metapb.ShardType_AllShards:
		return uint64(cr.GetShardCount(groupKey))
	default:
		return 0
	}
}

// ShardSize returns size of leader/resource-replica in the store
func (cr *CachedStore) ShardSize(groupKey string, kind metapb.ShardType) int64 {
	switch kind {
	case metapb.ShardType_LeaderOnly:
		return cr.GetLeaderSize(groupKey)
	case metapb.ShardType_AllShards:
		return cr.GetShardSize(groupKey)
	default:
		return 0
	}
}

// ShardWeight returns weight of leader/resource-replica in the score
func (cr *CachedStore) ShardWeight(kind metapb.ShardType) float64 {
	switch kind {
	case metapb.ShardType_LeaderOnly:
		leaderWeight := cr.GetLeaderWeight()
		if leaderWeight <= 0 {
			return minWeight
		}
		return leaderWeight
	case metapb.ShardType_AllShards:
		resourceWeight := cr.GetShardWeight()
		if resourceWeight <= 0 {
			return minWeight
		}
		return resourceWeight
	default:
		return 0
	}
}

var (
	// If a store's last heartbeat is storeDisconnectDuration ago, the store will
	// be marked as disconnected state. The value should be greater than storage application's
	// store heartbeat interval (default 10s).
	storeDisconnectDuration = 20 * time.Second
	storeUnhealthyDuration  = 10 * time.Minute
)

// IsDisconnected checks if a store is disconnected, which means Prophet misses
// storage application's store heartbeat for a short time, maybe caused by process restart or
// temporary network failure.
func (cr *CachedStore) IsDisconnected() bool {
	return cr.DownTime() > storeDisconnectDuration
}

// IsUnhealthy checks if a store is unhealthy.
func (cr *CachedStore) IsUnhealthy() bool {
	return cr.DownTime() > storeUnhealthyDuration
}

// GetLabelValue returns a label's value (if exists).
func (cr *CachedStore) GetLabelValue(key string) string {
	for _, label := range cr.Meta.GetLabels() {
		if strings.EqualFold(label.GetKey(), key) {
			return label.GetValue()
		}
	}
	return ""
}

// CompareLocation compares 2 stores' labels and returns at which level their
// locations are different. It returns -1 if they are at the same location.
func (cr *CachedStore) CompareLocation(other *CachedStore, labels []string) int {
	for i, key := range labels {
		v1, v2 := cr.GetLabelValue(key), other.GetLabelValue(key)
		// If label is not set, the store is considered at the same location
		// with any other store.
		if v1 != "" && v2 != "" && !strings.EqualFold(v1, v2) {
			return i
		}
	}
	return -1
}

const replicaBaseScore = 100

// DistinctScore returns the score that the other is distinct from the stores.
// A higher score means the other store is more different from the existed stores.
func DistinctScore(labels []string, stores []*CachedStore, other *CachedStore) float64 {
	var score float64
	for _, s := range stores {
		if s.Meta.GetID() == other.Meta.GetID() {
			continue
		}
		if index := s.CompareLocation(other, labels); index != -1 {
			score += math.Pow(replicaBaseScore, float64(len(labels)-index-1))
		}
	}
	return score
}

// MergeLabels merges the passed in labels with origins, overriding duplicated
// ones.
// ones.
func (cr *CachedStore) MergeLabels(labels []metapb.Label) []metapb.Label {
	storeLabels := cr.Meta.GetLabels()
L:
	for _, newLabel := range labels {
		for idx := range storeLabels {
			if strings.EqualFold(storeLabels[idx].Key, newLabel.Key) {
				storeLabels[idx].Value = newLabel.Value
				continue L
			}
		}
		storeLabels = append(storeLabels, newLabel)
	}
	res := storeLabels[:0]
	for _, l := range storeLabels {
		if l.Value != "" {
			res = append(res, l)
		}
	}
	return res
}

// StoresContainer contains information about all stores.
type StoresContainer struct {
	stores map[uint64]*CachedStore
}

// NewCachedStores create a CachedStore with map of storeID to CachedStore
func NewCachedStores() *StoresContainer {
	return &StoresContainer{
		stores: make(map[uint64]*CachedStore),
	}
}

// GetStore returns CachedStore with the specified storeID.
func (s *StoresContainer) GetStore(storeID uint64) *CachedStore {
	store, ok := s.stores[storeID]
	if !ok {
		return nil
	}
	return store
}

// SetStore sets a CachedStore with storeID.
func (s *StoresContainer) SetStore(store *CachedStore) {
	s.stores[store.Meta.GetID()] = store
}

// PauseLeaderTransfer pauses a CachedStore with storeID.
func (s *StoresContainer) PauseLeaderTransfer(storeID uint64) error {
	store, ok := s.stores[storeID]
	if !ok {
		return fmt.Errorf("store %d not found", storeID)
	}
	if !store.AllowLeaderTransfer() {
		return fmt.Errorf("store %d already paused leader transfer", storeID)
	}
	s.stores[storeID] = store.Clone(PauseLeaderTransfer())
	return nil
}

// ResumeLeaderTransfer cleans a store's pause state. The store can be selected
// as source or target of TransferLeader again.
func (s *StoresContainer) ResumeLeaderTransfer(storeID uint64) {
	store, ok := s.stores[storeID]
	if !ok {
		panic(fmt.Sprintf("try to clean a store %d pause state, but it is not found",
			storeID))
	}
	s.stores[storeID] = store.Clone(ResumeLeaderTransfer())
}

// AttachAvailableFunc attaches f to a specific store.
func (s *StoresContainer) AttachAvailableFunc(storeID uint64, limitType limit.Type, f func() bool) {
	if store, ok := s.stores[storeID]; ok {
		s.stores[storeID] = store.Clone(AttachAvailableFunc(limitType, f))
	}
}

// GetStores gets a complete set of CachedStore.
func (s *StoresContainer) GetStores() []*CachedStore {
	stores := make([]*CachedStore, 0, len(s.stores))
	for _, store := range s.stores {
		stores = append(stores, store)
	}
	return stores
}

// GetMetaStores gets a complete set of *metapb.Store
func (s *StoresContainer) GetMetaStores() []metapb.Store {
	metas := make([]metapb.Store, 0, len(s.stores))
	for _, store := range s.stores {
		metas = append(metas, store.Meta)
	}
	return metas
}

// DeleteStore deletes tombstone record form s.stores
func (s *StoresContainer) DeleteStore(store *CachedStore) {
	delete(s.stores, store.Meta.GetID())
}

// GetStoreCount returns the total count of CachedStore.
func (s *StoresContainer) GetStoreCount() int {
	return len(s.stores)
}

// UpdateStoreStatus updates the information of the store.
func (s *StoresContainer) UpdateStoreStatus(groupKey string, storeID uint64, leaderCount int, resourceCount int, pendingPeerCount int, leaderSize int64, resourceSize int64) {
	if store, ok := s.stores[storeID]; ok {
		newStore := store.ShallowClone(SetLeaderCount(groupKey, leaderCount),
			SetShardCount(groupKey, resourceCount),
			SetPendingPeerCount(groupKey, pendingPeerCount),
			SetLeaderSize(groupKey, leaderSize),
			SetShardSize(groupKey, resourceSize))
		s.SetStore(newStore)
	}
}
