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
	// Interval to save container meta (including heartbeat ts) to etcd.
	containerPersistInterval = 5 * time.Minute
	mb                       = 1 << 20 // megabyte
	gb                       = 1 << 30 // 1GB size
	initialMaxShardCounts    = 30      // exclude storage Threshold Filter when resource less than 30
	initialMinSpace          = 1 << 33 // 2^3=8GB
)

type counterAndSize struct {
	count int
	size  int64
}

// CachedStore is the container runtime info cached in the cache
type CachedStore struct {
	Meta metapb.Store
	*containerStats
	pauseLeaderTransfer bool // not allow to be used as source or target of transfer leader
	resourceInfo        map[string]counterAndSize
	leaderInfo          map[string]counterAndSize
	pendingPeerCounts   map[string]int
	lastPersistTime     time.Time
	leaderWeight        float64
	resourceWeight      float64
	available           map[limit.Type]func() bool
}

// NewCachedStore creates CachedStore with meta data.
func NewCachedStore(meta metapb.Store, opts ...StoreCreateOption) *CachedStore {
	container := &CachedStore{
		Meta:              meta,
		containerStats:    newStoreStats(),
		resourceInfo:      make(map[string]counterAndSize),
		leaderInfo:        make(map[string]counterAndSize),
		pendingPeerCounts: make(map[string]int),
		leaderWeight:      1.0,
		resourceWeight:    1.0,
	}
	for _, opt := range opts {
		opt(container)
	}
	return container
}

// Clone creates a copy of current CachedStore.
func (cr *CachedStore) Clone(opts ...StoreCreateOption) *CachedStore {
	container := &CachedStore{
		Meta:                cr.Meta.CloneValue(),
		containerStats:      cr.containerStats,
		pauseLeaderTransfer: cr.pauseLeaderTransfer,
		resourceInfo:        make(map[string]counterAndSize),
		leaderInfo:          make(map[string]counterAndSize),
		pendingPeerCounts:   make(map[string]int),
		lastPersistTime:     cr.lastPersistTime,
		leaderWeight:        cr.leaderWeight,
		resourceWeight:      cr.resourceWeight,
		available:           cr.available,
	}

	for k, v := range cr.resourceInfo {
		container.resourceInfo[k] = v
	}
	for k, v := range cr.leaderInfo {
		container.leaderInfo[k] = v
	}
	for k, v := range cr.pendingPeerCounts {
		container.pendingPeerCounts[k] = v
	}

	for _, opt := range opts {
		opt(container)
	}
	return container
}

// ShallowClone creates a copy of current CachedStore, but not clone 'Meta'.
func (cr *CachedStore) ShallowClone(opts ...StoreCreateOption) *CachedStore {
	container := &CachedStore{
		Meta:                cr.Meta,
		containerStats:      cr.containerStats,
		pauseLeaderTransfer: cr.pauseLeaderTransfer,
		resourceInfo:        make(map[string]counterAndSize),
		leaderInfo:          make(map[string]counterAndSize),
		pendingPeerCounts:   make(map[string]int),
		lastPersistTime:     cr.lastPersistTime,
		leaderWeight:        cr.leaderWeight,
		resourceWeight:      cr.resourceWeight,
		available:           cr.available,
	}

	for k, v := range cr.resourceInfo {
		container.resourceInfo[k] = v
	}
	for k, v := range cr.leaderInfo {
		container.leaderInfo[k] = v
	}
	for k, v := range cr.pendingPeerCounts {
		container.pendingPeerCounts[k] = v
	}

	for _, opt := range opts {
		opt(container)
	}
	return container
}

// AllowLeaderTransfer returns if the container is allowed to be selected
// as source or target of transfer leader.
func (cr *CachedStore) AllowLeaderTransfer() bool {
	return !cr.pauseLeaderTransfer
}

// IsAvailable returns if the container bucket of limitation is available
func (cr *CachedStore) IsAvailable(limitType limit.Type) bool {
	if cr.available != nil && cr.available[limitType] != nil {
		return cr.available[limitType]()
	}
	return true
}

// IsUp checks if the container's state is Up.
func (cr *CachedStore) IsUp() bool {
	return cr.GetState() == metapb.StoreState_Up
}

// IsOffline checks if the container's state is Offline.
func (cr *CachedStore) IsOffline() bool {
	return cr.GetState() == metapb.StoreState_Down
}

// IsTombstone checks if the container's state is Tombstone.
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

// GetState returns the state of the container.
func (cr *CachedStore) GetState() metapb.StoreState {
	return cr.Meta.GetState()
}

// GetLeaderCount returns the leader count of the container.
func (cr *CachedStore) GetLeaderCount(groupKey string) int {
	return cr.leaderInfo[groupKey].count
}

// GetTotalLeaderCount returns the leader count of the container.
func (cr *CachedStore) GetTotalLeaderCount() int {
	n := 0
	for _, v := range cr.leaderInfo {
		n += v.count
	}
	return n
}

// GetShardCount returns the Shard count of the container.
func (cr *CachedStore) GetShardCount(groupKey string) int {
	return cr.resourceInfo[groupKey].count
}

// GetTotalShardCount returns the Shard count of the container.
func (cr *CachedStore) GetTotalShardCount() int {
	n := 0
	for _, v := range cr.resourceInfo {
		n += v.count
	}
	return n
}

// GetTotalShardCount returns the Shard count of the container.
func (cr *CachedStore) GetGroupKeys() string {
	var v bytes.Buffer
	for k, vs := range cr.resourceInfo {
		v.WriteString(hex.EncodeToString([]byte(k)))
		v.WriteString(fmt.Sprintf("/%d", vs.count))
		v.WriteString(" ")
	}
	return v.String()
}

// GetLeaderSize returns the leader size of the container.
func (cr *CachedStore) GetLeaderSize(groupKey string) int64 {
	return cr.leaderInfo[groupKey].size
}

// GetTotalLeaderSize returns the leader size of the container.
func (cr *CachedStore) GetTotalLeaderSize() int64 {
	n := int64(0)
	for _, v := range cr.leaderInfo {
		n += v.size
	}
	return n
}

// GetShardSize returns the Shard size of the container.
func (cr *CachedStore) GetShardSize(groupKey string) int64 {
	return cr.resourceInfo[groupKey].size
}

// GetTotalShardSize returns the Shard size of the container.
func (cr *CachedStore) GetTotalShardSize() int64 {
	n := int64(0)
	for _, v := range cr.resourceInfo {
		n += v.size
	}
	return n
}

// GetPendingPeerCount returns the pending peer count of the container.
func (cr *CachedStore) GetPendingPeerCount() int {
	cnt := 0
	for _, v := range cr.pendingPeerCounts {
		cnt += v
	}
	return cnt
}

// GetLeaderWeight returns the leader weight of the container.
func (cr *CachedStore) GetLeaderWeight() float64 {
	return cr.leaderWeight
}

// GetShardWeight returns the Shard weight of the container.
func (cr *CachedStore) GetShardWeight() float64 {
	return cr.resourceWeight
}

// GetLastHeartbeatTS returns the last heartbeat timestamp of the container.
func (cr *CachedStore) GetLastHeartbeatTS() time.Time {
	return time.Unix(0, cr.Meta.GetLastHeartbeatTime())
}

// NeedPersist returns if it needs to save to etcd.
func (cr *CachedStore) NeedPersist() bool {
	return cr.GetLastHeartbeatTS().Sub(cr.lastPersistTime) > containerPersistInterval
}

const minWeight = 1e-6
const maxScore = 1024 * 1024 * 1024

// LeaderScore returns the container's leader score.
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

// ShardScore returns the container's resource score.
// Deviation It is used to control the direction of the deviation considered
// when calculating the resource score. It is set to -1 when it is the source
// container of balance, 1 when it is the target, and 0 in the rest of cases.
func (cr *CachedStore) ShardScore(groupKey string, version string, highSpaceRatio, lowSpaceRatio float64, delta int64, deviation int) float64 {
	switch version {
	case "v2":
		return cr.resourceScoreV2(groupKey, delta, deviation, highSpaceRatio, lowSpaceRatio)
	case "v1":
		fallthrough
	default:
		return cr.resourceScoreV1(groupKey, highSpaceRatio, lowSpaceRatio, delta)
	}
}

func (cr *CachedStore) resourceScoreV1(groupKey string, highSpaceRatio, lowSpaceRatio float64, delta int64) float64 {
	var score float64
	var amplification float64
	available := float64(cr.GetAvailable()) / mb
	used := float64(cr.GetUsedSize()) / mb
	capacity := float64(cr.GetCapacity()) / mb

	if cr.GetShardSize(groupKey) == 0 || used == 0 {
		amplification = 1
	} else {
		// because of db compression, resource size is larger than actual used size
		amplification = float64(cr.GetShardSize(groupKey)) / used
	}

	// highSpaceBound is the lower bound of the high space stage.
	highSpaceBound := (1 - highSpaceRatio) * capacity
	// lowSpaceBound is the upper bound of the low space stage.
	lowSpaceBound := (1 - lowSpaceRatio) * capacity
	if available-float64(delta)/amplification >= highSpaceBound {
		score = float64(cr.GetShardSize(groupKey) + delta)
	} else if available-float64(delta)/amplification <= lowSpaceBound {
		score = maxScore - (available - float64(delta)/amplification)
	} else {
		// to make the score function continuous, we use linear function y = k * x + b as transition period
		// from above we know that there are two points must on the function image
		// note that it is possible that other irrelative files occupy a lot of storage, so capacity == available + used + irrelative
		// and we regarded irrelative as a fixed value.
		// Then amp = size / used = size / (capacity - irrelative - available)
		//
		// When available == highSpaceBound,
		// we can conclude that size = (capacity - irrelative - highSpaceBound) * amp = (used + available - highSpaceBound) * amp
		// Similarly, when available == lowSpaceBound,
		// we can conclude that size = (capacity - irrelative - lowSpaceBound) * amp = (used + available - lowSpaceBound) * amp
		// These are the two fixed points' x-coordinates, and y-coordinates which can be easily obtained from the above two functions.
		x1, y1 := (used+available-highSpaceBound)*amplification, (used+available-highSpaceBound)*amplification
		x2, y2 := (used+available-lowSpaceBound)*amplification, maxScore-lowSpaceBound

		k := (y2 - y1) / (x2 - x1)
		b := y1 - k*x1
		score = k*float64(cr.GetShardSize(groupKey)+delta) + b
	}

	return score / math.Max(cr.GetShardWeight(), minWeight)
}

func (cr *CachedStore) resourceScoreV2(groupKey string, delta int64, deviation int, highSpaceRatio, lowSpaceRatio float64) float64 {
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
		// When remaining space is less then F, the score is mainly determined by available space.
		// store's score will increase rapidly after it has few space. and it will reach similar score when they has no space
		score = (K+M*math.Log(C)/C)*R + B*(F-A)/F
	}
	return score / math.Max(cr.GetShardWeight(), minWeight)
}

// StorageSize returns container's used storage size reported from your storage.
func (cr *CachedStore) StorageSize() uint64 {
	return cr.GetUsedSize()
}

// AvailableRatio is container's freeSpace/capacity.
func (cr *CachedStore) AvailableRatio() float64 {
	if cr.GetCapacity() == 0 {
		return 0
	}
	return float64(cr.GetAvailable()) / float64(cr.GetCapacity())
}

// IsLowSpace checks if the container is lack of space.
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

// ShardCount returns count of leader/resource-replica in the container.
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

// ShardSize returns size of leader/resource-replica in the container
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

// GetStartTime returns the start timestamp.
func (cr *CachedStore) GetStartTime() time.Time {
	return time.Unix(cr.Meta.GetStartTime(), 0)
}

// GetUptime returns the uptime.
func (cr *CachedStore) GetUptime() time.Duration {
	uptime := cr.GetLastHeartbeatTS().Sub(cr.GetStartTime())
	if uptime > 0 {
		return uptime
	}
	return 0
}

var (
	// If a container's last heartbeat is containerDisconnectDuration ago, the container will
	// be marked as disconnected state. The value should be greater than storage application's
	// container heartbeat interval (default 10s).
	containerDisconnectDuration = 20 * time.Second
	containerUnhealthyDuration  = 10 * time.Minute
)

// IsDisconnected checks if a container is disconnected, which means Prophet misses
// storage application's container heartbeat for a short time, maybe caused by process restart or
// temporary network failure.
func (cr *CachedStore) IsDisconnected() bool {
	return cr.DownTime() > containerDisconnectDuration
}

// IsUnhealthy checks if a container is unhealthy.
func (cr *CachedStore) IsUnhealthy() bool {
	return cr.DownTime() > containerUnhealthyDuration
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

// CompareLocation compares 2 containers' labels and returns at which level their
// locations are different. It returns -1 if they are at the same location.
func (cr *CachedStore) CompareLocation(other *CachedStore, labels []string) int {
	for i, key := range labels {
		v1, v2 := cr.GetLabelValue(key), other.GetLabelValue(key)
		// If label is not set, the container is considered at the same location
		// with any other container.
		if v1 != "" && v2 != "" && !strings.EqualFold(v1, v2) {
			return i
		}
	}
	return -1
}

const replicaBaseScore = 100

// DistinctScore returns the score that the other is distinct from the containers.
// A higher score means the other container is more different from the existed containers.
func DistinctScore(labels []string, containers []*CachedStore, other *CachedStore) float64 {
	var score float64
	for _, s := range containers {
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
func (cr *CachedStore) MergeLabels(labels []metapb.Label) []metapb.Label {
	containerLabels := cr.Meta.Clone().GetLabels()
L:
	for _, newLabel := range labels {
		for idx := range containerLabels {
			if strings.EqualFold(containerLabels[idx].Key, newLabel.Key) {
				containerLabels[idx].Value = newLabel.Value
				continue L
			}
		}
		containerLabels = append(containerLabels, newLabel)
	}
	res := containerLabels[:0]
	for _, l := range containerLabels {
		if l.Value != "" {
			res = append(res, l)
		}
	}
	return res
}

// CachedStores contains information about all container.
type CachedStores struct {
	containers map[uint64]*CachedStore
}

// NewCachedStores create a CachedStore with map of containerID to CachedStore
func NewCachedStores() *CachedStores {
	return &CachedStores{
		containers: make(map[uint64]*CachedStore),
	}
}

// GetStore returns a copy of the CachedStore with the specified containerID.
func (s *CachedStores) GetStore(containerID uint64) *CachedStore {
	container, ok := s.containers[containerID]
	if !ok {
		return nil
	}
	return container
}

// TakeStore returns the point of the origin CachedStore with the specified containerID.
func (s *CachedStores) TakeStore(containerID uint64) *CachedStore {
	container, ok := s.containers[containerID]
	if !ok {
		return nil
	}
	return container
}

// SetStore sets a CachedStore with containerID.
func (s *CachedStores) SetStore(container *CachedStore) {
	s.containers[container.Meta.GetID()] = container
}

// PauseLeaderTransfer pauses a CachedStore with containerID.
func (s *CachedStores) PauseLeaderTransfer(containerID uint64) error {
	container, ok := s.containers[containerID]
	if !ok {
		return fmt.Errorf("container %d not found", containerID)
	}
	if !container.AllowLeaderTransfer() {
		return fmt.Errorf("container %d pause transfer leader", containerID)
	}
	s.containers[containerID] = container.Clone(PauseLeaderTransfer())
	return nil
}

// ResumeLeaderTransfer cleans a container's pause state. The container can be selected
// as source or target of TransferLeader again.
func (s *CachedStores) ResumeLeaderTransfer(containerID uint64) {
	container, ok := s.containers[containerID]
	if !ok {
		panic(fmt.Sprintf("try to clean a container %d pause state, but it is not found",
			containerID))
	}
	s.containers[containerID] = container.Clone(ResumeLeaderTransfer())
}

// AttachAvailableFunc attaches f to a specific container.
func (s *CachedStores) AttachAvailableFunc(containerID uint64, limitType limit.Type, f func() bool) {
	if container, ok := s.containers[containerID]; ok {
		s.containers[containerID] = container.Clone(AttachAvailableFunc(limitType, f))
	}
}

// GetStores gets a complete set of CachedStore.
func (s *CachedStores) GetStores() []*CachedStore {
	containers := make([]*CachedStore, 0, len(s.containers))
	for _, container := range s.containers {
		containers = append(containers, container)
	}
	return containers
}

// GetMetaStores gets a complete set of *metapb.Store
func (s *CachedStores) GetMetaStores() []metapb.Store {
	metas := make([]metapb.Store, 0, len(s.containers))
	for _, container := range s.containers {
		metas = append(metas, container.Meta)
	}
	return metas
}

// DeleteStore deletes tombstone record form container
func (s *CachedStores) DeleteStore(container *CachedStore) {
	delete(s.containers, container.Meta.GetID())
}

// GetStoreCount returns the total count of CachedStore.
func (s *CachedStores) GetStoreCount() int {
	return len(s.containers)
}

// SetLeaderCount sets the leader count to a CachedStore.
func (s *CachedStores) SetLeaderCount(groupKey string, containerID uint64, leaderCount int) {
	if container, ok := s.containers[containerID]; ok {
		s.containers[containerID] = container.Clone(SetLeaderCount(groupKey, leaderCount))
	}
}

// SetShardCount sets the resource count to a CachedStore.
func (s *CachedStores) SetShardCount(groupKey string, containerID uint64, resourceCount int) {
	if container, ok := s.containers[containerID]; ok {
		s.containers[containerID] = container.Clone(SetShardCount(groupKey, resourceCount))
	}
}

// SetPendingPeerCount sets the pending count to a CachedStore.
func (s *CachedStores) SetPendingPeerCount(groupKey string, containerID uint64, pendingPeerCount int) {
	if container, ok := s.containers[containerID]; ok {
		s.containers[containerID] = container.Clone(SetPendingPeerCount(groupKey, pendingPeerCount))
	}
}

// SetLeaderSize sets the leader size to a CachedStore.
func (s *CachedStores) SetLeaderSize(groupKey string, containerID uint64, leaderSize int64) {
	if container, ok := s.containers[containerID]; ok {
		s.containers[containerID] = container.Clone(SetLeaderSize(groupKey, leaderSize))
	}
}

// SetShardSize sets the resource size to a CachedStore.
func (s *CachedStores) SetShardSize(groupKey string, containerID uint64, resourceSize int64) {
	if container, ok := s.containers[containerID]; ok {
		s.containers[containerID] = container.Clone(SetShardSize(groupKey, resourceSize))
	}
}

// UpdateStoreStatus updates the information of the container.
func (s *CachedStores) UpdateStoreStatus(groupKey string, containerID uint64, leaderCount int, resourceCount int, pendingPeerCount int, leaderSize int64, resourceSize int64) {
	if container, ok := s.containers[containerID]; ok {
		newStore := container.ShallowClone(SetLeaderCount(groupKey, leaderCount),
			SetShardCount(groupKey, resourceCount),
			SetPendingPeerCount(groupKey, pendingPeerCount),
			SetLeaderSize(groupKey, leaderSize),
			SetShardSize(groupKey, resourceSize))
		s.SetStore(newStore)
	}
}
