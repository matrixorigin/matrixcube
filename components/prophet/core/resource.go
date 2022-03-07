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
	"reflect"
	"sort"
	"strings"
	"sync"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
)

// errShardIsStale is error info for resource is stale.
var errShardIsStale = func(res metapb.Shard, origin metapb.Shard) error {
	return fmt.Errorf("resource is stale: resource %v, origin %v", res, origin)
}

// CachedShard resource runtime info cached in the cache
type CachedShard struct {
	sync.RWMutex
	Meta metapb.Shard

	term            uint64
	groupKey        string
	learners        []metapb.Replica
	voters          []metapb.Replica
	leader          *metapb.Replica
	downReplicas    []metapb.ReplicaStats
	pendingReplicas []metapb.Replica
	stats           metapb.ShardStats
}

// NewCachedShard creates CachedShard with resource's meta and leader peer.
func NewCachedShard(res metapb.Shard, leader *metapb.Replica, opts ...ShardCreateOption) *CachedShard {
	cr := &CachedShard{
		Meta:   res,
		leader: leader,
	}

	for _, opt := range opts {
		opt(cr)
	}
	classifyVoterAndLearner(cr)
	return cr
}

// classifyVoterAndLearner sorts out voter and learner from peers into different slice.
func classifyVoterAndLearner(res *CachedShard) {
	learners := make([]metapb.Replica, 0, 1)
	voters := make([]metapb.Replica, 0, len(res.Meta.GetReplicas()))
	for _, p := range res.Meta.GetReplicas() {
		if metadata.IsLearner(p) {
			learners = append(learners, p)
		} else {
			voters = append(voters, p)
		}
	}
	res.learners = learners
	res.voters = voters
}

const (
	// EmptyShardApproximateSize is the resource approximate size of an empty resource
	// (heartbeat size <= 1MB).
	EmptyShardApproximateSize = 1

	// ImpossibleFlowSize is an impossible flow size (such as written_bytes, read_keys, etc.)
	// It may be caused by overflow, refer to https://github.com/tikv/pd/issues/3379.
	// They need to be filtered so as not to affect downstream.
	// (flow size >= 1024TB)
	ImpossibleFlowSize = 1 << 50
)

// ShardFromHeartbeat constructs a Shard from resource heartbeat.
func ShardFromHeartbeat(heartbeat rpcpb.ShardHeartbeatReq, meta metapb.Shard) *CachedShard {
	// Convert unit to MB.
	// If resource is empty or less than 1MB, use 1MB instead.
	resourceSize := heartbeat.Stats.GetApproximateSize() / (1 << 20)
	if resourceSize < EmptyShardApproximateSize {
		resourceSize = EmptyShardApproximateSize
	}

	res := &CachedShard{
		Meta:            meta,
		groupKey:        heartbeat.GroupKey,
		term:            heartbeat.GetTerm(),
		leader:          heartbeat.GetLeader(),
		downReplicas:    heartbeat.GetDownReplicas(),
		pendingReplicas: heartbeat.GetPendingReplicas(),
		stats:           heartbeat.Stats,
	}
	res.stats.ApproximateSize = resourceSize

	if res.stats.WrittenKeys >= ImpossibleFlowSize || res.stats.WrittenBytes >= ImpossibleFlowSize {
		res.stats.WrittenKeys = 0
		res.stats.WrittenBytes = 0
	}
	if res.stats.ReadKeys >= ImpossibleFlowSize || res.stats.ReadBytes >= ImpossibleFlowSize {
		res.stats.ReadKeys = 0
		res.stats.ReadBytes = 0
	}

	sort.Sort(peerStatsSlice(res.downReplicas))
	sort.Sort(peerSlice(res.pendingReplicas))

	classifyVoterAndLearner(res)
	return res
}

// Clone returns a copy of current CachedShard.
func (r *CachedShard) Clone(opts ...ShardCreateOption) *CachedShard {
	downReplicas := make([]metapb.ReplicaStats, 0, len(r.downReplicas))
	for _, peer := range r.downReplicas {
		downReplicas = append(downReplicas, *(proto.Clone(&peer).(*metapb.ReplicaStats)))
	}
	pendingReplicas := make([]metapb.Replica, 0, len(r.pendingReplicas))
	for _, peer := range r.pendingReplicas {
		pendingReplicas = append(pendingReplicas, *(proto.Clone(&peer).(*metapb.Replica)))
	}

	res := &CachedShard{
		term:            r.term,
		Meta:            r.Meta,
		leader:          proto.Clone(r.leader).(*metapb.Replica),
		downReplicas:    downReplicas,
		pendingReplicas: pendingReplicas,
		stats:           r.stats,
	}
	res.stats.Interval = proto.Clone(r.stats.Interval).(*metapb.TimeInterval)

	for _, opt := range opts {
		opt(res)
	}
	classifyVoterAndLearner(res)
	return res
}

// GetGroupKey returns group key
func (r *CachedShard) GetGroupKey() string {
	return r.groupKey
}

// IsDestroyState resource in Destroyed or Destroying state
func (r *CachedShard) IsDestroyState() bool {
	r.RLock()
	defer r.RUnlock()
	return r.Meta.GetState() == metapb.ShardState_Destroyed ||
		r.Meta.GetState() == metapb.ShardState_Destroying
}

// GetTerm returns the current term of the resource
func (r *CachedShard) GetTerm() uint64 {
	return r.term
}

// GetLearners returns the learners.
func (r *CachedShard) GetLearners() []metapb.Replica {
	return r.learners
}

// GetVoters returns the voters.
func (r *CachedShard) GetVoters() []metapb.Replica {
	return r.voters
}

// GetPeer returns the peer with specified peer id.
func (r *CachedShard) GetPeer(peerID uint64) (metapb.Replica, bool) {
	for _, peer := range r.Meta.GetReplicas() {
		if peer.ID == peerID {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetDownPeer returns the down peer with specified peer id.
func (r *CachedShard) GetDownPeer(peerID uint64) (metapb.Replica, bool) {
	for _, down := range r.downReplicas {
		if down.Replica.ID == peerID {
			return down.Replica, true
		}
	}
	return metapb.Replica{}, false
}

// GetDownVoter returns the down voter with specified peer id.
func (r *CachedShard) GetDownVoter(peerID uint64) (metapb.Replica, bool) {
	for _, down := range r.downReplicas {
		if down.Replica.ID == peerID && !metadata.IsLearner(down.Replica) {
			return down.Replica, true
		}
	}
	return metapb.Replica{}, false
}

// GetDownLearner returns the down learner with soecified peer id.
func (r *CachedShard) GetDownLearner(peerID uint64) (metapb.Replica, bool) {
	for _, down := range r.downReplicas {
		if down.Replica.ID == peerID && metadata.IsLearner(down.Replica) {
			return down.Replica, true
		}
	}
	return metapb.Replica{}, false
}

// GetPendingPeer returns the pending peer with specified peer id.
func (r *CachedShard) GetPendingPeer(peerID uint64) (metapb.Replica, bool) {
	for _, peer := range r.pendingReplicas {
		if peer.ID == peerID {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetPendingVoter returns the pending voter with specified peer id.
func (r *CachedShard) GetPendingVoter(peerID uint64) (metapb.Replica, bool) {
	for _, peer := range r.pendingReplicas {
		if peer.ID == peerID && !metadata.IsLearner(peer) {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetPendingLearner returns the pending learner peer with specified peer id.
func (r *CachedShard) GetPendingLearner(peerID uint64) (metapb.Replica, bool) {
	for _, peer := range r.pendingReplicas {
		if peer.ID == peerID && metadata.IsLearner(peer) {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetStorePeer returns the peer in specified container.
func (r *CachedShard) GetStorePeer(containerID uint64) (metapb.Replica, bool) {
	for _, peer := range r.Meta.GetReplicas() {
		if peer.StoreID == containerID {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetStoreVoter returns the voter in specified container.
func (r *CachedShard) GetStoreVoter(containerID uint64) (metapb.Replica, bool) {
	for _, peer := range r.voters {
		if peer.StoreID == containerID {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetStoreLearner returns the learner peer in specified container.
func (r *CachedShard) GetStoreLearner(containerID uint64) (metapb.Replica, bool) {
	for _, peer := range r.learners {
		if peer.StoreID == containerID {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetStoreIDs returns a map indicate the resource distributed.
func (r *CachedShard) GetStoreIDs() map[uint64]struct{} {
	peers := r.Meta.GetReplicas()
	containerIDs := make(map[uint64]struct{}, len(peers))
	for _, peer := range peers {
		containerIDs[peer.StoreID] = struct{}{}
	}
	return containerIDs
}

// GetFollowers returns a map indicate the follow peers distributed.
func (r *CachedShard) GetFollowers() map[uint64]metapb.Replica {
	peers := r.GetVoters()
	followers := make(map[uint64]metapb.Replica, len(peers))
	for _, peer := range peers {
		if r.getLeaderID() != peer.ID {
			followers[peer.StoreID] = peer
		}
	}
	return followers
}

// GetFollower randomly returns a follow peer.
func (r *CachedShard) GetFollower() (metapb.Replica, bool) {
	for _, peer := range r.GetVoters() {
		if r.getLeaderID() != peer.ID {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetDiffFollowers returns the followers which is not located in the same
// container as any other followers of the another specified resource.
func (r *CachedShard) GetDiffFollowers(other *CachedShard) []metapb.Replica {
	res := make([]metapb.Replica, 0, len(r.Meta.GetReplicas()))
	for _, p := range r.GetFollowers() {
		diff := true
		for _, o := range other.GetFollowers() {
			if p.StoreID == o.StoreID {
				diff = false
				break
			}
		}
		if diff {
			res = append(res, p)
		}
	}
	return res
}

// GetStat returns the statistics of the resource.
func (r *CachedShard) GetStat() *metapb.ShardStats {
	if r == nil {
		return nil
	}
	stats := r.stats
	return &stats
}

// GetApproximateSize returns the approximate size of the resource.
func (r *CachedShard) GetApproximateSize() int64 {
	return int64(r.stats.ApproximateSize)
}

// GetApproximateKeys returns the approximate keys of the resource.
func (r *CachedShard) GetApproximateKeys() int64 {
	return int64(r.stats.ApproximateKeys)
}

// GetInterval returns the interval information of the resource.
func (r *CachedShard) GetInterval() *metapb.TimeInterval {
	return r.stats.Interval
}

// GetDownPeers returns the down peers of the resource.
func (r *CachedShard) GetDownPeers() []metapb.ReplicaStats {
	return r.downReplicas
}

// GetPendingPeers returns the pending peers of the resource.
func (r *CachedShard) GetPendingPeers() []metapb.Replica {
	return r.pendingReplicas
}

// GetBytesRead returns the read bytes of the resource.
func (r *CachedShard) GetBytesRead() uint64 {
	return r.stats.ReadBytes
}

// GetBytesWritten returns the written bytes of the resource.
func (r *CachedShard) GetBytesWritten() uint64 {
	return r.stats.WrittenBytes
}

// GetKeysWritten returns the written keys of the resource.
func (r *CachedShard) GetKeysWritten() uint64 {
	return r.stats.WrittenKeys
}

// GetKeysRead returns the read keys of the resource.
func (r *CachedShard) GetKeysRead() uint64 {
	return r.stats.ReadKeys
}

// GetLeader returns the leader of the resource.
func (r *CachedShard) GetLeader() *metapb.Replica {
	return r.leader
}

// GetStartKey returns the start key of the resource.
func (r *CachedShard) GetStartKey() []byte {
	v, _ := r.Meta.GetRange()
	return v
}

// GetEndKey returns the end key of the resource.
func (r *CachedShard) GetEndKey() []byte {
	_, v := r.Meta.GetRange()
	return v
}

func (r *CachedShard) getLeaderID() uint64 {
	if r.leader == nil {
		return 0
	}

	return r.leader.ID
}

// resourceMap wraps a map[uint64]*CachedShard and supports randomly pick a resource.
type resourceMap struct {
	m         map[uint64]*CachedShard
	totalSize int64
	totalKeys int64
}

func newShardMap() *resourceMap {
	return &resourceMap{
		m: make(map[uint64]*CachedShard),
	}
}

func (rm *resourceMap) Len() int {
	if rm == nil {
		return 0
	}
	return len(rm.m)
}

func (rm *resourceMap) Get(id uint64) *CachedShard {
	if rm == nil {
		return nil
	}
	if r, ok := rm.m[id]; ok {
		return r
	}
	return nil
}

func (rm *resourceMap) Put(res *CachedShard) {
	if old, ok := rm.m[res.Meta.GetID()]; ok {
		rm.totalSize -= int64(old.stats.ApproximateSize)
		rm.totalKeys -= int64(old.stats.ApproximateKeys)
	}
	rm.m[res.Meta.GetID()] = res
	rm.totalSize += int64(res.stats.ApproximateSize)
	rm.totalKeys += int64(res.stats.ApproximateKeys)
}

func (rm *resourceMap) Delete(id uint64) {
	if rm == nil {
		return
	}
	if old, ok := rm.m[id]; ok {
		delete(rm.m, id)
		rm.totalSize -= int64(old.stats.ApproximateSize)
		rm.totalKeys -= int64(old.stats.ApproximateKeys)
	}
}

func (rm *resourceMap) TotalSize() int64 {
	if rm.Len() == 0 {
		return 0
	}
	return rm.totalSize
}

// resourceSubTree is used to manager different types of resources.
type resourceSubTree struct {
	*resourceTree
	totalSize int64
	totalKeys int64
}

func newShardSubTree() *resourceSubTree {
	return &resourceSubTree{
		resourceTree: newShardTree(),
		totalSize:    0,
	}
}

func (rst *resourceSubTree) TotalSize() int64 {
	if rst.length() == 0 {
		return 0
	}
	return rst.totalSize
}

func (rst *resourceSubTree) scanRanges() []*CachedShard {
	if rst.length() == 0 {
		return nil
	}
	var resources []*CachedShard
	rst.scanRange([]byte(""), func(resource *CachedShard) bool {
		resources = append(resources, resource)
		return true
	})
	return resources
}

func (rst *resourceSubTree) update(res *CachedShard) {
	overlaps := rst.resourceTree.update(res)
	rst.totalSize += int64(res.stats.ApproximateSize)
	rst.totalKeys += int64(res.stats.ApproximateKeys)
	for _, r := range overlaps {
		rst.totalSize -= int64(r.stats.ApproximateSize)
		rst.totalKeys -= int64(r.stats.ApproximateKeys)
	}
}

func (rst *resourceSubTree) remove(res *CachedShard) {
	if rst.length() == 0 {
		return
	}
	if rst.resourceTree.remove(res) != nil {
		rst.totalSize -= int64(res.stats.ApproximateSize)
		rst.totalKeys -= int64(res.stats.ApproximateKeys)
	}
}

func (rst *resourceSubTree) length() int {
	if rst == nil {
		return 0
	}
	return rst.resourceTree.length()
}

func (rst *resourceSubTree) RandomShard(ranges []KeyRange) *CachedShard {
	if rst.length() == 0 {
		return nil
	}

	return rst.resourceTree.RandomShard(ranges)
}

func (rst *resourceSubTree) RandomShards(n int, ranges []KeyRange) []*CachedShard {
	if rst.length() == 0 {
		return nil
	}

	resources := make([]*CachedShard, 0, n)

	for i := 0; i < n; i++ {
		if resource := rst.resourceTree.RandomShard(ranges); resource != nil {
			resources = append(resources, resource)
		}
	}
	return resources
}

// CachedShards for export
type CachedShards struct {
	trees           map[uint64]*resourceTree               // group id -> resourceTree
	resources       *resourceMap                           // resourceID -> CachedShard
	leaders         map[string]map[uint64]*resourceSubTree // groupKey -> containerID -> resourceSubTree
	followers       map[string]map[uint64]*resourceSubTree // groupKey -> containerID -> resourceSubTree
	learners        map[string]map[uint64]*resourceSubTree // groupKey -> containerID -> resourceSubTree
	pendingReplicas map[string]map[uint64]*resourceSubTree // groupKey -> containerID -> resourceSubTree
}

// NewCachedShards creates CachedShards with tree, resources, leaders and followers
func NewCachedShards() *CachedShards {
	return &CachedShards{
		trees:           make(map[uint64]*resourceTree),
		resources:       newShardMap(),
		leaders:         make(map[string]map[uint64]*resourceSubTree),
		followers:       make(map[string]map[uint64]*resourceSubTree),
		learners:        make(map[string]map[uint64]*resourceSubTree),
		pendingReplicas: make(map[string]map[uint64]*resourceSubTree),
	}
}

func (r *CachedShards) maybeInitWithGroup(groupKey string) {
	if _, ok := r.leaders[groupKey]; !ok {
		r.leaders[groupKey] = make(map[uint64]*resourceSubTree)
	}
	if _, ok := r.followers[groupKey]; !ok {
		r.followers[groupKey] = make(map[uint64]*resourceSubTree)
	}
	if _, ok := r.learners[groupKey]; !ok {
		r.learners[groupKey] = make(map[uint64]*resourceSubTree)
	}
	if _, ok := r.pendingReplicas[groupKey]; !ok {
		r.pendingReplicas[groupKey] = make(map[uint64]*resourceSubTree)
	}
}

// ForeachShards foreach resource by group
func (r *CachedShards) ForeachShards(group uint64, fn func(res metapb.Shard)) {
	for _, res := range r.resources.m {
		if res.Meta.GetGroup() == group {
			fn(res.Meta)
		}
	}
}

// ForeachCachedShards foreach cached resource by group
func (r *CachedShards) ForeachCachedShards(group uint64, fn func(res *CachedShard)) {
	for _, res := range r.resources.m {
		if res.Meta.GetGroup() == group {
			fn(res)
		}
	}
}

// GetShard returns the CachedShard with resourceID
func (r *CachedShards) GetShard(resourceID uint64) *CachedShard {
	return r.resources.Get(resourceID)
}

// SetShard sets the CachedShard with resourceID
func (r *CachedShards) SetShard(res *CachedShard) []*CachedShard {
	if origin := r.resources.Get(res.Meta.GetID()); origin != nil {
		if !bytes.Equal(origin.GetStartKey(), res.GetStartKey()) ||
			!bytes.Equal(origin.GetEndKey(), res.GetEndKey()) {
			r.removeShardFromTreeAndMap(origin)
		}
		if r.shouldRemoveFromSubTree(res, origin) {
			r.removeShardFromSubTree(origin)
		}
	}
	return r.AddShard(res)
}

// Length returns the resourcesInfo length
func (r *CachedShards) Length() int {
	return r.resources.Len()
}

// GetOverlaps returns the resources which are overlapped with the specified resource range.
func (r *CachedShards) GetOverlaps(res *CachedShard) []*CachedShard {
	if tree, ok := r.trees[res.Meta.GetGroup()]; ok {
		return tree.getOverlaps(res)
	}

	return nil
}

// AddShard adds CachedShard to resourceTree and resourceMap, also update leaders and followers by resource peers
func (r *CachedShards) AddShard(res *CachedShard) []*CachedShard {
	if _, ok := r.trees[res.Meta.GetGroup()]; !ok {
		r.trees[res.Meta.GetGroup()] = newShardTree()
	}

	// Destroying resource cannot add to tree to avoid range overlaps,
	// and only wait schedule remove down replicas.
	if res.IsDestroyState() {
		r.RemoveShard(res)
	}

	tree := r.trees[res.Meta.GetGroup()]
	// the resources which are overlapped with the specified resource range.
	var overlaps []*CachedShard
	// when the value is true, add the resource to the tree.
	// Otherwise use the resource replace the origin resource in the tree.
	treeNeedAdd := !res.IsDestroyState()
	if origin := r.GetShard(res.Meta.GetID()); origin != nil {
		if resOld := tree.find(res); resOld != nil {
			// Update to tree.
			if bytes.Equal(resOld.res.GetStartKey(), res.GetStartKey()) &&
				bytes.Equal(resOld.res.GetEndKey(), res.GetEndKey()) &&
				resOld.res.Meta.GetID() == res.Meta.GetID() {
				resOld.res = res
				treeNeedAdd = false
			}
		}
	}
	if treeNeedAdd {
		// Add to tree.
		overlaps = tree.update(res)
		for _, item := range overlaps {
			r.RemoveShard(r.GetShard(item.Meta.GetID()))
		}
	}
	// Add to resources.
	r.resources.Put(res)

	r.maybeInitWithGroup(res.groupKey)

	// Add to leaders and followers.
	for _, peer := range res.GetVoters() {
		containerID := peer.StoreID
		if peer.ID == res.getLeaderID() {
			// Add leader peer to leaders.
			container, ok := r.leaders[res.groupKey][containerID]
			if !ok {
				container = newShardSubTree()
				r.leaders[res.groupKey][containerID] = container
			}
			container.update(res)
		} else {
			// Add follower peer to followers.
			container, ok := r.followers[res.groupKey][containerID]
			if !ok {
				container = newShardSubTree()
				r.followers[res.groupKey][containerID] = container
			}
			container.update(res)
		}
	}

	// Add to learners.
	for _, peer := range res.GetLearners() {
		containerID := peer.StoreID
		container, ok := r.learners[res.groupKey][containerID]
		if !ok {
			container = newShardSubTree()
			r.learners[res.groupKey][containerID] = container
		}
		container.update(res)
	}

	for _, peer := range res.pendingReplicas {
		containerID := peer.StoreID
		container, ok := r.pendingReplicas[res.groupKey][containerID]
		if !ok {
			container = newShardSubTree()
			r.pendingReplicas[res.groupKey][containerID] = container
		}
		container.update(res)
	}

	return overlaps
}

// RemoveShard removes CachedShard from resourceTree and resourceMap
func (r *CachedShards) RemoveShard(res *CachedShard) {
	// Remove from tree and resources.
	r.removeShardFromTreeAndMap(res)
	// Remove from leaders and followers.
	r.removeShardFromSubTree(res)
}

// removeShardFromTreeAndMap removes CachedShard from resourceTree and resourceMap
func (r *CachedShards) removeShardFromTreeAndMap(res *CachedShard) {
	// Remove from tree and resources.
	if tree, ok := r.trees[res.Meta.GetGroup()]; ok {
		tree.remove(res)
	}
	r.resources.Delete(res.Meta.GetID())
}

// removeShardFromSubTree removes CachedShard from resourcesubTrees
func (r *CachedShards) removeShardFromSubTree(res *CachedShard) {
	r.maybeInitWithGroup(res.groupKey)

	// Remove from leaders and followers.
	for _, peer := range res.Meta.GetReplicas() {
		containerID := peer.StoreID
		r.leaders[res.groupKey][containerID].remove(res)
		r.followers[res.groupKey][containerID].remove(res)
		r.learners[res.groupKey][containerID].remove(res)
		r.pendingReplicas[res.groupKey][containerID].remove(res)
	}
}

type peerSlice []metapb.Replica

func (s peerSlice) Len() int {
	return len(s)
}
func (s peerSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s peerSlice) Less(i, j int) bool {
	return s[i].ID < s[j].ID
}

// SortedPeersEqual judges whether two sorted `peerSlice` are equal
func SortedPeersEqual(peersA, peersB []metapb.Replica) bool {
	if len(peersA) != len(peersB) {
		return false
	}
	for i, peer := range peersA {
		if peer.GetID() != peersB[i].GetID() {
			return false
		}
	}
	return true
}

type peerStatsSlice []metapb.ReplicaStats

func (s peerStatsSlice) Len() int {
	return len(s)
}
func (s peerStatsSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s peerStatsSlice) Less(i, j int) bool {
	return s[i].GetReplica().ID < s[j].GetReplica().ID
}

// SortedPeersStatsEqual judges whether two sorted `peerStatsSlice` are equal
func SortedPeersStatsEqual(peersA, peersB []metapb.ReplicaStats) bool {
	if len(peersA) != len(peersB) {
		return false
	}
	for i, peerStats := range peersA {
		if peerStats.GetReplica().ID != peersB[i].GetReplica().ID ||
			peerStats.GetDownSeconds() != peersB[i].GetDownSeconds() {
			return false
		}
	}
	return true
}

// shouldRemoveFromSubTree return true when the resource leader changed, peer transferred,
// new peer was created, learners changed, pendingReplicas changed, and so on.
func (r *CachedShards) shouldRemoveFromSubTree(res *CachedShard, origin *CachedShard) bool {
	checkPeersChange := func(origin []metapb.Replica, other []metapb.Replica) bool {
		if len(origin) != len(other) {
			return true
		}
		sort.Sort(peerSlice(origin))
		sort.Sort(peerSlice(other))
		for index, peer := range origin {
			if peer.StoreID == other[index].StoreID && peer.ID == other[index].ID {
				continue
			}
			return true
		}
		return false
	}

	return origin.getLeaderID() != res.getLeaderID() ||
		checkPeersChange(origin.GetVoters(), res.GetVoters()) ||
		checkPeersChange(origin.GetLearners(), res.GetLearners()) ||
		checkPeersChange(origin.GetPendingPeers(), res.GetPendingPeers()) ||
		origin.groupKey != res.groupKey
}

// SearchShard searches CachedShard from resourceTree
func (r *CachedShards) SearchShard(group uint64, resKey []byte) *CachedShard {
	if tree, ok := r.trees[group]; ok {
		res := tree.search(resKey)
		if res == nil {
			return nil
		}
		return r.GetShard(res.Meta.GetID())
	}

	return nil
}

// SearchPrevShard searches previous CachedShard from resourceTree
func (r *CachedShards) SearchPrevShard(group uint64, resKey []byte) *CachedShard {
	if tree, ok := r.trees[group]; ok {
		res := tree.searchPrev(resKey)
		if res == nil {
			return nil
		}
		return r.GetShard(res.Meta.GetID())
	}

	return nil
}

// GetShards gets all CachedShard from resourceMap
func (r *CachedShards) GetShards() []*CachedShard {
	resources := make([]*CachedShard, 0, r.resources.Len())
	for _, res := range r.resources.m {
		resources = append(resources, res)
	}
	return resources
}

// GetStoreShards gets all CachedShard with a given containerID
func (r *CachedShards) GetStoreShards(groupKey string, containerID uint64) []*CachedShard {
	r.maybeInitWithGroup(groupKey)
	resources := make([]*CachedShard, 0, r.GetStoreShardCount(groupKey, containerID))
	if leaders, ok := r.leaders[groupKey][containerID]; ok {
		resources = append(resources, leaders.scanRanges()...)
	}
	if followers, ok := r.followers[groupKey][containerID]; ok {
		resources = append(resources, followers.scanRanges()...)
	}
	if learners, ok := r.learners[groupKey][containerID]; ok {
		resources = append(resources, learners.scanRanges()...)
	}
	return resources
}

// GetStoreLeaderShardSize get total size of container's leader resources
func (r *CachedShards) GetStoreLeaderShardSize(groupKey string, containerID uint64) int64 {
	r.maybeInitWithGroup(groupKey)
	return r.leaders[groupKey][containerID].TotalSize()
}

// GetStoreFollowerShardSize get total size of container's follower resources
func (r *CachedShards) GetStoreFollowerShardSize(groupKey string, containerID uint64) int64 {
	r.maybeInitWithGroup(groupKey)
	return r.followers[groupKey][containerID].TotalSize()
}

// GetStoreLearnerShardSize get total size of container's learner resources
func (r *CachedShards) GetStoreLearnerShardSize(groupKey string, containerID uint64) int64 {
	r.maybeInitWithGroup(groupKey)
	return r.learners[groupKey][containerID].TotalSize()
}

// GetStoreShardSize get total size of container's resources
func (r *CachedShards) GetStoreShardSize(groupKey string, containerID uint64) int64 {
	r.maybeInitWithGroup(groupKey)
	return r.GetStoreLeaderShardSize(groupKey, containerID) +
		r.GetStoreFollowerShardSize(groupKey, containerID) +
		r.GetStoreLearnerShardSize(groupKey, containerID)
}

// GetMetaShards gets a set of *metapb.Shard from resourceMap
func (r *CachedShards) GetMetaShards() []*metapb.Shard {
	resources := make([]*metapb.Shard, 0, r.resources.Len())
	for _, res := range r.resources.m {
		resources = append(resources, res.Meta.Clone())
	}
	return resources
}

// GetShardCount gets the total count of CachedShard of resourceMap
func (r *CachedShards) GetShardCount() int {
	return r.resources.Len()
}

// GetStoreShardCount gets the total count of a container's leader, follower and learner CachedShard by containerID
func (r *CachedShards) GetStoreShardCount(groupKey string, containerID uint64) int {
	r.maybeInitWithGroup(groupKey)
	return r.GetStoreLeaderCount(groupKey, containerID) +
		r.GetStoreFollowerCount(groupKey, containerID) +
		r.GetStoreLearnerCount(groupKey, containerID)
}

// GetStorePendingPeerCount gets the total count of a container's resource that includes pending peer
func (r *CachedShards) GetStorePendingPeerCount(groupKey string, containerID uint64) int {
	r.maybeInitWithGroup(groupKey)
	return r.pendingReplicas[groupKey][containerID].length()
}

// GetStoreLeaderCount get the total count of a container's leader CachedShard
func (r *CachedShards) GetStoreLeaderCount(groupKey string, containerID uint64) int {
	r.maybeInitWithGroup(groupKey)
	return r.leaders[groupKey][containerID].length()
}

// GetStoreFollowerCount get the total count of a container's follower CachedShard
func (r *CachedShards) GetStoreFollowerCount(groupKey string, containerID uint64) int {
	r.maybeInitWithGroup(groupKey)
	return r.followers[groupKey][containerID].length()
}

// GetStoreLearnerCount get the total count of a container's learner CachedShard
func (r *CachedShards) GetStoreLearnerCount(groupKey string, containerID uint64) int {
	r.maybeInitWithGroup(groupKey)
	return r.learners[groupKey][containerID].length()
}

// RandPendingShard randomly gets a container's resource with a pending peer.
func (r *CachedShards) RandPendingShard(groupKey string, containerID uint64, ranges []KeyRange) *CachedShard {
	r.maybeInitWithGroup(groupKey)
	return r.pendingReplicas[groupKey][containerID].RandomShard(ranges)
}

// RandPendingShards randomly gets a container's n resources with a pending peer.
func (r *CachedShards) RandPendingShards(groupKey string, containerID uint64, ranges []KeyRange, n int) []*CachedShard {
	r.maybeInitWithGroup(groupKey)
	return r.pendingReplicas[groupKey][containerID].RandomShards(n, ranges)
}

// RandLeaderShard randomly gets a container's leader resource.
func (r *CachedShards) RandLeaderShard(groupKey string, containerID uint64, ranges []KeyRange) *CachedShard {
	r.maybeInitWithGroup(groupKey)
	return r.leaders[groupKey][containerID].RandomShard(ranges)
}

// RandLeaderShards randomly gets a container's n leader resources.
func (r *CachedShards) RandLeaderShards(groupKey string, containerID uint64, ranges []KeyRange, n int) []*CachedShard {
	r.maybeInitWithGroup(groupKey)
	return r.leaders[groupKey][containerID].RandomShards(n, ranges)
}

// RandFollowerShard randomly gets a container's follower resource.
func (r *CachedShards) RandFollowerShard(groupKey string, containerID uint64, ranges []KeyRange) *CachedShard {
	r.maybeInitWithGroup(groupKey)
	return r.followers[groupKey][containerID].RandomShard(ranges)
}

// RandFollowerShards randomly gets a container's n follower resources.
func (r *CachedShards) RandFollowerShards(groupKey string, containerID uint64, ranges []KeyRange, n int) []*CachedShard {
	r.maybeInitWithGroup(groupKey)
	return r.followers[groupKey][containerID].RandomShards(n, ranges)
}

// RandLearnerShard randomly gets a container's learner resource.
func (r *CachedShards) RandLearnerShard(groupKey string, containerID uint64, ranges []KeyRange) *CachedShard {
	r.maybeInitWithGroup(groupKey)
	return r.learners[groupKey][containerID].RandomShard(ranges)
}

// RandLearnerShards randomly gets a container's n learner resources.
func (r *CachedShards) RandLearnerShards(groupKey string, containerID uint64, ranges []KeyRange, n int) []*CachedShard {
	r.maybeInitWithGroup(groupKey)
	return r.learners[groupKey][containerID].RandomShards(n, ranges)
}

// GetLeader return leader CachedShard by containerID and resourceID(now only used in test)
func (r *CachedShards) GetLeader(groupKey string, containerID uint64, res *CachedShard) *CachedShard {
	r.maybeInitWithGroup(groupKey)
	if leaders, ok := r.leaders[groupKey][containerID]; ok {
		return leaders.find(res).res
	}
	return nil
}

// GetFollower return follower CachedShard by containerID and resourceID(now only used in test)
func (r *CachedShards) GetFollower(groupKey string, containerID uint64, res *CachedShard) *CachedShard {
	r.maybeInitWithGroup(groupKey)
	if followers, ok := r.followers[groupKey][containerID]; ok {
		return followers.find(res).res
	}
	return nil
}

// ScanRange scans resources intersecting [start key, end key), returns at most
// `limit` resources. limit <= 0 means no limit.
func (r *CachedShards) ScanRange(group uint64, startKey, endKey []byte, limit int) []*CachedShard {
	var resources []*CachedShard
	if tree, ok := r.trees[group]; ok {
		tree.scanRange(startKey, func(resource *CachedShard) bool {
			if len(endKey) > 0 && bytes.Compare(resource.GetStartKey(), endKey) >= 0 {
				return false
			}
			if limit > 0 && len(resources) >= limit {
				return false
			}
			resources = append(resources, r.GetShard(resource.Meta.GetID()))
			return true
		})
	}
	return resources
}

// GetDestroyingShards returns all resources in destroying state
func (r *CachedShards) GetDestroyingShards() []*CachedShard {
	var resources []*CachedShard
	for _, res := range r.resources.m {
		if res.Meta.GetState() == metapb.ShardState_Destroying {
			resources = append(resources, res)
		}
	}
	return resources
}

// ScanRangeWithIterator scans from the first resource containing or behind start key,
// until iterator returns false.
func (r *CachedShards) ScanRangeWithIterator(group uint64, startKey []byte, iterator func(res *CachedShard) bool) {
	if tree, ok := r.trees[group]; ok {
		tree.scanRange(startKey, iterator)
	}
}

// GetAdjacentShards returns resource's info that is adjacent with specific resource
func (r *CachedShards) GetAdjacentShards(res *CachedShard) (*CachedShard, *CachedShard) {
	var prev, next *CachedShard
	if tree, ok := r.trees[res.Meta.GetGroup()]; ok {
		p, n := tree.getAdjacentShards(res)
		// check key to avoid key range hole
		if p != nil && bytes.Equal(p.res.GetEndKey(), res.GetStartKey()) {
			prev = r.GetShard(p.res.Meta.GetID())
		}
		if n != nil && bytes.Equal(res.GetEndKey(), n.res.GetStartKey()) {
			next = r.GetShard(n.res.Meta.GetID())
		}
	}
	return prev, next
}

// GetAverageShardSize returns the average resource approximate size.
func (r *CachedShards) GetAverageShardSize() int64 {
	if r.resources.Len() == 0 {
		return 0
	}
	return r.resources.TotalSize() / int64(r.resources.Len())
}

// DiffShardPeersInfo return the difference of peers info  between two CachedShard
func DiffShardPeersInfo(origin *CachedShard, other *CachedShard) string {
	var ret []string
	for _, a := range origin.Meta.GetReplicas() {
		both := false
		for _, b := range other.Meta.GetReplicas() {
			if reflect.DeepEqual(a, b) {
				both = true
				break
			}
		}
		if !both {
			ret = append(ret, fmt.Sprintf("Remove peer:{%v}", a))
		}
	}
	for _, b := range other.Meta.GetReplicas() {
		both := false
		for _, a := range origin.Meta.GetReplicas() {
			if reflect.DeepEqual(a, b) {
				both = true
				break
			}
		}
		if !both {
			ret = append(ret, fmt.Sprintf("Add peer:{%v}", b))
		}
	}
	return strings.Join(ret, ",")
}

// DiffShardKeyInfo return the difference of key info between two CachedShard
func DiffShardKeyInfo(origin *CachedShard, other *CachedShard) string {
	originStartKey, originEndKey := origin.Meta.GetRange()
	otherStartKey, otherEndKey := other.Meta.GetRange()

	var ret []string
	if !bytes.Equal(originStartKey, otherStartKey) {
		ret = append(ret, fmt.Sprintf("StartKey Changed:{%s} -> {%s}", HexShardKey(originStartKey), HexShardKey(otherStartKey)))
	} else {
		ret = append(ret, fmt.Sprintf("StartKey:{%s}", HexShardKey(originStartKey)))
	}
	if !bytes.Equal(originEndKey, otherEndKey) {
		ret = append(ret, fmt.Sprintf("EndKey Changed:{%s} -> {%s}", HexShardKey(originEndKey), HexShardKey(otherEndKey)))
	} else {
		ret = append(ret, fmt.Sprintf("EndKey:{%s}", HexShardKey(originEndKey)))
	}

	return strings.Join(ret, ", ")
}

func isInvolved(res *CachedShard, startKey, endKey []byte) bool {
	return bytes.Compare(res.GetStartKey(), startKey) >= 0 && (len(endKey) == 0 || (len(res.GetEndKey()) > 0 && bytes.Compare(res.GetEndKey(), endKey) <= 0))
}

// String converts slice of bytes to string without copy.
func String(b []byte) (s string) {
	if len(b) == 0 {
		return ""
	}
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

// ToUpperASCIIInplace bytes.ToUpper but zero-cost
func ToUpperASCIIInplace(s []byte) []byte {
	hasLower := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		hasLower = hasLower || ('a' <= c && c <= 'z')
	}

	if !hasLower {
		return s
	}
	var c byte
	for i := 0; i < len(s); i++ {
		c = s[i]
		if 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
		}
		s[i] = c
	}
	return s
}

// EncodeToString overrides hex.EncodeToString implementation. Difference: returns []byte, not string
func EncodeToString(src []byte) []byte {
	dst := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(dst, src)
	return dst
}

// HexShardKey converts resource key to hex format. Used for formating resource in
// logs.
func HexShardKey(key []byte) []byte {
	return ToUpperASCIIInplace(EncodeToString(key))
}

// HexShardKeyStr converts resource key to hex format. Used for formating resource in
// logs.
func HexShardKeyStr(key []byte) string {
	return String(HexShardKey(key))
}

// ShardToHexMeta converts a resource meta's keys to hex format. Used for formating
// resource in logs.
func ShardToHexMeta(meta metapb.Shard) HexShardMeta {
	start, end := meta.GetRange()
	meta.SetStartKey(HexShardKey(start))
	meta.SetEndKey(HexShardKey(end))
	return HexShardMeta{meta}
}

// HexShardMeta is a resource meta in the hex format. Used for formating resource in logs.
type HexShardMeta struct {
	meta metapb.Shard
}

func (h HexShardMeta) String() string {
	return fmt.Sprintf("resource %+v", h.meta)
}

// ShardsToHexMeta converts resources' meta keys to hex format. Used for formating
// resource in logs.
func ShardsToHexMeta(resources []*metapb.Shard) HexShardsMeta {
	hexShardMetas := make([]*metapb.Shard, len(resources))
	for i, res := range resources {
		meta := res.Clone()
		start, end := meta.GetRange()
		meta.SetStartKey(HexShardKey(start))
		meta.SetEndKey(HexShardKey(end))
		hexShardMetas[i] = meta
	}
	return hexShardMetas
}

// HexShardsMeta is a slice of resources' meta in the hex format. Used for formating
// resource in logs.
type HexShardsMeta []*metapb.Shard

func (h HexShardsMeta) String() string {
	var b strings.Builder
	for _, r := range h {
		b.WriteString(fmt.Sprintf("resource %+v", r))
	}
	return strings.TrimSpace(b.String())
}
