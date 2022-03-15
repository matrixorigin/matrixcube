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

// errShardIsStale is error info for shard is stale.
var errShardIsStale = func(res metapb.Shard, origin metapb.Shard) error {
	return fmt.Errorf("shard is stale: shard %v, origin %v", res, origin)
}

// CachedShard shard runtime info cached in the cache
type CachedShard struct {
	sync.RWMutex
	Meta metapb.Shard

	term            uint64
	groupKey        string
	learners        []metapb.Replica
	voters          []metapb.Replica
	leader          *metapb.Replica
	downReplicas    replicaStatsSlice
	pendingReplicas replicaSlice
	stats           metapb.ShardStats
}

// NewCachedShard creates CachedShard with shard's meta and leader peer.
func NewCachedShard(res metapb.Shard, leader *metapb.Replica, opts ...ShardCreateOption) *CachedShard {
	cr := &CachedShard{
		Meta:   res,
		leader: leader,
	}

	for _, opt := range opts {
		opt(cr)
	}
	fillVoterAndLearner(cr)
	return cr
}

// fillVoterAndLearner sorts out voter and learner from peers into different slice.
func fillVoterAndLearner(res *CachedShard) {
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
	// MinShardSize is the shard approximate size of an empty shard
	// (heartbeat size <= 1MB).
	MinShardSize = 1

	// MaximumFlowSize is an impossible flow size (such as written_bytes, read_keys, etc.)
	// It may be caused by overflow, refer to https://github.com/tikv/pd/issues/3379.
	// They need to be filtered so as not to affect downstream.
	// (flow size >= 1024TB)
	MaximumFlowSize = 1 << 50
)

// ShardFromHeartbeat constructs a Shard from shard heartbeat.
func ShardFromHeartbeat(heartbeat rpcpb.ShardHeartbeatReq, meta metapb.Shard) *CachedShard {
	// Convert unit to MB.
	// If shard is empty or less than 1MB, use 1MB instead.
	shardSize := heartbeat.Stats.GetApproximateSize() / (1 << 20)
	if shardSize < MinShardSize {
		shardSize = MinShardSize
	}

	shard := &CachedShard{
		Meta:            meta,
		groupKey:        heartbeat.GroupKey,
		term:            heartbeat.GetTerm(),
		leader:          heartbeat.GetLeader(),
		downReplicas:    heartbeat.GetDownReplicas(),
		pendingReplicas: heartbeat.GetPendingReplicas(),
		stats:           heartbeat.Stats,
	}
	shard.stats.ApproximateSize = shardSize

	if shard.stats.WrittenKeys >= MaximumFlowSize || shard.stats.WrittenBytes >= MaximumFlowSize {
		shard.stats.WrittenKeys = 0
		shard.stats.WrittenBytes = 0
	}
	if shard.stats.ReadKeys >= MaximumFlowSize || shard.stats.ReadBytes >= MaximumFlowSize {
		shard.stats.ReadKeys = 0
		shard.stats.ReadBytes = 0
	}

	sort.Sort(shard.downReplicas)
	sort.Sort(shard.pendingReplicas)

	fillVoterAndLearner(shard)
	return shard
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
	fillVoterAndLearner(res)
	return res
}

// GetGroupKey returns group key
func (r *CachedShard) GetGroupKey() string {
	return r.groupKey
}

// IsDestroyState shard in Destroyed or Destroying state
func (r *CachedShard) IsDestroyState() bool {
	r.RLock()
	defer r.RUnlock()
	return r.Meta.GetState() == metapb.ShardState_Destroyed ||
		r.Meta.GetState() == metapb.ShardState_Destroying
}

// GetTerm returns the current term of the shard
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

// GetDownLearner returns the down learner with specified peer id.
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
func (r *CachedShard) GetStorePeer(storeID uint64) (metapb.Replica, bool) {
	for _, peer := range r.Meta.GetReplicas() {
		if peer.StoreID == storeID {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetStoreVoter returns the voter in specified container.
func (r *CachedShard) GetStoreVoter(storeID uint64) (metapb.Replica, bool) {
	for _, peer := range r.voters {
		if peer.StoreID == storeID {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetStoreLearner returns the learner peer in specified container.
func (r *CachedShard) GetStoreLearner(storeID uint64) (metapb.Replica, bool) {
	for _, peer := range r.learners {
		if peer.StoreID == storeID {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetStoreIDs returns a map indicate the shard distributed.
func (r *CachedShard) GetStoreIDs() map[uint64]struct{} {
	peers := r.Meta.GetReplicas()
	storeIDs := make(map[uint64]struct{}, len(peers))
	for _, peer := range peers {
		storeIDs[peer.StoreID] = struct{}{}
	}
	return storeIDs
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
// container as any other followers of the another specified shard.
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

// GetStat returns the statistics of the shard.
func (r *CachedShard) GetStat() *metapb.ShardStats {
	if r == nil {
		return nil
	}
	stats := r.stats
	return &stats
}

// GetApproximateSize returns the approximate size of the shard.
func (r *CachedShard) GetApproximateSize() int64 {
	return int64(r.stats.ApproximateSize)
}

// GetApproximateKeys returns the approximate keys of the shard.
func (r *CachedShard) GetApproximateKeys() int64 {
	return int64(r.stats.ApproximateKeys)
}

// GetInterval returns the interval information of the shard.
func (r *CachedShard) GetInterval() *metapb.TimeInterval {
	return r.stats.Interval
}

// GetDownPeers returns the down peers of the shard.
func (r *CachedShard) GetDownPeers() []metapb.ReplicaStats {
	return r.downReplicas
}

// GetPendingPeers returns the pending peers of the shard.
func (r *CachedShard) GetPendingPeers() []metapb.Replica {
	return r.pendingReplicas
}

// GetBytesRead returns the read bytes of the shard.
func (r *CachedShard) GetBytesRead() uint64 {
	return r.stats.ReadBytes
}

// GetBytesWritten returns the written bytes of the shard.
func (r *CachedShard) GetBytesWritten() uint64 {
	return r.stats.WrittenBytes
}

// GetKeysWritten returns the written keys of the shard.
func (r *CachedShard) GetKeysWritten() uint64 {
	return r.stats.WrittenKeys
}

// GetKeysRead returns the read keys of the shard.
func (r *CachedShard) GetKeysRead() uint64 {
	return r.stats.ReadKeys
}

// GetLeader returns the leader of the shard.
func (r *CachedShard) GetLeader() *metapb.Replica {
	return r.leader
}

// GetStartKey returns the start key of the shard.
func (r *CachedShard) GetStartKey() []byte {
	v, _ := r.Meta.GetRange()
	return v
}

// GetEndKey returns the end key of the shard.
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

// shardMap wraps a map[uint64]*CachedShard and supports randomly pick a shard.
type shardMap struct {
	m         map[uint64]*CachedShard
	totalSize int64
	totalKeys int64
}

func newShardMap() *shardMap {
	return &shardMap{
		m: make(map[uint64]*CachedShard),
	}
}

func (rm *shardMap) Len() int {
	if rm == nil {
		return 0
	}
	return len(rm.m)
}

func (rm *shardMap) Get(id uint64) *CachedShard {
	if rm == nil {
		return nil
	}
	if r, ok := rm.m[id]; ok {
		return r
	}
	return nil
}

func (rm *shardMap) Put(res *CachedShard) {
	if old, ok := rm.m[res.Meta.GetID()]; ok {
		rm.totalSize -= int64(old.stats.ApproximateSize)
		rm.totalKeys -= int64(old.stats.ApproximateKeys)
	}
	rm.m[res.Meta.GetID()] = res
	rm.totalSize += int64(res.stats.ApproximateSize)
	rm.totalKeys += int64(res.stats.ApproximateKeys)
}

func (rm *shardMap) Delete(id uint64) {
	if rm == nil {
		return
	}
	if old, ok := rm.m[id]; ok {
		delete(rm.m, id)
		rm.totalSize -= int64(old.stats.ApproximateSize)
		rm.totalKeys -= int64(old.stats.ApproximateKeys)
	}
}

func (rm *shardMap) TotalSize() int64 {
	if rm.Len() == 0 {
		return 0
	}
	return rm.totalSize
}

// shardSubTree is used to manager different types of shards.
type shardSubTree struct {
	*shardTree
	totalSize int64
	totalKeys int64
}

func newShardSubTree() *shardSubTree {
	return &shardSubTree{
		shardTree: newShardTree(),
		totalSize: 0,
	}
}

func (rst *shardSubTree) TotalSize() int64 {
	if rst.length() == 0 {
		return 0
	}
	return rst.totalSize
}

func (rst *shardSubTree) scanRanges() []*CachedShard {
	if rst.length() == 0 {
		return nil
	}
	var shards []*CachedShard
	rst.scanRange([]byte(""), func(shard *CachedShard) bool {
		shards = append(shards, shard)
		return true
	})
	return shards
}

func (rst *shardSubTree) update(res *CachedShard) {
	overlaps := rst.shardTree.update(res)
	rst.totalSize += int64(res.stats.ApproximateSize)
	rst.totalKeys += int64(res.stats.ApproximateKeys)
	for _, r := range overlaps {
		rst.totalSize -= int64(r.stats.ApproximateSize)
		rst.totalKeys -= int64(r.stats.ApproximateKeys)
	}
}

func (rst *shardSubTree) remove(res *CachedShard) {
	if rst.length() == 0 {
		return
	}
	if rst.shardTree.remove(res) != nil {
		rst.totalSize -= int64(res.stats.ApproximateSize)
		rst.totalKeys -= int64(res.stats.ApproximateKeys)
	}
}

func (rst *shardSubTree) length() int {
	if rst == nil {
		return 0
	}
	return rst.shardTree.length()
}

func (rst *shardSubTree) RandomShards(n int, ranges []KeyRange) []*CachedShard {
	if rst.length() == 0 {
		return nil
	}

	shards := make([]*CachedShard, 0, n)

	for i := 0; i < n; i++ {
		if shard := rst.shardTree.RandomShard(ranges); shard != nil {
			shards = append(shards, shard)
		}
	}
	return shards
}

// ShardsContainer for export
type ShardsContainer struct {
	sync.RWMutex

	trees           map[uint64]*shardTree               // group id -> shardTree
	shards          *shardMap                           // shardID -> CachedShard
	leaders         map[string]map[uint64]*shardSubTree // groupKey -> storeID -> shardSubTree
	followers       map[string]map[uint64]*shardSubTree // groupKey -> storeID -> shardSubTree
	learners        map[string]map[uint64]*shardSubTree // groupKey -> storeID -> shardSubTree
	pendingReplicas map[string]map[uint64]*shardSubTree // groupKey -> storeID -> shardSubTree
}

// NewCachedShards creates ShardsContainer with tree, shards, leaders and followers
func NewCachedShards() *ShardsContainer {
	return &ShardsContainer{
		trees:           make(map[uint64]*shardTree),
		shards:          newShardMap(),
		leaders:         make(map[string]map[uint64]*shardSubTree),
		followers:       make(map[string]map[uint64]*shardSubTree),
		learners:        make(map[string]map[uint64]*shardSubTree),
		pendingReplicas: make(map[string]map[uint64]*shardSubTree),
	}
}

func (r *ShardsContainer) maybeInitWithGroup(groupKey string) {
	r.Lock()
	defer r.Unlock()

	if _, ok := r.leaders[groupKey]; !ok {
		r.leaders[groupKey] = make(map[uint64]*shardSubTree)
	}
	if _, ok := r.followers[groupKey]; !ok {
		r.followers[groupKey] = make(map[uint64]*shardSubTree)
	}
	if _, ok := r.learners[groupKey]; !ok {
		r.learners[groupKey] = make(map[uint64]*shardSubTree)
	}
	if _, ok := r.pendingReplicas[groupKey]; !ok {
		r.pendingReplicas[groupKey] = make(map[uint64]*shardSubTree)
	}
}

// ForeachShards foreach shard by group
func (r *ShardsContainer) ForeachShards(group uint64, fn func(res metapb.Shard)) {
	for _, res := range r.shards.m {
		if res.Meta.GetGroup() == group {
			fn(res.Meta)
		}
	}
}

// ForeachCachedShards foreach cached shard by group
func (r *ShardsContainer) ForeachCachedShards(group uint64, fn func(res *CachedShard)) {
	for _, res := range r.shards.m {
		if res.Meta.GetGroup() == group {
			fn(res)
		}
	}
}

// GetShard returns the CachedShard with shardID
func (r *ShardsContainer) GetShard(shardID uint64) *CachedShard {
	return r.shards.Get(shardID)
}

// SetShard sets the CachedShard with shardID
func (r *ShardsContainer) SetShard(res *CachedShard) []*CachedShard {
	if origin := r.shards.Get(res.Meta.GetID()); origin != nil {
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

// Length returns the shardsInfo length
func (r *ShardsContainer) Length() int {
	return r.shards.Len()
}

// GetOverlaps returns the shards which are overlapped with the specified shard range.
func (r *ShardsContainer) GetOverlaps(res *CachedShard) []*CachedShard {
	if tree, ok := r.trees[res.Meta.GetGroup()]; ok {
		return tree.getOverlaps(res)
	}

	return nil
}

// AddShard adds CachedShard to shardTree and shardMap, also update leaders and followers by shard peers
func (r *ShardsContainer) AddShard(res *CachedShard) []*CachedShard {
	if _, ok := r.trees[res.Meta.GetGroup()]; !ok {
		r.trees[res.Meta.GetGroup()] = newShardTree()
	}

	// Destroying shard cannot add to tree to avoid range overlaps,
	// and only wait schedule remove down replicas.
	if res.IsDestroyState() {
		r.RemoveShard(res)
	}

	tree := r.trees[res.Meta.GetGroup()]
	// the shards which are overlapped with the specified shard range.
	var overlaps []*CachedShard
	// when the value is true, add the shard to the tree.
	// Otherwise, use the shard replace the origin shard in the tree.
	treeNeedAdd := !res.IsDestroyState()
	if origin := r.GetShard(res.Meta.GetID()); origin != nil {
		if resOld := tree.find(res); resOld != nil {
			// Update to tree.
			if bytes.Equal(resOld.shard.GetStartKey(), res.GetStartKey()) &&
				bytes.Equal(resOld.shard.GetEndKey(), res.GetEndKey()) &&
				resOld.shard.Meta.GetID() == res.Meta.GetID() {
				resOld.shard = res
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
	// Add to shards.
	r.shards.Put(res)

	r.maybeInitWithGroup(res.groupKey)

	// Add to leaders and followers.
	for _, peer := range res.GetVoters() {
		storeID := peer.StoreID
		if peer.ID == res.getLeaderID() {
			// Add leader peer to leaders.
			store, ok := r.leaders[res.groupKey][storeID]
			if !ok {
				store = newShardSubTree()
				r.leaders[res.groupKey][storeID] = store
			}
			store.update(res)
		} else {
			// Add follower peer to followers.
			store, ok := r.followers[res.groupKey][storeID]
			if !ok {
				store = newShardSubTree()
				r.followers[res.groupKey][storeID] = store
			}
			store.update(res)
		}
	}

	// Add to learners.
	for _, peer := range res.GetLearners() {
		storeID := peer.StoreID
		store, ok := r.learners[res.groupKey][storeID]
		if !ok {
			store = newShardSubTree()
			r.learners[res.groupKey][storeID] = store
		}
		store.update(res)
	}

	for _, peer := range res.pendingReplicas {
		storeID := peer.StoreID
		store, ok := r.pendingReplicas[res.groupKey][storeID]
		if !ok {
			store = newShardSubTree()
			r.pendingReplicas[res.groupKey][storeID] = store
		}
		store.update(res)
	}

	return overlaps
}

// RemoveShard removes CachedShard from shardTree and shardMap
func (r *ShardsContainer) RemoveShard(res *CachedShard) {
	// Remove from tree and shards.
	r.removeShardFromTreeAndMap(res)
	// Remove from leaders and followers.
	r.removeShardFromSubTree(res)
}

// removeShardFromTreeAndMap removes CachedShard from shardTree and shardMap
func (r *ShardsContainer) removeShardFromTreeAndMap(res *CachedShard) {
	// Remove from tree and shards.
	if tree, ok := r.trees[res.Meta.GetGroup()]; ok {
		tree.remove(res)
	}
	r.shards.Delete(res.Meta.GetID())
}

// removeShardFromSubTree removes CachedShard from shardSubTrees
func (r *ShardsContainer) removeShardFromSubTree(res *CachedShard) {
	r.maybeInitWithGroup(res.groupKey)

	// Remove from leaders and followers.
	for _, peer := range res.Meta.GetReplicas() {
		storeID := peer.StoreID
		r.leaders[res.groupKey][storeID].remove(res)
		r.followers[res.groupKey][storeID].remove(res)
		r.learners[res.groupKey][storeID].remove(res)
		r.pendingReplicas[res.groupKey][storeID].remove(res)
	}
}

type replicaSlice []metapb.Replica

func (s replicaSlice) Len() int {
	return len(s)
}
func (s replicaSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s replicaSlice) Less(i, j int) bool {
	return s[i].ID < s[j].ID
}

// SortedPeersEqual judges whether two sorted `replicaSlice` are equal
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

type replicaStatsSlice []metapb.ReplicaStats

func (s replicaStatsSlice) Len() int {
	return len(s)
}
func (s replicaStatsSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s replicaStatsSlice) Less(i, j int) bool {
	return s[i].GetReplica().ID < s[j].GetReplica().ID
}

// SortedPeersStatsEqual judges whether two sorted `replicaStatsSlice` are equal
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

// shouldRemoveFromSubTree return true when the shard leader changed, peer transferred,
// new peer was created, learners changed, pendingReplicas changed, and so on.
func (r *ShardsContainer) shouldRemoveFromSubTree(res *CachedShard, origin *CachedShard) bool {
	checkPeersChange := func(origin []metapb.Replica, other []metapb.Replica) bool {
		if len(origin) != len(other) {
			return true
		}
		sort.Sort(replicaSlice(origin))
		sort.Sort(replicaSlice(other))
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

// SearchShard searches CachedShard from shardTree
func (r *ShardsContainer) SearchShard(group uint64, resKey []byte) *CachedShard {
	if tree, ok := r.trees[group]; ok {
		res := tree.search(resKey)
		if res == nil {
			return nil
		}
		return r.GetShard(res.Meta.GetID())
	}

	return nil
}

// SearchPrevShard searches previous CachedShard from shardTree
func (r *ShardsContainer) SearchPrevShard(group uint64, resKey []byte) *CachedShard {
	if tree, ok := r.trees[group]; ok {
		res := tree.searchPrev(resKey)
		if res == nil {
			return nil
		}
		return r.GetShard(res.Meta.GetID())
	}

	return nil
}

// GetShards gets all CachedShard from shardMap
func (r *ShardsContainer) GetShards() []*CachedShard {
	shards := make([]*CachedShard, 0, r.shards.Len())
	for _, res := range r.shards.m {
		shards = append(shards, res)
	}
	return shards
}

// GetStoreShards gets all CachedShard with a given storeID
func (r *ShardsContainer) GetStoreShards(groupKey string, storeID uint64) []*CachedShard {
	r.maybeInitWithGroup(groupKey)
	shards := make([]*CachedShard, 0, r.GetStoreShardCount(groupKey, storeID))
	if leaders, ok := r.leaders[groupKey][storeID]; ok {
		shards = append(shards, leaders.scanRanges()...)
	}
	if followers, ok := r.followers[groupKey][storeID]; ok {
		shards = append(shards, followers.scanRanges()...)
	}
	if learners, ok := r.learners[groupKey][storeID]; ok {
		shards = append(shards, learners.scanRanges()...)
	}
	return shards
}

// GetStoreLeaderShardSize get total size of container's leader shards
func (r *ShardsContainer) GetStoreLeaderShardSize(groupKey string, storeID uint64) int64 {
	r.maybeInitWithGroup(groupKey)
	return r.leaders[groupKey][storeID].TotalSize()
}

// GetStoreFollowerShardSize get total size of container's follower shards
func (r *ShardsContainer) GetStoreFollowerShardSize(groupKey string, storeID uint64) int64 {
	r.maybeInitWithGroup(groupKey)
	return r.followers[groupKey][storeID].TotalSize()
}

// GetStoreLearnerShardSize get total size of container's learner shards
func (r *ShardsContainer) GetStoreLearnerShardSize(groupKey string, storeID uint64) int64 {
	r.maybeInitWithGroup(groupKey)
	return r.learners[groupKey][storeID].TotalSize()
}

// GetStoreShardSize get total size of container's shards
func (r *ShardsContainer) GetStoreShardSize(groupKey string, storeID uint64) int64 {
	r.maybeInitWithGroup(groupKey)
	return r.GetStoreLeaderShardSize(groupKey, storeID) +
		r.GetStoreFollowerShardSize(groupKey, storeID) +
		r.GetStoreLearnerShardSize(groupKey, storeID)
}

// GetMetaShards gets a set of metapb.Shard from shardMap
func (r *ShardsContainer) GetMetaShards() []metapb.Shard {
	shards := make([]metapb.Shard, 0, r.shards.Len())
	for _, res := range r.shards.m {
		shards = append(shards, res.Meta)
	}
	return shards
}

// GetShardCount gets the total count of CachedShard of shardMap
func (r *ShardsContainer) GetShardCount() int {
	return r.shards.Len()
}

// GetStoreShardCount gets the total count of a container's leader, follower and learner CachedShard by storeID
func (r *ShardsContainer) GetStoreShardCount(groupKey string, storeID uint64) int {
	r.maybeInitWithGroup(groupKey)
	return r.GetStoreLeaderCount(groupKey, storeID) +
		r.GetStoreFollowerCount(groupKey, storeID) +
		r.GetStoreLearnerCount(groupKey, storeID)
}

// GetStorePendingPeerCount gets the total count of a container's shard that includes pending peer
func (r *ShardsContainer) GetStorePendingPeerCount(groupKey string, storeID uint64) int {
	r.maybeInitWithGroup(groupKey)
	return r.pendingReplicas[groupKey][storeID].length()
}

// GetStoreLeaderCount get the total count of a container's leader CachedShard
func (r *ShardsContainer) GetStoreLeaderCount(groupKey string, storeID uint64) int {
	r.maybeInitWithGroup(groupKey)
	return r.leaders[groupKey][storeID].length()
}

// GetStoreFollowerCount get the total count of a container's follower CachedShard
func (r *ShardsContainer) GetStoreFollowerCount(groupKey string, storeID uint64) int {
	r.maybeInitWithGroup(groupKey)
	return r.followers[groupKey][storeID].length()
}

// GetStoreLearnerCount get the total count of a container's learner CachedShard
func (r *ShardsContainer) GetStoreLearnerCount(groupKey string, storeID uint64) int {
	r.maybeInitWithGroup(groupKey)
	return r.learners[groupKey][storeID].length()
}

// RandPendingShards randomly gets a container's n shards with a pending peer.
func (r *ShardsContainer) RandPendingShards(groupKey string, storeID uint64, ranges []KeyRange, n int) []*CachedShard {
	r.maybeInitWithGroup(groupKey)
	return r.pendingReplicas[groupKey][storeID].RandomShards(n, ranges)
}

// RandLeaderShards randomly gets a container's n leader shards.
func (r *ShardsContainer) RandLeaderShards(groupKey string, storeID uint64, ranges []KeyRange, n int) []*CachedShard {
	r.maybeInitWithGroup(groupKey)
	return r.leaders[groupKey][storeID].RandomShards(n, ranges)
}

// RandFollowerShards randomly gets a container's n follower shards.
func (r *ShardsContainer) RandFollowerShards(groupKey string, storeID uint64, ranges []KeyRange, n int) []*CachedShard {
	r.maybeInitWithGroup(groupKey)
	return r.followers[groupKey][storeID].RandomShards(n, ranges)
}

// RandLearnerShards randomly gets a container's n learner shards.
func (r *ShardsContainer) RandLearnerShards(groupKey string, storeID uint64, ranges []KeyRange, n int) []*CachedShard {
	r.maybeInitWithGroup(groupKey)
	return r.learners[groupKey][storeID].RandomShards(n, ranges)
}

// GetLeader return leader CachedShard by storeID and shardID(now only used in test)
func (r *ShardsContainer) GetLeader(groupKey string, storeID uint64, res *CachedShard) *CachedShard {
	r.maybeInitWithGroup(groupKey)
	if leaders, ok := r.leaders[groupKey][storeID]; ok {
		return leaders.find(res).shard
	}
	return nil
}

// GetFollower return follower CachedShard by storeID and shardID(now only used in test)
func (r *ShardsContainer) GetFollower(groupKey string, storeID uint64, res *CachedShard) *CachedShard {
	r.maybeInitWithGroup(groupKey)
	if followers, ok := r.followers[groupKey][storeID]; ok {
		return followers.find(res).shard
	}
	return nil
}

// ScanRange scans shards intersecting [start key, end key), returns at most
// `limit` shards. limit <= 0 means no limit.
func (r *ShardsContainer) ScanRange(group uint64, startKey, endKey []byte, limit int) []*CachedShard {
	var shards []*CachedShard
	if tree, ok := r.trees[group]; ok {
		tree.scanRange(startKey, func(shard *CachedShard) bool {
			if len(endKey) > 0 && bytes.Compare(shard.GetStartKey(), endKey) >= 0 {
				return false
			}
			if limit > 0 && len(shards) >= limit {
				return false
			}
			shards = append(shards, r.GetShard(shard.Meta.GetID()))
			return true
		})
	}
	return shards
}

// GetDestroyingShards returns all shards in destroying state
func (r *ShardsContainer) GetDestroyingShards() []*CachedShard {
	var shards []*CachedShard
	for _, res := range r.shards.m {
		if res.Meta.GetState() == metapb.ShardState_Destroying {
			shards = append(shards, res)
		}
	}
	return shards
}

// ScanRangeWithIterator scans from the first shard containing or behind start key,
// until iterator returns false.
func (r *ShardsContainer) ScanRangeWithIterator(group uint64, startKey []byte, iterator func(res *CachedShard) bool) {
	if tree, ok := r.trees[group]; ok {
		tree.scanRange(startKey, iterator)
	}
}

// GetAdjacentShards returns shard's info that is adjacent with specific shard
func (r *ShardsContainer) GetAdjacentShards(res *CachedShard) (*CachedShard, *CachedShard) {
	var prev, next *CachedShard
	if tree, ok := r.trees[res.Meta.GetGroup()]; ok {
		p, n := tree.getAdjacentShards(res)
		// check key to avoid key range hole
		if p != nil && bytes.Equal(p.shard.GetEndKey(), res.GetStartKey()) {
			prev = r.GetShard(p.shard.Meta.GetID())
		}
		if n != nil && bytes.Equal(res.GetEndKey(), n.shard.GetStartKey()) {
			next = r.GetShard(n.shard.Meta.GetID())
		}
	}
	return prev, next
}

// GetAverageShardSize returns the average shard approximate size.
func (r *ShardsContainer) GetAverageShardSize() int64 {
	if r.shards.Len() == 0 {
		return 0
	}
	return r.shards.TotalSize() / int64(r.shards.Len())
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

// HexShardKey converts shard key to hex format. Used for formatting shard in
// logs.
func HexShardKey(key []byte) []byte {
	return ToUpperASCIIInplace(EncodeToString(key))
}

// ShardToHexMeta converts a shard meta's keys to hex format. Used for formatting
// shard in logs.
func ShardToHexMeta(meta metapb.Shard) HexShardMeta {
	start, end := meta.GetRange()
	meta.SetStartKey(HexShardKey(start))
	meta.SetEndKey(HexShardKey(end))
	return HexShardMeta{meta}
}

// HexShardMeta is a shard meta in the hex format. Used for formatting shard in logs.
type HexShardMeta struct {
	meta metapb.Shard
}

func (h HexShardMeta) String() string {
	return fmt.Sprintf("shard %+v", h.meta)
}
