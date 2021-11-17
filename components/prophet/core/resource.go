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
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
)

// errResourceIsStale is error info for resource is stale.
var errResourceIsStale = func(res metadata.Resource, origin metadata.Resource) error {
	return fmt.Errorf("resource is stale: resource %v, origin %v", res, origin)
}

// CachedResource resource runtime info cached in the cache
type CachedResource struct {
	Meta metadata.Resource

	term            uint64
	learners        []metapb.Replica
	voters          []metapb.Replica
	leader          *metapb.Replica
	downReplicas    []metapb.ReplicaStats
	pendingReplicas []metapb.Replica
	stats           metapb.ResourceStats
}

// NewCachedResource creates CachedResource with resource's meta and leader peer.
func NewCachedResource(res metadata.Resource, leader *metapb.Replica, opts ...ResourceCreateOption) *CachedResource {
	cr := &CachedResource{
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
func classifyVoterAndLearner(res *CachedResource) {
	learners := make([]metapb.Replica, 0, 1)
	voters := make([]metapb.Replica, 0, len(res.Meta.Peers()))
	for _, p := range res.Meta.Peers() {
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
	// EmptyResourceApproximateSize is the resource approximate size of an empty resource
	// (heartbeat size <= 1MB).
	EmptyResourceApproximateSize = 1

	// ImpossibleFlowSize is an impossible flow size (such as written_bytes, read_keys, etc.)
	// It may be caused by overflow, refer to https://github.com/tikv/pd/issues/3379.
	// They need to be filtered so as not to affect downstream.
	// (flow size >= 1024TB)
	ImpossibleFlowSize = 1 << 50
)

// ResourceFromHeartbeat constructs a Resource from resource heartbeat.
func ResourceFromHeartbeat(heartbeat rpcpb.ResourceHeartbeatReq, meta metadata.Resource) *CachedResource {
	// Convert unit to MB.
	// If resource is empty or less than 1MB, use 1MB instead.
	resourceSize := heartbeat.Stats.GetApproximateSize() / (1 << 20)
	if resourceSize < EmptyResourceApproximateSize {
		resourceSize = EmptyResourceApproximateSize
	}

	res := &CachedResource{
		Meta:            meta,
		term:            heartbeat.GetTerm(),
		leader:          heartbeat.GetLeader(),
		downReplicas:    heartbeat.GetDownReplicas(),
		pendingReplicas: heartbeat.GetPendingReplicas(),
		stats:           heartbeat.Stats,
	}

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

// Clone returns a copy of current CachedResource.
func (r *CachedResource) Clone(opts ...ResourceCreateOption) *CachedResource {
	downReplicas := make([]metapb.ReplicaStats, 0, len(r.downReplicas))
	for _, peer := range r.downReplicas {
		downReplicas = append(downReplicas, *(proto.Clone(&peer).(*metapb.ReplicaStats)))
	}
	pendingReplicas := make([]metapb.Replica, 0, len(r.pendingReplicas))
	for _, peer := range r.pendingReplicas {
		pendingReplicas = append(pendingReplicas, *(proto.Clone(&peer).(*metapb.Replica)))
	}

	res := &CachedResource{
		term:            r.term,
		Meta:            r.Meta.Clone(),
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

// IsDestroyState resource in Destroyed or Destroying state
func (r *CachedResource) IsDestroyState() bool {
	return r.Meta.State() == metapb.ResourceState_Destroyed ||
		r.Meta.State() == metapb.ResourceState_Destroying
}

// GetTerm returns the current term of the resource
func (r *CachedResource) GetTerm() uint64 {
	return r.term
}

// GetLearners returns the learners.
func (r *CachedResource) GetLearners() []metapb.Replica {
	return r.learners
}

// GetVoters returns the voters.
func (r *CachedResource) GetVoters() []metapb.Replica {
	return r.voters
}

// GetPeer returns the peer with specified peer id.
func (r *CachedResource) GetPeer(peerID uint64) (metapb.Replica, bool) {
	for _, peer := range r.Meta.Peers() {
		if peer.ID == peerID {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetDownPeer returns the down peer with specified peer id.
func (r *CachedResource) GetDownPeer(peerID uint64) (metapb.Replica, bool) {
	for _, down := range r.downReplicas {
		if down.Replica.ID == peerID {
			return down.Replica, true
		}
	}
	return metapb.Replica{}, false
}

// GetDownVoter returns the down voter with specified peer id.
func (r *CachedResource) GetDownVoter(peerID uint64) (metapb.Replica, bool) {
	for _, down := range r.downReplicas {
		if down.Replica.ID == peerID && !metadata.IsLearner(down.Replica) {
			return down.Replica, true
		}
	}
	return metapb.Replica{}, false
}

// GetDownLearner returns the down learner with soecified peer id.
func (r *CachedResource) GetDownLearner(peerID uint64) (metapb.Replica, bool) {
	for _, down := range r.downReplicas {
		if down.Replica.ID == peerID && metadata.IsLearner(down.Replica) {
			return down.Replica, true
		}
	}
	return metapb.Replica{}, false
}

// GetPendingPeer returns the pending peer with specified peer id.
func (r *CachedResource) GetPendingPeer(peerID uint64) (metapb.Replica, bool) {
	for _, peer := range r.pendingReplicas {
		if peer.ID == peerID {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetPendingVoter returns the pending voter with specified peer id.
func (r *CachedResource) GetPendingVoter(peerID uint64) (metapb.Replica, bool) {
	for _, peer := range r.pendingReplicas {
		if peer.ID == peerID && !metadata.IsLearner(peer) {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetPendingLearner returns the pending learner peer with specified peer id.
func (r *CachedResource) GetPendingLearner(peerID uint64) (metapb.Replica, bool) {
	for _, peer := range r.pendingReplicas {
		if peer.ID == peerID && metadata.IsLearner(peer) {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetContainerPeer returns the peer in specified container.
func (r *CachedResource) GetContainerPeer(containerID uint64) (metapb.Replica, bool) {
	for _, peer := range r.Meta.Peers() {
		if peer.ContainerID == containerID {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetContainerVoter returns the voter in specified container.
func (r *CachedResource) GetContainerVoter(containerID uint64) (metapb.Replica, bool) {
	for _, peer := range r.voters {
		if peer.ContainerID == containerID {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetContainerLearner returns the learner peer in specified container.
func (r *CachedResource) GetContainerLearner(containerID uint64) (metapb.Replica, bool) {
	for _, peer := range r.learners {
		if peer.ContainerID == containerID {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetContainerIDs returns a map indicate the resource distributed.
func (r *CachedResource) GetContainerIDs() map[uint64]struct{} {
	peers := r.Meta.Peers()
	containerIDs := make(map[uint64]struct{}, len(peers))
	for _, peer := range peers {
		containerIDs[peer.ContainerID] = struct{}{}
	}
	return containerIDs
}

// GetFollowers returns a map indicate the follow peers distributed.
func (r *CachedResource) GetFollowers() map[uint64]metapb.Replica {
	peers := r.GetVoters()
	followers := make(map[uint64]metapb.Replica, len(peers))
	for _, peer := range peers {
		if r.getLeaderID() != peer.ID {
			followers[peer.ContainerID] = peer
		}
	}
	return followers
}

// GetFollower randomly returns a follow peer.
func (r *CachedResource) GetFollower() (metapb.Replica, bool) {
	for _, peer := range r.GetVoters() {
		if r.getLeaderID() != peer.ID {
			return peer, true
		}
	}
	return metapb.Replica{}, false
}

// GetDiffFollowers returns the followers which is not located in the same
// container as any other followers of the another specified resource.
func (r *CachedResource) GetDiffFollowers(other *CachedResource) []metapb.Replica {
	res := make([]metapb.Replica, 0, len(r.Meta.Peers()))
	for _, p := range r.GetFollowers() {
		diff := true
		for _, o := range other.GetFollowers() {
			if p.ContainerID == o.ContainerID {
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
func (r *CachedResource) GetStat() *metapb.ResourceStats {
	if r == nil {
		return nil
	}
	stats := r.stats
	return &stats
}

// GetApproximateSize returns the approximate size of the resource.
func (r *CachedResource) GetApproximateSize() int64 {
	return int64(r.stats.ApproximateSize)
}

// GetApproximateKeys returns the approximate keys of the resource.
func (r *CachedResource) GetApproximateKeys() int64 {
	return int64(r.stats.ApproximateKeys)
}

// GetInterval returns the interval information of the resource.
func (r *CachedResource) GetInterval() *metapb.TimeInterval {
	return r.stats.Interval
}

// GetDownPeers returns the down peers of the resource.
func (r *CachedResource) GetDownPeers() []metapb.ReplicaStats {
	return r.downReplicas
}

// GetPendingPeers returns the pending peers of the resource.
func (r *CachedResource) GetPendingPeers() []metapb.Replica {
	return r.pendingReplicas
}

// GetBytesRead returns the read bytes of the resource.
func (r *CachedResource) GetBytesRead() uint64 {
	return r.stats.ReadBytes
}

// GetBytesWritten returns the written bytes of the resource.
func (r *CachedResource) GetBytesWritten() uint64 {
	return r.stats.WrittenBytes
}

// GetKeysWritten returns the written keys of the resource.
func (r *CachedResource) GetKeysWritten() uint64 {
	return r.stats.WrittenKeys
}

// GetKeysRead returns the read keys of the resource.
func (r *CachedResource) GetKeysRead() uint64 {
	return r.stats.ReadKeys
}

// GetLeader returns the leader of the resource.
func (r *CachedResource) GetLeader() *metapb.Replica {
	return r.leader
}

// GetStartKey returns the start key of the resource.
func (r *CachedResource) GetStartKey() []byte {
	v, _ := r.Meta.Range()
	return v
}

// GetEndKey returns the end key of the resource.
func (r *CachedResource) GetEndKey() []byte {
	_, v := r.Meta.Range()
	return v
}

func (r *CachedResource) getLeaderID() uint64 {
	if r.leader == nil {
		return 0
	}

	return r.leader.ID
}

// resourceMap wraps a map[uint64]*CachedResource and supports randomly pick a resource.
type resourceMap struct {
	m         map[uint64]*CachedResource
	totalSize int64
	totalKeys int64
}

func newResourceMap() *resourceMap {
	return &resourceMap{
		m: make(map[uint64]*CachedResource),
	}
}

func (rm *resourceMap) Len() int {
	if rm == nil {
		return 0
	}
	return len(rm.m)
}

func (rm *resourceMap) Get(id uint64) *CachedResource {
	if rm == nil {
		return nil
	}
	if r, ok := rm.m[id]; ok {
		return r
	}
	return nil
}

func (rm *resourceMap) Put(res *CachedResource) {
	if old, ok := rm.m[res.Meta.ID()]; ok {
		rm.totalSize -= int64(old.stats.ApproximateSize)
		rm.totalKeys -= int64(old.stats.ApproximateKeys)
	}
	rm.m[res.Meta.ID()] = res
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

func newResourceSubTree(factory func() metadata.Resource) *resourceSubTree {
	return &resourceSubTree{
		resourceTree: newResourceTree(factory),
		totalSize:    0,
	}
}

func (rst *resourceSubTree) TotalSize() int64 {
	if rst.length() == 0 {
		return 0
	}
	return rst.totalSize
}

func (rst *resourceSubTree) scanRanges() []*CachedResource {
	if rst.length() == 0 {
		return nil
	}
	var resources []*CachedResource
	rst.scanRange([]byte(""), func(resource *CachedResource) bool {
		resources = append(resources, resource)
		return true
	})
	return resources
}

func (rst *resourceSubTree) update(res *CachedResource) {
	overlaps := rst.resourceTree.update(res)
	rst.totalSize += int64(res.stats.ApproximateSize)
	rst.totalKeys += int64(res.stats.ApproximateKeys)
	for _, r := range overlaps {
		rst.totalSize -= int64(r.stats.ApproximateSize)
		rst.totalKeys -= int64(r.stats.ApproximateKeys)
	}
}

func (rst *resourceSubTree) remove(res *CachedResource) {
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

func (rst *resourceSubTree) RandomResource(ranges []KeyRange) *CachedResource {
	if rst.length() == 0 {
		return nil
	}

	return rst.resourceTree.RandomResource(ranges)
}

func (rst *resourceSubTree) RandomResources(n int, ranges []KeyRange) []*CachedResource {
	if rst.length() == 0 {
		return nil
	}

	resources := make([]*CachedResource, 0, n)

	for i := 0; i < n; i++ {
		if resource := rst.resourceTree.RandomResource(ranges); resource != nil {
			resources = append(resources, resource)
		}
	}
	return resources
}

// CachedResources for export
type CachedResources struct {
	factory         func() metadata.Resource
	trees           map[uint64]*resourceTree
	resources       *resourceMap                // resourceID -> CachedResource
	leaders         map[uint64]*resourceSubTree // containerID -> resourceSubTree
	followers       map[uint64]*resourceSubTree // containerID -> resourceSubTree
	learners        map[uint64]*resourceSubTree // containerID -> resourceSubTree
	pendingReplicas map[uint64]*resourceSubTree // containerID -> resourceSubTree
}

// NewCachedResources creates CachedResources with tree, resources, leaders and followers
func NewCachedResources(factory func() metadata.Resource) *CachedResources {
	return &CachedResources{
		factory:         factory,
		trees:           make(map[uint64]*resourceTree),
		resources:       newResourceMap(),
		leaders:         make(map[uint64]*resourceSubTree),
		followers:       make(map[uint64]*resourceSubTree),
		learners:        make(map[uint64]*resourceSubTree),
		pendingReplicas: make(map[uint64]*resourceSubTree),
	}
}

// ForeachResources foreach resource by group
func (r *CachedResources) ForeachResources(group uint64, fn func(res metadata.Resource)) {
	for _, res := range r.resources.m {
		if res.Meta.Group() == group {
			fn(res.Meta)
		}
	}
}

// GetResource returns the CachedResource with resourceID
func (r *CachedResources) GetResource(resourceID uint64) *CachedResource {
	res := r.resources.Get(resourceID)
	if res == nil {
		return nil
	}
	return res
}

// SetResource sets the CachedResource with resourceID
func (r *CachedResources) SetResource(res *CachedResource) []*CachedResource {
	if origin := r.resources.Get(res.Meta.ID()); origin != nil {
		if !bytes.Equal(origin.GetStartKey(), res.GetStartKey()) || !bytes.Equal(origin.GetEndKey(), res.GetEndKey()) {
			r.removeResourceFromTreeAndMap(origin)
		}
		if r.shouldRemoveFromSubTree(res, origin) {
			r.removeResourceFromSubTree(origin)
		}
	}
	return r.AddResource(res)
}

// Length returns the resourcesInfo length
func (r *CachedResources) Length() int {
	return r.resources.Len()
}

// GetOverlaps returns the resources which are overlapped with the specified resource range.
func (r *CachedResources) GetOverlaps(res *CachedResource) []*CachedResource {
	if tree, ok := r.trees[res.Meta.Group()]; ok {
		return tree.getOverlaps(res)
	}

	return nil
}

// AddResource adds CachedResource to resourceTree and resourceMap, also update leaders and followers by resource peers
func (r *CachedResources) AddResource(res *CachedResource) []*CachedResource {
	if _, ok := r.trees[res.Meta.Group()]; !ok {
		r.trees[res.Meta.Group()] = newResourceTree(r.factory)
	}

	tree := r.trees[res.Meta.Group()]

	// the resources which are overlapped with the specified resource range.
	var overlaps []*CachedResource
	// when the value is true, add the resource to the tree. otherwise use the resource replace the origin resource in the tree.
	treeNeedAdd := true
	if origin := r.GetResource(res.Meta.ID()); origin != nil {
		if resOld := tree.find(res); resOld != nil {
			// Update to tree.
			if bytes.Equal(resOld.res.GetStartKey(), res.GetStartKey()) &&
				bytes.Equal(resOld.res.GetEndKey(), res.GetEndKey()) &&
				resOld.res.Meta.ID() == res.Meta.ID() {
				resOld.res = res
				treeNeedAdd = false
			}
		}
	}
	if treeNeedAdd {
		// Add to tree.
		overlaps = tree.update(res)
		for _, item := range overlaps {
			r.RemoveResource(r.GetResource(item.Meta.ID()))
		}
	}
	// Add to resources.
	r.resources.Put(res)

	// Add to leaders and followers.
	for _, peer := range res.GetVoters() {
		containerID := peer.ContainerID
		if peer.ID == res.getLeaderID() {
			// Add leader peer to leaders.
			container, ok := r.leaders[containerID]
			if !ok {
				container = newResourceSubTree(r.factory)
				r.leaders[containerID] = container
			}
			container.update(res)
		} else {
			// Add follower peer to followers.
			container, ok := r.followers[containerID]
			if !ok {
				container = newResourceSubTree(r.factory)
				r.followers[containerID] = container
			}
			container.update(res)
		}
	}

	// Add to learners.
	for _, peer := range res.GetLearners() {
		containerID := peer.ContainerID
		container, ok := r.learners[containerID]
		if !ok {
			container = newResourceSubTree(r.factory)
			r.learners[containerID] = container
		}
		container.update(res)
	}

	for _, peer := range res.pendingReplicas {
		containerID := peer.ContainerID
		container, ok := r.pendingReplicas[containerID]
		if !ok {
			container = newResourceSubTree(r.factory)
			r.pendingReplicas[containerID] = container
		}
		container.update(res)
	}

	return overlaps
}

// RemoveResource removes CachedResource from resourceTree and resourceMap
func (r *CachedResources) RemoveResource(res *CachedResource) {
	// Remove from tree and resources.
	r.removeResourceFromTreeAndMap(res)
	// Remove from leaders and followers.
	r.removeResourceFromSubTree(res)
}

// removeResourceFromTreeAndMap removes CachedResource from resourceTree and resourceMap
func (r *CachedResources) removeResourceFromTreeAndMap(res *CachedResource) {
	// Remove from tree and resources.
	if tree, ok := r.trees[res.Meta.Group()]; ok {
		tree.remove(res)
	}
	r.resources.Delete(res.Meta.ID())
}

// removeResourceFromSubTree removes CachedResource from resourcesubTrees
func (r *CachedResources) removeResourceFromSubTree(res *CachedResource) {
	// Remove from leaders and followers.
	for _, peer := range res.Meta.Peers() {
		containerID := peer.ContainerID
		r.leaders[containerID].remove(res)
		r.followers[containerID].remove(res)
		r.learners[containerID].remove(res)
		r.pendingReplicas[containerID].remove(res)
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
		if peerStats.GetReplica().ID != peersB[i].GetReplica().ID {
			return false
		}
	}
	return true
}

// shouldRemoveFromSubTree return true when the resource leader changed, peer transferred,
// new peer was created, learners changed, pendingReplicas changed, and so on.
func (r *CachedResources) shouldRemoveFromSubTree(res *CachedResource, origin *CachedResource) bool {
	checkPeersChange := func(origin []metapb.Replica, other []metapb.Replica) bool {
		if len(origin) != len(other) {
			return true
		}
		sort.Sort(peerSlice(origin))
		sort.Sort(peerSlice(other))
		for index, peer := range origin {
			if peer.ContainerID == other[index].ContainerID && peer.ID == other[index].ID {
				continue
			}
			return true
		}
		return false
	}

	return origin.getLeaderID() != res.getLeaderID() ||
		checkPeersChange(origin.GetVoters(), res.GetVoters()) ||
		checkPeersChange(origin.GetLearners(), res.GetLearners()) ||
		checkPeersChange(origin.GetPendingPeers(), res.GetPendingPeers())
}

// SearchResource searches CachedResource from resourceTree
func (r *CachedResources) SearchResource(group uint64, resKey []byte) *CachedResource {
	if tree, ok := r.trees[group]; ok {
		res := tree.search(resKey)
		if res == nil {
			return nil
		}
		return r.GetResource(res.Meta.ID())
	}

	return nil
}

// SearchPrevResource searches previous CachedResource from resourceTree
func (r *CachedResources) SearchPrevResource(group uint64, resKey []byte) *CachedResource {
	if tree, ok := r.trees[group]; ok {
		res := tree.searchPrev(resKey)
		if res == nil {
			return nil
		}
		return r.GetResource(res.Meta.ID())
	}

	return nil
}

// GetResources gets all CachedResource from resourceMap
func (r *CachedResources) GetResources() []*CachedResource {
	resources := make([]*CachedResource, 0, r.resources.Len())
	for _, res := range r.resources.m {
		resources = append(resources, res)
	}
	return resources
}

// GetContainerResources gets all CachedResource with a given containerID
func (r *CachedResources) GetContainerResources(containerID uint64) []*CachedResource {
	resources := make([]*CachedResource, 0, r.GetContainerResourceCount(containerID))
	if leaders, ok := r.leaders[containerID]; ok {
		resources = append(resources, leaders.scanRanges()...)
	}
	if followers, ok := r.followers[containerID]; ok {
		resources = append(resources, followers.scanRanges()...)
	}
	if learners, ok := r.learners[containerID]; ok {
		resources = append(resources, learners.scanRanges()...)
	}
	return resources
}

// GetContainerLeaderResourceSize get total size of container's leader resources
func (r *CachedResources) GetContainerLeaderResourceSize(containerID uint64) int64 {
	return r.leaders[containerID].TotalSize()
}

// GetContainerFollowerResourceSize get total size of container's follower resources
func (r *CachedResources) GetContainerFollowerResourceSize(containerID uint64) int64 {
	return r.followers[containerID].TotalSize()
}

// GetContainerLearnerResourceSize get total size of container's learner resources
func (r *CachedResources) GetContainerLearnerResourceSize(containerID uint64) int64 {
	return r.learners[containerID].TotalSize()
}

// GetContainerResourceSize get total size of container's resources
func (r *CachedResources) GetContainerResourceSize(containerID uint64) int64 {
	return r.GetContainerLeaderResourceSize(containerID) + r.GetContainerFollowerResourceSize(containerID) + r.GetContainerLearnerResourceSize(containerID)
}

// GetMetaResources gets a set of metadata.Resource from resourceMap
func (r *CachedResources) GetMetaResources() []metadata.Resource {
	resources := make([]metadata.Resource, 0, r.resources.Len())
	for _, res := range r.resources.m {
		resources = append(resources, res.Meta.Clone())
	}
	return resources
}

// GetResourceCount gets the total count of CachedResource of resourceMap
func (r *CachedResources) GetResourceCount() int {
	return r.resources.Len()
}

// GetContainerResourceCount gets the total count of a container's leader, follower and learner CachedResource by containerID
func (r *CachedResources) GetContainerResourceCount(containerID uint64) int {
	return r.GetContainerLeaderCount(containerID) + r.GetContainerFollowerCount(containerID) + r.GetContainerLearnerCount(containerID)
}

// GetContainerPendingPeerCount gets the total count of a container's resource that includes pending peer
func (r *CachedResources) GetContainerPendingPeerCount(containerID uint64) int {
	return r.pendingReplicas[containerID].length()
}

// GetContainerLeaderCount get the total count of a container's leader CachedResource
func (r *CachedResources) GetContainerLeaderCount(containerID uint64) int {
	return r.leaders[containerID].length()
}

// GetContainerFollowerCount get the total count of a container's follower CachedResource
func (r *CachedResources) GetContainerFollowerCount(containerID uint64) int {
	return r.followers[containerID].length()
}

// GetContainerLearnerCount get the total count of a container's learner CachedResource
func (r *CachedResources) GetContainerLearnerCount(containerID uint64) int {
	return r.learners[containerID].length()
}

// RandPendingResource randomly gets a container's resource with a pending peer.
func (r *CachedResources) RandPendingResource(containerID uint64, ranges []KeyRange) *CachedResource {
	return r.pendingReplicas[containerID].RandomResource(ranges)
}

// RandPendingResources randomly gets a container's n resources with a pending peer.
func (r *CachedResources) RandPendingResources(containerID uint64, ranges []KeyRange, n int) []*CachedResource {
	return r.pendingReplicas[containerID].RandomResources(n, ranges)
}

// RandLeaderResource randomly gets a container's leader resource.
func (r *CachedResources) RandLeaderResource(containerID uint64, ranges []KeyRange) *CachedResource {
	return r.leaders[containerID].RandomResource(ranges)
}

// RandLeaderResources randomly gets a container's n leader resources.
func (r *CachedResources) RandLeaderResources(containerID uint64, ranges []KeyRange, n int) []*CachedResource {
	return r.leaders[containerID].RandomResources(n, ranges)
}

// RandFollowerResource randomly gets a container's follower resource.
func (r *CachedResources) RandFollowerResource(containerID uint64, ranges []KeyRange) *CachedResource {
	return r.followers[containerID].RandomResource(ranges)
}

// RandFollowerResources randomly gets a container's n follower resources.
func (r *CachedResources) RandFollowerResources(containerID uint64, ranges []KeyRange, n int) []*CachedResource {
	return r.followers[containerID].RandomResources(n, ranges)
}

// RandLearnerResource randomly gets a container's learner resource.
func (r *CachedResources) RandLearnerResource(containerID uint64, ranges []KeyRange) *CachedResource {
	return r.learners[containerID].RandomResource(ranges)
}

// RandLearnerResources randomly gets a container's n learner resources.
func (r *CachedResources) RandLearnerResources(containerID uint64, ranges []KeyRange, n int) []*CachedResource {
	return r.learners[containerID].RandomResources(n, ranges)
}

// GetLeader return leader CachedResource by containerID and resourceID(now only used in test)
func (r *CachedResources) GetLeader(containerID uint64, res *CachedResource) *CachedResource {
	if leaders, ok := r.leaders[containerID]; ok {
		return leaders.find(res).res
	}
	return nil
}

// GetFollower return follower CachedResource by containerID and resourceID(now only used in test)
func (r *CachedResources) GetFollower(containerID uint64, res *CachedResource) *CachedResource {
	if followers, ok := r.followers[containerID]; ok {
		return followers.find(res).res
	}
	return nil
}

// ScanRange scans resources intersecting [start key, end key), returns at most
// `limit` resources. limit <= 0 means no limit.
func (r *CachedResources) ScanRange(group uint64, startKey, endKey []byte, limit int) []*CachedResource {
	var resources []*CachedResource
	if tree, ok := r.trees[group]; ok {
		tree.scanRange(startKey, func(resource *CachedResource) bool {
			if len(endKey) > 0 && bytes.Compare(resource.GetStartKey(), endKey) >= 0 {
				return false
			}
			if limit > 0 && len(resources) >= limit {
				return false
			}
			resources = append(resources, r.GetResource(resource.Meta.ID()))
			return true
		})
	}
	return resources
}

// ScanRangeWithIterator scans from the first resource containing or behind start key,
// until iterator returns false.
func (r *CachedResources) ScanRangeWithIterator(group uint64, startKey []byte, iterator func(res *CachedResource) bool) {
	if tree, ok := r.trees[group]; ok {
		tree.scanRange(startKey, iterator)
	}
}

// GetAdjacentResources returns resource's info that is adjacent with specific resource
func (r *CachedResources) GetAdjacentResources(res *CachedResource) (*CachedResource, *CachedResource) {
	var prev, next *CachedResource
	if tree, ok := r.trees[res.Meta.Group()]; ok {
		p, n := tree.getAdjacentResources(res)
		// check key to avoid key range hole
		if p != nil && bytes.Equal(p.res.GetEndKey(), res.GetStartKey()) {
			prev = r.GetResource(p.res.Meta.ID())
		}
		if n != nil && bytes.Equal(res.GetEndKey(), n.res.GetStartKey()) {
			next = r.GetResource(n.res.Meta.ID())
		}
	}
	return prev, next
}

// GetAverageResourceSize returns the average resource approximate size.
func (r *CachedResources) GetAverageResourceSize() int64 {
	if r.resources.Len() == 0 {
		return 0
	}
	return r.resources.TotalSize() / int64(r.resources.Len())
}

// DiffResourcePeersInfo return the difference of peers info  between two CachedResource
func DiffResourcePeersInfo(origin *CachedResource, other *CachedResource) string {
	var ret []string
	for _, a := range origin.Meta.Peers() {
		both := false
		for _, b := range other.Meta.Peers() {
			if reflect.DeepEqual(a, b) {
				both = true
				break
			}
		}
		if !both {
			ret = append(ret, fmt.Sprintf("Remove peer:{%v}", a))
		}
	}
	for _, b := range other.Meta.Peers() {
		both := false
		for _, a := range origin.Meta.Peers() {
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

// DiffResourceKeyInfo return the difference of key info between two CachedResource
func DiffResourceKeyInfo(origin *CachedResource, other *CachedResource) string {
	originStartKey, originEndKey := origin.Meta.Range()
	otherStartKey, otherEndKey := other.Meta.Range()

	var ret []string
	if !bytes.Equal(originStartKey, otherStartKey) {
		ret = append(ret, fmt.Sprintf("StartKey Changed:{%s} -> {%s}", HexResourceKey(originStartKey), HexResourceKey(otherStartKey)))
	} else {
		ret = append(ret, fmt.Sprintf("StartKey:{%s}", HexResourceKey(originStartKey)))
	}
	if !bytes.Equal(originEndKey, otherEndKey) {
		ret = append(ret, fmt.Sprintf("EndKey Changed:{%s} -> {%s}", HexResourceKey(originEndKey), HexResourceKey(otherEndKey)))
	} else {
		ret = append(ret, fmt.Sprintf("EndKey:{%s}", HexResourceKey(originEndKey)))
	}

	return strings.Join(ret, ", ")
}

func isInvolved(res *CachedResource, startKey, endKey []byte) bool {
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

// HexResourceKey converts resource key to hex format. Used for formating resource in
// logs.
func HexResourceKey(key []byte) []byte {
	return ToUpperASCIIInplace(EncodeToString(key))
}

// HexResourceKeyStr converts resource key to hex format. Used for formating resource in
// logs.
func HexResourceKeyStr(key []byte) string {
	return String(HexResourceKey(key))
}

// ResourceToHexMeta converts a resource meta's keys to hex format. Used for formating
// resource in logs.
func ResourceToHexMeta(meta metadata.Resource) HexResourceMeta {
	if meta == nil {
		return HexResourceMeta{}
	}
	meta = meta.Clone()
	start, end := meta.Range()
	meta.SetStartKey(HexResourceKey(start))
	meta.SetEndKey(HexResourceKey(end))
	return HexResourceMeta{meta}
}

// HexResourceMeta is a resource meta in the hex format. Used for formating resource in logs.
type HexResourceMeta struct {
	meta metadata.Resource
}

func (h HexResourceMeta) String() string {
	return fmt.Sprintf("resource %+v", h.meta)
}

// ResourcesToHexMeta converts resources' meta keys to hex format. Used for formating
// resource in logs.
func ResourcesToHexMeta(resources []metadata.Resource) HexResourcesMeta {
	hexResourceMetas := make([]metadata.Resource, len(resources))
	for i, res := range resources {
		meta := res.Clone()
		start, end := meta.Range()
		meta.SetStartKey(HexResourceKey(start))
		meta.SetEndKey(HexResourceKey(end))
		hexResourceMetas[i] = meta
	}
	return hexResourceMetas
}

// HexResourcesMeta is a slice of resources' meta in the hex format. Used for formating
// resource in logs.
type HexResourcesMeta []metadata.Resource

func (h HexResourcesMeta) String() string {
	var b strings.Builder
	for _, r := range h {
		b.WriteString(fmt.Sprintf("resource %+v", r))
	}
	return strings.TrimSpace(b.String())
}
