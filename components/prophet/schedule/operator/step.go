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

package operator

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
)

// OpStep describes the basic scheduling steps that can not be subdivided.
type OpStep interface {
	fmt.Stringer
	ConfVerChanged(res *core.CachedShard) uint64
	IsFinish(res *core.CachedShard) bool
	CheckSafety(res *core.CachedShard) error
	Influence(opInfluence OpInfluence, res *core.CachedShard)
}

// TransferLeader is an OpStep that transfers a resource's leader.
type TransferLeader struct {
	FromStore, ToStore uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (tl TransferLeader) ConfVerChanged(res *core.CachedShard) uint64 {
	return 0 // transfer leader never change the conf version
}

func (tl TransferLeader) String() string {
	return fmt.Sprintf("transfer leader from container %v to container %v", tl.FromStore, tl.ToStore)
}

// IsFinish checks if current step is finished.
func (tl TransferLeader) IsFinish(res *core.CachedShard) bool {
	return res.GetLeader().GetStoreID() == tl.ToStore
}

// CheckSafety checks if the step meets the safety properties.
func (tl TransferLeader) CheckSafety(res *core.CachedShard) error {
	peer, ok := res.GetStorePeer(tl.ToStore)
	if !ok {
		return errors.New("peer does not existed")
	}
	if metadata.IsLearner(peer) {
		return errors.New("peer already is a learner")
	}
	return nil
}

// Influence calculates the container difference that current step makes.
func (tl TransferLeader) Influence(opInfluence OpInfluence, res *core.CachedShard) {
	from := opInfluence.GetStoreInfluence(tl.FromStore)
	to := opInfluence.GetStoreInfluence(tl.ToStore)

	groupKey := res.GetGroupKey()

	fromStats := from.InfluenceStats[groupKey]
	fromStats.LeaderSize -= res.GetApproximateSize()
	fromStats.LeaderCount--
	from.InfluenceStats[groupKey] = fromStats

	toStats := to.InfluenceStats[groupKey]
	toStats.LeaderSize += res.GetApproximateSize()
	toStats.LeaderCount++
	to.InfluenceStats[groupKey] = toStats
}

// AddPeer is an OpStep that adds a resource peer.
type AddPeer struct {
	ToStore, PeerID uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (ap AddPeer) ConfVerChanged(res *core.CachedShard) uint64 {
	peer, _ := res.GetStoreVoter(ap.ToStore)
	return typeutil.BoolToUint64(peer.ID == ap.PeerID)
}

func (ap AddPeer) String() string {
	return fmt.Sprintf("add peer %v on container %v", ap.PeerID, ap.ToStore)
}

// IsFinish checks if current step is finished.
func (ap AddPeer) IsFinish(res *core.CachedShard) bool {
	if peer, ok := res.GetStoreVoter(ap.ToStore); ok {
		if peer.ID != ap.PeerID {
			return false
		}
		_, ok := res.GetPendingVoter(peer.ID)
		return !ok
	}
	return false
}

// Influence calculates the container difference that current step makes.
func (ap AddPeer) Influence(opInfluence OpInfluence, res *core.CachedShard) {
	to := opInfluence.GetStoreInfluence(ap.ToStore)

	size := res.GetApproximateSize()

	groupKey := res.GetGroupKey()
	stats := to.InfluenceStats[groupKey]
	stats.ShardSize += size
	stats.ShardCount++
	to.InfluenceStats[groupKey] = stats

	to.AdjustStepCost(limit.AddPeer, size)
}

// CheckSafety checks if the step meets the safety properties.
func (ap AddPeer) CheckSafety(res *core.CachedShard) error {
	peer, ok := res.GetStorePeer(ap.ToStore)
	if ok && peer.ID != ap.PeerID {
		return fmt.Errorf("peer %d has already existed in container %d, the operator is trying to add peer %d on the same container", peer.ID, ap.ToStore, ap.PeerID)
	}
	return nil
}

// AddLearner is an OpStep that adds a resource learner peer.
type AddLearner struct {
	ToStore, PeerID uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (al AddLearner) ConfVerChanged(res *core.CachedShard) uint64 {
	peer, _ := res.GetStorePeer(al.ToStore)
	return typeutil.BoolToUint64(peer.ID == al.PeerID)
}

func (al AddLearner) String() string {
	return fmt.Sprintf("add learner peer %v on container %v", al.PeerID, al.ToStore)
}

// IsFinish checks if current step is finished.
func (al AddLearner) IsFinish(res *core.CachedShard) bool {
	if peer, ok := res.GetStoreLearner(al.ToStore); ok {
		if peer.ID != al.PeerID {
			return false
		}
		_, ok := res.GetPendingLearner(peer.ID)
		return !ok
	}
	return false
}

// CheckSafety checks if the step meets the safety properties.
func (al AddLearner) CheckSafety(res *core.CachedShard) error {
	peer, ok := res.GetStorePeer(al.ToStore)
	if !ok {
		return nil
	}
	if peer.ID != al.PeerID {
		return fmt.Errorf("peer %d has already existed in container %d, the operator is trying to add peer %d on the same container", peer.ID, al.ToStore, al.PeerID)
	}
	if !metadata.IsLearner(peer) {
		return errors.New("peer already is a voter")
	}
	return nil
}

// Influence calculates the container difference that current step makes.
func (al AddLearner) Influence(opInfluence OpInfluence, res *core.CachedShard) {
	to := opInfluence.GetStoreInfluence(al.ToStore)

	size := res.GetApproximateSize()
	groupKey := res.GetGroupKey()
	stats := to.InfluenceStats[groupKey]
	stats.ShardSize += size
	stats.ShardCount++
	to.InfluenceStats[groupKey] = stats

	to.AdjustStepCost(limit.AddPeer, size)
}

// PromoteLearner is an OpStep that promotes a resource learner peer to normal voter.
type PromoteLearner struct {
	ToStore, PeerID uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (pl PromoteLearner) ConfVerChanged(res *core.CachedShard) uint64 {
	peer, _ := res.GetStoreVoter(pl.ToStore)
	return typeutil.BoolToUint64(peer.ID == pl.PeerID)
}

func (pl PromoteLearner) String() string {
	return fmt.Sprintf("promote learner peer %v on container %v to voter", pl.PeerID, pl.ToStore)
}

// IsFinish checks if current step is finished.
func (pl PromoteLearner) IsFinish(res *core.CachedShard) bool {
	if peer, ok := res.GetStoreVoter(pl.ToStore); ok {
		return peer.ID == pl.PeerID
	}
	return false
}

// CheckSafety checks if the step meets the safety properties.
func (pl PromoteLearner) CheckSafety(res *core.CachedShard) error {
	peer, _ := res.GetStorePeer(pl.ToStore)
	if peer.ID != pl.PeerID {
		return errors.New("peer does not exist")
	}
	return nil
}

// Influence calculates the container difference that current step makes.
func (pl PromoteLearner) Influence(opInfluence OpInfluence, res *core.CachedShard) {}

// RemovePeer is an OpStep that removes a resource peer.
type RemovePeer struct {
	FromStore, PeerID uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (rp RemovePeer) ConfVerChanged(res *core.CachedShard) uint64 {
	p, _ := res.GetStorePeer(rp.FromStore)
	id := p.ID
	// 1. id == 0 -> The peer does not exist, it needs to return 1.
	// 2. id != 0 && rp.PeerId == 0 -> No rp.PeerID is specified, and there is a Peer on the container, it needs to return 0.
	// 3. id != 0 && rp.PeerID != 0 && id == rp.PeerID -> The peer still exists, it needs to return 0.
	// 4. id != 0 && rp.PeerID != 0 && id != rp.PeerID -> The rp.PeerID is specified,
	//     and although there is a Peer on the container, but the Id has changed, it should return 1.
	//     This is for the following case:
	//     If DemoteFollower step is not allowed, it will be split into RemovePeer and AddLearner.
	//     After the AddLearner step, ConfVerChanged of RemovePeer should still return 1.
	return typeutil.BoolToUint64(id == 0 || (rp.PeerID != 0 && id != rp.PeerID))
}

func (rp RemovePeer) String() string {
	return fmt.Sprintf("remove peer on container %v", rp.FromStore)
}

// IsFinish checks if current step is finished.
func (rp RemovePeer) IsFinish(res *core.CachedShard) bool {
	_, ok := res.GetStorePeer(rp.FromStore)
	return !ok
}

// CheckSafety checks if the step meets the safety properties.
func (rp RemovePeer) CheckSafety(res *core.CachedShard) error {
	if rp.FromStore == res.GetLeader().GetStoreID() {
		return errors.New("cannot remove leader peer")
	}
	return nil
}

// Influence calculates the container difference that current step makes.
func (rp RemovePeer) Influence(opInfluence OpInfluence, res *core.CachedShard) {
	from := opInfluence.GetStoreInfluence(rp.FromStore)

	size := res.GetApproximateSize()
	groupKey := res.GetGroupKey()
	stats := from.InfluenceStats[groupKey]
	stats.ShardSize -= size
	stats.ShardCount--
	from.InfluenceStats[groupKey] = stats

	from.AdjustStepCost(limit.RemovePeer, size)
}

// MergeShard is an OpStep that merge two resources.
type MergeShard struct {
	FromShard metapb.Shard
	ToShard   metapb.Shard
	// there are two resources involved in merge process,
	// so to keep them from other scheduler,
	// both of them should add Merresource operatorStep.
	// But actually, your storage application just needs the resource want to be merged to get the merge request,
	// thus use a IsPassive mark to indicate that
	// this resource doesn't need to send merge request to your storage application.
	IsPassive bool
}

// ConfVerChanged returns the delta value for version increased by this step.
func (mr MergeShard) ConfVerChanged(res *core.CachedShard) uint64 {
	return 0
}

func (mr MergeShard) String() string {
	return fmt.Sprintf("merge resource %v into resource %v", mr.FromShard.GetID(), mr.ToShard.GetID())
}

// IsFinish checks if current step is finished.
func (mr MergeShard) IsFinish(res *core.CachedShard) bool {
	if mr.IsPassive {
		start, end := res.Meta.GetRange()
		toStart, toEnd := mr.ToShard.GetRange()
		return !bytes.Equal(start, toStart) || !bytes.Equal(end, toEnd)
	}
	return false
}

// CheckSafety checks if the step meets the safety properties.
func (mr MergeShard) CheckSafety(res *core.CachedShard) error {
	return nil
}

// Influence calculates the container difference that current step makes.
func (mr MergeShard) Influence(opInfluence OpInfluence, res *core.CachedShard) {
	if mr.IsPassive {
		for _, peer := range res.Meta.GetReplicas() {
			o := opInfluence.GetStoreInfluence(peer.StoreID)

			groupKey := res.GetGroupKey()
			stats := o.InfluenceStats[groupKey]
			stats.ShardCount--
			if res.GetLeader().GetID() == peer.ID {
				stats.LeaderCount--
			}

			o.InfluenceStats[groupKey] = stats
		}
	}
}

// SplitShard is an OpStep that splits a resource.
type SplitShard struct {
	StartKey, EndKey []byte
	Policy           metapb.CheckPolicy
	SplitKeys        [][]byte
}

// ConfVerChanged returns the delta value for version increased by this step.
func (sr SplitShard) ConfVerChanged(res *core.CachedShard) uint64 {
	return 0
}

func (sr SplitShard) String() string {
	return fmt.Sprintf("split resource with policy %s", sr.Policy.String())
}

// IsFinish checks if current step is finished.
func (sr SplitShard) IsFinish(res *core.CachedShard) bool {
	start, end := res.Meta.GetRange()
	return !bytes.Equal(start, sr.StartKey) || !bytes.Equal(end, sr.EndKey)
}

// Influence calculates the container difference that current step makes.
func (sr SplitShard) Influence(opInfluence OpInfluence, res *core.CachedShard) {
	for _, peer := range res.Meta.GetReplicas() {
		inf := opInfluence.GetStoreInfluence(peer.StoreID)

		groupKey := res.GetGroupKey()
		stats := inf.InfluenceStats[groupKey]
		stats.ShardCount++
		if res.GetLeader().GetID() == peer.ID {
			stats.LeaderCount++
		}
		inf.InfluenceStats[groupKey] = stats
	}
}

// CheckSafety checks if the step meets the safety properties.
func (sr SplitShard) CheckSafety(res *core.CachedShard) error {
	return nil
}

// AddLightPeer is an OpStep that adds a resource peer without considering the influence.
type AddLightPeer struct {
	ToStore, PeerID uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (ap AddLightPeer) ConfVerChanged(res *core.CachedShard) uint64 {
	peer, _ := res.GetStoreVoter(ap.ToStore)
	return typeutil.BoolToUint64(peer.ID == ap.PeerID)
}

func (ap AddLightPeer) String() string {
	return fmt.Sprintf("add peer %v on container %v", ap.PeerID, ap.ToStore)
}

// IsFinish checks if current step is finished.
func (ap AddLightPeer) IsFinish(res *core.CachedShard) bool {
	if peer, ok := res.GetStoreVoter(ap.ToStore); ok {
		if peer.ID != ap.PeerID {
			return false
		}
		_, ok := res.GetPendingVoter(peer.ID)
		return !ok
	}
	return false
}

// CheckSafety checks if the step meets the safety properties.
func (ap AddLightPeer) CheckSafety(res *core.CachedShard) error {
	peer, ok := res.GetStorePeer(ap.ToStore)
	if ok && peer.ID != ap.PeerID {
		return fmt.Errorf("peer %d has already existed in container %d, the operator is trying to add peer %d on the same container", peer.ID, ap.ToStore, ap.PeerID)
	}
	return nil
}

// Influence calculates the container difference that current step makes.
func (ap AddLightPeer) Influence(opInfluence OpInfluence, res *core.CachedShard) {
	to := opInfluence.GetStoreInfluence(ap.ToStore)

	groupKey := res.GetGroupKey()
	stats := to.InfluenceStats[groupKey]
	stats.ShardSize += res.GetApproximateSize()
	stats.ShardCount++
	to.InfluenceStats[groupKey] = stats
}

// AddLightLearner is an OpStep that adds a resource learner peer without considering the influence.
type AddLightLearner struct {
	ToStore, PeerID uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (al AddLightLearner) ConfVerChanged(res *core.CachedShard) uint64 {
	peer, _ := res.GetStorePeer(al.ToStore)
	return typeutil.BoolToUint64(peer.ID == al.PeerID)
}

func (al AddLightLearner) String() string {
	return fmt.Sprintf("add learner peer %v on container %v", al.PeerID, al.ToStore)
}

// IsFinish checks if current step is finished.
func (al AddLightLearner) IsFinish(res *core.CachedShard) bool {
	if peer, ok := res.GetStoreLearner(al.ToStore); ok {
		if peer.ID != al.PeerID {
			return false
		}
		_, ok := res.GetPendingLearner(peer.ID)
		return !ok
	}
	return false
}

// CheckSafety checks if the step meets the safety properties.
func (al AddLightLearner) CheckSafety(res *core.CachedShard) error {
	peer, ok := res.GetStorePeer(al.ToStore)
	if !ok {
		return nil
	}
	if peer.ID != al.PeerID {
		return fmt.Errorf("peer %d has already existed in container %d, the operator is trying to add peer %d on the same container", peer.ID, al.ToStore, al.PeerID)
	}
	if !metadata.IsLearner(peer) {
		return errors.New("peer already is a voter")
	}
	return nil
}

// Influence calculates the container difference that current step makes.
func (al AddLightLearner) Influence(opInfluence OpInfluence, res *core.CachedShard) {
	to := opInfluence.GetStoreInfluence(al.ToStore)

	groupKey := res.GetGroupKey()
	stats := to.InfluenceStats[groupKey]
	stats.ShardSize += res.GetApproximateSize()
	stats.ShardCount++
	to.InfluenceStats[groupKey] = stats
}

// DemoteFollower is an OpStep that demotes a resource follower peer to learner.
type DemoteFollower struct {
	ToStore, PeerID uint64
}

func (df DemoteFollower) String() string {
	return fmt.Sprintf("demote follower peer %v on container %v to learner", df.PeerID, df.ToStore)
}

// ConfVerChanged returns the delta value for version increased by this step.
func (df DemoteFollower) ConfVerChanged(res *core.CachedShard) uint64 {
	peer, _ := res.GetStoreLearner(df.ToStore)
	return typeutil.BoolToUint64(peer.ID == df.PeerID)
}

// IsFinish checks if current step is finished.
func (df DemoteFollower) IsFinish(res *core.CachedShard) bool {
	if peer, ok := res.GetStoreLearner(df.ToStore); ok {
		return peer.ID == df.PeerID
	}
	return false
}

// CheckSafety checks if the step meets the safety properties.
func (df DemoteFollower) CheckSafety(res *core.CachedShard) error {
	peer, _ := res.GetStorePeer(df.ToStore)
	if peer.ID != df.PeerID {
		return errors.New("peer does not exist")
	}
	if peer.ID == res.GetLeader().GetID() {
		return errors.New("cannot demote leader peer")
	}
	return nil
}

// Influence calculates the container difference that current step makes.
func (df DemoteFollower) Influence(opInfluence OpInfluence, res *core.CachedShard) {}

// DemoteVoter is very similar to DemoteFollower. But it allows Demote Leader.
// Note: It is not an OpStep, only a sub step in ChangePeerV2Enter and ChangePeerV2Leave.
type DemoteVoter struct {
	ToStore, PeerID uint64
}

func (dv DemoteVoter) String() string {
	return fmt.Sprintf("demote voter peer %v on container %v to learner", dv.PeerID, dv.ToStore)
}

// ConfVerChanged returns the delta value for version increased by this step.
func (dv DemoteVoter) ConfVerChanged(res *core.CachedShard) bool {
	peer, _ := res.GetStoreLearner(dv.ToStore)
	return peer.ID == dv.PeerID
}

// IsFinish checks if current step is finished.
func (dv DemoteVoter) IsFinish(res *core.CachedShard) bool {
	if peer, ok := res.GetStoreLearner(dv.ToStore); ok {
		return peer.ID == dv.PeerID
	}
	return false
}

// ChangePeerV2Enter is an OpStep that uses joint consensus to request all PromoteLearner and DemoteVoter.
type ChangePeerV2Enter struct {
	PromoteLearners []PromoteLearner
	DemoteVoters    []DemoteVoter
}

func (cpe ChangePeerV2Enter) String() string {
	b := &strings.Builder{}
	_, _ = b.WriteString("use joint consensus")
	for _, pl := range cpe.PromoteLearners {
		_, _ = fmt.Fprintf(b, ", promote learner peer %v on container %v to voter", pl.PeerID, pl.ToStore)
	}
	for _, dv := range cpe.DemoteVoters {
		_, _ = fmt.Fprintf(b, ", demote voter peer %v on container %v to learner", dv.PeerID, dv.ToStore)
	}
	return b.String()
}

// ConfVerChanged returns the delta value for version increased by this step.
func (cpe ChangePeerV2Enter) ConfVerChanged(res *core.CachedShard) uint64 {
	for _, pl := range cpe.PromoteLearners {
		peer, ok := res.GetStoreVoter(pl.ToStore)
		if !ok || peer.ID != pl.PeerID || !metadata.IsVoterOrIncomingVoter(peer) {
			return 0
		}
	}
	for _, dv := range cpe.DemoteVoters {
		peer, ok := res.GetStoreVoter(dv.ToStore)
		if ok && (peer.ID != dv.PeerID || !metadata.IsLearnerOrDemotingVoter(peer)) {
			return 0
		}
	}
	return uint64(len(cpe.PromoteLearners) + len(cpe.DemoteVoters))
}

// IsFinish checks if current step is finished.
func (cpe ChangePeerV2Enter) IsFinish(res *core.CachedShard) bool {
	for _, pl := range cpe.PromoteLearners {
		peer, ok := res.GetStoreVoter(pl.ToStore)
		if !ok || peer.ID != pl.PeerID || peer.Role != metapb.ReplicaRole_IncomingVoter {
			return false
		}
	}
	for _, dv := range cpe.DemoteVoters {
		peer, ok := res.GetStoreVoter(dv.ToStore)
		if !ok || peer.ID != dv.PeerID || peer.Role != metapb.ReplicaRole_DemotingVoter {
			return false
		}
	}
	return true
}

// CheckSafety checks if the step meets the safety properties.
func (cpe ChangePeerV2Enter) CheckSafety(res *core.CachedShard) error {
	inJointState, notInJointState := false, false
	for _, pl := range cpe.PromoteLearners {
		peer, ok := res.GetStorePeer(pl.ToStore)
		if !ok || peer.ID != pl.PeerID {
			return errors.New("peer does not exist")
		}
		switch peer.Role {
		case metapb.ReplicaRole_Learner:
			notInJointState = true
		case metapb.ReplicaRole_IncomingVoter:
			inJointState = true
		case metapb.ReplicaRole_Voter:
			return errors.New("peer already is a voter")
		case metapb.ReplicaRole_DemotingVoter:
			return errors.New("cannot promote a demoting voter")
		default:
			return errors.New("unexpected peer role")
		}
	}
	for _, dv := range cpe.DemoteVoters {
		peer, ok := res.GetStorePeer(dv.ToStore)
		if !ok || peer.ID != dv.PeerID {
			return errors.New("peer does not exist")
		}
		switch peer.Role {
		case metapb.ReplicaRole_Voter:
			notInJointState = true
		case metapb.ReplicaRole_DemotingVoter:
			inJointState = true
		case metapb.ReplicaRole_Learner:
			return errors.New("peer already is a learner")
		case metapb.ReplicaRole_IncomingVoter:
			return errors.New("cannot demote a incoming voter")
		default:
			return errors.New("unexpected peer role")
		}
	}

	switch count := metadata.CountInJointState(res.Meta.GetReplicas()...); {
	case notInJointState && inJointState:
		return errors.New("non-atomic joint consensus")
	case notInJointState && count != 0:
		return errors.New("some other peers are in joint state, when the resource is in joint state")
	case inJointState && count != len(cpe.PromoteLearners)+len(cpe.DemoteVoters):
		return errors.New("some other peers are in joint state, when the resource is not in joint state")
	}

	return nil
}

// Influence calculates the container difference that current step makes.
func (cpe ChangePeerV2Enter) Influence(opInfluence OpInfluence, res *core.CachedShard) {}

// GetRequest get the ChangePeerV2 request
func (cpe ChangePeerV2Enter) GetRequest() *rpcpb.ConfigChangeV2 {
	changes := make([]rpcpb.ConfigChange, 0, len(cpe.PromoteLearners)+len(cpe.DemoteVoters))
	for _, pl := range cpe.PromoteLearners {
		changes = append(changes, rpcpb.ConfigChange{
			ChangeType: metapb.ConfigChangeType_AddNode,
			Replica: metapb.Replica{
				ID:      pl.PeerID,
				StoreID: pl.ToStore,
				Role:    metapb.ReplicaRole_Voter,
			},
		})
	}
	for _, dv := range cpe.DemoteVoters {
		changes = append(changes, rpcpb.ConfigChange{
			ChangeType: metapb.ConfigChangeType_AddLearnerNode,
			Replica: metapb.Replica{
				ID:      dv.PeerID,
				StoreID: dv.ToStore,
				Role:    metapb.ReplicaRole_Learner,
			},
		})
	}
	return &rpcpb.ConfigChangeV2{
		Changes: changes,
	}
}

// ChangePeerV2Leave is an OpStep that leaves the joint state.
type ChangePeerV2Leave struct {
	PromoteLearners []PromoteLearner
	DemoteVoters    []DemoteVoter
}

func (cpl ChangePeerV2Leave) String() string {
	b := &strings.Builder{}
	_, _ = b.WriteString("leave joint state")
	for _, pl := range cpl.PromoteLearners {
		_, _ = fmt.Fprintf(b, ", promote learner peer %v on container %v to voter", pl.PeerID, pl.ToStore)
	}
	for _, dv := range cpl.DemoteVoters {
		_, _ = fmt.Fprintf(b, ", demote voter peer %v on container %v to learner", dv.PeerID, dv.ToStore)
	}
	return b.String()
}

// ConfVerChanged returns the delta value for version increased by this step.
func (cpl ChangePeerV2Leave) ConfVerChanged(res *core.CachedShard) uint64 {
	for _, pl := range cpl.PromoteLearners {
		peer, ok := res.GetStoreVoter(pl.ToStore)
		if !ok || peer.ID != pl.PeerID || peer.Role != metapb.ReplicaRole_Voter {
			return 0
		}
	}
	for _, dv := range cpl.DemoteVoters {
		if _, ok := res.GetStorePeer(dv.PeerID); ok && !dv.ConfVerChanged(res) {
			return 0
		}
	}
	return uint64(len(cpl.PromoteLearners) + len(cpl.DemoteVoters))
}

// IsFinish checks if current step is finished.
func (cpl ChangePeerV2Leave) IsFinish(res *core.CachedShard) bool {
	for _, pl := range cpl.PromoteLearners {
		peer, ok := res.GetStoreVoter(pl.ToStore)
		if !ok || peer.ID != pl.PeerID || peer.Role != metapb.ReplicaRole_Voter {
			return false
		}
	}
	for _, dv := range cpl.DemoteVoters {
		if !dv.IsFinish(res) {
			return false
		}
	}
	if metadata.IsInJointState(res.Meta.GetReplicas()...) {
		return false
	}
	return true
}

// CheckSafety checks if the step meets the safety properties.
func (cpl ChangePeerV2Leave) CheckSafety(res *core.CachedShard) error {
	inJointState, notInJointState, demoteLeader := false, false, false
	leaderStoreID := res.GetLeader().GetStoreID()

	for _, pl := range cpl.PromoteLearners {
		peer, ok := res.GetStorePeer(pl.ToStore)
		if !ok || peer.ID != pl.PeerID {
			return errors.New("peer does not exist")
		}
		switch peer.Role {
		case metapb.ReplicaRole_Voter:
			notInJointState = true
		case metapb.ReplicaRole_IncomingVoter:
			inJointState = true
		case metapb.ReplicaRole_Learner:
			return errors.New("peer is still a learner")
		case metapb.ReplicaRole_DemotingVoter:
			return errors.New("cannot promote a demoting voter")
		default:
			return errors.New("unexpected peer role")
		}
	}
	for _, dv := range cpl.DemoteVoters {
		peer, ok := res.GetStorePeer(dv.ToStore)
		if !ok || peer.ID != dv.PeerID {
			return errors.New("peer does not exist")
		}
		switch peer.Role {
		case metapb.ReplicaRole_Learner:
			notInJointState = true
		case metapb.ReplicaRole_DemotingVoter:
			inJointState = true
			if peer.StoreID == leaderStoreID {
				demoteLeader = true
			}
		case metapb.ReplicaRole_Voter:
			return errors.New("peer is still a voter")
		case metapb.ReplicaRole_IncomingVoter:
			return errors.New("cannot demote a incoming voter")
		default:
			return errors.New("unexpected peer role")
		}
	}

	switch count := metadata.CountInJointState(res.Meta.GetReplicas()...); {
	case notInJointState && inJointState:
		return errors.New("non-atomic joint consensus")
	case notInJointState && count != 0:
		return errors.New("some other peers are in joint state, when the resource is in joint state")
	case inJointState && count != len(cpl.PromoteLearners)+len(cpl.DemoteVoters):
		return errors.New("some other peers are in joint state, when the resource is not in joint state")
	case demoteLeader:
		return errors.New("cannot demote leader peer")
	}

	return nil
}

// Influence calculates the container difference that current step makes.
func (cpl ChangePeerV2Leave) Influence(opInfluence OpInfluence, res *core.CachedShard) {}

// DestroyDirectly is an OpStep that destroy current peer directly without raft.
type DestroyDirectly struct {
}

// ConfVerChanged returns the delta value for version increased by this step.
func (tl DestroyDirectly) ConfVerChanged(res *core.CachedShard) uint64 {
	return 0 // RemoveDirectly never change the conf version
}

func (tl DestroyDirectly) String() string {
	return "destroy peer directly"
}

// IsFinish checks if current step is finished.
func (tl DestroyDirectly) IsFinish(res *core.CachedShard) bool {
	return true
}

// CheckSafety checks if the step meets the safety properties.
func (tl DestroyDirectly) CheckSafety(res *core.CachedShard) error {
	return nil
}

// Influence calculates the container difference that current step makes.
func (tl DestroyDirectly) Influence(opInfluence OpInfluence, res *core.CachedShard) {
}
