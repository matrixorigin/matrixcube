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
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
)

// OpStep describes the basic scheduling steps that can not be subdivided.
type OpStep interface {
	fmt.Stringer
	ConfVerChanged(res *core.CachedResource) uint64
	IsFinish(res *core.CachedResource) bool
	CheckSafety(res *core.CachedResource) error
	Influence(opInfluence OpInfluence, res *core.CachedResource)
}

// TransferLeader is an OpStep that transfers a resource's leader.
type TransferLeader struct {
	FromContainer, ToContainer uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (tl TransferLeader) ConfVerChanged(res *core.CachedResource) uint64 {
	return 0 // transfer leader never change the conf version
}

func (tl TransferLeader) String() string {
	return fmt.Sprintf("transfer leader from container %v to container %v", tl.FromContainer, tl.ToContainer)
}

// IsFinish checks if current step is finished.
func (tl TransferLeader) IsFinish(res *core.CachedResource) bool {
	return res.GetLeader().GetContainerID() == tl.ToContainer
}

// CheckSafety checks if the step meets the safety properties.
func (tl TransferLeader) CheckSafety(res *core.CachedResource) error {
	peer, ok := res.GetContainerPeer(tl.ToContainer)
	if !ok {
		return errors.New("peer does not existed")
	}
	if metadata.IsLearner(peer) {
		return errors.New("peer already is a learner")
	}
	return nil
}

// Influence calculates the container difference that current step makes.
func (tl TransferLeader) Influence(opInfluence OpInfluence, res *core.CachedResource) {
	from := opInfluence.GetContainerInfluence(tl.FromContainer)
	to := opInfluence.GetContainerInfluence(tl.ToContainer)

	from.LeaderSize -= res.GetApproximateSize()
	from.LeaderCount--
	to.LeaderSize += res.GetApproximateSize()
	to.LeaderCount++
}

// AddPeer is an OpStep that adds a resource peer.
type AddPeer struct {
	ToContainer, PeerID uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (ap AddPeer) ConfVerChanged(res *core.CachedResource) uint64 {
	peer, _ := res.GetContainerVoter(ap.ToContainer)
	return typeutil.BoolToUint64(peer.ID == ap.PeerID)
}

func (ap AddPeer) String() string {
	return fmt.Sprintf("add peer %v on container %v", ap.PeerID, ap.ToContainer)
}

// IsFinish checks if current step is finished.
func (ap AddPeer) IsFinish(res *core.CachedResource) bool {
	if peer, ok := res.GetContainerVoter(ap.ToContainer); ok {
		if peer.ID != ap.PeerID {
			util.GetLogger().Warningf("%s obtain unexpected peer %+v", ap.String(), peer)
			return false
		}
		_, ok := res.GetPendingVoter(peer.ID)
		return !ok
	}
	return false
}

// Influence calculates the container difference that current step makes.
func (ap AddPeer) Influence(opInfluence OpInfluence, res *core.CachedResource) {
	to := opInfluence.GetContainerInfluence(ap.ToContainer)

	size := res.GetApproximateSize()
	to.ResourceSize += size
	to.ResourceCount++
	to.AdjustStepCost(limit.AddPeer, size)
}

// CheckSafety checks if the step meets the safety properties.
func (ap AddPeer) CheckSafety(res *core.CachedResource) error {
	peer, ok := res.GetContainerPeer(ap.ToContainer)
	if ok && peer.ID != ap.PeerID {
		return fmt.Errorf("peer %d has already existed in container %d, the operator is trying to add peer %d on the same container", peer.ID, ap.ToContainer, ap.PeerID)
	}
	return nil
}

// AddLearner is an OpStep that adds a resource learner peer.
type AddLearner struct {
	ToContainer, PeerID uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (al AddLearner) ConfVerChanged(res *core.CachedResource) uint64 {
	peer, _ := res.GetContainerPeer(al.ToContainer)
	return typeutil.BoolToUint64(peer.ID == al.PeerID)
}

func (al AddLearner) String() string {
	return fmt.Sprintf("add learner peer %v on container %v", al.PeerID, al.ToContainer)
}

// IsFinish checks if current step is finished.
func (al AddLearner) IsFinish(res *core.CachedResource) bool {
	if peer, ok := res.GetContainerLearner(al.ToContainer); ok {
		if peer.ID != al.PeerID {
			util.GetLogger().Warningf("%+v obtain unexpected peer %+v",
				al.String(), peer.ID)
			return false
		}
		_, ok := res.GetPendingLearner(peer.ID)
		return !ok
	}
	return false
}

// CheckSafety checks if the step meets the safety properties.
func (al AddLearner) CheckSafety(res *core.CachedResource) error {
	peer, ok := res.GetContainerPeer(al.ToContainer)
	if !ok {
		return nil
	}
	if peer.ID != al.PeerID {
		return fmt.Errorf("peer %d has already existed in container %d, the operator is trying to add peer %d on the same container", peer.ID, al.ToContainer, al.PeerID)
	}
	if !metadata.IsLearner(peer) {
		return errors.New("peer already is a voter")
	}
	return nil
}

// Influence calculates the container difference that current step makes.
func (al AddLearner) Influence(opInfluence OpInfluence, res *core.CachedResource) {
	to := opInfluence.GetContainerInfluence(al.ToContainer)

	size := res.GetApproximateSize()
	to.ResourceSize += size
	to.ResourceCount++
	to.AdjustStepCost(limit.AddPeer, size)
}

// PromoteLearner is an OpStep that promotes a resource learner peer to normal voter.
type PromoteLearner struct {
	ToContainer, PeerID uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (pl PromoteLearner) ConfVerChanged(res *core.CachedResource) uint64 {
	peer, _ := res.GetContainerVoter(pl.ToContainer)
	return typeutil.BoolToUint64(peer.ID == pl.PeerID)
}

func (pl PromoteLearner) String() string {
	return fmt.Sprintf("promote learner peer %v on container %v to voter", pl.PeerID, pl.ToContainer)
}

// IsFinish checks if current step is finished.
func (pl PromoteLearner) IsFinish(res *core.CachedResource) bool {
	if peer, ok := res.GetContainerVoter(pl.ToContainer); ok {
		if peer.ID != pl.PeerID {
			util.GetLogger().Warningf("%+v obtain unexpected peer %+v",
				pl.String(), peer.ID)
		}
		return peer.ID == pl.PeerID
	}
	return false
}

// CheckSafety checks if the step meets the safety properties.
func (pl PromoteLearner) CheckSafety(res *core.CachedResource) error {
	peer, _ := res.GetContainerPeer(pl.ToContainer)
	if peer.ID != pl.PeerID {
		return errors.New("peer does not exist")
	}
	return nil
}

// Influence calculates the container difference that current step makes.
func (pl PromoteLearner) Influence(opInfluence OpInfluence, res *core.CachedResource) {}

// RemovePeer is an OpStep that removes a resource peer.
type RemovePeer struct {
	FromContainer, PeerID uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (rp RemovePeer) ConfVerChanged(res *core.CachedResource) uint64 {
	p, _ := res.GetContainerPeer(rp.FromContainer)
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
	return fmt.Sprintf("remove peer on container %v", rp.FromContainer)
}

// IsFinish checks if current step is finished.
func (rp RemovePeer) IsFinish(res *core.CachedResource) bool {
	_, ok := res.GetContainerPeer(rp.FromContainer)
	return !ok
}

// CheckSafety checks if the step meets the safety properties.
func (rp RemovePeer) CheckSafety(res *core.CachedResource) error {
	if rp.FromContainer == res.GetLeader().GetContainerID() {
		return errors.New("cannot remove leader peer")
	}
	return nil
}

// Influence calculates the container difference that current step makes.
func (rp RemovePeer) Influence(opInfluence OpInfluence, res *core.CachedResource) {
	from := opInfluence.GetContainerInfluence(rp.FromContainer)

	size := res.GetApproximateSize()
	from.ResourceSize -= size
	from.ResourceCount--
	from.AdjustStepCost(limit.RemovePeer, size)
}

// MergeResource is an OpStep that merge two resources.
type MergeResource struct {
	FromResource metadata.Resource
	ToResource   metadata.Resource
	// there are two resources involved in merge process,
	// so to keep them from other scheduler,
	// both of them should add Merresource operatorStep.
	// But actually, your storage application just needs the resource want to be merged to get the merge request,
	// thus use a IsPassive mark to indicate that
	// this resource doesn't need to send merge request to your storage application.
	IsPassive bool
}

// ConfVerChanged returns the delta value for version increased by this step.
func (mr MergeResource) ConfVerChanged(res *core.CachedResource) uint64 {
	return 0
}

func (mr MergeResource) String() string {
	return fmt.Sprintf("merge resource %v into resource %v", mr.FromResource.ID(), mr.ToResource.ID())
}

// IsFinish checks if current step is finished.
func (mr MergeResource) IsFinish(res *core.CachedResource) bool {
	if mr.IsPassive {
		start, end := res.Meta.Range()
		toStart, toEnd := mr.ToResource.Range()
		return !bytes.Equal(start, toStart) || !bytes.Equal(end, toEnd)
	}
	return false
}

// CheckSafety checks if the step meets the safety properties.
func (mr MergeResource) CheckSafety(res *core.CachedResource) error {
	return nil
}

// Influence calculates the container difference that current step makes.
func (mr MergeResource) Influence(opInfluence OpInfluence, res *core.CachedResource) {
	if mr.IsPassive {
		for _, peer := range res.Meta.Peers() {
			o := opInfluence.GetContainerInfluence(peer.ContainerID)
			o.ResourceCount--
			if res.GetLeader().GetID() == peer.ID {
				o.LeaderCount--
			}
		}
	}
}

// SplitResource is an OpStep that splits a resource.
type SplitResource struct {
	StartKey, EndKey []byte
	Policy           metapb.CheckPolicy
	SplitKeys        [][]byte
}

// ConfVerChanged returns the delta value for version increased by this step.
func (sr SplitResource) ConfVerChanged(res *core.CachedResource) uint64 {
	return 0
}

func (sr SplitResource) String() string {
	return fmt.Sprintf("split resource with policy %s", sr.Policy.String())
}

// IsFinish checks if current step is finished.
func (sr SplitResource) IsFinish(res *core.CachedResource) bool {
	start, end := res.Meta.Range()
	return !bytes.Equal(start, sr.StartKey) || !bytes.Equal(end, sr.EndKey)
}

// Influence calculates the container difference that current step makes.
func (sr SplitResource) Influence(opInfluence OpInfluence, res *core.CachedResource) {
	for _, peer := range res.Meta.Peers() {
		inf := opInfluence.GetContainerInfluence(peer.ContainerID)
		inf.ResourceCount++
		if res.GetLeader().GetID() == peer.ID {
			inf.LeaderCount++
		}
	}
}

// CheckSafety checks if the step meets the safety properties.
func (sr SplitResource) CheckSafety(res *core.CachedResource) error {
	return nil
}

// AddLightPeer is an OpStep that adds a resource peer without considering the influence.
type AddLightPeer struct {
	ToContainer, PeerID uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (ap AddLightPeer) ConfVerChanged(res *core.CachedResource) uint64 {
	peer, _ := res.GetContainerVoter(ap.ToContainer)
	return typeutil.BoolToUint64(peer.ID == ap.PeerID)
}

func (ap AddLightPeer) String() string {
	return fmt.Sprintf("add peer %v on container %v", ap.PeerID, ap.ToContainer)
}

// IsFinish checks if current step is finished.
func (ap AddLightPeer) IsFinish(res *core.CachedResource) bool {
	if peer, ok := res.GetContainerVoter(ap.ToContainer); ok {
		if peer.ID != ap.PeerID {
			util.GetLogger().Warningf("%+v obtain unexpected peer %+v",
				ap.String(), peer)
			return false
		}
		_, ok := res.GetPendingVoter(peer.ID)
		return !ok
	}
	return false
}

// CheckSafety checks if the step meets the safety properties.
func (ap AddLightPeer) CheckSafety(res *core.CachedResource) error {
	peer, ok := res.GetContainerPeer(ap.ToContainer)
	if ok && peer.ID != ap.PeerID {
		return fmt.Errorf("peer %d has already existed in container %d, the operator is trying to add peer %d on the same container", peer.ID, ap.ToContainer, ap.PeerID)
	}
	return nil
}

// Influence calculates the container difference that current step makes.
func (ap AddLightPeer) Influence(opInfluence OpInfluence, res *core.CachedResource) {
	to := opInfluence.GetContainerInfluence(ap.ToContainer)

	to.ResourceSize += res.GetApproximateSize()
	to.ResourceCount++
}

// AddLightLearner is an OpStep that adds a resource learner peer without considering the influence.
type AddLightLearner struct {
	ToContainer, PeerID uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (al AddLightLearner) ConfVerChanged(res *core.CachedResource) uint64 {
	peer, _ := res.GetContainerPeer(al.ToContainer)
	return typeutil.BoolToUint64(peer.ID == al.PeerID)
}

func (al AddLightLearner) String() string {
	return fmt.Sprintf("add learner peer %v on container %v", al.PeerID, al.ToContainer)
}

// IsFinish checks if current step is finished.
func (al AddLightLearner) IsFinish(res *core.CachedResource) bool {
	if peer, ok := res.GetContainerLearner(al.ToContainer); ok {
		if peer.ID != al.PeerID {
			util.GetLogger().Warningf("%+v obtain unexpected peer %+v",
				al.String(), peer)
			return false
		}
		_, ok := res.GetPendingLearner(peer.ID)
		return !ok
	}
	return false
}

// CheckSafety checks if the step meets the safety properties.
func (al AddLightLearner) CheckSafety(res *core.CachedResource) error {
	peer, ok := res.GetContainerPeer(al.ToContainer)
	if !ok {
		return nil
	}
	if peer.ID != al.PeerID {
		return fmt.Errorf("peer %d has already existed in container %d, the operator is trying to add peer %d on the same container", peer.ID, al.ToContainer, al.PeerID)
	}
	if !metadata.IsLearner(peer) {
		return errors.New("peer already is a voter")
	}
	return nil
}

// Influence calculates the container difference that current step makes.
func (al AddLightLearner) Influence(opInfluence OpInfluence, res *core.CachedResource) {
	to := opInfluence.GetContainerInfluence(al.ToContainer)

	to.ResourceSize += res.GetApproximateSize()
	to.ResourceCount++
}

// DemoteFollower is an OpStep that demotes a resource follower peer to learner.
type DemoteFollower struct {
	ToContainer, PeerID uint64
}

func (df DemoteFollower) String() string {
	return fmt.Sprintf("demote follower peer %v on container %v to learner", df.PeerID, df.ToContainer)
}

// ConfVerChanged returns the delta value for version increased by this step.
func (df DemoteFollower) ConfVerChanged(res *core.CachedResource) uint64 {
	peer, _ := res.GetContainerLearner(df.ToContainer)
	return typeutil.BoolToUint64(peer.ID == df.PeerID)
}

// IsFinish checks if current step is finished.
func (df DemoteFollower) IsFinish(res *core.CachedResource) bool {
	if peer, ok := res.GetContainerLearner(df.ToContainer); ok {
		if peer.ID != df.PeerID {
			util.GetLogger().Warningf("%+v obtain unexpected peer %+v",
				df.String(), peer)
		}
		return peer.ID == df.PeerID
	}
	return false
}

// CheckSafety checks if the step meets the safety properties.
func (df DemoteFollower) CheckSafety(res *core.CachedResource) error {
	peer, _ := res.GetContainerPeer(df.ToContainer)
	if peer.ID != df.PeerID {
		return errors.New("peer does not exist")
	}
	if peer.ID == res.GetLeader().GetID() {
		return errors.New("cannot demote leader peer")
	}
	return nil
}

// Influence calculates the container difference that current step makes.
func (df DemoteFollower) Influence(opInfluence OpInfluence, res *core.CachedResource) {}

// DemoteVoter is very similar to DemoteFollower. But it allows Demote Leader.
// Note: It is not an OpStep, only a sub step in ChangePeerV2Enter and ChangePeerV2Leave.
type DemoteVoter struct {
	ToContainer, PeerID uint64
}

func (dv DemoteVoter) String() string {
	return fmt.Sprintf("demote voter peer %v on container %v to learner", dv.PeerID, dv.ToContainer)
}

// ConfVerChanged returns the delta value for version increased by this step.
func (dv DemoteVoter) ConfVerChanged(res *core.CachedResource) bool {
	peer, _ := res.GetContainerLearner(dv.ToContainer)
	return peer.ID == dv.PeerID
}

// IsFinish checks if current step is finished.
func (dv DemoteVoter) IsFinish(res *core.CachedResource) bool {
	if peer, ok := res.GetContainerLearner(dv.ToContainer); ok {
		if peer.ID != dv.PeerID {
			util.GetLogger().Warningf("%+v obtain unexpected peer %+v",
				dv.String(), peer)
		}
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
		_, _ = fmt.Fprintf(b, ", promote learner peer %v on container %v to voter", pl.PeerID, pl.ToContainer)
	}
	for _, dv := range cpe.DemoteVoters {
		_, _ = fmt.Fprintf(b, ", demote voter peer %v on container %v to learner", dv.PeerID, dv.ToContainer)
	}
	return b.String()
}

// ConfVerChanged returns the delta value for version increased by this step.
func (cpe ChangePeerV2Enter) ConfVerChanged(res *core.CachedResource) uint64 {
	for _, pl := range cpe.PromoteLearners {
		peer, ok := res.GetContainerVoter(pl.ToContainer)
		if !ok || peer.ID != pl.PeerID || !metadata.IsVoterOrIncomingVoter(peer) {
			return 0
		}
	}
	for _, dv := range cpe.DemoteVoters {
		peer, ok := res.GetContainerVoter(dv.ToContainer)
		if ok && (peer.ID != dv.PeerID || !metadata.IsLearnerOrDemotingVoter(peer)) {
			return 0
		}
	}
	return uint64(len(cpe.PromoteLearners) + len(cpe.DemoteVoters))
}

// IsFinish checks if current step is finished.
func (cpe ChangePeerV2Enter) IsFinish(res *core.CachedResource) bool {
	for _, pl := range cpe.PromoteLearners {
		peer, ok := res.GetContainerVoter(pl.ToContainer)
		if ok && peer.ID != pl.PeerID {
			util.GetLogger().Warningf("%+v obtain unexpected peer %+v",
				pl.String(), peer)
		}
		if !ok || peer.ID != pl.PeerID || peer.Role != metapb.ReplicaRole_IncomingVoter {
			return false
		}
	}
	for _, dv := range cpe.DemoteVoters {
		peer, ok := res.GetContainerVoter(dv.ToContainer)
		if ok && peer.ID != dv.PeerID {
			util.GetLogger().Warningf("%+v obtain unexpected peer %+v",
				dv.String(), peer)
		}
		if !ok || peer.ID != dv.PeerID || peer.Role != metapb.ReplicaRole_DemotingVoter {
			return false
		}
	}
	return true
}

// CheckSafety checks if the step meets the safety properties.
func (cpe ChangePeerV2Enter) CheckSafety(res *core.CachedResource) error {
	inJointState, notInJointState := false, false
	for _, pl := range cpe.PromoteLearners {
		peer, ok := res.GetContainerPeer(pl.ToContainer)
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
		peer, ok := res.GetContainerPeer(dv.ToContainer)
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

	switch count := metadata.CountInJointState(res.Meta.Peers()...); {
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
func (cpe ChangePeerV2Enter) Influence(opInfluence OpInfluence, res *core.CachedResource) {}

// GetRequest get the ChangePeerV2 request
func (cpe ChangePeerV2Enter) GetRequest() *rpcpb.ConfigChangeV2 {
	changes := make([]rpcpb.ConfigChange, 0, len(cpe.PromoteLearners)+len(cpe.DemoteVoters))
	for _, pl := range cpe.PromoteLearners {
		changes = append(changes, rpcpb.ConfigChange{
			ChangeType: metapb.ConfigChangeType_AddNode,
			Replica: metapb.Replica{
				ID:          pl.PeerID,
				ContainerID: pl.ToContainer,
				Role:        metapb.ReplicaRole_Voter,
			},
		})
	}
	for _, dv := range cpe.DemoteVoters {
		changes = append(changes, rpcpb.ConfigChange{
			ChangeType: metapb.ConfigChangeType_AddLearnerNode,
			Replica: metapb.Replica{
				ID:          dv.PeerID,
				ContainerID: dv.ToContainer,
				Role:        metapb.ReplicaRole_Learner,
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
		_, _ = fmt.Fprintf(b, ", promote learner peer %v on container %v to voter", pl.PeerID, pl.ToContainer)
	}
	for _, dv := range cpl.DemoteVoters {
		_, _ = fmt.Fprintf(b, ", demote voter peer %v on container %v to learner", dv.PeerID, dv.ToContainer)
	}
	return b.String()
}

// ConfVerChanged returns the delta value for version increased by this step.
func (cpl ChangePeerV2Leave) ConfVerChanged(res *core.CachedResource) uint64 {
	for _, pl := range cpl.PromoteLearners {
		peer, ok := res.GetContainerVoter(pl.ToContainer)
		if !ok || peer.ID != pl.PeerID || peer.Role != metapb.ReplicaRole_Voter {
			return 0
		}
	}
	for _, dv := range cpl.DemoteVoters {
		if _, ok := res.GetContainerPeer(dv.PeerID); ok && !dv.ConfVerChanged(res) {
			return 0
		}
	}
	return uint64(len(cpl.PromoteLearners) + len(cpl.DemoteVoters))
}

// IsFinish checks if current step is finished.
func (cpl ChangePeerV2Leave) IsFinish(res *core.CachedResource) bool {
	for _, pl := range cpl.PromoteLearners {
		peer, ok := res.GetContainerVoter(pl.ToContainer)
		if ok && peer.ID != pl.PeerID {
			util.GetLogger().Warningf("%+v obtain unexpected peer %+v",
				pl.String(), peer)
		}
		if !ok || peer.ID != pl.PeerID || peer.Role != metapb.ReplicaRole_Voter {
			return false
		}
	}
	for _, dv := range cpl.DemoteVoters {
		if !dv.IsFinish(res) {
			return false
		}
	}
	if metadata.IsInJointState(res.Meta.Peers()...) {
		util.GetLogger().Warningf("resource %d is still in the joint state",
			res.Meta.ID())
		return false
	}
	return true
}

// CheckSafety checks if the step meets the safety properties.
func (cpl ChangePeerV2Leave) CheckSafety(res *core.CachedResource) error {
	inJointState, notInJointState, demoteLeader := false, false, false
	leaderContainerID := res.GetLeader().GetContainerID()

	for _, pl := range cpl.PromoteLearners {
		peer, ok := res.GetContainerPeer(pl.ToContainer)
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
		peer, ok := res.GetContainerPeer(dv.ToContainer)
		if !ok || peer.ID != dv.PeerID {
			return errors.New("peer does not exist")
		}
		switch peer.Role {
		case metapb.ReplicaRole_Learner:
			notInJointState = true
		case metapb.ReplicaRole_DemotingVoter:
			inJointState = true
			if peer.ContainerID == leaderContainerID {
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

	switch count := metadata.CountInJointState(res.Meta.Peers()...); {
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
func (cpl ChangePeerV2Leave) Influence(opInfluence OpInfluence, res *core.CachedResource) {}

// DestoryDirectly is an OpStep that destory current peer directly without raft.
type DestoryDirectly struct {
}

// ConfVerChanged returns the delta value for version increased by this step.
func (tl DestoryDirectly) ConfVerChanged(res *core.CachedResource) uint64 {
	return 0 // RemoveDirectly never change the conf version
}

func (tl DestoryDirectly) String() string {
	return "destory peer directly"
}

// IsFinish checks if current step is finished.
func (tl DestoryDirectly) IsFinish(res *core.CachedResource) bool {
	return true
}

// CheckSafety checks if the step meets the safety properties.
func (tl DestoryDirectly) CheckSafety(res *core.CachedResource) error {
	return nil
}

// Influence calculates the container difference that current step makes.
func (tl DestoryDirectly) Influence(opInfluence OpInfluence, res *core.CachedResource) {
}
