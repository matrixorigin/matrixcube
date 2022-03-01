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
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

// CreateAddPeerOperator creates an operator that adds a new peer.
func CreateAddPeerOperator(desc string, cluster opt.Cluster, res *core.CachedShard, peer metapb.Replica, kind OpKind) (*Operator, error) {
	return NewBuilder(desc, cluster, res).
		AddPeer(peer).
		Build(kind)
}

// CreatePromoteLearnerOperator creates an operator that promotes a learner.
func CreatePromoteLearnerOperator(desc string, cluster opt.Cluster, res *core.CachedShard, peer metapb.Replica) (*Operator, error) {
	return NewBuilder(desc, cluster, res).
		PromoteLearner(peer.StoreID).
		Build(0)
}

// CreateRemovePeerOperator creates an operator that removes a peer from resource.
func CreateRemovePeerOperator(desc string, cluster opt.Cluster, kind OpKind, res *core.CachedShard, containerID uint64) (*Operator, error) {
	return NewBuilder(desc, cluster, res).
		RemovePeer(containerID).
		Build(kind)
}

// CreateTransferLeaderOperator creates an operator that transfers the leader from a source container to a target container.
func CreateTransferLeaderOperator(desc string, cluster opt.Cluster, res *core.CachedShard, sourceStoreID uint64, targetStoreID uint64, kind OpKind) (*Operator, error) {
	return NewBuilder(desc, cluster, res, SkipOriginJointStateCheck).
		SetLeader(targetStoreID).
		Build(kind)
}

// CreateForceTransferLeaderOperator creates an operator that transfers the leader from a source container to a target container forcible.
func CreateForceTransferLeaderOperator(desc string, cluster opt.Cluster, res *core.CachedShard, sourceStoreID uint64, targetStoreID uint64, kind OpKind) (*Operator, error) {
	return NewBuilder(desc, cluster, res, SkipOriginJointStateCheck).
		SetLeader(targetStoreID).
		EnableForceTargetLeader().
		Build(kind)
}

// CreateMoveShardOperator creates an operator that moves a resource to specified containers.
func CreateMoveShardOperator(desc string, cluster opt.Cluster, res *core.CachedShard, kind OpKind, roles map[uint64]placement.ReplicaRoleType) (*Operator, error) {
	// construct the peers from roles
	peers := make(map[uint64]metapb.Replica)
	for containerID, role := range roles {
		peers[containerID] = metapb.Replica{
			StoreID: containerID,
			Role:    role.MetaPeerRole(),
		}
	}
	builder := NewBuilder(desc, cluster, res).SetPeers(peers).SetExpectedRoles(roles)
	return builder.Build(kind)
}

// CreateMovePeerOperator creates an operator that replaces an old peer with a new peer.
func CreateMovePeerOperator(desc string, cluster opt.Cluster, res *core.CachedShard, kind OpKind, oldStore uint64, peer metapb.Replica) (*Operator, error) {
	return NewBuilder(desc, cluster, res).
		RemovePeer(oldStore).
		AddPeer(peer).
		Build(kind)
}

// CreateMoveLeaderOperator creates an operator that replaces an old leader with a new leader.
func CreateMoveLeaderOperator(desc string, cluster opt.Cluster, res *core.CachedShard, kind OpKind, oldStore uint64, peer metapb.Replica) (*Operator, error) {
	return NewBuilder(desc, cluster, res).
		RemovePeer(oldStore).
		AddPeer(peer).
		SetLeader(peer.StoreID).
		Build(kind)
}

// CreateSplitShardOperator creates an operator that splits a resource.
func CreateSplitShardOperator(desc string, res *core.CachedShard, kind OpKind, policy metapb.CheckPolicy, keys [][]byte) (*Operator, error) {
	if metadata.IsInJointState(res.Meta.GetReplicas()...) {
		return nil, fmt.Errorf("cannot split resource which is in joint state")
	}

	start, end := res.Meta.GetRange()
	step := SplitShard{
		StartKey:  start,
		EndKey:    end,
		Policy:    policy,
		SplitKeys: keys,
	}
	brief := fmt.Sprintf("split: resource %v use policy %s", res.Meta.GetID(), policy)
	if len(keys) > 0 {
		hexKeys := make([]string, len(keys))
		for i := range keys {
			hexKeys[i] = hex.EncodeToString(keys[i])
		}
		brief += fmt.Sprintf(" and keys %v", hexKeys)
	}
	return NewOperator(desc, brief, res.Meta.GetID(), res.Meta.GetEpoch(), kind|OpSplit, step), nil
}

// CreateMergeShardOperator creates an operator that merge two resource into one.
func CreateMergeShardOperator(desc string, cluster opt.Cluster, source *core.CachedShard, target *core.CachedShard, kind OpKind) ([]*Operator, error) {
	if metadata.IsInJointState(source.Meta.GetReplicas()...) || metadata.IsInJointState(target.Meta.GetReplicas()...) {
		return nil, errors.New("cannot merge resources which are in joint state")
	}

	var steps []OpStep
	if !isShardMatch(source, target) {
		peers := make(map[uint64]metapb.Replica)
		for _, p := range target.Meta.GetReplicas() {
			peers[p.StoreID] = metapb.Replica{
				StoreID: p.StoreID,
				Role:    p.Role,
			}
		}
		matchOp, err := NewBuilder("", cluster, source).
			SetPeers(peers).
			Build(kind)
		if err != nil {
			return nil, err
		}

		steps = append(steps, matchOp.steps...)
		kind = matchOp.Kind()
	}

	steps = append(steps, MergeShard{
		FromShard: source.Meta,
		ToShard:   target.Meta,
		IsPassive: false,
	})

	brief := fmt.Sprintf("merge: resource %v to %v", source.Meta.GetID(), target.Meta.GetID())
	op1 := NewOperator(desc, brief, source.Meta.GetID(), source.Meta.GetEpoch(), kind|OpMerge, steps...)
	op2 := NewOperator(desc, brief, target.Meta.GetID(), target.Meta.GetEpoch(), kind|OpMerge, MergeShard{
		FromShard: source.Meta,
		ToShard:   target.Meta,
		IsPassive: true,
	})

	return []*Operator{op1, op2}, nil
}

func isShardMatch(a, b *core.CachedShard) bool {
	if len(a.Meta.GetReplicas()) != len(b.Meta.GetReplicas()) {
		return false
	}
	for _, pa := range a.Meta.GetReplicas() {
		pb, ok := b.GetStorePeer(pa.StoreID)
		if !ok || metadata.IsLearner(pb) != metadata.IsLearner(pa) {
			return false
		}
	}
	return true
}

// CreateScatterShardOperator creates an operator that scatters the specified resource.
func CreateScatterShardOperator(desc string, cluster opt.Cluster, origin *core.CachedShard, targetPeers map[uint64]metapb.Replica, targetLeader uint64) (*Operator, error) {
	// randomly pick a leader.
	var ids []uint64
	for id, peer := range targetPeers {
		if !metadata.IsLearner(peer) {
			ids = append(ids, id)
		}
	}
	var leader uint64
	if len(ids) > 0 {
		leader = ids[rand.Intn(len(ids))]
	}
	if targetLeader != 0 {
		leader = targetLeader
	}
	return NewBuilder(desc, cluster, origin).
		SetPeers(targetPeers).
		SetLeader(leader).
		EnableLightWeight().
		Build(0)
}

// CreateLeaveJointStateOperator creates an operator that let resource leave joint state.
func CreateLeaveJointStateOperator(desc string, cluster opt.Cluster, origin *core.CachedShard) (*Operator, error) {
	b := NewBuilder(desc, cluster, origin, SkipOriginJointStateCheck)

	if b.err == nil && !metadata.IsInJointState(origin.Meta.GetReplicas()...) {
		b.err = fmt.Errorf("cannot build leave joint state operator for resource which is not in joint state")
	}

	if b.err != nil {
		return nil, b.err
	}

	// prepareBuild
	b.toDemote = newPeersMap()
	b.toPromote = newPeersMap()
	for _, o := range b.originPeers {
		switch o.Role {
		case metapb.ReplicaRole_IncomingVoter:
			b.toPromote.Set(o)
		case metapb.ReplicaRole_DemotingVoter:
			b.toDemote.Set(o)
		}
	}

	leader, ok := b.originPeers[b.originLeaderStoreID]
	if !ok || !b.allowLeader(leader, true) {
		b.targetLeaderStoreID = 0
	} else {
		b.targetLeaderStoreID = b.originLeaderStoreID
	}

	b.currentPeers, b.currentLeaderStoreID = b.originPeers.Copy(), b.originLeaderStoreID
	b.peerAddStep = make(map[uint64]int)
	brief := b.brief()

	// buildStepsWithJointConsensus
	var kind OpKind

	b.setTargetLeaderIfNotExist()
	if b.targetLeaderStoreID == 0 {
		// Because the demote leader will be rejected by TiKV,
		// when the target leader cannot be found, we need to force a target to be found.
		b.forceTargetLeader = true
		b.setTargetLeaderIfNotExist()
	}

	if b.targetLeaderStoreID == 0 {
		cluster.GetLogger().Error(
			"resource unable to find target leader",
			log.ResourceField(origin.Meta.GetID()))
		b.originLeaderStoreID = 0
	} else if b.originLeaderStoreID != b.targetLeaderStoreID {
		kind |= OpLeader
	}

	b.execChangePeerV2(false, true)
	return NewOperator(b.desc, brief, b.resourceID, b.resourceEpoch, kind, b.steps...), nil
}
