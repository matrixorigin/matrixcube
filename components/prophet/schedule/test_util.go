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

package schedule

import (
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

// ApplyOperatorStep applies operator step. Only for test purpose.
func ApplyOperatorStep(resource *core.CachedShard, op *operator.Operator) *core.CachedShard {
	_ = op.Start()
	if step := op.Check(resource); step != nil {
		switch s := step.(type) {
		case operator.TransferLeader:
			if p, ok := resource.GetStorePeer(s.ToStore); ok {
				resource = resource.Clone(core.WithLeader(&p))
			} else {
				resource = resource.Clone(core.WithLeader(nil))
			}
		case operator.AddPeer:
			if _, ok := resource.GetStorePeer(s.ToStore); ok {
				panic("Add peer that exists")
			}
			peer := metapb.Replica{
				ID:          s.PeerID,
				StoreID: s.ToStore,
			}
			resource = resource.Clone(core.WithAddPeer(peer))
		case operator.AddLightPeer:
			if _, ok := resource.GetStorePeer(s.ToStore); ok {
				panic("Add peer that exists")
			}
			peer := metapb.Replica{
				ID:          s.PeerID,
				StoreID: s.ToStore,
			}
			resource = resource.Clone(core.WithAddPeer(peer))
		case operator.RemovePeer:
			if _, ok := resource.GetStorePeer(s.FromStore); !ok {
				panic("Remove peer that doesn't exist")
			}
			if resource.GetLeader().GetStoreID() == s.FromStore {
				panic("Cannot remove the leader peer")
			}
			resource = resource.Clone(core.WithRemoveStorePeer(s.FromStore))
		case operator.AddLearner:
			if _, ok := resource.GetStorePeer(s.ToStore); ok {
				panic("Add learner that exists")
			}
			peer := metapb.Replica{
				ID:          s.PeerID,
				StoreID: s.ToStore,
				Role:        metapb.ReplicaRole_Learner,
			}
			resource = resource.Clone(core.WithAddPeer(peer))
		case operator.AddLightLearner:
			if _, ok := resource.GetStorePeer(s.ToStore); ok {
				panic("Add learner that exists")
			}
			peer := metapb.Replica{
				ID:          s.PeerID,
				StoreID: s.ToStore,
				Role:        metapb.ReplicaRole_Learner,
			}
			resource = resource.Clone(core.WithAddPeer(peer))
		case operator.PromoteLearner:
			if _, ok := resource.GetStoreLearner(s.ToStore); !ok {
				panic("Promote peer that doesn't exist")
			}
			peer := metapb.Replica{
				ID:          s.PeerID,
				StoreID: s.ToStore,
			}
			resource = resource.Clone(core.WithRemoveStorePeer(s.ToStore), core.WithAddPeer(peer))
		default:
			panic("Unknown operator step")
		}
	}
	return resource
}

// ApplyOperator applies operator. Only for test purpose.
func ApplyOperator(mc *mockcluster.Cluster, op *operator.Operator) {
	origin := mc.GetShard(op.ShardID())
	resource := origin
	for !op.IsEnd() {
		resource = ApplyOperatorStep(resource, op)
	}
	mc.PutShard(resource)
	for id := range resource.GetStoreIDs() {
		mc.UpdateStoreStatus(id)
	}
	for id := range origin.GetStoreIDs() {
		mc.UpdateStoreStatus(id)
	}
}
