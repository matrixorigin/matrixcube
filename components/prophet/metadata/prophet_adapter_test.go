// Copyright 2020 MatrixOrigin.
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

package metadata

import (
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestShardAdapter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ma := NewShardWithRWLockFromShard(metapb.Shard{})

	data := []byte("data")
	ma.SetData(data)
	assert.Equal(t, data, ma.Shard.Data)
	assert.Equal(t, data, ma.Data())

	ma.SetStartKey(data)
	assert.Equal(t, data, ma.Shard.Start)
	ma.SetEndKey(data)
	assert.Equal(t, data, ma.Shard.End)
	s, e := ma.Range()
	assert.Equal(t, data, s)
	assert.Equal(t, data, e)

	epoch := metapb.ShardEpoch{Version: 1, ConfVer: 2}
	ma.SetEpoch(epoch)
	assert.Equal(t, epoch, ma.Shard.Epoch)
	assert.Equal(t, epoch, ma.Epoch())

	ma.SetGroup(1)
	assert.Equal(t, uint64(1), ma.Shard.Group)
	assert.Equal(t, uint64(1), ma.Group())

	ma.SetID(1)
	assert.Equal(t, uint64(1), ma.Shard.ID)
	assert.Equal(t, uint64(1), ma.ID())

	peers := []metapb.Replica{{ID: 1, StoreID: 1}, {ID: 2, StoreID: 2}}
	ma.SetReplicas(peers)
	assert.Equal(t, peers, ma.Shard.Replicas)
	assert.Equal(t, peers, ma.Replicas())

	peer := metapb.Replica{ID: 0, StoreID: 0}
	peers = append(peers, peer)
	ma.AppendReplica(peer)
	assert.Equal(t, peers, ma.Shard.Replicas)
	assert.Equal(t, peers, ma.Replicas())

	rules := []string{"r1", "r2"}
	ma.SetRuleGroups(rules...)
	assert.Equal(t, rules, ma.Shard.RuleGroups)
	assert.Equal(t, rules, ma.RuleGroups())

	ma.SetState(metapb.ShardState_Destroyed)
	assert.Equal(t, metapb.ShardState_Destroyed, ma.Shard.State)
	assert.Equal(t, metapb.ShardState_Destroyed, ma.State())

	ma.SetUnique("unique")
	assert.Equal(t, "unique", ma.Shard.Unique)
	assert.Equal(t, "unique", ma.Unique())

	v := ma.Clone()
	assert.Equal(t, ma, v)
}

func TestStoreAdapter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ca := NewStoreWithRWLock()

	ca.SetAddrs("a1", "a2")
	assert.Equal(t, "a1", ca.Store.ClientAddr)
	assert.Equal(t, "a2", ca.Store.RaftAddr)
	assert.Equal(t, "a1", ca.Addr())
	assert.Equal(t, "a2", ca.ShardAddr())

	ca.SetDeployPath("dp")
	assert.Equal(t, "dp", ca.Store.DeployPath)
	assert.Equal(t, "dp", ca.DeployPath())

	ca.SetID(1)
	assert.Equal(t, uint64(1), ca.Store.ID)
	assert.Equal(t, uint64(1), ca.ID())

	labels := []metapb.Pair{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}}
	ca.SetLabels(labels)
	assert.Equal(t, labels, ca.Store.Labels)
	assert.Equal(t, labels, ca.Labels())

	ca.SetLastHeartbeat(1)
	assert.Equal(t, int64(1), ca.Store.LastHeartbeatTime)
	assert.Equal(t, int64(1), ca.LastHeartbeat())

	ca.SetPhysicallyDestroyed(true)
	assert.True(t, ca.Store.PhysicallyDestroyed)
	assert.True(t, ca.PhysicallyDestroyed())

	ca.SetStartTimestamp(1)
	assert.Equal(t, int64(1), ca.Store.StartTime)
	assert.Equal(t, int64(1), ca.StartTimestamp())

	ca.SetState(metapb.StoreState_StoreTombstone)
	assert.Equal(t, metapb.StoreState_StoreTombstone, ca.Store.State)
	assert.Equal(t, metapb.StoreState_StoreTombstone, ca.State())

	ca.SetVersion("v1", "v2")
	v, g := ca.Version()
	assert.Equal(t, "v1", ca.Store.Version)
	assert.Equal(t, "v1", v)
	assert.Equal(t, "v2", ca.Store.GitHash)
	assert.Equal(t, "v2", g)

	cv := ca.Clone()
	assert.Equal(t, ca, cv)
}
