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

package checker

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/testutil"
	"github.com/matrixorigin/matrixcube/components/prophet/util/cache"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

const (
	KB = 1024
	MB = 1024 * KB
)

type testReplicaChecker struct {
	cluster *mockcluster.Cluster
	rc      *ReplicaChecker
}

func (s *testReplicaChecker) setup() {
	cfg := config.NewTestOptions()
	s.cluster = mockcluster.NewCluster(cfg)
	s.rc = NewReplicaChecker(s.cluster, cache.NewDefaultCache(10))
	stats := &metapb.StoreStats{
		Capacity:  100,
		Available: 100,
	}
	containers := []*core.CachedStore{
		core.NewCachedStore(
			&metadata.TestStore{
				CID:    1,
				CState: metapb.StoreState_Offline,
			},
			core.SetStoreStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
		core.NewCachedStore(
			&metadata.TestStore{
				CID:    3,
				CState: metapb.StoreState_UP,
			},
			core.SetStoreStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
		core.NewCachedStore(
			&metadata.TestStore{
				CID:    4,
				CState: metapb.StoreState_UP,
			}, core.SetStoreStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
	}
	for _, container := range containers {
		s.cluster.PutStore(container)
	}
	s.cluster.AddLabelsStore(2, 1, map[string]string{"noleader": "true"})
}

func (s *testReplicaChecker) downPeerAndCheck(t *testing.T, aliveRole metapb.ReplicaRole) *operator.Operator {
	s.cluster.SetMaxReplicas(2)
	s.cluster.SetStoreUP(1)
	downStoreID := uint64(3)
	peers := []metapb.Replica{
		{
			ID:          4,
			StoreID: 1,
			Role:        aliveRole,
		},
		{
			ID:          14,
			StoreID: downStoreID,
		},
		{
			ID:          15,
			StoreID: 4,
		},
	}
	r := core.NewCachedShard(&metadata.TestShard{ResID: 2, ResPeers: peers}, &peers[0])
	s.cluster.PutShard(r)
	s.cluster.SetStoreDown(downStoreID)
	downPeer := metapb.ReplicaStats{
		Replica: metapb.Replica{
			ID:          14,
			StoreID: downStoreID,
		},
		DownSeconds: 24 * 60 * 60,
	}
	r = r.Clone(core.WithDownPeers(append(r.GetDownPeers(), downPeer)))
	assert.Equal(t, 1, len(r.GetDownPeers()))
	return s.rc.Check(r)
}

func TestReplacePendingPeer(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	peers := []metapb.Replica{
		{
			ID:          2,
			StoreID: 1,
		},
		{
			ID:          3,
			StoreID: 2,
		},
		{
			ID:          4,
			StoreID: 3,
		},
	}
	r := core.NewCachedShard(&metadata.TestShard{ResID: 1, ResPeers: peers}, &peers[1], core.WithPendingPeers(peers[0:1]))
	s.cluster.PutShard(r)
	op := s.rc.Check(r)
	assert.NotNil(t, op)
	assert.Equal(t, uint64(4), op.Step(0).(operator.AddLearner).ToStore)
	assert.Equal(t, uint64(4), op.Step(1).(operator.PromoteLearner).ToStore)
	assert.Equal(t, uint64(1), op.Step(2).(operator.RemovePeer).FromStore)
}

func TestReplaceOfflinePeer(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	s.cluster.SetLabelPropertyConfig(config.LabelPropertyConfig{
		opt.RejectLeader: {{Key: "noleader", Value: "true"}},
	})
	peers := []metapb.Replica{
		{
			ID:          4,
			StoreID: 1,
		},
		{
			ID:          5,
			StoreID: 2,
		},
		{
			ID:          6,
			StoreID: 3,
		},
	}
	r := core.NewCachedShard(&metadata.TestShard{ResID: 2, ResPeers: peers}, &peers[0])
	s.cluster.PutShard(r)
	op := s.rc.Check(r)
	assert.NotNil(t, op)
	assert.Equal(t, uint64(3), op.Step(0).(operator.TransferLeader).ToStore)
	assert.Equal(t, uint64(4), op.Step(1).(operator.AddLearner).ToStore)
	assert.Equal(t, uint64(4), op.Step(2).(operator.PromoteLearner).ToStore)
	assert.Equal(t, uint64(1), op.Step(3).(operator.RemovePeer).FromStore)
}

func TestOfflineWithOneReplica(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	s.cluster.SetMaxReplicas(1)
	peers := []metapb.Replica{
		{
			ID:          4,
			StoreID: 1,
		},
	}
	r := core.NewCachedShard(&metadata.TestShard{ResID: 2, ResPeers: peers}, &peers[0])
	s.cluster.PutShard(r)
	op := s.rc.Check(r)
	assert.NotNil(t, op)
	assert.Equal(t, "replace-offline-replica", op.Desc())
}

func TestFillReplicas(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxSnapshotCount(2)
	rc := NewReplicaChecker(tc, cache.NewDefaultCache(10))

	// Add containers 1,2,3
	tc.AddShardStore(1, 1)
	tc.AddShardStore(2, 1)
	tc.AddShardStore(3, 1)

	res := core.NewTestCachedShard(nil, nil)
	res.Meta.SetPeers([]metapb.Replica{{ID: 1, StoreID: 1}})
	err := rc.FillReplicas(res, 0)
	assert.Error(t, err)

	res.Meta.SetPeers(nil)
	err = rc.FillReplicas(res, 0)
	assert.NoError(t, err)
	assert.Equal(t, rc.cluster.GetOpts().GetMaxReplicas(), len(res.Meta.Peers()))
}

func TestDownPeer(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	// down a peer, the number of normal peers(except learner) is enough.
	op := s.downPeerAndCheck(t, metapb.ReplicaRole_Voter)
	assert.NotNil(t, op)
	assert.Equal(t, "remove-extra-down-replica", op.Desc())

	// down a peer,the number of peers(except learner) is not enough.
	op = s.downPeerAndCheck(t, metapb.ReplicaRole_Learner)
	assert.NotNil(t, op)
	assert.Equal(t, "replace-down-replica", op.Desc())
}

func TestReplicaCheckerBasic(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxSnapshotCount(2)
	rc := NewReplicaChecker(tc, cache.NewDefaultCache(10))

	// Add containers 1,2,3,4.
	tc.AddShardStore(1, 4)
	tc.AddShardStore(2, 3)
	tc.AddShardStore(3, 2)
	tc.AddShardStore(4, 1)
	// Add resource 1 with leader in container 1 and follower in container 2.
	tc.AddLeaderShard(1, 1, 2)

	// resource has 2 peers, we need to add a new peer.
	resource := tc.GetShard(1)
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 4)

	// Disable make up replica feature.
	tc.SetEnableMakeUpReplica(false)
	assert.Nil(t, rc.Check(resource))
	tc.SetEnableMakeUpReplica(true)

	// Test healthFilter.
	// If container 4 is down, we add to container 3.
	tc.SetStoreDown(4)
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 3)
	tc.SetStoreUP(4)
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 4)

	// Test snapshotCountFilter.
	// If snapshotCount > MaxSnapshotCount, we add to container 3.
	tc.UpdateSnapshotCount(4, 3)
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 3)
	// If snapshotCount < MaxSnapshotCount, we can add peer again.
	tc.UpdateSnapshotCount(4, 1)
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 4)

	// Add peer in container 4, and we have enough replicas.
	peer4, _ := tc.AllocPeer(4)
	resource = resource.Clone(core.WithAddPeer(peer4))
	assert.Nil(t, rc.Check(resource))

	// Add peer in container 3, and we have redundant replicas.
	peer3, _ := tc.AllocPeer(3)
	resource = resource.Clone(core.WithAddPeer(peer3))
	testutil.CheckRemovePeer(t, rc.Check(resource), 1)

	// Disable remove extra replica feature.
	tc.SetEnableRemoveExtraReplica(false)
	assert.Nil(t, rc.Check(resource))
	tc.SetEnableRemoveExtraReplica(true)

	p, ok := resource.GetStorePeer(3)
	assert.True(t, ok)
	resource = resource.Clone(core.WithRemoveStorePeer(1), core.WithLeader(&p))

	// Peer in container 2 is down, remove it.
	tc.SetStoreDown(2)
	p, ok = resource.GetStorePeer(2)
	assert.True(t, ok)
	downPeer := metapb.ReplicaStats{
		Replica:     p,
		DownSeconds: 24 * 60 * 60,
	}

	resource = resource.Clone(core.WithDownPeers(append(resource.GetDownPeers(), downPeer)))
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 2, 1)
	resource = resource.Clone(core.WithDownPeers(nil))
	assert.Nil(t, rc.Check(resource))

	// Peer in container 3 is offline, transfer peer to container 1.
	tc.SetStoreOffline(3)
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 3, 1)
}

func TestLostStores(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.AddShardStore(1, 1)
	tc.AddShardStore(2, 1)

	rc := NewReplicaChecker(tc, cache.NewDefaultCache(10))

	// now resource peer in container 1,2,3.but we just have container 1,2
	// This happens only in recovering the PD tc
	// should not panic
	tc.AddLeaderShard(1, 1, 2, 3)
	resource := tc.GetShard(1)
	op := rc.Check(resource)
	assert.Nil(t, op)
}

func TestOffline(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "rack", "host"})

	rc := NewReplicaChecker(tc, cache.NewDefaultCache(10))

	tc.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 2, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(3, 3, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(4, 4, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})

	tc.AddLeaderShard(1, 1)
	resource := tc.GetShard(1)

	// container 2 has different zone and smallest resource score.
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 2)
	peer2, _ := tc.AllocPeer(2)
	resource = resource.Clone(core.WithAddPeer(peer2))

	// container 3 has different zone and smallest resource score.
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 3)
	peer3, _ := tc.AllocPeer(3)
	resource = resource.Clone(core.WithAddPeer(peer3))

	// container 4 has the same zone with container 3 and larger resource score.
	peer4, _ := tc.AllocPeer(4)
	resource = resource.Clone(core.WithAddPeer(peer4))
	testutil.CheckRemovePeer(t, rc.Check(resource), 4)

	// Test offline
	// the number of resource peers more than the maxReplicas
	// remove the peer
	tc.SetStoreOffline(3)
	testutil.CheckRemovePeer(t, rc.Check(resource), 3)
	resource = resource.Clone(core.WithRemoveStorePeer(4))
	// the number of resource peers equals the maxReplicas
	// Transfer peer to container 4.
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 3, 4)

	// container 5 has a same label score with container 4,but the resource score smaller than container 4, we will choose container 5.
	tc.AddLabelsStore(5, 3, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 3, 5)
	// container 5 has too many snapshots, choose container 4
	tc.UpdateSnapshotCount(5, 10)
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 3, 4)
	tc.UpdatePendingPeerCount(4, 30)
	assert.Nil(t, rc.Check(resource))
}

func TestDistinctScore(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "rack", "host"})

	rc := NewReplicaChecker(tc, cache.NewDefaultCache(10))

	tc.AddLabelsStore(1, 9, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 8, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})

	// We need 3 replicas.
	tc.AddLeaderShard(1, 1)
	resource := tc.GetShard(1)
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 2)
	peer2, _ := tc.AllocPeer(2)
	resource = resource.Clone(core.WithAddPeer(peer2))

	// container 1,2,3 have the same zone, rack, and host.
	tc.AddLabelsStore(3, 5, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 3)

	// container 4 has smaller resource score.
	tc.AddLabelsStore(4, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 4)

	// container 5 has a different host.
	tc.AddLabelsStore(5, 5, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 5)

	// container 6 has a different rack.
	tc.AddLabelsStore(6, 6, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 6)

	// container 7 has a different zone.
	tc.AddLabelsStore(7, 7, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 7)

	// Test stateFilter.
	tc.SetStoreOffline(7)
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 6)
	tc.SetStoreUP(7)
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 7)

	// Add peer to container 7.
	peer7, _ := tc.AllocPeer(7)
	resource = resource.Clone(core.WithAddPeer(peer7))

	// Replace peer in container 1 with container 6 because it has a different rack.
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 1, 6)
	// Disable locationReplacement feature.
	tc.SetEnableLocationReplacement(false)
	assert.Nil(t, rc.Check(resource))
	tc.SetEnableLocationReplacement(true)
	peer6, _ := tc.AllocPeer(6)
	resource = resource.Clone(core.WithAddPeer(peer6))
	testutil.CheckRemovePeer(t, rc.Check(resource), 1)
	p, ok := resource.GetStorePeer(2)
	assert.True(t, ok)
	resource = resource.Clone(core.WithRemoveStorePeer(1), core.WithLeader(&p))
	assert.Nil(t, rc.Check(resource))

	// container 8 has the same zone and different rack with container 7.
	// container 1 has the same zone and different rack with container 6.
	// So container 8 and container 1 are equivalent.
	tc.AddLabelsStore(8, 1, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"})
	assert.Nil(t, rc.Check(resource))

	// container 10 has a different zone.
	// container 2 and 6 have the same distinct score, but container 2 has larger resource score.
	// So replace peer in container 2 with container 10.
	tc.AddLabelsStore(10, 1, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 2, 10)
	peer10, _ := tc.AllocPeer(10)
	resource = resource.Clone(core.WithAddPeer(peer10))
	testutil.CheckRemovePeer(t, rc.Check(resource), 2)
	resource = resource.Clone(core.WithRemoveStorePeer(2))
	assert.Nil(t, rc.Check(resource))
}

func TestDistinctScore2(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(5)
	tc.SetLocationLabels([]string{"zone", "host"})

	rc := NewReplicaChecker(tc, cache.NewDefaultCache(10))

	tc.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 1, map[string]string{"zone": "z1", "host": "h2"})
	tc.AddLabelsStore(3, 1, map[string]string{"zone": "z1", "host": "h3"})
	tc.AddLabelsStore(4, 1, map[string]string{"zone": "z2", "host": "h1"})
	tc.AddLabelsStore(5, 1, map[string]string{"zone": "z2", "host": "h2"})
	tc.AddLabelsStore(6, 1, map[string]string{"zone": "z3", "host": "h1"})

	tc.AddLeaderShard(1, 1, 2, 4)
	resource := tc.GetShard(1)

	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 6)
	peer6, _ := tc.AllocPeer(6)
	resource = resource.Clone(core.WithAddPeer(peer6))

	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 5)
	peer5, _ := tc.AllocPeer(5)
	resource = resource.Clone(core.WithAddPeer(peer5))

	assert.Nil(t, rc.Check(resource))
}

func TestStorageThreshold(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetLocationLabels([]string{"zone"})
	rc := NewReplicaChecker(tc, cache.NewDefaultCache(10))

	tc.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	tc.UpdateStorageRatio(1, 0.5, 0.5)
	tc.UpdateStoreShardSize(1, 500*MB)
	tc.AddLabelsStore(2, 1, map[string]string{"zone": "z1"})
	tc.UpdateStorageRatio(2, 0.1, 0.9)
	tc.UpdateStoreShardSize(2, 100*MB)
	tc.AddLabelsStore(3, 1, map[string]string{"zone": "z2"})
	tc.AddLabelsStore(4, 31, map[string]string{"zone": "z3"})

	tc.AddLeaderShard(1, 1, 2, 3)
	resource := tc.GetShard(1)

	// Move peer to better location.
	tc.UpdateStorageRatio(4, 0, 1)
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 1, 4)
	// If container4 is almost full, do not add peer on it.
	tc.UpdateStorageRatio(4, 0.9, 0.1)
	assert.Nil(t, rc.Check(resource))

	tc.AddLeaderShard(2, 1, 3)
	resource = tc.GetShard(2)
	// Add peer on container4.
	tc.UpdateStorageRatio(4, 0, 1)
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 4)
	// If container4 is almost full, do not add peer on it.
	tc.UpdateStorageRatio(4, 0.8, 0)
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 2)
}

func TestOpts(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	rc := NewReplicaChecker(tc, cache.NewDefaultCache(10))

	tc.AddShardStore(1, 100)
	tc.AddShardStore(2, 100)
	tc.AddShardStore(3, 100)
	tc.AddShardStore(4, 100)
	tc.AddLeaderShard(1, 1, 2, 3)

	resource := tc.GetShard(1)
	// Test remove down replica and replace offline replica.
	tc.SetStoreDown(1)
	p, ok := resource.GetStorePeer(1)
	assert.True(t, ok)
	resource = resource.Clone(core.WithDownPeers([]metapb.ReplicaStats{
		{
			Replica:     p,
			DownSeconds: 24 * 60 * 60,
		},
	}))
	tc.SetStoreOffline(2)
	// RemoveDownReplica has higher priority than replaceOfflineReplica.
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 1, 4)
	tc.SetEnableRemoveDownReplica(false)
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 2, 4)
	tc.SetEnableReplaceOfflineReplica(false)
	assert.Nil(t, rc.Check(resource))
}
