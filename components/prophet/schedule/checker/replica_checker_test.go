package checker

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/testutil"
	"github.com/matrixorigin/matrixcube/components/prophet/util/cache"
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
	stats := &rpcpb.ContainerStats{
		Capacity:  100,
		Available: 100,
	}
	containers := []*core.CachedContainer{
		core.NewCachedContainer(
			&metadata.TestContainer{
				CID:    1,
				CState: metapb.ContainerState_Offline,
			},
			core.SetContainerStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
		core.NewCachedContainer(
			&metadata.TestContainer{
				CID:    3,
				CState: metapb.ContainerState_UP,
			},
			core.SetContainerStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
		core.NewCachedContainer(
			&metadata.TestContainer{
				CID:    4,
				CState: metapb.ContainerState_UP,
			}, core.SetContainerStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
	}
	for _, container := range containers {
		s.cluster.PutContainer(container)
	}
	s.cluster.AddLabelsContainer(2, 1, map[string]string{"noleader": "true"})
}

func TestReplacePendingPeer(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	peers := []metapb.Peer{
		{
			ID:          2,
			ContainerID: 1,
		},
		{
			ID:          3,
			ContainerID: 2,
		},
		{
			ID:          4,
			ContainerID: 3,
		},
	}
	r := core.NewCachedResource(&metadata.TestResource{ResID: 1, ResPeers: peers}, &peers[1], core.WithPendingPeers(peers[0:1]))
	s.cluster.PutResource(r)
	op := s.rc.Check(r)
	assert.NotNil(t, op)
	assert.Equal(t, uint64(4), op.Step(0).(operator.AddLearner).ToContainer)
	assert.Equal(t, uint64(4), op.Step(1).(operator.PromoteLearner).ToContainer)
	assert.Equal(t, uint64(1), op.Step(2).(operator.RemovePeer).FromContainer)
}

func TestReplaceOfflinePeer(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	s.cluster.SetLabelPropertyConfig(config.LabelPropertyConfig{
		opt.RejectLeader: {{Key: "noleader", Value: "true"}},
	})
	peers := []metapb.Peer{
		{
			ID:          4,
			ContainerID: 1,
		},
		{
			ID:          5,
			ContainerID: 2,
		},
		{
			ID:          6,
			ContainerID: 3,
		},
	}
	r := core.NewCachedResource(&metadata.TestResource{ResID: 2, ResPeers: peers}, &peers[0])
	s.cluster.PutResource(r)
	op := s.rc.Check(r)
	assert.NotNil(t, op)
	assert.Equal(t, uint64(3), op.Step(0).(operator.TransferLeader).ToContainer)
	assert.Equal(t, uint64(4), op.Step(1).(operator.AddLearner).ToContainer)
	assert.Equal(t, uint64(4), op.Step(2).(operator.PromoteLearner).ToContainer)
	assert.Equal(t, uint64(1), op.Step(3).(operator.RemovePeer).FromContainer)
}

func TestOfflineWithOneReplica(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	s.cluster.SetMaxReplicas(1)
	peers := []metapb.Peer{
		{
			ID:          4,
			ContainerID: 1,
		},
	}
	r := core.NewCachedResource(&metadata.TestResource{ResID: 2, ResPeers: peers}, &peers[0])
	s.cluster.PutResource(r)
	op := s.rc.Check(r)
	assert.NotNil(t, op)
	assert.Equal(t, "replace-offline-replica", op.Desc())
}

func TestReplicaCheckerBasic(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxSnapshotCount(2)
	rc := NewReplicaChecker(tc, cache.NewDefaultCache(10))

	// Add containers 1,2,3,4.
	tc.AddResourceContainer(1, 4)
	tc.AddResourceContainer(2, 3)
	tc.AddResourceContainer(3, 2)
	tc.AddResourceContainer(4, 1)
	// Add resource 1 with leader in container 1 and follower in container 2.
	tc.AddLeaderResource(1, 1, 2)

	// resource has 2 peers, we need to add a new peer.
	resource := tc.GetResource(1)
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 4)

	// Disable make up replica feature.
	tc.SetEnableMakeUpReplica(false)
	assert.Nil(t, rc.Check(resource))
	tc.SetEnableMakeUpReplica(true)

	// Test healthFilter.
	// If container 4 is down, we add to container 3.
	tc.SetContainerDown(4)
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 3)
	tc.SetContainerUP(4)
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

	p, ok := resource.GetContainerPeer(3)
	assert.True(t, ok)
	resource = resource.Clone(core.WithRemoveContainerPeer(1), core.WithLeader(&p))

	// Peer in container 2 is down, remove it.
	tc.SetContainerDown(2)
	p, ok = resource.GetContainerPeer(2)
	assert.True(t, ok)
	downPeer := metapb.PeerStats{
		Peer:        p,
		DownSeconds: 24 * 60 * 60,
	}

	resource = resource.Clone(core.WithDownPeers(append(resource.GetDownPeers(), downPeer)))
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 2, 1)
	resource = resource.Clone(core.WithDownPeers(nil))
	assert.Nil(t, rc.Check(resource))

	// Peer in container 3 is offline, transfer peer to container 1.
	tc.SetContainerOffline(3)
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 3, 1)
}

func TestLostContainers(t *testing.T) {
	s := &testReplicaChecker{}
	s.setup()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.AddResourceContainer(1, 1)
	tc.AddResourceContainer(2, 1)

	rc := NewReplicaChecker(tc, cache.NewDefaultCache(10))

	// now resource peer in container 1,2,3.but we just have container 1,2
	// This happens only in recovering the PD tc
	// should not panic
	tc.AddLeaderResource(1, 1, 2, 3)
	resource := tc.GetResource(1)
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

	tc.AddLabelsContainer(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsContainer(2, 2, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.AddLabelsContainer(3, 3, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.AddLabelsContainer(4, 4, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})

	tc.AddLeaderResource(1, 1)
	resource := tc.GetResource(1)

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
	tc.SetContainerOffline(3)
	testutil.CheckRemovePeer(t, rc.Check(resource), 3)
	resource = resource.Clone(core.WithRemoveContainerPeer(4))
	// the number of resource peers equals the maxReplicas
	// Transfer peer to container 4.
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 3, 4)

	// container 5 has a same label score with container 4,but the resource score smaller than container 4, we will choose container 5.
	tc.AddLabelsContainer(5, 3, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})
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

	tc.AddLabelsContainer(1, 9, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsContainer(2, 8, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})

	// We need 3 replicas.
	tc.AddLeaderResource(1, 1)
	resource := tc.GetResource(1)
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 2)
	peer2, _ := tc.AllocPeer(2)
	resource = resource.Clone(core.WithAddPeer(peer2))

	// container 1,2,3 have the same zone, rack, and host.
	tc.AddLabelsContainer(3, 5, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 3)

	// container 4 has smaller resource score.
	tc.AddLabelsContainer(4, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 4)

	// container 5 has a different host.
	tc.AddLabelsContainer(5, 5, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 5)

	// container 6 has a different rack.
	tc.AddLabelsContainer(6, 6, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 6)

	// container 7 has a different zone.
	tc.AddLabelsContainer(7, 7, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 7)

	// Test stateFilter.
	tc.SetContainerOffline(7)
	testutil.CheckAddPeer(t, rc.Check(resource), operator.OpReplica, 6)
	tc.SetContainerUP(7)
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
	p, ok := resource.GetContainerPeer(2)
	assert.True(t, ok)
	resource = resource.Clone(core.WithRemoveContainerPeer(1), core.WithLeader(&p))
	assert.Nil(t, rc.Check(resource))

	// container 8 has the same zone and different rack with container 7.
	// container 1 has the same zone and different rack with container 6.
	// So container 8 and container 1 are equivalent.
	tc.AddLabelsContainer(8, 1, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"})
	assert.Nil(t, rc.Check(resource))

	// container 10 has a different zone.
	// container 2 and 6 have the same distinct score, but container 2 has larger resource score.
	// So replace peer in container 2 with container 10.
	tc.AddLabelsContainer(10, 1, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 2, 10)
	peer10, _ := tc.AllocPeer(10)
	resource = resource.Clone(core.WithAddPeer(peer10))
	testutil.CheckRemovePeer(t, rc.Check(resource), 2)
	resource = resource.Clone(core.WithRemoveContainerPeer(2))
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

	tc.AddLabelsContainer(1, 1, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsContainer(2, 1, map[string]string{"zone": "z1", "host": "h2"})
	tc.AddLabelsContainer(3, 1, map[string]string{"zone": "z1", "host": "h3"})
	tc.AddLabelsContainer(4, 1, map[string]string{"zone": "z2", "host": "h1"})
	tc.AddLabelsContainer(5, 1, map[string]string{"zone": "z2", "host": "h2"})
	tc.AddLabelsContainer(6, 1, map[string]string{"zone": "z3", "host": "h1"})

	tc.AddLeaderResource(1, 1, 2, 4)
	resource := tc.GetResource(1)

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

	tc.AddLabelsContainer(1, 1, map[string]string{"zone": "z1"})
	tc.UpdateStorageRatio(1, 0.5, 0.5)
	tc.UpdateContainerResourceSize(1, 500*MB)
	tc.AddLabelsContainer(2, 1, map[string]string{"zone": "z1"})
	tc.UpdateStorageRatio(2, 0.1, 0.9)
	tc.UpdateContainerResourceSize(2, 100*MB)
	tc.AddLabelsContainer(3, 1, map[string]string{"zone": "z2"})
	tc.AddLabelsContainer(4, 0, map[string]string{"zone": "z3"})

	tc.AddLeaderResource(1, 1, 2, 3)
	resource := tc.GetResource(1)

	// Move peer to better location.
	tc.UpdateStorageRatio(4, 0, 1)
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 1, 4)
	// If container4 is almost full, do not add peer on it.
	tc.UpdateStorageRatio(4, 0.9, 0.1)
	assert.Nil(t, rc.Check(resource))

	tc.AddLeaderResource(2, 1, 3)
	resource = tc.GetResource(2)
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

	tc.AddResourceContainer(1, 100)
	tc.AddResourceContainer(2, 100)
	tc.AddResourceContainer(3, 100)
	tc.AddResourceContainer(4, 100)
	tc.AddLeaderResource(1, 1, 2, 3)

	resource := tc.GetResource(1)
	// Test remove down replica and replace offline replica.
	tc.SetContainerDown(1)
	p, ok := resource.GetContainerPeer(1)
	assert.True(t, ok)
	resource = resource.Clone(core.WithDownPeers([]metapb.PeerStats{
		{
			Peer:        p,
			DownSeconds: 24 * 60 * 60,
		},
	}))
	tc.SetContainerOffline(2)
	// RemoveDownReplica has higher priority than replaceOfflineReplica.
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 1, 4)
	tc.SetEnableRemoveDownReplica(false)
	testutil.CheckTransferPeer(t, rc.Check(resource), operator.OpReplica, 2, 4)
	tc.SetEnableReplaceOfflineReplica(false)
	assert.Nil(t, rc.Check(resource))
}
