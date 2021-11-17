package raftstore

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/util/task"
	"github.com/stretchr/testify/assert"
)

func TestResourceAdapter(t *testing.T) {
	ma := NewResourceAdapterWithShard(Shard{}).(*resourceAdapter)

	data := []byte("data")
	ma.SetData(data)
	assert.Equal(t, data, ma.meta.Data)
	assert.Equal(t, data, ma.Data())

	ma.SetStartKey(data)
	assert.Equal(t, data, ma.meta.Start)
	ma.SetEndKey(data)
	assert.Equal(t, data, ma.meta.End)
	s, e := ma.Range()
	assert.Equal(t, data, s)
	assert.Equal(t, data, e)

	epoch := metapb.ResourceEpoch{Version: 1, ConfVer: 2}
	ma.SetEpoch(epoch)
	assert.Equal(t, epoch, ma.meta.Epoch)
	assert.Equal(t, epoch, ma.Epoch())

	ma.SetGroup(1)
	assert.Equal(t, uint64(1), ma.meta.Group)
	assert.Equal(t, uint64(1), ma.Group())

	ma.SetID(1)
	assert.Equal(t, uint64(1), ma.meta.ID)
	assert.Equal(t, uint64(1), ma.ID())

	peers := []Replica{{ID: 1, ContainerID: 1}, {ID: 2, ContainerID: 2}}
	ma.SetPeers(peers)
	assert.Equal(t, peers, ma.meta.Replicas)
	assert.Equal(t, peers, ma.Peers())

	rules := []string{"r1", "r2"}
	ma.SetRuleGroups(rules...)
	assert.Equal(t, rules, ma.meta.RuleGroups)
	assert.Equal(t, rules, ma.RuleGroups())

	ma.SetState(metapb.ResourceState_Destroyed)
	assert.Equal(t, metapb.ResourceState_Destroyed, ma.meta.State)
	assert.Equal(t, metapb.ResourceState_Destroyed, ma.State())

	ma.SetUnique("unique")
	assert.Equal(t, "unique", ma.meta.Unique)
	assert.Equal(t, "unique", ma.Unique())

	v := ma.Clone()
	assert.Equal(t, ma, v)
}

func TestContainerAdapter(t *testing.T) {
	ca := newContainerAdapter().(*containerAdapter)

	ca.SetAddrs("a1", "a2")
	assert.Equal(t, "a1", ca.meta.ClientAddr)
	assert.Equal(t, "a2", ca.meta.RaftAddr)
	assert.Equal(t, "a1", ca.Addr())
	assert.Equal(t, "a2", ca.ShardAddr())

	ca.SetDeployPath("dp")
	assert.Equal(t, "dp", ca.meta.DeployPath)
	assert.Equal(t, "dp", ca.DeployPath())

	ca.SetID(1)
	assert.Equal(t, uint64(1), ca.meta.ID)
	assert.Equal(t, uint64(1), ca.ID())

	labels := []metapb.Pair{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}}
	ca.SetLabels(labels)
	assert.Equal(t, labels, ca.meta.Labels)
	assert.Equal(t, labels, ca.Labels())

	ca.SetLastHeartbeat(1)
	assert.Equal(t, int64(1), ca.meta.LastHeartbeatTime)
	assert.Equal(t, int64(1), ca.LastHeartbeat())

	ca.SetPhysicallyDestroyed(true)
	assert.True(t, ca.meta.PhysicallyDestroyed)
	assert.True(t, ca.PhysicallyDestroyed())

	ca.SetStartTimestamp(1)
	assert.Equal(t, int64(1), ca.meta.StartTime)
	assert.Equal(t, int64(1), ca.StartTimestamp())

	ca.SetState(metapb.ContainerState_Tombstone)
	assert.Equal(t, metapb.ContainerState_Tombstone, ca.meta.State)
	assert.Equal(t, metapb.ContainerState_Tombstone, ca.State())

	ca.SetVersion("v1", "v2")
	v, g := ca.Version()
	assert.Equal(t, "v1", ca.meta.Version)
	assert.Equal(t, "v1", v)
	assert.Equal(t, "v2", ca.meta.GitHash)
	assert.Equal(t, "v2", g)

	cv := ca.Clone()
	assert.Equal(t, ca, cv)
}

func TestDoShardHeartbeat(t *testing.T) {
	cases := []struct {
		pr        *replica
		action    action
		hasAction bool
	}{
		{
			pr:        &replica{leaderID: 1, startedC: make(chan struct{}), actions: task.New(32)},
			hasAction: false,
		},
		{
			pr:        &replica{startedC: make(chan struct{}), actions: task.New(32)},
			hasAction: true,
			action:    action{actionType: heartbeatAction},
		},
	}

	for _, c := range cases {
		s := NewSingleTestClusterStore(t).GetStore(0).(*store)
		c.pr.store = s
		close(c.pr.startedC)
		s.addReplica(c.pr)
		s.doShardHeartbeat()
		assert.Equal(t, c.hasAction, c.pr.actions.Len() > 0)
		if c.hasAction {
			v, err := c.pr.actions.Peek()
			assert.NoError(t, err)
			assert.Equal(t, c.action, v)
		}
	}
}

func TestGetStoreHeartbeat(t *testing.T) {
	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	s.addReplica(&replica{shardID: 1})
	s.addReplica(&replica{shardID: 2})
	s.trans = &testTransport{}
	req, err := s.getStoreHeartbeat(time.Now())
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), req.Stats.ResourceCount)
}

func TestDoResourceHeartbeatRsp(t *testing.T) {
	cases := []struct {
		rsp      rpcpb.ResourceHeartbeatRsp
		fn       func(*store) *replica
		adminReq rpc.AdminRequest
	}{
		{
			rsp: rpcpb.ResourceHeartbeatRsp{ResourceID: 1, ConfigChange: &rpcpb.ConfigChange{
				Replica:    metapb.Replica{ID: 1, ContainerID: 1},
				ChangeType: metapb.ConfigChangeType_AddLearnerNode,
			}},
			fn: func(s *store) *replica {
				pr := &replica{shardID: 1, startedC: make(chan struct{}), requests: task.New(32), actions: task.New(32)}
				pr.store = s
				close(pr.startedC)
				s.addReplica(pr)
				return pr
			},
			adminReq: rpc.AdminRequest{
				CmdType: rpc.AdminCmdType_ConfigChange,
				ConfigChange: &rpc.ConfigChangeRequest{
					ChangeType: metapb.ConfigChangeType_AddLearnerNode,
					Replica:    metapb.Replica{ID: 1, ContainerID: 1},
				},
			},
		},
		{
			rsp: rpcpb.ResourceHeartbeatRsp{ResourceID: 1, TransferLeader: &rpcpb.TransferLeader{
				Replica: metapb.Replica{ID: 1, ContainerID: 1},
			}},
			fn: func(s *store) *replica {
				pr := &replica{shardID: 1, startedC: make(chan struct{}), requests: task.New(32), actions: task.New(32)}
				pr.store = s
				close(pr.startedC)
				s.addReplica(pr)
				return pr
			},
			adminReq: rpc.AdminRequest{
				CmdType: rpc.AdminCmdType_TransferLeader,
				TransferLeader: &rpc.TransferLeaderRequest{
					Replica: metapb.Replica{ID: 1, ContainerID: 1},
				},
			},
		},
	}

	for _, c := range cases {
		s := NewSingleTestClusterStore(t).GetStore(0).(*store)
		s.workerPool.close() // avoid admin request real handled by event worker
		pr := c.fn(s)
		pr.sm = &stateMachine{}
		pr.sm.metadataMu.shard = Shard{}
		s.doResourceHeartbeatRsp(c.rsp)

		v, err := pr.requests.Peek()
		assert.NoError(t, err)
		assert.Equal(t, c.adminReq, v.(reqCtx).admin)
	}
}
