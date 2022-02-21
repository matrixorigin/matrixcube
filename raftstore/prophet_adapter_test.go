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

package raftstore

import (
	"testing"
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/transport"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/task"
	"github.com/stretchr/testify/assert"
)

func TestShardAdapter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ma := NewShardAdapterWithShard(Shard{}).(*resourceAdapter)

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

	epoch := metapb.ShardEpoch{Version: 1, ConfVer: 2}
	ma.SetEpoch(epoch)
	assert.Equal(t, epoch, ma.meta.Epoch)
	assert.Equal(t, epoch, ma.Epoch())

	ma.SetGroup(1)
	assert.Equal(t, uint64(1), ma.meta.Group)
	assert.Equal(t, uint64(1), ma.Group())

	ma.SetID(1)
	assert.Equal(t, uint64(1), ma.meta.ID)
	assert.Equal(t, uint64(1), ma.ID())

	peers := []Replica{{ID: 1, StoreID: 1}, {ID: 2, StoreID: 2}}
	ma.SetPeers(peers)
	assert.Equal(t, peers, ma.meta.Replicas)
	assert.Equal(t, peers, ma.Peers())

	rules := []string{"r1", "r2"}
	ma.SetRuleGroups(rules...)
	assert.Equal(t, rules, ma.meta.RuleGroups)
	assert.Equal(t, rules, ma.RuleGroups())

	ma.SetState(metapb.ShardState_Destroyed)
	assert.Equal(t, metapb.ShardState_Destroyed, ma.meta.State)
	assert.Equal(t, metapb.ShardState_Destroyed, ma.State())

	ma.SetUnique("unique")
	assert.Equal(t, "unique", ma.meta.Unique)
	assert.Equal(t, "unique", ma.Unique())

	v := ma.Clone()
	assert.Equal(t, ma, v)
}

func TestStoreAdapter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ca := newStoreAdapter().(*containerAdapter)

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

	ca.SetState(metapb.StoreState_StoreTombstone)
	assert.Equal(t, metapb.StoreState_StoreTombstone, ca.meta.State)
	assert.Equal(t, metapb.StoreState_StoreTombstone, ca.State())

	ca.SetVersion("v1", "v2")
	v, g := ca.Version()
	assert.Equal(t, "v1", ca.meta.Version)
	assert.Equal(t, "v1", v)
	assert.Equal(t, "v2", ca.meta.GitHash)
	assert.Equal(t, "v2", g)

	cv := ca.Clone()
	assert.Equal(t, ca, cv)
}

func TestGetStoreHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, cancel := newTestStore(t)
	defer cancel()

	s.addReplica(&replica{shardID: 1})
	s.addReplica(&replica{shardID: 2})
	s.trans = transport.NewTransport(nil, "", 0, nil, nil, nil, nil, nil, s.cfg.FS)
	defer s.trans.Close()
	req, err := s.getStoreHeartbeat(time.Now())
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), req.Stats.ShardCount)
}

func TestDoShardHeartbeatRsp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		rsp            rpcpb.ShardHeartbeatRsp
		fn             func(*store) *replica
		adminReq       protoc.PB
		adminTargetReq protoc.PB
	}{
		{
			rsp: rpcpb.ShardHeartbeatRsp{ShardID: 1, ConfigChange: &rpcpb.ConfigChange{
				Replica:    metapb.Replica{ID: 1, StoreID: 1},
				ChangeType: metapb.ConfigChangeType_AddLearnerNode,
			}},
			fn: func(s *store) *replica {
				pr := &replica{shardID: 1, startedC: make(chan struct{}), requests: task.New(32), actions: task.New(32)}
				pr.store = s
				close(pr.startedC)
				s.addReplica(pr)
				return pr
			},
			adminReq: &rpcpb.ConfigChangeRequest{
				ChangeType: metapb.ConfigChangeType_AddLearnerNode,
				Replica:    metapb.Replica{ID: 1, StoreID: 1},
			},
			adminTargetReq: &rpcpb.ConfigChangeRequest{},
		},
		{
			rsp: rpcpb.ShardHeartbeatRsp{ShardID: 1, TransferLeader: &rpcpb.TransferLeader{
				Replica: metapb.Replica{ID: 1, StoreID: 1},
			}},
			fn: func(s *store) *replica {
				pr := &replica{shardID: 1, startedC: make(chan struct{}), requests: task.New(32), actions: task.New(32)}
				pr.store = s
				close(pr.startedC)
				s.addReplica(pr)
				return pr
			},
			adminReq: &rpcpb.TransferLeaderRequest{
				Replica: metapb.Replica{ID: 1, StoreID: 1},
			},
			adminTargetReq: &rpcpb.TransferLeaderRequest{},
		},
	}

	for _, c := range cases {
		s, cancel := newTestStore(t)
		defer cancel()
		s.workerPool.close() // avoid admin request real handled by event worker
		pr := c.fn(s)
		pr.sm = &stateMachine{}
		pr.sm.metadataMu.shard = Shard{}
		s.doShardHeartbeatRsp(c.rsp)

		v, err := pr.requests.Peek()
		assert.NoError(t, err)

		protoc.MustUnmarshal(c.adminTargetReq, v.(reqCtx).req.Cmd)
		assert.Equal(t, c.adminReq, c.adminTargetReq)
	}
}
