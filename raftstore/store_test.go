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
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/transport"
	"github.com/matrixorigin/matrixcube/util"
	keysutil "github.com/matrixorigin/matrixcube/util/keys"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/task"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestStartAndStop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := NewSingleTestClusterStore(t)
	c.Start()
	defer c.Stop()
}

func TestSearchShard(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, cancel := newTestStore(t)
	defer cancel()
	tree := util.NewShardTree()
	s.keyRanges.Store(uint64(1), tree)
	tree.Update(Shard{ID: 1, Start: []byte("a"), End: []byte("b")})
	tree.Update(Shard{ID: 2, Start: []byte("b"), End: []byte("c")})
	tree.Update(Shard{ID: 3, Start: []byte("c"), End: []byte("d")})

	assert.Equal(t, uint64(1), s.searchShard(1, []byte("a")).ID)
	assert.Equal(t, uint64(2), s.searchShard(1, []byte("b")).ID)
	assert.Equal(t, uint64(3), s.searchShard(1, []byte("c")).ID)
	assert.Equal(t, uint64(0), s.searchShard(0, []byte("c")).ID)
}

func TestNextShard(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, cancel := newTestStore(t)
	defer cancel()
	tree := util.NewShardTree()
	s.keyRanges.Store(uint64(0), tree)
	tree.Update(Shard{ID: 1, Start: []byte("a"), End: []byte("b")})
	tree.Update(Shard{ID: 2, Start: []byte("b"), End: []byte("c")})
	tree.Update(Shard{ID: 3, Start: []byte("c"), End: []byte("d")})

	assert.Equal(t, uint64(1), s.nextShard(Shard{Start: []byte(""), End: []byte("a")}).ID)
	assert.Equal(t, uint64(2), s.nextShard(Shard{Start: []byte("a"), End: []byte("b")}).ID)
	assert.Equal(t, uint64(3), s.nextShard(Shard{Start: []byte("b"), End: []byte("c")}).ID)
	assert.Nil(t, s.nextShard(Shard{Start: []byte("c"), End: []byte("d")}))
	assert.Nil(t, s.nextShard(Shard{Group: 1, Start: []byte("c"), End: []byte("d")}))
}

func TestRemoveShardKeyRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, cancel := newTestStore(t)
	defer cancel()

	tree := util.NewShardTree()
	s.keyRanges.Store(uint64(0), tree)
	tree.Update(Shard{ID: 1, Start: []byte("a"), End: []byte("b")})

	assert.False(t, s.removeShardKeyRange(Shard{ID: 1, Start: []byte(""), End: []byte("a")}))
	assert.False(t, s.removeShardKeyRange(Shard{ID: 1, Group: 1, Start: []byte("a"), End: []byte("b")}))
	assert.True(t, s.removeShardKeyRange(Shard{ID: 1, Start: []byte("a"), End: []byte("b")}))
}

func TestUpdateShardKeyRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, cancel := newTestStore(t)
	defer cancel()

	s.updateShardKeyRange(0, Shard{ID: 1, Start: []byte("a"), End: []byte("b")})
	assert.Equal(t, uint64(1), s.searchShard(0, []byte("a")).ID)

	s.updateShardKeyRange(0, Shard{ID: 1, Start: []byte("a"), End: []byte("c")})
	assert.Equal(t, uint64(1), s.searchShard(0, []byte("b")).ID)
}

func TestStoreSelectShard(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, cancel := newTestStore(t)
	defer cancel()
	tree := util.NewShardTree()
	s.keyRanges.Store(uint64(0), tree)
	tree.Update(Shard{ID: 1, Start: []byte("a"), End: []byte("b")})

	_, err := s.selectShard(1, []byte("a"))
	assert.Error(t, err)

	_, err = s.selectShard(0, []byte("a"))
	assert.Error(t, err)

	s.addReplica(&replica{shardID: 1})
	v, err := s.selectShard(0, []byte("a"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), v.shardID)
}

func TestStoreRemoveReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, cancel := newTestStore(t)
	defer cancel()
	aware := newTestShardAware(0)
	s.aware = aware
	pr := &replica{shardID: 1, sm: &stateMachine{}}
	pr.sm.metadataMu.shard = Shard{ID: 1}
	s.addReplica(pr)
	assert.NotNil(t, s.getReplica(1, false))

	s.removeReplica(pr.getShard())
	assert.Nil(t, s.getReplica(1, false))

	assert.Equal(t, 1, len(aware.removed))
}

func TestValidateShard(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		pr    *replica
		req   rpcpb.RequestBatch
		epoch Epoch
		err   string
		ok    bool
	}{
		{
			pr:  &replica{shardID: 1, startedC: make(chan struct{}), actions: task.New(32)},
			req: rpcpb.RequestBatch{},
			err: errShardNotFound.Error(),
			ok:  true,
		},
		{
			pr:  &replica{replica: Replica{ID: 1}, startedC: make(chan struct{}), actions: task.New(32)},
			req: rpcpb.RequestBatch{},
			err: errNotLeader.Error(),
			ok:  true,
		},
		// FIXME:
		// the way how expected err is defined & checked below is incorrect, should
		// define an error type, e.g. ErrMismatchedReplica, use errors.Wrapf to attach
		// extra info when necessary and then use errors.Is(err, ErrMismatchedReplica)
		// to do error comparison
		{
			pr:  &replica{replica: Replica{ID: 1}, leaderID: 1, startedC: make(chan struct{}), actions: task.New(32)},
			req: rpcpb.RequestBatch{},
			err: "mismatch replica id, want 1, but 0",
			ok:  true,
		},
		{
			pr:    &replica{replica: Replica{ID: 1}, leaderID: 1, startedC: make(chan struct{}), actions: task.New(32)},
			epoch: Epoch{Generation: 1},
			req:   rpcpb.RequestBatch{Header: rpcpb.RequestBatchHeader{Replica: Replica{ID: 1}}, Requests: []rpcpb.Request{{}}},
			err:   errStaleEpoch.Error(),
			ok:    true,
		},
	}

	for idx, c := range cases {
		func() {
			s, cancel := newTestStore(t)
			defer cancel()
			c.pr.store = s
			c.pr.replicaID = c.pr.replica.ID
			c.pr.sm = &stateMachine{}
			c.pr.sm.metadataMu.shard = Shard{ID: c.pr.shardID, Epoch: c.epoch}
			close(c.pr.startedC)
			s.addReplica(c.pr)

			err, ok := s.validateShard(c.req)
			assert.Equal(t, c.ok, ok, "index %d", idx)
			assert.Equal(t, c.err, err.Message, "index %d", idx)
		}()
	}
}

func TestVacuum(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, cancel := newTestStore(t)
	defer cancel()
	kv := s.DataStorageByGroup(0).(storage.KVStorageWrapper).GetKVStorage()
	shard := Shard{Start: []byte("a"), End: []byte("b"), Replicas: []Replica{{ID: 1}, {ID: 2}}}
	assert.NoError(t, kv.Set(keysutil.EncodeDataKey([]byte("a1"), nil), []byte("hello"), false))
	assert.NoError(t, kv.Set(keysutil.EncodeDataKey([]byte("a2"), nil), []byte("hello"), false))
	assert.NoError(t, s.vacuum(vacuumTask{
		shard:      shard,
		removeData: true,
	}))

	c := 0
	assert.NoError(t, kv.Scan(keysutil.EncodeShardStart(shard.Start, nil), keysutil.EncodeShardEnd(shard.End, nil), func(key, value []byte) (bool, error) {
		c++
		return true, nil
	}, false))
	assert.Equal(t, 0, c)
}

func TestCheckEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		req   rpcpb.RequestBatch
		shard Shard
		ok    bool
	}{
		{
			req:   newTestAdminRequestBatch("", 0, rpcpb.CmdBatchSplit, nil),
			shard: Shard{},
			ok:    true,
		},
		{
			req:   newTestAdminRequestBatch("", 0, rpcpb.CmdBatchSplit, nil),
			shard: Shard{Epoch: Epoch{Generation: 1}},
			ok:    false,
		},

		{
			req:   newTestAdminRequestBatch("", 0, rpcpb.CmdConfigChange, nil),
			shard: Shard{},
			ok:    true,
		},
		{
			req:   newTestAdminRequestBatch("", 0, rpcpb.CmdConfigChange, nil),
			shard: Shard{Epoch: Epoch{ConfigVer: 1}},
			ok:    false,
		},
		{
			req:   newTestAdminRequestBatch("", 0, rpcpb.CmdTransferLeader, nil),
			shard: Shard{},
			ok:    true,
		},
		{
			req:   newTestAdminRequestBatch("", 0, rpcpb.CmdTransferLeader, nil),
			shard: Shard{Epoch: Epoch{ConfigVer: 1}},
			ok:    false,
		},
		{
			req:   newTestAdminRequestBatch("", 0, rpcpb.CmdTransferLeader, nil),
			shard: Shard{Epoch: Epoch{Generation: 1}},
			ok:    false,
		},
		{
			req:   newTestAdminRequestBatch("", 0, rpcpb.CmdTransferLeader, nil),
			shard: Shard{Epoch: Epoch{Generation: 1, ConfigVer: 1}},
			ok:    false,
		},

		{
			req:   rpcpb.RequestBatch{Requests: []rpcpb.Request{{}}},
			shard: Shard{},
			ok:    true,
		},
		{
			req:   rpcpb.RequestBatch{Requests: []rpcpb.Request{{}}},
			shard: Shard{Epoch: Epoch{ConfigVer: 1}},
			ok:    true,
		},
		{
			req:   rpcpb.RequestBatch{Requests: []rpcpb.Request{{}}},
			shard: Shard{Epoch: Epoch{Generation: 1}},
			ok:    false,
		},

		{
			req:   rpcpb.RequestBatch{Header: rpcpb.RequestBatchHeader{}, Requests: []rpcpb.Request{{IgnoreEpochCheck: true}}},
			shard: Shard{},
			ok:    true,
		},
		{
			req:   rpcpb.RequestBatch{Header: rpcpb.RequestBatchHeader{}, Requests: []rpcpb.Request{{IgnoreEpochCheck: true}}},
			shard: Shard{Epoch: Epoch{ConfigVer: 1}},
			ok:    true,
		},
		{
			req:   rpcpb.RequestBatch{Header: rpcpb.RequestBatchHeader{}, Requests: []rpcpb.Request{{IgnoreEpochCheck: true}}},
			shard: Shard{Epoch: Epoch{Generation: 1}},
			ok:    true,
		},
	}

	for idx, c := range cases {
		c.req.Header.ShardID = 1
		assert.Equal(t, c.ok, checkEpoch(c.shard, c.req), "index %d", idx)
	}
}

func TestValidateStoreID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := &store{}
	s.meta = metapb.Store{}

	assert.Nil(t, s.validateStoreID(rpcpb.RequestBatch{Header: rpcpb.RequestBatchHeader{Replica: Replica{StoreID: 0}}}))
	assert.NotNil(t, s.validateStoreID(rpcpb.RequestBatch{Header: rpcpb.RequestBatchHeader{Replica: Replica{StoreID: 1}}}))
}

func TestCacheAndRemoveDroppedVoteMsg(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := &store{}
	s.cacheDroppedVoteMsg(1, metapb.RaftMessage{})
	v, ok := s.removeDroppedVoteMsg(1)
	assert.False(t, ok)
	assert.Equal(t, metapb.RaftMessage{}, v)

	s.cacheDroppedVoteMsg(1, metapb.RaftMessage{Message: raftpb.Message{Type: raftpb.MsgVote}})
	v, ok = s.removeDroppedVoteMsg(1)
	assert.True(t, ok)
	assert.Equal(t, metapb.RaftMessage{Message: raftpb.Message{Type: raftpb.MsgVote}}, v)

	s.cacheDroppedVoteMsg(1, metapb.RaftMessage{Message: raftpb.Message{Type: raftpb.MsgPreVote}})
	v, ok = s.removeDroppedVoteMsg(1)
	assert.True(t, ok)
	assert.Equal(t, metapb.RaftMessage{Message: raftpb.Message{Type: raftpb.MsgPreVote}}, v)
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
