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

	"github.com/fagongzi/util/task"
	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestStartAndStop(t *testing.T) {
	c := NewSingleTestClusterStore(t)
	c.Start()
	defer c.Stop()
}

func TestSearchShard(t *testing.T) {
	s := NewSingleTestClusterStore(t).GetStore(0).(*store)

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
	s := NewSingleTestClusterStore(t).GetStore(0).(*store)

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
	s := NewSingleTestClusterStore(t).GetStore(0).(*store)

	tree := util.NewShardTree()
	s.keyRanges.Store(uint64(0), tree)
	tree.Update(Shard{ID: 1, Start: []byte("a"), End: []byte("b")})

	assert.False(t, s.removeShardKeyRange(Shard{ID: 1, Start: []byte(""), End: []byte("a")}))
	assert.False(t, s.removeShardKeyRange(Shard{ID: 1, Group: 1, Start: []byte("a"), End: []byte("b")}))
	assert.True(t, s.removeShardKeyRange(Shard{ID: 1, Start: []byte("a"), End: []byte("b")}))
}

func TestUpdateShardKeyRange(t *testing.T) {
	s := NewSingleTestClusterStore(t).GetStore(0).(*store)

	s.updateShardKeyRange(Shard{ID: 1, Start: []byte("a"), End: []byte("b")})
	assert.Equal(t, uint64(1), s.searchShard(0, []byte("a")).ID)

	s.updateShardKeyRange(Shard{ID: 1, Start: []byte("a"), End: []byte("c")})
	assert.Equal(t, uint64(1), s.searchShard(0, []byte("b")).ID)
}

func TestStoreSelectShard(t *testing.T) {
	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
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
	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	aware := newTestShardAware()
	s.aware = aware
	pr := &replica{shardID: 1, sm: &stateMachine{}}
	pr.sm.metadataMu.shard = Shard{ID: 1}
	s.addReplica(pr)
	assert.NotNil(t, s.getReplica(1, false))

	s.removeReplica(pr)
	assert.Nil(t, s.getReplica(1, false))

	assert.Equal(t, 1, len(aware.removed))
}

func TestValidateShard(t *testing.T) {
	cases := []struct {
		pr    *replica
		req   rpc.RequestBatch
		epoch Epoch
		err   string
		ok    bool
	}{
		{
			pr:  &replica{shardID: 1, startedC: make(chan struct{}), actions: task.New(32)},
			req: rpc.RequestBatch{},
			err: errShardNotFound.Error(),
			ok:  true,
		},
		{
			pr:  &replica{replica: Replica{ID: 1}, startedC: make(chan struct{}), actions: task.New(32)},
			req: rpc.RequestBatch{},
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
			req: rpc.RequestBatch{},
			err: "mismatch replica id, want 1, but 0",
			ok:  true,
		},
		{
			pr:    &replica{replica: Replica{ID: 1}, leaderID: 1, startedC: make(chan struct{}), actions: task.New(32)},
			epoch: Epoch{Version: 1},
			req:   rpc.RequestBatch{Header: rpc.RequestBatchHeader{Replica: Replica{ID: 1}}, Requests: []rpc.Request{{}}},
			err:   errStaleEpoch.Error(),
			ok:    true,
		},
	}

	for idx, c := range cases {
		s := NewSingleTestClusterStore(t, WithDisableTestParallel()).GetStore(0).(*store)
		c.pr.store = s
		c.pr.sm = &stateMachine{}
		c.pr.sm.metadataMu.shard = Shard{ID: c.pr.shardID, Epoch: c.epoch}
		close(c.pr.startedC)
		s.addReplica(c.pr)

		err, ok := s.validateShard(c.req)
		assert.Equal(t, c.ok, ok, "index %d", idx)
		assert.Equal(t, c.err, err.Message, "index %d", idx)
	}
}

func TestStartClearDataJob(t *testing.T) {
	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	kv := s.DataStorageByGroup(0).(storage.KVStorageWrapper).GetKVStorage()
	shard := Shard{Start: []byte("a"), End: []byte("b"), Replicas: []Replica{{ID: 1}, {ID: 2}}}
	kv.Set(keys.EncodeDataKey(0, []byte("a1"), nil), []byte("hello"), false)
	kv.Set(keys.EncodeDataKey(0, []byte("a2"), nil), []byte("hello"), false)
	assert.NoError(t, s.startClearDataJob(shard))

	c := 0
	kv.Scan(keys.EncodeDataKey(0, shard.Start, nil), keys.EncodeDataKey(0, shard.End, nil), func(key, value []byte) (bool, error) {
		c++
		return true, nil
	}, false)
	assert.Equal(t, 0, c)
}

func TestCheckEpoch(t *testing.T) {
	cases := []struct {
		req   rpc.RequestBatch
		shard Shard
		ok    bool
	}{
		{
			req:   rpc.RequestBatch{AdminRequest: rpc.AdminRequest{CmdType: rpc.AdminCmdType_BatchSplit}},
			shard: Shard{},
			ok:    true,
		},
		{
			req:   rpc.RequestBatch{AdminRequest: rpc.AdminRequest{CmdType: rpc.AdminCmdType_BatchSplit}},
			shard: Shard{Epoch: Epoch{Version: 1}},
			ok:    false,
		},

		{
			req:   rpc.RequestBatch{AdminRequest: rpc.AdminRequest{CmdType: rpc.AdminCmdType_ConfigChange}},
			shard: Shard{},
			ok:    true,
		},
		{
			req:   rpc.RequestBatch{AdminRequest: rpc.AdminRequest{CmdType: rpc.AdminCmdType_ConfigChange}},
			shard: Shard{Epoch: Epoch{ConfVer: 1}},
			ok:    false,
		},
		{
			req:   rpc.RequestBatch{AdminRequest: rpc.AdminRequest{CmdType: rpc.AdminCmdType_ConfigChangeV2}},
			shard: Shard{},
			ok:    true,
		},
		{
			req:   rpc.RequestBatch{AdminRequest: rpc.AdminRequest{CmdType: rpc.AdminCmdType_ConfigChangeV2}},
			shard: Shard{Epoch: Epoch{ConfVer: 1}},
			ok:    false,
		},

		{
			req:   rpc.RequestBatch{AdminRequest: rpc.AdminRequest{CmdType: rpc.AdminCmdType_TransferLeader}},
			shard: Shard{},
			ok:    true,
		},
		{
			req:   rpc.RequestBatch{AdminRequest: rpc.AdminRequest{CmdType: rpc.AdminCmdType_TransferLeader}},
			shard: Shard{Epoch: Epoch{ConfVer: 1}},
			ok:    false,
		},
		{
			req:   rpc.RequestBatch{AdminRequest: rpc.AdminRequest{CmdType: rpc.AdminCmdType_TransferLeader}},
			shard: Shard{Epoch: Epoch{Version: 1}},
			ok:    false,
		},
		{
			req:   rpc.RequestBatch{AdminRequest: rpc.AdminRequest{CmdType: rpc.AdminCmdType_TransferLeader}},
			shard: Shard{Epoch: Epoch{Version: 1, ConfVer: 1}},
			ok:    false,
		},

		{
			req:   rpc.RequestBatch{Requests: []rpc.Request{{}}},
			shard: Shard{},
			ok:    true,
		},
		{
			req:   rpc.RequestBatch{Requests: []rpc.Request{{}}},
			shard: Shard{Epoch: Epoch{ConfVer: 1}},
			ok:    true,
		},
		{
			req:   rpc.RequestBatch{Requests: []rpc.Request{{}}},
			shard: Shard{Epoch: Epoch{Version: 1}},
			ok:    false,
		},

		{
			req:   rpc.RequestBatch{Header: rpc.RequestBatchHeader{}, Requests: []rpc.Request{{IgnoreEpochCheck: true}}},
			shard: Shard{},
			ok:    true,
		},
		{
			req:   rpc.RequestBatch{Header: rpc.RequestBatchHeader{}, Requests: []rpc.Request{{IgnoreEpochCheck: true}}},
			shard: Shard{Epoch: Epoch{ConfVer: 1}},
			ok:    true,
		},
		{
			req:   rpc.RequestBatch{Header: rpc.RequestBatchHeader{}, Requests: []rpc.Request{{IgnoreEpochCheck: true}}},
			shard: Shard{Epoch: Epoch{Version: 1}},
			ok:    true,
		},
	}

	for idx, c := range cases {
		c.req.Header.ShardID = 1
		assert.Equal(t, c.ok, checkEpoch(c.shard, c.req), "index %d", idx)
	}
}

func TestValidateStoreID(t *testing.T) {
	s := &store{}
	s.meta = &containerAdapter{}

	assert.Nil(t, s.validateStoreID(rpc.RequestBatch{Header: rpc.RequestBatchHeader{Replica: Replica{ContainerID: 0}}}))
	assert.NotNil(t, s.validateStoreID(rpc.RequestBatch{Header: rpc.RequestBatchHeader{Replica: Replica{ContainerID: 1}}}))
}

func TestCacheAndRemoveDroppedVoteMsg(t *testing.T) {
	s := &store{}
	s.cacheDroppedVoteMsg(1, raftpb.Message{})
	v, ok := s.removeDroppedVoteMsg(1)
	assert.False(t, ok)
	assert.Equal(t, raftpb.Message{}, v)

	s.cacheDroppedVoteMsg(1, raftpb.Message{Type: raftpb.MsgVote})
	v, ok = s.removeDroppedVoteMsg(1)
	assert.True(t, ok)
	assert.Equal(t, raftpb.Message{Type: raftpb.MsgVote}, v)

	s.cacheDroppedVoteMsg(1, raftpb.Message{Type: raftpb.MsgPreVote})
	v, ok = s.removeDroppedVoteMsg(1)
	assert.True(t, ok)
	assert.Equal(t, raftpb.Message{Type: raftpb.MsgPreVote}, v)
}
