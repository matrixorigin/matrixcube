package raftstore

import (
	"errors"
	"testing"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestLeaseUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewTestClusterStore(t)
	c.Start()
	defer c.Stop()

	c.WaitLeadersByCount(1, testWaitTimeout)
	shard := c.GetShardByIndex(0, 0)
	store := c.GetShardLeaderStore(shard.ID)
	leaderReplicaID := findReplica(shard, store.Meta().ID).ID
	c.WaitShardLeaseChangedTo(shard.ID, &metapb.EpochLease{Epoch: 1, ReplicaID: leaderReplicaID}, testWaitTimeout)
}

func TestLeaseUpdateWithRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewTestClusterStore(t,
		WithTestClusterUseDisk())
	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.WaitLeadersByCount(1, testWaitTimeout)
	shard := c.GetShardByIndex(0, 0)
	n1 := c.GetShardLeaderNode(shard.ID)
	store := c.GetShardLeaderStore(shard.ID)

	// wait lease
	leaderReplicaID := findReplica(shard, store.Meta().ID).ID
	c.WaitShardLeaseChangedTo(shard.ID, &metapb.EpochLease{Epoch: 1, ReplicaID: leaderReplicaID}, testWaitTimeout)

	c.Restart()
	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.WaitLeadersByCount(1, testWaitTimeout)

	n2 := c.GetShardLeaderNode(shard.ID)
	if n2 == n1 {
		old := c.GetShardLeaderStore(shard.ID)
		var nodes []int
		c.EveryStore(func(i int, store Store) {
			if i != n2 {
				nodes = append(nodes, i)
			}
		})
		c.RestartNode(n2)
		c.WaitShardOldLeaderChanged(nodes, shard.ID, old.Meta().ID, testWaitTimeout)
	}

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.WaitLeadersByCount(1, testWaitTimeout)
	store = c.GetShardLeaderStore(shard.ID)
	leaderReplicaID = findReplica(shard, store.Meta().ID).ID
	c.WaitShardLeaseChangedTo(shard.ID, &metapb.EpochLease{Epoch: 2, ReplicaID: leaderReplicaID}, testWaitTimeout)
}

func TestLeaseRequestHandler(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	k1 := "k1"
	v1 := "v1"
	n := 0
	kvs := make(map[string]string)
	handler := func(shard metapb.Shard, lease metapb.EpochLease, req rpcpb.Request, cb func(resp []byte, err error)) error {
		n++

		if req.CustomType == uint64(rpcpb.CmdKVSet) {
			kvReq := rpcpb.KVSetRequest{}
			protoc.MustUnmarshal(&kvReq, req.Cmd)
			kvs[string(kvReq.Key)] = string(kvReq.Value)
			cb(protoc.MustMarshal(&rpcpb.KVSetResponse{}), nil)
		} else if req.CustomType == uint64(rpcpb.CmdKVGet) {
			kvReq := rpcpb.KVGetRequest{}
			protoc.MustUnmarshal(&kvReq, req.Cmd)
			cb(protoc.MustMarshal(&rpcpb.KVGetResponse{
				Value: []byte(kvs[string(kvReq.Key)]),
			}), nil)
		}

		return errors.New("not support")
	}
	c := NewSingleTestClusterStore(t,
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Customize.CustomLeaseHolderRequestHandler = handler
		}))
	c.Start()
	defer c.Stop()

	c.WaitLeadersByCount(1, testWaitTimeout)
	shard := c.GetShardByIndex(0, 0)
	store := c.GetShardLeaderStore(shard.ID)
	leaderReplicaID := findReplica(shard, store.Meta().ID).ID
	c.WaitShardLeaseChangedTo(shard.ID, &metapb.EpochLease{Epoch: 1, ReplicaID: leaderReplicaID}, testWaitTimeout)

	kv := c.CreateTestKVClient(0)
	assert.NoError(t, kv.SetWithShardAndPolicy(k1, v1, 0, rpcpb.SelectLeaseHolder, testWaitTimeout))
	assert.Equal(t, 1, n)

	v, err := kv.GetWithShardAndPolicy(k1, 0, rpcpb.SelectLeaseHolder, testWaitTimeout)
	assert.NoError(t, err)
	assert.Equal(t, v1, v)
	assert.Equal(t, 2, n)
}

func TestLeaseMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewSingleTestClusterStore(t)
	c.Start()
	defer c.Stop()

	c.WaitLeadersByCount(1, testWaitTimeout)
	shard := c.GetShardByIndex(0, 0)
	store := c.GetShardLeaderStore(shard.ID)
	leaderReplicaID := findReplica(shard, store.Meta().ID).ID
	c.WaitShardLeaseChangedTo(shard.ID, &metapb.EpochLease{Epoch: 1, ReplicaID: leaderReplicaID}, testWaitTimeout)

	kv := c.CreateTestKVClientWithAdjust(0, func(req *rpcpb.Request) {
		req.Lease = &metapb.EpochLease{Epoch: 0, ReplicaID: leaderReplicaID}
	})
	err := kv.Set("k1", "v1", testWaitTimeout)
	assert.Error(t, err)
	assert.True(t, IsShardLeaseMismatchErr(err))
}
