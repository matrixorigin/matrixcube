package test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestIssue123(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := raftstore.NewSingleTestClusterStore(t,
		raftstore.WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
			cfg.Customize.CustomInitShardsFactory = func() []raftstore.Shard { return []raftstore.Shard{{Start: []byte("a"), End: []byte("b")}} }
		}))
	defer c.Stop()

	c.Start()
	c.WaitShardByCountPerNode(1, testWaitTimeout)

	p, err := c.GetStore(0).CreateResourcePool(metapb.ResourcePool{
		RangePrefix: []byte("b"),
		Capacity:    20,
	})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	c.WaitShardByCountPerNode(21, testWaitTimeout)

	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	for i := 0; i < 20; i++ {
		s, err := p.Alloc(0, []byte(fmt.Sprintf("%d", i)))
		assert.NoError(t, err)
		assert.NoError(t, kv.Set(string(c.GetShardByID(0, s.ShardID).Start), "OK", time.Second))
	}
}

func TestIssue166(t *testing.T) {
	c := raftstore.NewSingleTestClusterStore(t, raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
		cfg.Test.SaveDynamicallyShardInitStateWait = time.Second
		cfg.Customize.CustomInitShardsFactory = func() []raftstore.Shard { return []raftstore.Shard{{Start: []byte("a"), End: []byte("b")}} }
	}))
	defer c.Stop()

	c.Start()
	c.WaitLeadersByCount(1, testWaitTimeout)

	_, err := c.GetStore(0).CreateResourcePool(metapb.ResourcePool{
		Capacity:    1,
		Group:       0,
		RangePrefix: []byte("b"),
	})
	assert.NoError(t, err)
	c.WaitLeadersByCount(2, testWaitTimeout)
	// make sure slow point
	time.Sleep(time.Second)

	// TODO(fagongzi): check commit
	// v, err := c.GetStore(0).MetadataStorage().Get(keys.GetRaftLocalStateKey(c.GetShardByIndex(0, 1).ID))
	// assert.NoError(t, err)
	// state := &bhraftpb.RaftLocalState{}
	// protoc.MustUnmarshal(state, v)
	// assert.Equal(t, uint64(6), state.HardState.Commit)
}

func TestIssue192(t *testing.T) {
	defer leaktest.AfterTest(t)()

	wc := make(chan struct{})
	c := raftstore.NewSingleTestClusterStore(t, raftstore.WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
		cfg.Customize.CustomInitShardsFactory = func() []raftstore.Shard { return []raftstore.Shard{{Start: []byte("a"), End: []byte("b")}} }
		cfg.Test.ShardPoolCreateWaitC = wc
	}))
	defer c.Stop()

	c.Start()

	p, err := c.GetStore(0).CreateResourcePool(metapb.ResourcePool{Group: 0, Capacity: 1, RangePrefix: []byte("b")})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	_, err = p.Alloc(0, []byte("purpose"))
	assert.Error(t, err)
	assert.False(t, strings.Contains(err.Error(), "timeout"))
}

func TestIssue133(t *testing.T) {
	// time line
	// 1. leader send message to a follower
	// 2. follower create with empty apply state
	// 3. leader send snapshot to follower
	// 4. receive the snapshot message
	// 5. advance raft with 0 applied index
	// 6. peerReplica delegate create

	defer leaktest.AfterTest(t)()
	c := raftstore.NewTestClusterStore(t, raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
		cfg.Customize.CustomInitShardsFactory = func() []raftstore.Shard {
			return []raftstore.Shard{
				{Start: []byte("a"), End: []byte("b")},
				{Start: []byte("b"), End: []byte("c")},
				{Start: []byte("c"), End: []byte("d")},
			}
		}

		if node > 0 {
			cfg.Test.ReplicaSetSnapshotJobWait = time.Second
		}
	}))
	defer c.Stop()

	c.Start()
	c.WaitShardByCounts([]int{3, 3, 3}, testWaitTimeout)
	c.GetStore(0).Stop()
	time.Sleep(time.Second)
}

// TODO(fagongzi): fix
// func TestIssue90(t *testing.T) {
// 	// Incorrect implementation of ReadIndex
// 	// See https://github.com/matrixorigin/matrixcube/issues/90

// 	defer leaktest.AfterTest(t)()
// 	testMaxProposalRequestCount = 1
// 	testMaxOnceCommitEntryCount = 1
// 	defer func() {
// 		testMaxProposalRequestCount = 0
// 		testMaxOnceCommitEntryCount = 0
// 	}()

// 	var storeID uint64
// 	committedC := make(chan string, 2)
// 	c := NewTestClusterStore(t, WithTestClusterWriteHandler(1, func(s Shard, r *rpc.Request, c command.Context) (uint64, int64, *rpc.Response) {
// 		if storeID == c.StoreID() {
// 			committedC <- string(r.ID)

// 			if string(r.ID) == "w2" {
// 				time.Sleep(time.Second * 5)
// 			}
// 		}
// 		resp := pb.AcquireResponse()
// 		assert.NoError(t, c.WriteBatch().Set(r.Key, r.Cmd))
// 		resp.Value = []byte("OK")
// 		return 0, 0, resp
// 	}), DisableScheduleTestCluster)
// 	c.Start()
// 	defer c.Stop()

// 	c.WaitLeadersByCount(1, testWaitTimeout)
// 	id := c.GetShardByIndex(0, 0).ID
// 	s := c.GetShardLeaderStore(id)
// 	assert.NotNil(t, s)

// 	storeID = s.Meta().ID

// 	// send w1(set k1=1), r1(k1, at w1 committed), w2(k1=2), r2(k1, (k1, at w2 committed))
// 	// r1 read 1, r2 read 2
// 	w1 := createTestWriteReq("w1", "key1", "1")
// 	r1 := createTestReadReq("r1", "key1")
// 	w2 := createTestWriteReq("w2", "key1", "2")
// 	r2 := createTestReadReq("r2", "key1")

// 	resps, err := sendTestReqs(s,
// 		testWaitTimeout,
// 		committedC,
// 		map[string]string{
// 			"r1": "w1", // r1 wait w1 commit
// 			"r2": "w2", // r2 wait w2 commit
// 		}, w1, r1, w2, r2)
// 	assert.NoError(t, err)
// 	assert.Nil(t, resps["w1"].Header)
// 	assert.Equal(t, "OK", string(resps["w1"].Responses[0].Value))
// 	assert.Nil(t, resps["r1"].Header)
// 	assert.Equal(t, "1", string(resps["r1"].Responses[0].Value))
// 	assert.Nil(t, resps["w2"].Header)
// 	assert.Equal(t, "OK", string(resps["w1"].Responses[0].Value))
// 	assert.Nil(t, resps["r2"].Header)
// 	assert.Equal(t, "2", string(resps["r2"].Responses[0].Value))
// }

// func TestIssue84(t *testing.T) {
// 	defer leaktest.AfterTest(t)()
// 	// issue 84, lost event notification after cluster restart
// 	// See https://github.com/matrixorigin/matrixcube/issues/84
// 	c, closer := createDiskDataStorageCluster(t)
// 	app := c.Applications[0]
// 	cmdSet := testRequest{
// 		Op:    "SET",
// 		Key:   "key",
// 		Value: "value",
// 	}
// 	resp, err := app.Exec(&cmdSet, 10*time.Second)

// 	assert.NoError(t, err)
// 	assert.Equal(t, "OK", string(resp))
// 	closer()

// 	// restart
// 	fn := func(i int) {
// 		c, closer := createDiskDataStorageCluster(t, raftstore.WithTestClusterRecreate(false))
// 		defer closer()

// 		app = c.Applications[0]
// 		value, err := app.Exec(&testRequest{
// 			Op:  "GET",
// 			Key: "key",
// 		}, 10*time.Second)
// 		assert.Equal(t, "value", string(value))
// 		if err != nil {
// 			log.Fatalf("failed %d", i)
// 		}
// 	}

// 	for i := 0; i < 1; i++ {
// 		fn(i)
// 	}
// }
