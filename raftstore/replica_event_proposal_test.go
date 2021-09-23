package raftstore

// TODO(fagongzi): fix
// import (
// 	"testing"
// 	"time"

// 	"github.com/matrixorigin/matrixcube/command"
// 	"github.com/matrixorigin/matrixcube/pb"
// 	"github.com/matrixorigin/matrixcube/pb/meta"
// 	"github.com/matrixorigin/matrixcube/pb/rpc"
// 	"github.com/matrixorigin/matrixcube/util/leaktest"
// 	"github.com/stretchr/testify/assert"
// )

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
