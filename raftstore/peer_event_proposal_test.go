package raftstore

import (
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/stretchr/testify/assert"
)

func TestIssue90(t *testing.T) {
	// Incorrect implementation of ReadIndex
	// See https://github.com/matrixorigin/matrixcube/issues/90

	testMaxProposalRequestCount = 1
	testMaxOnceCommitEntryCount = 1
	defer func() {
		testMaxProposalRequestCount = 0
		testMaxOnceCommitEntryCount = 0
	}()

	var storeID uint64
	committedC := make(chan string, 2)
	c := NewTestClusterStore(t, WithTestClusterWriteHandler(1, func(s bhmetapb.Shard, r *raftcmdpb.Request, c command.Context) (uint64, int64, *raftcmdpb.Response) {
		if storeID == c.StoreID() {
			committedC <- string(r.ID)

			if string(r.ID) == "w2" {
				time.Sleep(time.Second * 5)
			}
		}
		resp := pb.AcquireResponse()
		assert.NoError(t, c.WriteBatch().Set(r.Key, r.Cmd))
		resp.Value = []byte("OK")
		return 0, 0, resp
	}), WithTestClusterReadHandler(2, func(s bhmetapb.Shard, r *raftcmdpb.Request, c command.Context) (*raftcmdpb.Response, uint64) {
		resp := pb.AcquireResponse()
		value, err := c.DataStorage().(storage.KVStorage).Get(r.Key)
		assert.NoError(t, err)
		resp.Value = value
		return resp, 0
	}), WithTestClusterDisableSchedule(), WithTestClusterLogLevel("info"))
	c.Start()
	defer c.Stop()

	c.WaitLeadersByCount(t, 1, time.Second*10)
	id := c.GetShardByIndex(0).ID
	s := c.GetShardLeaderStore(id)
	assert.NotNil(t, s)

	storeID = s.Meta().ID

	// send w1(set k1=1), r1(k1, at w1 committed), w2(k1=2), r2(k1, (k1, at w2 committed))
	// r1 read 1, r2 read 2
	w1 := pb.AcquireRequest()
	w1.ID = []byte("w1")
	w1.CustemType = 1
	w1.Type = raftcmdpb.CMDType_Write
	w1.Key = []byte("key1")
	w1.Cmd = []byte("1")

	r1 := pb.AcquireRequest()
	r1.ID = []byte("r1")
	r1.CustemType = 2
	r1.Type = raftcmdpb.CMDType_Read
	r1.Key = []byte("key1")

	w2 := pb.AcquireRequest()
	w2.ID = []byte("w2")
	w2.CustemType = 1
	w2.Type = raftcmdpb.CMDType_Write
	w2.Key = []byte("key1")
	w2.Cmd = []byte("2")

	r2 := pb.AcquireRequest()
	r2.ID = []byte("r2")
	r2.CustemType = 2
	r2.Type = raftcmdpb.CMDType_Read
	r2.Key = []byte("key1")

	resps, err := sendTestReqs(s,
		time.Second*10,
		committedC,
		map[string]string{
			"r1": "w1", // r1 wait w1 commit
			"r2": "w2", // r2 wait w2 commit
		}, w1, r1, w2, r2)
	assert.NoError(t, err)
	assert.Nil(t, resps["w1"].Header)
	assert.Equal(t, "OK", string(resps["w1"].Responses[0].Value))
	assert.Nil(t, resps["r1"].Header)
	assert.Equal(t, "1", string(resps["r1"].Responses[0].Value))
	assert.Nil(t, resps["w2"].Header)
	assert.Equal(t, "OK", string(resps["w1"].Responses[0].Value))
	assert.Nil(t, resps["r2"].Header)
	assert.Equal(t, "2", string(resps["r2"].Responses[0].Value))
}

func sendTestReqs(s Store, timeout time.Duration, waiterC chan string, waiters map[string]string, reqs ...*raftcmdpb.Request) (map[string]*raftcmdpb.RaftCMDResponse, error) {
	if waiters == nil {
		waiters = make(map[string]string)
	}

	resps := make(map[string]*raftcmdpb.RaftCMDResponse)
	c := make(chan *raftcmdpb.RaftCMDResponse, len(reqs))
	defer close(c)

	cb := func(resp *raftcmdpb.RaftCMDResponse) {
		c <- resp
	}

	for _, req := range reqs {
		if v, ok := waiters[string(req.ID)]; ok {
		OUTER:
			for {
				select {
				case <-time.After(timeout):
					return nil, errors.New("timeout error")
				case c := <-waiterC:
					if c == v {
						break OUTER
					}
				}
			}
		}
		err := s.(*store).onRequestWithCB(req, cb)
		if err != nil {
			return nil, err
		}
	}

	for {
		select {
		case <-time.After(timeout):
			return nil, errors.New("timeout error")
		case resp := <-c:
			resps[string(resp.Responses[0].ID)] = resp
			if len(resps) == len(reqs) {
				return resps, nil
			}
		}
	}
}
