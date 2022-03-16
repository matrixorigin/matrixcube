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

package prophet

import (
	"fmt"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/event"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/stretchr/testify/assert"
)

func TestClientLeaderChange(t *testing.T) {
	cluster := newTestClusterProphet(t, 3, nil)
	defer func() {
		for _, p := range cluster {
			p.Stop()
		}
	}()

	id, err := cluster[0].GetClient().AllocID()
	assert.NoError(t, err)
	assert.True(t, id > 0)

	// stop current leader
	useIdx := 0
	l := cluster[0].GetLeader()
	assert.NotNil(t, l)
	for i, c := range cluster {
		if l.Name == c.GetConfig().Name {
			c.Stop()
		} else {
			useIdx = i
		}
	}

	for _, c := range cluster {
		c.GetConfig().TestContext.EnableSkipResponse()
	}

	// rpc timeout error
	_, err = cluster[useIdx].GetClient().AllocID()
	assert.Error(t, err)

	for _, c := range cluster {
		c.GetConfig().TestContext.DisableSkipResponse()
	}

	id, err = cluster[useIdx].GetClient().AllocID()
	assert.NoError(t, err)
	assert.True(t, id > 0)
}

func TestClientGetStore(t *testing.T) {
	p := newTestSingleProphet(t, nil)
	defer p.Stop()

	c := p.GetClient()
	value, err := c.GetStore(0)
	assert.Error(t, err)
	assert.Nil(t, value)
}

func TestAsyncCreateShards(t *testing.T) {
	p := newTestSingleProphet(t, nil)
	defer p.Stop()

	c := p.GetClient()

	assert.NoError(t, c.PutStore(newTestStoreMeta(1)))
	_, err := c.StoreHeartbeat(newTestStoreHeartbeat(1, 1))
	assert.NoError(t, err)
	assert.Error(t, c.AsyncAddShards(newTestShardMeta(1)))

	assert.NoError(t, c.PutStore(newTestStoreMeta(2)))
	_, err = c.StoreHeartbeat(newTestStoreHeartbeat(2, 1))
	assert.NoError(t, err)
	assert.Error(t, c.AsyncAddShards(newTestShardMeta(1)))

	assert.NoError(t, c.PutStore(newTestStoreMeta(3)))
	_, err = c.StoreHeartbeat(newTestStoreHeartbeat(3, 1))
	assert.NoError(t, err)
	w, err := c.NewWatcher(uint32(event.EventFlagAll))
	assert.NoError(t, err)
	assert.NoError(t, c.AsyncAddShards(newTestShardMeta(1)))

	select {
	case e := <-w.GetNotify():
		assert.Equal(t, event.EventInit, e.Type)
	case <-time.After(time.Second):
		assert.FailNow(t, "timeout")
	}

	for i := 0; i < 2; i++ {
		select {
		case e := <-w.GetNotify():
			assert.True(t, e.ShardEvent.Create)
		case <-time.After(time.Second * 11):
			assert.FailNow(t, "timeout")
		}
	}
}

func TestCheckShardState(t *testing.T) {
	p := newTestSingleProphet(t, nil)
	defer p.Stop()

	c := p.GetClient()
	rsp, err := c.CheckShardState(roaring64.BitmapOf(2))
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), util.MustUnmarshalBM64(rsp.Destroyed).GetCardinality())
}

func TestScheduleGroupRule(t *testing.T) {
	p := newTestSingleProphet(t, nil)
	defer p.Stop()

	c := p.GetClient()

	ruleName := "RuleTable"
	labelName := "LabelTable"

	// try to add 10 rules
	for id := 1; id <= 10; id++ {
		var err error

		err = c.AddSchedulingRule(uint64(id), ruleName, labelName)
		assert.NoError(t, err)
		// duplicated add
		err = c.AddSchedulingRule(uint64(id), ruleName, labelName)
		assert.NoError(t, err)
	}

	// get all rules
	rules, err := c.GetSchedulingRules()
	assert.NoError(t, err)
	assert.Equal(t, 10, len(rules))
}

func TestPutPlacementRule(t *testing.T) {
	p := newTestSingleProphet(t, nil)
	defer p.Stop()

	c := p.GetClient()
	assert.NoError(t, c.PutPlacementRule(rpcpb.PlacementRule{
		GroupID: "group01",
		ID:      "rule01",
		Count:   3,
	}))

	assert.NoError(t, c.PutStore(newTestStoreMeta(1)))
	_, err := c.StoreHeartbeat(newTestStoreHeartbeat(1, 1))
	assert.NoError(t, err)

	peer := metapb.Replica{ID: 1, StoreID: 1}
	assert.NoError(t, c.ShardHeartbeat(newTestShardMeta(2, peer), rpcpb.ShardHeartbeatReq{
		StoreID: 1,
		Leader:  &peer}))
	rules, err := c.GetAppliedRules(2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(rules))

	peer = metapb.Replica{ID: 2, StoreID: 1}
	res := newTestShardMeta(3, peer)
	res.SetRuleGroups("group01")
	assert.NoError(t, c.ShardHeartbeat(res, rpcpb.ShardHeartbeatReq{
		StoreID: 1,
		Leader:  &peer}))
	rules, err = c.GetAppliedRules(3)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(rules))
}

func TestIssue106(t *testing.T) {
	clusterSize := 3
	cluster := newTestClusterProphet(t, clusterSize, func(c *config.Config) {
		c.RPCTimeout.Duration = time.Millisecond * 200
	})
	defer func() {
		for _, p := range cluster {
			p.Stop()
		}
	}()

	var leader Prophet
	for i := 0; i < clusterSize; i++ {
		p := cluster[i]
		if p.GetMember().ID() == p.GetLeader().ID {
			leader = p
			break
		}
	}
	assert.NotNil(t, leader)

	cfg := leader.GetConfig()
	cli := leader.GetClient()
	cfg.TestContext.EnableResponseNotLeader()
	go func() {
		time.Sleep(time.Millisecond * 50)
		cfg.TestContext.DisableResponseNotLeader()
	}()
	id, err := cli.AllocID()
	assert.NoError(t, err)
	assert.True(t, id > 0)
}

func TestIssue112(t *testing.T) {
	p := newTestSingleProphet(t, nil)
	defer p.Stop()

	m := p.GetMember().Member()
	leader := &metapb.Member{
		ID:   m.ID,
		Addr: m.Addr,
		Name: m.Name,
	}
	c := NewClient(WithLeaderGetter(func() *metapb.Member {
		return leader
	}))
	id, err := c.AllocID()
	assert.NoError(t, err)
	assert.True(t, id > 0)

	p.GetConfig().TestContext.EnableResponseNotLeader()
	leader.Addr = "127.0.0.1:60000"

	ch := make(chan error)
	go func() {
		_, err := c.AllocID()
		assert.Error(t, err)
		ch <- err
	}()

	go func() {
		time.Sleep(time.Millisecond * 200)
		assert.NoError(t, c.Close())
	}()

	select {
	case err := <-ch:
		assert.Equal(t, ErrClosed, err)
	case <-time.After(time.Second * 2):
		assert.FailNow(t, "timeout")
	}
}

func newTestShardMeta(resourceID uint64, peers ...metapb.Replica) metapb.Shard {
	return metapb.Shard{
		ID:       resourceID,
		Start:    []byte(fmt.Sprintf("%20d", resourceID)),
		End:      []byte(fmt.Sprintf("%20d", resourceID+1)),
		Epoch:    metapb.ShardEpoch{Generation: 1, ConfigVer: 1},
		Replicas: peers,
	}
}

func newTestStoreMeta(containerID uint64) metapb.Store {
	return metapb.Store{
		ID:            containerID,
		ClientAddress: fmt.Sprintf("127.0.0.1:%d", containerID),
		RaftAddress:   fmt.Sprintf("127.0.0.2:%d", containerID),
	}
}

func newTestStoreHeartbeat(containerID uint64, resourceCount int, resourceSizes ...uint64) rpcpb.StoreHeartbeatReq {
	var resourceSize uint64
	if len(resourceSizes) == 0 {
		resourceSize = uint64(resourceCount) * 10
	} else {
		resourceSize = resourceSizes[0]
	}

	stats := metapb.StoreStats{}
	stats.Capacity = 100 * (1 << 30)
	stats.UsedSize = resourceSize * (1 << 20)
	stats.Available = stats.Capacity - stats.UsedSize
	stats.StoreID = containerID
	stats.ShardCount = uint64(resourceCount)
	stats.StartTime = uint64(time.Now().Add(-time.Minute).Unix())
	stats.IsBusy = false
	stats.Interval = &metapb.TimeInterval{
		Start: stats.StartTime,
		End:   uint64(time.Now().Unix()),
	}

	return rpcpb.StoreHeartbeatReq{
		Stats: stats,
	}
}
