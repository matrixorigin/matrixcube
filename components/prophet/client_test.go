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

	"github.com/matrixorigin/matrixcube/components/prophet/event"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/pilosa/pilosa/roaring"
	"github.com/stretchr/testify/assert"
)

func TestClientLeaderChange(t *testing.T) {
	cluster := newTestClusterProphet(t, 3, nil)
	defer func() {
		for _, p := range cluster {
			p.Stop()
		}
	}()

	id, err := cluster[2].GetClient().AllocID()
	assert.NoError(t, err)
	assert.True(t, id > 0)

	// stop current leader
	cluster[0].Stop()

	for _, c := range cluster {
		c.GetConfig().DisableResponse = true
	}

	// rpc timeout error
	_, err = cluster[2].GetClient().AllocID()
	assert.Error(t, err)

	for _, c := range cluster {
		c.GetConfig().DisableResponse = false
	}

	id, err = cluster[2].GetClient().AllocID()
	assert.NoError(t, err)
	assert.True(t, id > 0)
}

func TestClientGetContainer(t *testing.T) {
	p := newTestSingleProphet(t, nil)
	defer p.Stop()

	c := p.GetClient()
	value, err := c.GetContainer(0)
	assert.Error(t, err)
	assert.Nil(t, value)
}

func TestAsyncCreateResources(t *testing.T) {
	p := newTestSingleProphet(t, nil)
	defer p.Stop()

	c := p.GetClient()

	assert.NoError(t, c.PutContainer(newTestContainerMeta(1)))
	_, err := c.ContainerHeartbeat(newTestContainerHeartbeat(1, 1))
	assert.NoError(t, err)
	assert.Error(t, c.AsyncAddResources(newTestResourceMeta(1)))

	assert.NoError(t, c.PutContainer(newTestContainerMeta(2)))
	_, err = c.ContainerHeartbeat(newTestContainerHeartbeat(2, 1))
	assert.NoError(t, err)
	assert.Error(t, c.AsyncAddResources(newTestResourceMeta(1)))

	assert.NoError(t, c.PutContainer(newTestContainerMeta(3)))
	_, err = c.ContainerHeartbeat(newTestContainerHeartbeat(3, 1))
	assert.NoError(t, err)
	w, err := c.NewWatcher(uint32(event.EventFlagAll))
	assert.NoError(t, err)
	assert.NoError(t, c.AsyncAddResources(newTestResourceMeta(1)))

	select {
	case e := <-w.GetNotify():
		assert.Equal(t, event.EventInit, e.Type)
	case <-time.After(time.Second):
		assert.FailNow(t, "timeout")
	}

	for i := 0; i < 2; i++ {
		select {
		case e := <-w.GetNotify():
			assert.True(t, e.ResourceEvent.Create)
		case <-time.After(time.Second * 11):
			assert.FailNow(t, "timeout")
		}
	}
}

func TestCheckResourceState(t *testing.T) {
	p := newTestSingleProphet(t, nil)
	defer p.Stop()

	c := p.GetClient()
	rsp, err := c.CheckResourceState(roaring.NewBitmap(2))
	assert.NoError(t, err)
	assert.Empty(t, rsp.Removed)
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

	assert.NoError(t, c.PutContainer(newTestContainerMeta(1)))
	_, err := c.ContainerHeartbeat(newTestContainerHeartbeat(1, 1))
	assert.NoError(t, err)

	peer := metapb.Peer{ID: 1, ContainerID: 1}
	assert.NoError(t, c.ResourceHeartbeat(newTestResourceMeta(2, peer), rpcpb.ResourceHeartbeatReq{
		ContainerID: 1,
		Leader:      &peer}))
	rules, err := c.GetAppliedRules(2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(rules))

	peer = metapb.Peer{ID: 2, ContainerID: 1}
	res := newTestResourceMeta(3, peer)
	res.SetRuleGroups("group01")
	assert.NoError(t, c.ResourceHeartbeat(res, rpcpb.ResourceHeartbeatReq{
		ContainerID: 1,
		Leader:      &peer}))
	rules, err = c.GetAppliedRules(3)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(rules))
}

func newTestResourceMeta(resourceID uint64, peers ...metapb.Peer) metadata.Resource {
	return &metadata.TestResource{
		ResID:    resourceID,
		Start:    []byte(fmt.Sprintf("%20d", resourceID)),
		End:      []byte(fmt.Sprintf("%20d", resourceID+1)),
		ResEpoch: metapb.ResourceEpoch{Version: 1, ConfVer: 1},
		ResPeers: peers,
	}
}

func newTestContainerMeta(containerID uint64) metadata.Container {
	return &metadata.TestContainer{
		CID:        containerID,
		CAddr:      fmt.Sprintf("127.0.0.1:%d", containerID),
		CShardAddr: fmt.Sprintf("127.0.0.2:%d", containerID),
	}
}

func newTestContainerHeartbeat(containerID uint64, resourceCount int, resourceSizes ...uint64) rpcpb.ContainerHeartbeatReq {
	var resourceSize uint64
	if len(resourceSizes) == 0 {
		resourceSize = uint64(resourceCount) * 10
	} else {
		resourceSize = resourceSizes[0]
	}

	stats := metapb.ContainerStats{}
	stats.Capacity = 100 * (1 << 30)
	stats.UsedSize = resourceSize * (1 << 20)
	stats.Available = stats.Capacity - stats.UsedSize
	stats.ContainerID = containerID
	stats.ResourceCount = uint64(resourceCount)
	stats.StartTime = uint64(time.Now().Add(-time.Minute).Unix())
	stats.IsBusy = false
	stats.Interval = &metapb.TimeInterval{
		Start: stats.StartTime,
		End:   uint64(time.Now().Unix()),
	}

	return rpcpb.ContainerHeartbeatReq{
		Stats: stats,
	}
}
