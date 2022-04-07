// Copyright 2021 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless assertd by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package raftstore

import (
	"fmt"
	"testing"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/uuid"
	"github.com/stretchr/testify/assert"
)

func TestAdvertiseAddr(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewTestClusterStore(t,
		WithTestClusterUseDisk(),
		WithTestClusterEnableAdvertiseAddr())
	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.WaitLeadersByCount(1, testWaitTimeout)
	c.CheckShardCount(1)

	for i := 0; i < 3; i++ {
		kv := c.CreateTestKVClient(i)
		defer kv.Close()
		assert.NoError(t, kv.Set("key", "value", testWaitTimeout))
	}

	c.Restart()
	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.WaitLeadersByCount(1, testWaitTimeout)
	c.CheckShardCount(1)

	for i := 0; i < 3; i++ {
		kv2 := c.CreateTestKVClient(i)
		defer kv2.Close()
		v, err := kv2.Get("key", testWaitTimeout)
		assert.NoError(t, err)
		assert.Equal(t, "value", v)
	}
}

func TestSingleClusterStartAndStop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewSingleTestClusterStore(t, DiskTestCluster)
	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.CheckShardCount(1)
}

func TestClusterStartAndStop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewTestClusterStore(t)
	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.CheckShardCount(1)
}

func TestClusterWithInitClusterStartAndStop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewTestClusterStore(t,
		WithTestClusterUseDisk(),
		WithTestClusterUseInitProphetCluster())
	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.CheckShardCount(1)

	c.Restart()
	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.CheckShardCount(1)
}

func TestClusterStartWithMoreNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewTestClusterStore(t,
		WithTestClusterNodeCount(5),
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Prophet.Replication.MaxReplicas = 5
		}))
	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.CheckShardCount(1)
}

func TestReadAndWriteAndRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	fn := func(n int) {
		c := NewTestClusterStore(t,
			WithTestClusterNodeCount(n),
			DiskTestCluster)

		c.Start()
		defer c.Stop()

		c.WaitShardByCountPerNode(1, testWaitTimeout)
		c.WaitLeadersByCount(1, testWaitTimeout)
		c.CheckShardCount(1)

		kv := c.CreateTestKVClient(0)
		defer kv.Close()

		for i := 0; i < 1; i++ {
			assert.NoError(t, kv.Set(fmt.Sprintf("k-%d", i), fmt.Sprintf("v-%d", i), testWaitTimeout))
		}

		c.Restart()
		c.WaitShardByCountPerNode(1, testWaitTimeout)
		c.WaitLeadersByCount(1, testWaitTimeout)
		c.CheckShardCount(1)

		kv2 := c.CreateTestKVClient(0)
		defer kv2.Close()

		for i := 0; i < 1; i++ {
			v, err := kv2.Get(fmt.Sprintf("k-%d", i), testWaitTimeout)
			assert.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("v-%d", i), v)
		}
	}

	fn(1)
	fn(3)
}

func TestAddShardLabel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewTestClusterStore(t)
	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)

	sid := c.GetShardByIndex(0, 0).ID
	c.WaitAllReplicasChangeToVoter(sid, testWaitTimeout)

	for {
		ch := make(chan rpcpb.ResponseBatch)
		assert.NoError(t, c.GetStore(0).OnRequestWithCB(rpcpb.Request{
			ID:         uuid.NewV4().Bytes(),
			Group:      0,
			Type:       rpcpb.Admin,
			CustomType: uint64(rpcpb.CmdUpdateLabels),
			ToShard:    sid,
			Epoch:      c.GetShardByIndex(0, 0).Epoch,
			Cmd: protoc.MustMarshal(&rpcpb.UpdateLabelsRequest{
				Labels: []metapb.Label{{Key: "label1", Value: "value1"}},
				Policy: rpcpb.Add,
			}),
		}, func(resp rpcpb.ResponseBatch) {
			ch <- resp
		}))

		resp := <-ch
		if resp.Header.IsEmpty() {
			assert.True(t, resp.Header.IsEmpty()) // no error
			assert.True(t, resp.IsAdmin())
			assert.Equal(t, rpcpb.CmdUpdateLabels, resp.GetAdminCmdType())
			break
		}
	}

	c.WaitShardByLabel(sid, "label1", "value1", testWaitTimeout)
}
