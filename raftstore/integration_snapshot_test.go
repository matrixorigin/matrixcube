// Copyright 2021 MatrixOrigin.
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/transport"
	"github.com/matrixorigin/matrixcube/util/leaktest"
)

func TestCompactionAndSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	skipStore := uint64(0)
	filter := func(msg meta.RaftMessage) bool {
		return msg.To.ContainerID == atomic.LoadUint64(&skipStore) ||
			msg.From.ContainerID == atomic.LoadUint64(&skipStore)
	}

	var c TestRaftCluster
	c = NewTestClusterStore(t,
		DiskTestCluster,
		OldTestCluster,
		WithTestClusterNodeCount(3),
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Customize.CustomTransportFactory = func() transport.Trans {
				return newTestTransport(c, filter)
			}
		}))

	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.WaitReplicaChangeToVoter(c.GetShardByIndex(0, 0).ID, testWaitTimeout)

	shardID := c.GetShardByIndex(0, 0).ID

	// isolate the selected store
	atomic.StoreUint64(&skipStore, c.GetStore(2).Meta().ID)

	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	assert.NoError(t, kv.Set("k1", "v1", testWaitTimeout))
	assert.NoError(t, kv.Set("k2", "v2", testWaitTimeout))
	assert.NoError(t, kv.Set("k3", "v3", testWaitTimeout))
	assert.NoError(t, kv.Set("k4", "v4", testWaitTimeout))

	for i := 0; i < 2; i++ {
		pr := c.GetStore(i).(*store).getReplica(shardID, true)
		if pr != nil {
			pr.addAdminRequest(rpc.AdminRequest{
				CmdType: rpc.AdminCmdType_CompactLog,
				CompactLog: &rpc.CompactLogRequest{
					CompactIndex: 3,
				},
			})
		}
	}

	time.Sleep(5 * time.Second)
	// restore the network
	atomic.StoreUint64(&skipStore, 0)
	assert.NoError(t, kv.Set("k3", "v3", testWaitTimeout))
	assert.NoError(t, kv.Set("k4", "v4", testWaitTimeout))
	time.Sleep(5 * time.Second)
}
