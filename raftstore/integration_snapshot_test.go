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
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"

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

	snapshotTestTimeout := 20 * time.Second
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

	c.WaitShardByCountPerNode(1, snapshotTestTimeout)
	c.WaitReplicaChangeToVoter(c.GetShardByIndex(0, 0).ID, snapshotTestTimeout)

	shardID := c.GetShardByIndex(0, 0).ID

	// isolate the selected store
	atomic.StoreUint64(&skipStore, c.GetStore(2).Meta().ID)

	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	assert.NoError(t, kv.Set("k1", "v1", snapshotTestTimeout))
	assert.NoError(t, kv.Set("k2", "v2", snapshotTestTimeout))
	assert.NoError(t, kv.Set("k3", "v3", snapshotTestTimeout))
	assert.NoError(t, kv.Set("k4", "v4", snapshotTestTimeout))

	compactionCompleted := false
	for i := 0; i < 2; i++ {
		store := c.GetStore(i).(*store)
		if pr := store.getReplica(shardID, true); pr != nil {
			hasLog := func(index uint64) bool {
				lr := pr.lr
				_, err := lr.Entries(index, index+1, math.MaxUint64)
				if err == nil {
					return true
				}
				if err == raft.ErrCompacted {
					return false
				}
				panic(err)
			}

			assert.True(t, hasLog(2))
			pr.addAdminRequest(rpc.AdminCmdType_CompactLog, &rpc.CompactLogRequest{
				CompactIndex: 3,
			})
			for i := 0; i < 10; i++ {
				if hasLog(2) {
					time.Sleep(time.Second)
				} else {
					compactionCompleted = true
					break
				}
				if i == 9 {
					t.Fatalf("failed to remove log entries from logdb")
				}
			}
		}
	}

	assert.True(t, compactionCompleted)

	// restore the network
	atomic.StoreUint64(&skipStore, 0)
	assert.NoError(t, kv.Set("k3", "v3", snapshotTestTimeout))
	assert.NoError(t, kv.Set("k4", "v4", snapshotTestTimeout))
	time.Sleep(5 * time.Second)
	index := uint64(0)
	term := uint64(0)
	if pr := c.GetStore(2).(*store).getReplica(shardID, false); pr != nil {
		index, term = pr.sm.getAppliedIndexTerm()
	} else {
		t.Fatalf("failed to get replica")
	}
	if pr := c.GetStore(2).(*store).getReplica(shardID, false); pr != nil {
		index2, term2 := pr.sm.getAppliedIndexTerm()
		assert.Equal(t, index, index2)
		assert.Equal(t, term, term2)
	} else {
		t.Fatalf("failed to get replica")
	}
}
