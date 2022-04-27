// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
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

package cluster

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestStoreHeartbeat(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(nil))

	n, np := uint64(3), uint64(3)
	stores := newTestStores(n, "2.0.0")
	storeMetasAfterHeartbeat := make([]metapb.Store, 0, n)
	shards := newTestShards(n, np)

	for _, res := range shards {
		cluster.core.PutShard(res)
	}
	assert.Equal(t, int(n), cluster.core.Shards.GetShardCount())

	for i, store := range stores {
		storeStats := &metapb.StoreStats{
			StoreID:    store.Meta.GetID(),
			Capacity:   100,
			Available:  50,
			ShardCount: 1,
		}
		assert.Error(t, cluster.HandleStoreHeartbeat(storeStats))
		assert.Nil(t, cluster.putStoreLocked(store))
		assert.Equal(t, i+1, cluster.GetStoreCount())
		assert.Equal(t, int64(0), store.GetLastHeartbeatTS().UnixNano())
		assert.NoError(t, cluster.HandleStoreHeartbeat(storeStats))

		s := cluster.GetStore(store.Meta.GetID())
		assert.NotEqual(t, int64(0), s.GetLastHeartbeatTS().UnixNano())
		assert.True(t, reflect.DeepEqual(storeStats, s.GetStoreStats()))
		storeMetasAfterHeartbeat = append(storeMetasAfterHeartbeat, s.Meta)
	}

	assert.Equal(t, int(n), cluster.GetStoreCount())

	for i, store := range stores {
		tmp, err := cluster.storage.GetStore(store.Meta.GetID())
		assert.NoError(t, err)
		assert.NotNil(t, tmp)
		assert.True(t, reflect.DeepEqual(*tmp, storeMetasAfterHeartbeat[i]))
	}
}

func TestFilterUnhealthyStore(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(nil))

	stores := newTestStores(3, "2.0.0")
	for _, store := range stores {
		storeStats := &metapb.StoreStats{
			StoreID:    store.Meta.GetID(),
			Capacity:   100,
			Available:  50,
			ShardCount: 1,
		}
		assert.NoError(t, cluster.putStoreLocked(store))
		assert.NoError(t, cluster.HandleStoreHeartbeat(storeStats))
	}

	for _, store := range stores {
		storeStats := &metapb.StoreStats{
			StoreID:    store.Meta.GetID(),
			Capacity:   100,
			Available:  50,
			ShardCount: 1,
		}
		newStore := store.Clone(core.TombstoneStore())
		assert.NoError(t, cluster.putStoreLocked(newStore))
		assert.NoError(t, cluster.HandleStoreHeartbeat(storeStats))
	}
}

func TestSetOfflineStore(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(nil))

	// Put 4 stores.
	for _, store := range newTestStores(4, "2.0.0") {
		assert.NoError(t, cluster.PutStore(store.Meta))
	}

	// store 1: up -> offline
	assert.NoError(t, cluster.RemoveStore(1, false))
	store := cluster.GetStore(1)
	assert.True(t, store.IsOffline())
	assert.False(t, store.IsPhysicallyDestroyed())

	// store 1: set physically to true success
	assert.NoError(t, cluster.RemoveStore(1, true))
	store = cluster.GetStore(1)
	assert.True(t, store.IsOffline())
	assert.True(t, store.IsPhysicallyDestroyed())

	// store 2:up -> offline & physically destroyed
	assert.NoError(t, cluster.RemoveStore(2, true))
	// store 2: set physically destroyed to false failed
	assert.Error(t, cluster.RemoveStore(2, false))
	assert.NoError(t, cluster.RemoveStore(2, true))

	// store 3: up to offline
	assert.NoError(t, cluster.RemoveStore(3, false))
	assert.NoError(t, cluster.RemoveStore(3, false))

	cluster.checkStores()
	// store 1,2,3 shuold be to tombstone
	for storeID := uint64(1); storeID <= 3; storeID++ {
		assert.True(t, cluster.GetStore(storeID).IsTombstone())
	}
	// test bury store
	for storeID := uint64(0); storeID <= 4; storeID++ {
		store := cluster.GetStore(storeID)
		if store == nil || store.IsUp() {
			assert.Error(t, cluster.buryStore(storeID))
		} else {
			assert.NoError(t, cluster.buryStore(storeID))
		}
	}
}

func TestReuseAddress(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(nil))

	// Put 4 stores.
	for _, store := range newTestStores(4, "2.0.0") {
		assert.NoError(t, cluster.PutStore(store.Meta))
	}
	// store 1: up
	// store 2: offline
	assert.NoError(t, cluster.RemoveStore(2, false))
	// store 3: offline and physically destroyed
	assert.NoError(t, cluster.RemoveStore(3, true))
	// store 4: tombstone
	assert.NoError(t, cluster.RemoveStore(4, true))
	assert.NoError(t, cluster.buryStore(4))

	for id := uint64(1); id <= 4; id++ {
		store := cluster.GetStore(id)
		storeID := store.Meta.GetID() + 1000
		v, _ := store.Meta.GetVersionAndGitHash()
		newStore := metapb.Store{
			ID:            storeID,
			ClientAddress: store.Meta.GetClientAddress(),
			State:         metapb.StoreState_Up,
			Version:       v,
			DeployPath:    fmt.Sprintf("test/store%d", storeID),
		}

		if store.IsPhysicallyDestroyed() || store.IsTombstone() {
			// try to start a new store with the same address with store which is physically destryed or tombstone should be success
			assert.NoError(t, cluster.PutStore(newStore))
		} else {
			assert.Error(t, cluster.PutStore(newStore))
		}
	}
}

func TestUpStore(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(nil))
	// Put 3 stores.
	for _, store := range newTestStores(3, "2.0.0") {
		assert.NoError(t, cluster.PutStore(store.Meta))
	}

	// set store 1 offline
	assert.NoError(t, cluster.RemoveStore(1, false))
	// up a offline store should be success.
	assert.NoError(t, cluster.UpStore(1))

	// set store 2 offline and physically destroyed
	assert.NoError(t, cluster.RemoveStore(2, true))
	assert.Error(t, cluster.UpStore(2))

	// bury store 2
	cluster.checkStores()
	// store is tombstone
	err = cluster.UpStore(2)
	assert.True(t, strings.Contains(err.Error(), "tombstone"))

	// store 3 is up
	assert.NoError(t, cluster.UpStore(3))

	// store 4 not exist
	err = cluster.UpStore(4)
	assert.True(t, strings.Contains(err.Error(), "not found"))
}

func TestShardHeartbeat(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(nil))

	n, np := uint64(3), uint64(3)

	stores := newTestStores(3, "2.0.0")
	shards := newTestShards(n, np)

	for _, store := range stores {
		assert.NoError(t, cluster.putStoreLocked(store))
	}

	for i, res := range shards {
		// shard does not exist.
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])
		checkShardsKV(t, cluster.storage, shards[:i+1])

		// shard is the same, not updated.
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])
		checkShardsKV(t, cluster.storage, shards[:i+1])
		origin := res
		// shard is updated.
		res = origin.Clone(core.WithIncVersion())
		shards[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])
		checkShardsKV(t, cluster.storage, shards[:i+1])

		// shard is stale (Version).
		stale := origin.Clone(core.WithIncConfVer())
		assert.Error(t, cluster.processShardHeartbeat(stale))
		checkShards(t, cluster.core.Shards, shards[:i+1])
		checkShardsKV(t, cluster.storage, shards[:i+1])

		// shard is updated.
		res = origin.Clone(
			core.WithIncVersion(),
			core.WithIncConfVer(),
		)
		shards[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])
		checkShardsKV(t, cluster.storage, shards[:i+1])

		// shard is stale (ConfVer).
		stale = origin.Clone(core.WithIncConfVer())
		assert.Error(t, cluster.processShardHeartbeat(stale))
		checkShards(t, cluster.core.Shards, shards[:i+1])
		checkShardsKV(t, cluster.storage, shards[:i+1])

		// Add a down peer.
		res = res.Clone(core.WithDownPeers([]metapb.ReplicaStats{
			{
				Replica:     res.Meta.GetReplicas()[rand.Intn(len(res.Meta.GetReplicas()))],
				DownSeconds: 42,
			},
		}))
		shards[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])

		// Add a pending peer.
		res = res.Clone(core.WithPendingPeers([]metapb.Replica{res.Meta.GetReplicas()[rand.Intn(len(res.Meta.GetReplicas()))]}))
		shards[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])

		// Clear down peers.
		res = res.Clone(core.WithDownPeers(nil))
		shards[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])

		// Clear pending peers.
		res = res.Clone(core.WithPendingPeers(nil))
		shards[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])

		// Remove peers.
		origin = res
		res = origin.Clone(core.SetPeers(res.Meta.GetReplicas()[:1]))
		shards[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])
		checkShardsKV(t, cluster.storage, shards[:i+1])
		// Add peers.
		res = origin
		shards[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])
		checkShardsKV(t, cluster.storage, shards[:i+1])

		// Change leader.
		res = res.Clone(core.WithLeader(&res.Meta.GetReplicas()[1]))
		shards[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])

		// Change ApproximateSize.
		res = res.Clone(core.SetApproximateSize(144))
		shards[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])

		// Change ApproximateKeys.
		res = res.Clone(core.SetApproximateKeys(144000))
		shards[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])

		// Change bytes written.
		res = res.Clone(core.SetWrittenBytes(24000))
		shards[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])

		// Change keys written.
		res = res.Clone(core.SetWrittenKeys(240))
		shards[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])

		// Change bytes read.
		res = res.Clone(core.SetReadBytes(1080000))
		shards[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])

		// Change keys read.
		res = res.Clone(core.SetReadKeys(1080))
		shards[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, shards[:i+1])
	}

	shardCounts := make(map[uint64]int)
	for _, res := range shards {
		for _, peer := range res.Meta.GetReplicas() {
			shardCounts[peer.StoreID]++
		}
	}
	for id, count := range shardCounts {
		assert.Equal(t, count, cluster.GetStoreShardCount("", id))
	}

	for _, res := range cluster.GetShards() {
		checkShard(t, res, shards[res.Meta.GetID()])
	}
	for _, res := range cluster.GetMetaShards() {
		assert.True(t, reflect.DeepEqual(res, shards[res.GetID()].Meta))
	}

	for _, res := range shards {
		for _, store := range cluster.GetShardStores(res) {
			_, ok := res.GetStorePeer(store.Meta.GetID())
			assert.True(t, ok)
		}
		for _, store := range cluster.GetFollowerStores(res) {
			peer, _ := res.GetStorePeer(store.Meta.GetID())
			assert.NotEqual(t, res.GetLeader().GetID(), peer.ID)
		}
	}

	for _, store := range cluster.core.Stores.GetStores() {
		assert.Equal(t, store.GetLeaderCount(""), cluster.core.Shards.GetStoreLeaderCount("", store.Meta.GetID()))
		assert.Equal(t, store.GetShardCount(""), cluster.core.Shards.GetStoreShardCount("", store.Meta.GetID()))
		assert.Equal(t, store.GetLeaderSize(""), cluster.core.Shards.GetStoreLeaderShardSize("", store.Meta.GetID()))
		assert.Equal(t, store.GetShardSize(""), cluster.core.Shards.GetStoreShardSize("", store.Meta.GetID()))
	}

	// Test with storage.
	if storage := cluster.storage; storage != nil {
		for _, res := range shards {
			tmp, err := storage.GetShard(res.Meta.GetID())
			assert.NoError(t, err)
			assert.NotNil(t, tmp)
			assert.True(t, reflect.DeepEqual(*tmp, res.Meta))
		}

		// Check overlap with stale version
		overlapShard := shards[n-1].Clone(
			core.WithStartKey([]byte("")),
			core.WithEndKey([]byte("")),
			core.WithNewShardID(10000),
			core.WithDecVersion(),
		)
		assert.Error(t, cluster.processShardHeartbeat(overlapShard))

		res, err := storage.GetShard(shards[n-1].Meta.GetID())
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(*res, shards[n-1].Meta))

		res, err = storage.GetShard(shards[n-2].Meta.GetID())
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(*res, shards[n-2].Meta))

		res, err = storage.GetShard(overlapShard.Meta.GetID())
		assert.NoError(t, err)
		assert.Nil(t, res)

		// Check overlap
		overlapShard = shards[n-1].Clone(
			core.WithStartKey(shards[n-2].GetStartKey()),
			core.WithNewShardID(shards[n-1].Meta.GetID()+1),
		)
		assert.NoError(t, cluster.processShardHeartbeat(overlapShard))

		res, err = storage.GetShard(shards[n-1].Meta.GetID())
		assert.NoError(t, err)
		assert.Nil(t, res)

		res, err = storage.GetShard(shards[n-2].Meta.GetID())
		assert.NoError(t, err)
		assert.Nil(t, res)

		res, err = storage.GetShard(overlapShard.Meta.GetID())
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(*res, overlapShard.Meta))
	}
}

func TestShardFlowChanged(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(nil))
	shards := []*core.CachedShard{core.NewTestCachedShard([]byte{}, []byte{})}
	processShards := func(shards []*core.CachedShard) {
		for _, r := range shards {
			cluster.processShardHeartbeat(r)
		}
	}
	shards = core.SplitTestShards(shards)
	processShards(shards)
	// update shard
	res := shards[0]
	shards[0] = res.Clone(core.SetReadBytes(1000))
	processShards(shards)
	newShard := cluster.GetShard(res.Meta.GetID())
	assert.Equal(t, uint64(1000), newShard.GetBytesRead())

	// do not trace the flow changes
	processShards([]*core.CachedShard{res})
	newShard = cluster.GetShard(res.Meta.GetID())
	assert.Equal(t, uint64(0), res.GetBytesRead())
	assert.Equal(t, uint64(0), newShard.GetBytesRead())
}

func TestConcurrentShardHeartbeat(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(nil))

	shards := []*core.CachedShard{core.NewTestCachedShard([]byte{}, []byte{})}
	shards = core.SplitTestShards(shards)
	heartbeatShards(t, cluster, shards)

	// Merge shards manually
	source, target := shards[0], shards[1]
	target.Meta.SetStartKey([]byte{})
	target.Meta.SetEndKey([]byte{})
	epoch := source.Meta.GetEpoch()
	epoch.Generation++
	source.Meta.SetEpoch(epoch)
	if source.Meta.GetEpoch().Generation > target.Meta.GetEpoch().Generation {
		epoch = target.Meta.GetEpoch()
		epoch.Generation = source.Meta.GetEpoch().Generation
		target.Meta.SetEpoch(epoch)
	}
	epoch = target.Meta.GetEpoch()
	epoch.Generation++
	target.Meta.SetEpoch(epoch)
}

func TestHeartbeatSplit(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(nil))

	// 1: [nil, nil)
	shard1 := core.NewCachedShard(metapb.Shard{ID: 1, Epoch: metapb.ShardEpoch{Generation: 1, ConfigVer: 1}}, nil)
	assert.NoError(t, cluster.processShardHeartbeat(shard1))
	checkShard(t, cluster.GetShardByKey(0, []byte("foo")), shard1)

	// split 1 to 2: [nil, m) 1: [m, nil), sync 2 first.
	shard1 = shard1.Clone(
		core.WithStartKey([]byte("m")),
		core.WithIncVersion(),
	)
	shard2 := core.NewCachedShard(metapb.Shard{
		ID:    2,
		End:   []byte("m"),
		Epoch: metapb.ShardEpoch{Generation: 1, ConfigVer: 1},
	}, nil)
	assert.NoError(t, cluster.processShardHeartbeat(shard2))
	checkShard(t, cluster.GetShardByKey(0, []byte("a")), shard2)
	// [m, nil) is missing before r1's heartbeat.
	assert.Nil(t, cluster.GetShardByKey(0, []byte("z")))

	assert.NoError(t, cluster.processShardHeartbeat(shard1))
	checkShard(t, cluster.GetShardByKey(0, []byte("z")), shard1)

	// split 1 to 3: [m, q) 1: [q, nil), sync 1 first.
	shard1 = shard1.Clone(
		core.WithStartKey([]byte("q")),
		core.WithIncVersion(),
	)
	shard3 := core.NewCachedShard(metapb.Shard{
		ID:    3,
		Start: []byte("m"),
		End:   []byte("q"),
		Epoch: metapb.ShardEpoch{Generation: 1, ConfigVer: 1},
	}, nil)
	assert.NoError(t, cluster.processShardHeartbeat(shard1))
	checkShard(t, cluster.GetShardByKey(0, []byte("z")), shard1)
	checkShard(t, cluster.GetShardByKey(0, []byte("a")), shard2)
	// [m, q) is missing before r3's heartbeat.
	assert.Nil(t, cluster.GetShardByKey(0, []byte("n")))
	assert.Nil(t, cluster.processShardHeartbeat(shard3))
	checkShard(t, cluster.GetShardByKey(0, []byte("n")), shard3)
}

func TestShardSplitAndMerge(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(nil))

	shards := []*core.CachedShard{core.NewTestCachedShard([]byte{}, []byte{})}

	// Byte will underflow/overflow if n > 7.
	n := 7

	// Split.
	for i := 0; i < n; i++ {
		shards = core.SplitTestShards(shards)
		heartbeatShards(t, cluster, shards)
	}

	// Merge.
	for i := 0; i < n; i++ {
		shards = core.MergeTestShards(shards)
		heartbeatShards(t, cluster, shards)
	}

	// Split twice and merge once.
	for i := 0; i < n*2; i++ {
		if (i+1)%3 == 0 {
			shards = core.MergeTestShards(shards)
		} else {
			shards = core.SplitTestShards(shards)
		}
		heartbeatShards(t, cluster, shards)
	}
}

func TestUpdateStorePendingPeerCount(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	tc := newTestCluster(opt)
	stores := newTestStores(5, "2.0.0")
	for _, s := range stores {
		assert.Nil(t, tc.putStoreLocked(s))
	}
	peers := []metapb.Replica{
		{
			ID:      2,
			StoreID: 1,
		},
		{
			ID:      3,
			StoreID: 2,
		},
		{
			ID:      3,
			StoreID: 3,
		},
		{
			ID:      4,
			StoreID: 4,
		},
	}
	origin := core.NewCachedShard(metapb.Shard{ID: 1, Replicas: peers[:3]}, &peers[0], core.WithPendingPeers(peers[1:3]))
	assert.NoError(t, tc.processShardHeartbeat(origin))
	checkPendingPeerCount(t, []int{0, 1, 1, 0}, tc.RaftCluster)
	newShard := core.NewCachedShard(metapb.Shard{ID: 1, Replicas: peers[1:]}, &peers[1], core.WithPendingPeers(peers[3:4]))
	assert.NoError(t, tc.processShardHeartbeat(newShard))
	checkPendingPeerCount(t, []int{0, 0, 0, 1}, tc.RaftCluster)
}

func TestStores(t *testing.T) {
	n := uint64(10)
	cache := core.NewCachedStores()
	stores := newTestStores(n, "2.0.0")

	for i, store := range stores {
		id := store.Meta.GetID()
		assert.Nil(t, cache.GetStore(id))
		assert.NotNil(t, cache.PauseLeaderTransfer(id))
		cache.SetStore(store)
		assert.True(t, reflect.DeepEqual(store, cache.GetStore(id)))
		assert.Equal(t, i+1, cache.GetStoreCount())
		assert.Nil(t, cache.PauseLeaderTransfer(id))
		assert.False(t, cache.GetStore(id).AllowLeaderTransfer())
		assert.NotNil(t, cache.PauseLeaderTransfer(id))
		cache.ResumeLeaderTransfer(id)
		assert.True(t, cache.GetStore(id).AllowLeaderTransfer())
	}
	assert.Equal(t, int(n), cache.GetStoreCount())

	for _, store := range cache.GetStores() {
		assert.True(t, reflect.DeepEqual(store, stores[store.Meta.GetID()-1]))
	}
	for _, store := range cache.GetMetaStores() {
		assert.True(t, reflect.DeepEqual(store, stores[store.GetID()-1].Meta))
	}

	assert.Equal(t, int(n), cache.GetStoreCount())
}

func TestShards(t *testing.T) {
	n, np := uint64(10), uint64(3)
	shards := newTestShards(n, np)
	_, opts, err := newTestScheduleConfig()
	assert.NoError(t, err)
	tc := newTestRaftCluster(opts, storage.NewTestStorage(), core.NewBasicCluster(nil))
	cache := tc.core.Shards

	for i := uint64(0); i < n; i++ {
		res := shards[i]
		resKey := []byte{byte(i)}

		assert.Nil(t, cache.GetShard(i))
		assert.Nil(t, cache.SearchShard(0, resKey))
		checkShards(t, cache, shards[0:i])

		cache.AddShard(res)
		checkShard(t, cache.GetShard(i), res)
		checkShard(t, cache.SearchShard(0, resKey), res)
		checkShards(t, cache, shards[0:(i+1)])
		// previous shard
		if i == 0 {
			assert.Nil(t, cache.SearchPrevShard(0, resKey))
		} else {
			checkShard(t, cache.SearchPrevShard(0, resKey), shards[i-1])
		}
		// Update leader to peer np-1.
		newShard := res.Clone(core.WithLeader(&res.Meta.GetReplicas()[np-1]))
		shards[i] = newShard
		cache.SetShard(newShard)
		checkShard(t, cache.GetShard(i), newShard)
		checkShard(t, cache.SearchShard(0, resKey), newShard)
		checkShards(t, cache, shards[0:(i+1)])

		cache.RemoveShard(res)
		assert.Nil(t, cache.GetShard(i))
		assert.Nil(t, cache.SearchShard(0, resKey))
		checkShards(t, cache, shards[0:i])

		// Reset leader to peer 0.
		newShard = res.Clone(core.WithLeader(&res.Meta.GetReplicas()[0]))
		shards[i] = newShard
		cache.AddShard(newShard)
		checkShard(t, cache.GetShard(i), newShard)
		checkShards(t, cache, shards[0:(i+1)])
		checkShard(t, cache.SearchShard(0, resKey), newShard)
	}

	for i := uint64(0); i < n; i++ {
		res := tc.RandLeaderShard("", i, []core.KeyRange{core.NewKeyRange(0, "", "")}, opt.HealthShard(tc))
		assert.Equal(t, i, res.GetLeader().GetStoreID())

		res = tc.RandFollowerShard("", i, []core.KeyRange{core.NewKeyRange(0, "", "")}, opt.HealthShard(tc))
		assert.NotEqual(t, i, res.GetLeader().GetStoreID())
		_, ok := res.GetStorePeer(i)
		assert.True(t, ok)
	}

	// check overlaps
	// clone it otherwise there are two items with the same key in the tree
	overlapShard := shards[n-1].Clone(core.WithStartKey(shards[n-2].GetStartKey()))
	cache.AddShard(overlapShard)
	assert.Nil(t, cache.GetShard(n-2))
	assert.NotNil(t, cache.GetShard(n-1))

	// All shards will be filtered out if they have pending peers.
	for i := uint64(0); i < n; i++ {
		for j := 0; j < cache.GetStoreLeaderCount("", i); j++ {
			res := tc.RandLeaderShard("", i, []core.KeyRange{core.NewKeyRange(0, "", "")}, opt.HealthShard(tc))
			newRes := res.Clone(core.WithPendingPeers(res.Meta.GetReplicas()))
			cache.SetShard(newRes)
		}
		assert.Nil(t, tc.RandLeaderShard("", i, []core.KeyRange{core.NewKeyRange(0, "", "")}, opt.HealthShard(tc)))
	}
	for i := uint64(0); i < n; i++ {
		assert.Nil(t, tc.RandFollowerShard("", i, []core.KeyRange{core.NewKeyRange(0, "", "")}, opt.HealthShard(tc)))
	}
}

func TestCheckStaleShard(t *testing.T) {
	// (0, 0) v.s. (0, 0)
	shard := core.NewTestCachedShard([]byte{}, []byte{})
	origin := core.NewTestCachedShard([]byte{}, []byte{})
	assert.Nil(t, checkStaleShard(shard.Meta, origin.Meta))
	assert.Nil(t, checkStaleShard(origin.Meta, shard.Meta))

	// (1, 0) v.s. (0, 0)
	shard.Meta.Epoch.Generation++
	assert.Nil(t, checkStaleShard(origin.Meta, shard.Meta))
	assert.NotNil(t, checkStaleShard(shard.Meta, origin.Meta))

	// (1, 1) v.s. (0, 0)
	shard.Meta.Epoch.Generation++
	assert.Nil(t, checkStaleShard(origin.Meta, shard.Meta))
	assert.NotNil(t, checkStaleShard(shard.Meta, origin.Meta))

	// (0, 1) v.s. (0, 0)
	shard.Meta.Epoch.Generation++
	assert.Nil(t, checkStaleShard(origin.Meta, shard.Meta))
	assert.NotNil(t, checkStaleShard(shard.Meta, origin.Meta))
}

type testCluster struct {
	*RaftCluster
}

func newTestScheduleConfig() (*config.ScheduleConfig, *config.PersistOptions, error) {
	cfg := config.NewConfig()
	cfg.Schedule.TolerantSizeRatio = 5
	if err := cfg.Adjust(nil, false); err != nil {
		return nil, nil, err
	}
	opt := config.NewPersistOptions(cfg, nil)
	return &cfg.Schedule, opt, nil
}

func newTestCluster(opt *config.PersistOptions) *testCluster {
	storage := storage.NewTestStorage()
	rc := newTestRaftCluster(opt, storage, core.NewBasicCluster(nil))
	rc.ruleManager = placement.NewRuleManager(storage, rc, nil)
	if opt.IsPlacementRulesEnabled() {
		err := rc.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels())
		if err != nil {
			panic(err)
		}
	}

	return &testCluster{RaftCluster: rc}
}

func newTestRaftCluster(opt *config.PersistOptions, storage storage.Storage, basicCluster *core.BasicCluster) *RaftCluster {
	rc := &RaftCluster{ctx: context.TODO(), logger: log.Adjust(nil)}
	rc.InitCluster(opt, storage, basicCluster)
	basicCluster.ScheduleGroupKeys[""] = struct{}{}
	return rc
}

func newTestShardMeta(shardID uint64) *metapb.Shard {
	return &metapb.Shard{
		ID:    shardID,
		Start: []byte(fmt.Sprintf("%20d", shardID)),
		End:   []byte(fmt.Sprintf("%20d", shardID+1)),
		Epoch: metapb.ShardEpoch{Generation: 1, ConfigVer: 1},
	}
}

// Create n stores (0..n).
func newTestStores(n uint64, version string) []*core.CachedStore {
	stores := make([]*core.CachedStore, 0, n)
	for i := uint64(1); i <= n; i++ {
		store := metapb.Store{
			ID:            i,
			ClientAddress: fmt.Sprintf("127.0.0.1:%d", i),
			State:         metapb.StoreState_Up,
			Version:       version,
			DeployPath:    fmt.Sprintf("test/store%d", i),
		}
		stores = append(stores, core.NewCachedStore(store))
	}
	return stores
}

// Create n shards (0..n) of n stores (0..n).
// Each shard contains np peers, the first peer is the leader.
func newTestShards(n, np uint64) []*core.CachedShard {
	shards := make([]*core.CachedShard, 0, n)
	for i := uint64(0); i < n; i++ {
		peers := make([]metapb.Replica, 0, np)
		for j := uint64(0); j < np; j++ {
			peer := metapb.Replica{
				ID: i*np + j,
			}
			peer.StoreID = (i + j) % n
			peers = append(peers, peer)
		}
		res := metapb.Shard{
			ID:       i,
			Replicas: peers,
			Start:    []byte{byte(i)},
			End:      []byte{byte(i + 1)},
			Epoch:    metapb.ShardEpoch{ConfigVer: 2, Generation: 2},
		}
		shards = append(shards, core.NewCachedShard(res, &peers[0]))
	}
	return shards
}

func heartbeatShards(t *testing.T, cluster *RaftCluster, shards []*core.CachedShard) {
	// Heartbeat and check shard one by one.
	for _, r := range shards {
		assert.NoError(t, cluster.processShardHeartbeat(r))

		checkShard(t, cluster.GetShard(r.Meta.GetID()), r)
		checkShard(t, cluster.GetShardByKey(0, r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkShard(t, cluster.GetShardByKey(0, []byte{end - 1}), r)
		}
	}

	// Check all shards after handling all heartbeats.
	for _, r := range shards {
		checkShard(t, cluster.GetShard(r.Meta.GetID()), r)
		checkShard(t, cluster.GetShardByKey(0, r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkShard(t, cluster.GetShardByKey(0, []byte{end - 1}), r)
			result := cluster.GetShardByKey(0, []byte{end + 1})
			assert.NotEqual(t, r.Meta.GetID(), result.Meta.GetID())
		}
	}
}

func checkShard(t *testing.T, a *core.CachedShard, b *core.CachedShard) {
	assert.True(t, reflect.DeepEqual(a, b))
	assert.True(t, reflect.DeepEqual(a.Meta, b.Meta))
	assert.True(t, reflect.DeepEqual(a.GetLeader(), b.GetLeader()))
	assert.True(t, reflect.DeepEqual(a.Meta.GetReplicas(), b.Meta.GetReplicas()))
	if len(a.GetDownPeers()) > 0 || len(b.GetDownPeers()) > 0 {
		assert.True(t, reflect.DeepEqual(a.GetDownPeers(), b.GetDownPeers()))
	}
	if len(a.GetPendingPeers()) > 0 || len(b.GetPendingPeers()) > 0 {
		assert.True(t, reflect.DeepEqual(a.GetPendingPeers(), b.GetPendingPeers()))
	}
}

func checkShardsKV(t *testing.T, s storage.Storage, shards []*core.CachedShard) {
	if s != nil {
		for _, res := range shards {
			meta, err := s.GetShard(res.Meta.GetID())
			assert.NoError(t, err)
			assert.NotNil(t, meta)
			assert.True(t, reflect.DeepEqual(*meta, res.Meta))
		}
	}
}

func checkShards(t *testing.T, cache *core.ShardsContainer, shards []*core.CachedShard) {
	shardCount := make(map[uint64]int)
	leaderCount := make(map[uint64]int)
	followerCount := make(map[uint64]int)
	for _, res := range shards {
		for _, peer := range res.Meta.GetReplicas() {
			shardCount[peer.StoreID]++
			if peer.ID == res.GetLeader().ID {
				leaderCount[peer.StoreID]++
				checkShard(t, cache.GetLeader("", peer.StoreID, res), res)
			} else {
				followerCount[peer.StoreID]++
				checkShard(t, cache.GetFollower("", peer.StoreID, res), res)
			}
		}
	}

	assert.Equal(t, len(shards), cache.GetShardCount())
	for id, count := range shardCount {
		assert.Equal(t, cache.GetStoreShardCount("", id), count)
	}
	for id, count := range leaderCount {
		assert.Equal(t, cache.GetStoreLeaderCount("", id), count)
	}
	for id, count := range followerCount {
		assert.Equal(t, cache.GetStoreFollowerCount("", id), count)
	}

	for _, res := range cache.GetShards() {
		checkShard(t, res, shards[res.Meta.GetID()])
	}
	for _, res := range cache.GetMetaShards() {
		assert.True(t, reflect.DeepEqual(res, shards[res.GetID()].Meta))
	}
}

func checkPendingPeerCount(t *testing.T, expect []int, cluster *RaftCluster) {
	for i, e := range expect {
		s := cluster.core.Stores.GetStore(uint64(i + 1))
		assert.Equal(t, e, s.GetPendingPeerCount())
	}
}

func checkStaleShard(origin, res metapb.Shard) error {
	o := origin.GetEpoch()
	e := res.GetEpoch()

	if e.GetGeneration() < o.GetGeneration() || e.GetConfigVer() < o.GetConfigVer() {
		return fmt.Errorf("shard is stale: shard %v origin %v", res, origin)
	}

	return nil
}
