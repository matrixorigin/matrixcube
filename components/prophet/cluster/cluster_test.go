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
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestStoreHeartbeat(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestShardFactory, nil))

	n, np := uint64(3), uint64(3)
	containers := newTestStores(n, "2.0.0")
	containerMetasAfterHeartbeat := make([]metadata.Store, 0, n)
	resources := newTestShards(n, np)

	for _, res := range resources {
		cluster.core.PutShard(res)
	}
	assert.Equal(t, int(n), cluster.core.Shards.GetShardCount())

	for i, container := range containers {
		containerStats := &metapb.StoreStats{
			StoreID:   container.Meta.ID(),
			Capacity:      100,
			Available:     50,
			ShardCount: 1,
		}
		assert.Error(t, cluster.HandleStoreHeartbeat(containerStats))
		assert.Nil(t, cluster.putStoreLocked(container))
		assert.Equal(t, i+1, cluster.GetStoreCount())
		assert.Equal(t, int64(0), container.GetLastHeartbeatTS().UnixNano())
		assert.NoError(t, cluster.HandleStoreHeartbeat(containerStats))

		s := cluster.GetStore(container.Meta.ID())
		assert.NotEqual(t, int64(0), s.GetLastHeartbeatTS().UnixNano())
		assert.True(t, reflect.DeepEqual(containerStats, s.GetStoreStats()))
		containerMetasAfterHeartbeat = append(containerMetasAfterHeartbeat, s.Meta)
	}

	assert.Equal(t, int(n), cluster.GetStoreCount())

	for i, container := range containers {
		tmp, err := cluster.storage.GetStore(container.Meta.ID())
		assert.NoError(t, err)
		assert.NotNil(t, tmp)
		assert.True(t, reflect.DeepEqual(tmp, containerMetasAfterHeartbeat[i]))
	}
}

func TestFilterUnhealthyStore(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestShardFactory, nil))

	containers := newTestStores(3, "2.0.0")
	for _, container := range containers {
		containerStats := &metapb.StoreStats{
			StoreID:   container.Meta.ID(),
			Capacity:      100,
			Available:     50,
			ShardCount: 1,
		}
		assert.NoError(t, cluster.putStoreLocked(container))
		assert.NoError(t, cluster.HandleStoreHeartbeat(containerStats))
		assert.NotNil(t, cluster.hotStat.GetRollingStoreStats(container.Meta.ID()))
	}

	for _, container := range containers {
		containerStats := &metapb.StoreStats{
			StoreID:   container.Meta.ID(),
			Capacity:      100,
			Available:     50,
			ShardCount: 1,
		}
		newStore := container.Clone(core.TombstoneStore())
		assert.NoError(t, cluster.putStoreLocked(newStore))
		assert.NoError(t, cluster.HandleStoreHeartbeat(containerStats))
		assert.Nil(t, cluster.hotStat.GetRollingStoreStats(container.Meta.ID()))
	}
}

func TestSetOfflineStore(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestShardFactory, nil))

	// Put 4 containers.
	for _, container := range newTestStores(4, "2.0.0") {
		assert.NoError(t, cluster.PutStore(container.Meta))
	}

	// container 1: up -> offline
	assert.NoError(t, cluster.RemoveStore(1, false))
	container := cluster.GetStore(1)
	assert.True(t, container.IsOffline())
	assert.False(t, container.IsPhysicallyDestroyed())

	// container 1: set physically to true success
	assert.NoError(t, cluster.RemoveStore(1, true))
	container = cluster.GetStore(1)
	assert.True(t, container.IsOffline())
	assert.True(t, container.IsPhysicallyDestroyed())

	// container 2:up -> offline & physically destroyed
	assert.NoError(t, cluster.RemoveStore(2, true))
	// container 2: set physically destroyed to false failed
	assert.Error(t, cluster.RemoveStore(2, false))
	assert.NoError(t, cluster.RemoveStore(2, true))

	// container 3: up to offline
	assert.NoError(t, cluster.RemoveStore(3, false))
	assert.NoError(t, cluster.RemoveStore(3, false))

	cluster.checkStores()
	// container 1,2,3 shuold be to tombstone
	for containerID := uint64(1); containerID <= 3; containerID++ {
		assert.True(t, cluster.GetStore(containerID).IsTombstone())
	}
	// test bury container
	for containerID := uint64(0); containerID <= 4; containerID++ {
		container := cluster.GetStore(containerID)
		if container == nil || container.IsUp() {
			assert.Error(t, cluster.buryStore(containerID))
		} else {
			assert.NoError(t, cluster.buryStore(containerID))
		}
	}
}

func TestReuseAddress(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestShardFactory, nil))

	// Put 4 containers.
	for _, container := range newTestStores(4, "2.0.0") {
		assert.NoError(t, cluster.PutStore(container.Meta))
	}
	// container 1: up
	// container 2: offline
	assert.NoError(t, cluster.RemoveStore(2, false))
	// container 3: offline and physically destroyed
	assert.NoError(t, cluster.RemoveStore(3, true))
	// container 4: tombstone
	assert.NoError(t, cluster.RemoveStore(4, true))
	assert.NoError(t, cluster.buryStore(4))

	for id := uint64(1); id <= 4; id++ {
		container := cluster.GetStore(id)
		containerID := container.Meta.ID() + 1000
		v, _ := container.Meta.Version()
		newStore := &metadata.TestStore{
			CID:         containerID,
			CAddr:       container.Meta.Addr(),
			CState:      metapb.StoreState_UP,
			CVerion:     v,
			CDeployPath: fmt.Sprintf("test/container%d", containerID),
		}

		if container.IsPhysicallyDestroyed() || container.IsTombstone() {
			// try to start a new container with the same address with container which is physically destryed or tombstone should be success
			assert.NoError(t, cluster.PutStore(newStore))
		} else {
			assert.Error(t, cluster.PutStore(newStore))
		}
	}
}

func TestUpStore(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestShardFactory, nil))
	// Put 3 stores.
	for _, container := range newTestStores(3, "2.0.0") {
		assert.NoError(t, cluster.PutStore(container.Meta))
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
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestShardFactory, nil))

	n, np := uint64(3), uint64(3)

	containers := newTestStores(3, "2.0.0")
	resources := newTestShards(n, np)

	for _, container := range containers {
		assert.NoError(t, cluster.putStoreLocked(container))
	}

	for i, res := range resources {
		// resource does not exist.
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])
		checkShardsKV(t, cluster.storage, resources[:i+1])

		// resource is the same, not updated.
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])
		checkShardsKV(t, cluster.storage, resources[:i+1])
		origin := res
		// resource is updated.
		res = origin.Clone(core.WithIncVersion())
		resources[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])
		checkShardsKV(t, cluster.storage, resources[:i+1])

		// resource is stale (Version).
		stale := origin.Clone(core.WithIncConfVer())
		assert.Error(t, cluster.processShardHeartbeat(stale))
		checkShards(t, cluster.core.Shards, resources[:i+1])
		checkShardsKV(t, cluster.storage, resources[:i+1])

		// resource is updated.
		res = origin.Clone(
			core.WithIncVersion(),
			core.WithIncConfVer(),
		)
		resources[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])
		checkShardsKV(t, cluster.storage, resources[:i+1])

		// resource is stale (ConfVer).
		stale = origin.Clone(core.WithIncConfVer())
		assert.Error(t, cluster.processShardHeartbeat(stale))
		checkShards(t, cluster.core.Shards, resources[:i+1])
		checkShardsKV(t, cluster.storage, resources[:i+1])

		// Add a down peer.
		res = res.Clone(core.WithDownPeers([]metapb.ReplicaStats{
			{
				Replica:     res.Meta.Peers()[rand.Intn(len(res.Meta.Peers()))],
				DownSeconds: 42,
			},
		}))
		resources[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])

		// Add a pending peer.
		res = res.Clone(core.WithPendingPeers([]metapb.Replica{res.Meta.Peers()[rand.Intn(len(res.Meta.Peers()))]}))
		resources[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])

		// Clear down peers.
		res = res.Clone(core.WithDownPeers(nil))
		resources[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])

		// Clear pending peers.
		res = res.Clone(core.WithPendingPeers(nil))
		resources[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])

		// Remove peers.
		origin = res
		res = origin.Clone(core.SetPeers(res.Meta.Peers()[:1]))
		resources[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])
		checkShardsKV(t, cluster.storage, resources[:i+1])
		// Add peers.
		res = origin
		resources[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])
		checkShardsKV(t, cluster.storage, resources[:i+1])

		// Change leader.
		res = res.Clone(core.WithLeader(&res.Meta.Peers()[1]))
		resources[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])

		// Change ApproximateSize.
		res = res.Clone(core.SetApproximateSize(144))
		resources[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])

		// Change ApproximateKeys.
		res = res.Clone(core.SetApproximateKeys(144000))
		resources[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])

		// Change bytes written.
		res = res.Clone(core.SetWrittenBytes(24000))
		resources[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])

		// Change keys written.
		res = res.Clone(core.SetWrittenKeys(240))
		resources[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])

		// Change bytes read.
		res = res.Clone(core.SetReadBytes(1080000))
		resources[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])

		// Change keys read.
		res = res.Clone(core.SetReadKeys(1080))
		resources[i] = res
		assert.NoError(t, cluster.processShardHeartbeat(res))
		checkShards(t, cluster.core.Shards, resources[:i+1])
	}

	resourceCounts := make(map[uint64]int)
	for _, res := range resources {
		for _, peer := range res.Meta.Peers() {
			resourceCounts[peer.StoreID]++
		}
	}
	for id, count := range resourceCounts {
		assert.Equal(t, count, cluster.GetStoreShardCount("", id))
	}

	for _, res := range cluster.GetShards() {
		checkShard(t, res, resources[res.Meta.ID()])
	}
	for _, res := range cluster.GetMetaShards() {
		assert.True(t, reflect.DeepEqual(res, resources[res.ID()].Meta))
	}

	for _, res := range resources {
		for _, container := range cluster.GetShardStores(res) {
			_, ok := res.GetStorePeer(container.Meta.ID())
			assert.True(t, ok)
		}
		for _, container := range cluster.GetFollowerStores(res) {
			peer, _ := res.GetStorePeer(container.Meta.ID())
			assert.NotEqual(t, res.GetLeader().GetID(), peer.ID)
		}
	}

	for _, container := range cluster.core.Stores.GetStores() {
		assert.Equal(t, container.GetLeaderCount(""), cluster.core.Shards.GetStoreLeaderCount("", container.Meta.ID()))
		assert.Equal(t, container.GetShardCount(""), cluster.core.Shards.GetStoreShardCount("", container.Meta.ID()))
		assert.Equal(t, container.GetLeaderSize(""), cluster.core.Shards.GetStoreLeaderShardSize("", container.Meta.ID()))
		assert.Equal(t, container.GetShardSize(""), cluster.core.Shards.GetStoreShardSize("", container.Meta.ID()))
	}

	// Test with storage.
	if storage := cluster.storage; storage != nil {
		for _, res := range resources {
			tmp, err := storage.GetShard(res.Meta.ID())
			assert.NoError(t, err)
			assert.NotNil(t, tmp)
			assert.True(t, reflect.DeepEqual(tmp, res.Meta))
		}

		// Check overlap with stale version
		overlapShard := resources[n-1].Clone(
			core.WithStartKey([]byte("")),
			core.WithEndKey([]byte("")),
			core.WithNewShardID(10000),
			core.WithDecVersion(),
		)
		assert.Error(t, cluster.processShardHeartbeat(overlapShard))

		res, err := storage.GetShard(resources[n-1].Meta.ID())
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(res, resources[n-1].Meta))

		res, err = storage.GetShard(resources[n-2].Meta.ID())
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(res, resources[n-2].Meta))

		res, err = storage.GetShard(overlapShard.Meta.ID())
		assert.NoError(t, err)
		assert.Nil(t, res)

		// Check overlap
		overlapShard = resources[n-1].Clone(
			core.WithStartKey(resources[n-2].GetStartKey()),
			core.WithNewShardID(resources[n-1].Meta.ID()+1),
		)
		assert.NoError(t, cluster.processShardHeartbeat(overlapShard))

		res, err = storage.GetShard(resources[n-1].Meta.ID())
		assert.NoError(t, err)
		assert.Nil(t, res)

		res, err = storage.GetShard(resources[n-2].Meta.ID())
		assert.NoError(t, err)
		assert.Nil(t, res)

		res, err = storage.GetShard(overlapShard.Meta.ID())
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(res, overlapShard.Meta))
	}
}

func TestShardFlowChanged(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestShardFactory, nil))
	resources := []*core.CachedShard{core.NewTestCachedShard([]byte{}, []byte{})}
	processShards := func(resources []*core.CachedShard) {
		for _, r := range resources {
			cluster.processShardHeartbeat(r)
		}
	}
	resources = core.SplitTestShards(resources)
	processShards(resources)
	// update resource
	res := resources[0]
	resources[0] = res.Clone(core.SetReadBytes(1000))
	processShards(resources)
	newShard := cluster.GetShard(res.Meta.ID())
	assert.Equal(t, uint64(1000), newShard.GetBytesRead())

	// do not trace the flow changes
	processShards([]*core.CachedShard{res})
	newShard = cluster.GetShard(res.Meta.ID())
	assert.Equal(t, uint64(0), res.GetBytesRead())
	assert.Equal(t, uint64(0), newShard.GetBytesRead())
}

func TestConcurrentShardHeartbeat(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestShardFactory, nil))

	resources := []*core.CachedShard{core.NewTestCachedShard([]byte{}, []byte{})}
	resources = core.SplitTestShards(resources)
	heartbeatShards(t, cluster, resources)

	// Merge resources manually
	source, target := resources[0], resources[1]
	target.Meta.SetStartKey([]byte{})
	target.Meta.SetEndKey([]byte{})
	epoch := source.Meta.Epoch()
	epoch.Version++
	source.Meta.SetEpoch(epoch)
	if source.Meta.Epoch().Version > target.Meta.Epoch().Version {
		epoch = target.Meta.Epoch()
		epoch.Version = source.Meta.Epoch().Version
		target.Meta.SetEpoch(epoch)
	}
	epoch = target.Meta.Epoch()
	epoch.Version++
	target.Meta.SetEpoch(epoch)
}

func TestHeartbeatSplit(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestShardFactory, nil))

	// 1: [nil, nil)
	resource1 := core.NewCachedShard(&metadata.TestShard{ResID: 1, ResEpoch: metapb.ShardEpoch{Version: 1, ConfVer: 1}}, nil)
	assert.NoError(t, cluster.processShardHeartbeat(resource1))
	checkShard(t, cluster.GetShardByKey(0, []byte("foo")), resource1)

	// split 1 to 2: [nil, m) 1: [m, nil), sync 2 first.
	resource1 = resource1.Clone(
		core.WithStartKey([]byte("m")),
		core.WithIncVersion(),
	)
	resource2 := core.NewCachedShard(&metadata.TestShard{ResID: 2, End: []byte("m"), ResEpoch: metapb.ShardEpoch{Version: 1, ConfVer: 1}}, nil)
	assert.NoError(t, cluster.processShardHeartbeat(resource2))
	checkShard(t, cluster.GetShardByKey(0, []byte("a")), resource2)
	// [m, nil) is missing before r1's heartbeat.
	assert.Nil(t, cluster.GetShardByKey(0, []byte("z")))

	assert.NoError(t, cluster.processShardHeartbeat(resource1))
	checkShard(t, cluster.GetShardByKey(0, []byte("z")), resource1)

	// split 1 to 3: [m, q) 1: [q, nil), sync 1 first.
	resource1 = resource1.Clone(
		core.WithStartKey([]byte("q")),
		core.WithIncVersion(),
	)
	resource3 := core.NewCachedShard(&metadata.TestShard{ResID: 3, Start: []byte("m"), End: []byte("q"), ResEpoch: metapb.ShardEpoch{Version: 1, ConfVer: 1}}, nil)
	assert.NoError(t, cluster.processShardHeartbeat(resource1))
	checkShard(t, cluster.GetShardByKey(0, []byte("z")), resource1)
	checkShard(t, cluster.GetShardByKey(0, []byte("a")), resource2)
	// [m, q) is missing before r3's heartbeat.
	assert.Nil(t, cluster.GetShardByKey(0, []byte("n")))
	assert.Nil(t, cluster.processShardHeartbeat(resource3))
	checkShard(t, cluster.GetShardByKey(0, []byte("n")), resource3)
}

func TestShardSplitAndMerge(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestShardFactory, nil))

	resources := []*core.CachedShard{core.NewTestCachedShard([]byte{}, []byte{})}

	// Byte will underflow/overflow if n > 7.
	n := 7

	// Split.
	for i := 0; i < n; i++ {
		resources = core.SplitTestShards(resources)
		heartbeatShards(t, cluster, resources)
	}

	// Merge.
	for i := 0; i < n; i++ {
		resources = core.MergeTestShards(resources)
		heartbeatShards(t, cluster, resources)
	}

	// Split twice and merge once.
	for i := 0; i < n*2; i++ {
		if (i+1)%3 == 0 {
			resources = core.MergeTestShards(resources)
		} else {
			resources = core.SplitTestShards(resources)
		}
		heartbeatShards(t, cluster, resources)
	}
}

func TestUpdateStorePendingPeerCount(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	tc := newTestCluster(opt)
	containers := newTestStores(5, "2.0.0")
	for _, s := range containers {
		assert.Nil(t, tc.putStoreLocked(s))
	}
	peers := []metapb.Replica{
		{
			ID:          2,
			StoreID: 1,
		},
		{
			ID:          3,
			StoreID: 2,
		},
		{
			ID:          3,
			StoreID: 3,
		},
		{
			ID:          4,
			StoreID: 4,
		},
	}
	origin := core.NewCachedShard(&metadata.TestShard{ResID: 1, ResPeers: peers[:3]}, &peers[0], core.WithPendingPeers(peers[1:3]))
	assert.NoError(t, tc.processShardHeartbeat(origin))
	checkPendingPeerCount(t, []int{0, 1, 1, 0}, tc.RaftCluster)
	newShard := core.NewCachedShard(&metadata.TestShard{ResID: 1, ResPeers: peers[1:]}, &peers[1], core.WithPendingPeers(peers[3:4]))
	assert.NoError(t, tc.processShardHeartbeat(newShard))
	checkPendingPeerCount(t, []int{0, 0, 0, 1}, tc.RaftCluster)
}

func TestStores(t *testing.T) {
	n := uint64(10)
	cache := core.NewCachedStores()
	containers := newTestStores(n, "2.0.0")

	for i, container := range containers {
		id := container.Meta.ID()
		assert.Nil(t, cache.GetStore(id))
		assert.NotNil(t, cache.PauseLeaderTransfer(id))
		cache.SetStore(container)
		assert.True(t, reflect.DeepEqual(container, cache.GetStore(id)))
		assert.Equal(t, i+1, cache.GetStoreCount())
		assert.Nil(t, cache.PauseLeaderTransfer(id))
		assert.False(t, cache.GetStore(id).AllowLeaderTransfer())
		assert.NotNil(t, cache.PauseLeaderTransfer(id))
		cache.ResumeLeaderTransfer(id)
		assert.True(t, cache.GetStore(id).AllowLeaderTransfer())
	}
	assert.Equal(t, int(n), cache.GetStoreCount())

	for _, container := range cache.GetStores() {
		assert.True(t, reflect.DeepEqual(container, containers[container.Meta.ID()-1]))
	}
	for _, container := range cache.GetMetaStores() {
		assert.True(t, reflect.DeepEqual(container, containers[container.ID()-1].Meta))
	}

	assert.Equal(t, int(n), cache.GetStoreCount())
}

func TestShards(t *testing.T) {
	n, np := uint64(10), uint64(3)
	resources := newTestShards(n, np)
	_, opts, err := newTestScheduleConfig()
	assert.NoError(t, err)
	tc := newTestRaftCluster(opts, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestShardFactory, nil))
	cache := tc.core.Shards

	for i := uint64(0); i < n; i++ {
		res := resources[i]
		resKey := []byte{byte(i)}

		assert.Nil(t, cache.GetShard(i))
		assert.Nil(t, cache.SearchShard(0, resKey))
		checkShards(t, cache, resources[0:i])

		cache.AddShard(res)
		checkShard(t, cache.GetShard(i), res)
		checkShard(t, cache.SearchShard(0, resKey), res)
		checkShards(t, cache, resources[0:(i+1)])
		// previous resource
		if i == 0 {
			assert.Nil(t, cache.SearchPrevShard(0, resKey))
		} else {
			checkShard(t, cache.SearchPrevShard(0, resKey), resources[i-1])
		}
		// Update leader to peer np-1.
		newShard := res.Clone(core.WithLeader(&res.Meta.Peers()[np-1]))
		resources[i] = newShard
		cache.SetShard(newShard)
		checkShard(t, cache.GetShard(i), newShard)
		checkShard(t, cache.SearchShard(0, resKey), newShard)
		checkShards(t, cache, resources[0:(i+1)])

		cache.RemoveShard(res)
		assert.Nil(t, cache.GetShard(i))
		assert.Nil(t, cache.SearchShard(0, resKey))
		checkShards(t, cache, resources[0:i])

		// Reset leader to peer 0.
		newShard = res.Clone(core.WithLeader(&res.Meta.Peers()[0]))
		resources[i] = newShard
		cache.AddShard(newShard)
		checkShard(t, cache.GetShard(i), newShard)
		checkShards(t, cache, resources[0:(i+1)])
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
	overlapShard := resources[n-1].Clone(core.WithStartKey(resources[n-2].GetStartKey()))
	cache.AddShard(overlapShard)
	assert.Nil(t, cache.GetShard(n-2))
	assert.NotNil(t, cache.GetShard(n-1))

	// All resources will be filtered out if they have pending peers.
	for i := uint64(0); i < n; i++ {
		for j := 0; j < cache.GetStoreLeaderCount("", i); j++ {
			res := tc.RandLeaderShard("", i, []core.KeyRange{core.NewKeyRange(0, "", "")}, opt.HealthShard(tc))
			newRes := res.Clone(core.WithPendingPeers(res.Meta.Peers()))
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
	resource := core.NewTestCachedShard([]byte{}, []byte{})
	origin := core.NewTestCachedShard([]byte{}, []byte{})
	assert.Nil(t, checkStaleShard(resource.Meta, origin.Meta))
	assert.Nil(t, checkStaleShard(origin.Meta, resource.Meta))

	// (1, 0) v.s. (0, 0)
	resource.Meta.(*metadata.TestShard).ResEpoch.Version++
	assert.Nil(t, checkStaleShard(origin.Meta, resource.Meta))
	assert.NotNil(t, checkStaleShard(resource.Meta, origin.Meta))

	// (1, 1) v.s. (0, 0)
	resource.Meta.(*metadata.TestShard).ResEpoch.Version++
	assert.Nil(t, checkStaleShard(origin.Meta, resource.Meta))
	assert.NotNil(t, checkStaleShard(resource.Meta, origin.Meta))

	// (0, 1) v.s. (0, 0)
	resource.Meta.(*metadata.TestShard).ResEpoch.Version++
	assert.Nil(t, checkStaleShard(origin.Meta, resource.Meta))
	assert.NotNil(t, checkStaleShard(resource.Meta, origin.Meta))
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
	rc := newTestRaftCluster(opt, storage, core.NewBasicCluster(metadata.TestShardFactory, nil))
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
	rc := &RaftCluster{ctx: context.TODO(), adapter: metadata.NewTestAdapter(), logger: log.Adjust(nil)}
	rc.InitCluster(opt, storage, basicCluster)
	basicCluster.ScheduleGroupKeys[""] = struct{}{}
	return rc
}

func newTestShardMeta(resourceID uint64) metadata.Shard {
	return &metadata.TestShard{
		ResID:    resourceID,
		Start:    []byte(fmt.Sprintf("%20d", resourceID)),
		End:      []byte(fmt.Sprintf("%20d", resourceID+1)),
		ResEpoch: metapb.ShardEpoch{Version: 1, ConfVer: 1},
	}
}

// Create n containers (0..n).
func newTestStores(n uint64, version string) []*core.CachedStore {
	containers := make([]*core.CachedStore, 0, n)
	for i := uint64(1); i <= n; i++ {
		container := &metadata.TestStore{
			CID:         i,
			CAddr:       fmt.Sprintf("127.0.0.1:%d", i),
			CState:      metapb.StoreState_UP,
			CVerion:     version,
			CDeployPath: fmt.Sprintf("test/container%d", i),
		}
		containers = append(containers, core.NewCachedStore(container))
	}
	return containers
}

// Create n resources (0..n) of n containers (0..n).
// Each resource contains np peers, the first peer is the leader.
func newTestShards(n, np uint64) []*core.CachedShard {
	resources := make([]*core.CachedShard, 0, n)
	for i := uint64(0); i < n; i++ {
		peers := make([]metapb.Replica, 0, np)
		for j := uint64(0); j < np; j++ {
			peer := metapb.Replica{
				ID: i*np + j,
			}
			peer.StoreID = (i + j) % n
			peers = append(peers, peer)
		}
		res := &metadata.TestShard{
			ResID:    i,
			ResPeers: peers,
			Start:    []byte{byte(i)},
			End:      []byte{byte(i + 1)},
			ResEpoch: metapb.ShardEpoch{ConfVer: 2, Version: 2},
		}
		resources = append(resources, core.NewCachedShard(res, &peers[0]))
	}
	return resources
}

func heartbeatShards(t *testing.T, cluster *RaftCluster, resources []*core.CachedShard) {
	// Heartbeat and check resource one by one.
	for _, r := range resources {
		assert.NoError(t, cluster.processShardHeartbeat(r))

		checkShard(t, cluster.GetShard(r.Meta.ID()), r)
		checkShard(t, cluster.GetShardByKey(0, r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkShard(t, cluster.GetShardByKey(0, []byte{end - 1}), r)
		}
	}

	// Check all resources after handling all heartbeats.
	for _, r := range resources {
		checkShard(t, cluster.GetShard(r.Meta.ID()), r)
		checkShard(t, cluster.GetShardByKey(0, r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkShard(t, cluster.GetShardByKey(0, []byte{end - 1}), r)
			result := cluster.GetShardByKey(0, []byte{end + 1})
			assert.NotEqual(t, r.Meta.ID(), result.Meta.ID())
		}
	}
}

func checkShard(t *testing.T, a *core.CachedShard, b *core.CachedShard) {
	assert.True(t, reflect.DeepEqual(a, b))
	assert.True(t, reflect.DeepEqual(a.Meta, b.Meta))
	assert.True(t, reflect.DeepEqual(a.GetLeader(), b.GetLeader()))
	assert.True(t, reflect.DeepEqual(a.Meta.Peers(), b.Meta.Peers()))
	if len(a.GetDownPeers()) > 0 || len(b.GetDownPeers()) > 0 {
		assert.True(t, reflect.DeepEqual(a.GetDownPeers(), b.GetDownPeers()))
	}
	if len(a.GetPendingPeers()) > 0 || len(b.GetPendingPeers()) > 0 {
		assert.True(t, reflect.DeepEqual(a.GetPendingPeers(), b.GetPendingPeers()))
	}
}

func checkShardsKV(t *testing.T, s storage.Storage, resources []*core.CachedShard) {
	if s != nil {
		for _, res := range resources {
			meta, err := s.GetShard(res.Meta.ID())
			assert.NoError(t, err)
			assert.NotNil(t, meta)
			assert.True(t, reflect.DeepEqual(meta, res.Meta))
		}
	}
}

func checkShards(t *testing.T, cache *core.CachedShards, resources []*core.CachedShard) {
	resourceCount := make(map[uint64]int)
	leaderCount := make(map[uint64]int)
	followerCount := make(map[uint64]int)
	for _, res := range resources {
		for _, peer := range res.Meta.Peers() {
			resourceCount[peer.StoreID]++
			if peer.ID == res.GetLeader().ID {
				leaderCount[peer.StoreID]++
				checkShard(t, cache.GetLeader("", peer.StoreID, res), res)
			} else {
				followerCount[peer.StoreID]++
				checkShard(t, cache.GetFollower("", peer.StoreID, res), res)
			}
		}
	}

	assert.Equal(t, len(resources), cache.GetShardCount())
	for id, count := range resourceCount {
		assert.Equal(t, cache.GetStoreShardCount("", id), count)
	}
	for id, count := range leaderCount {
		assert.Equal(t, cache.GetStoreLeaderCount("", id), count)
	}
	for id, count := range followerCount {
		assert.Equal(t, cache.GetStoreFollowerCount("", id), count)
	}

	for _, res := range cache.GetShards() {
		checkShard(t, res, resources[res.Meta.ID()])
	}
	for _, res := range cache.GetMetaShards() {
		assert.True(t, reflect.DeepEqual(res, resources[res.ID()].Meta))
	}
}

func checkPendingPeerCount(t *testing.T, expect []int, cluster *RaftCluster) {
	for i, e := range expect {
		s := cluster.core.Stores.GetStore(uint64(i + 1))
		assert.Equal(t, e, s.GetPendingPeerCount())
	}
}

func checkStaleShard(origin, res metadata.Shard) error {
	o := origin.Epoch()
	e := res.Epoch()

	if e.GetVersion() < o.GetVersion() || e.GetConfVer() < o.GetConfVer() {
		return fmt.Errorf("resource is stale: resource %v origin %v", res, origin)
	}

	return nil
}
