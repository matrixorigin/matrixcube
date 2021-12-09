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
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/stretchr/testify/assert"
)

func TestContainerHeartbeat(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory, nil))

	n, np := uint64(3), uint64(3)
	containers := newTestContainers(n, "2.0.0")
	containerMetasAfterHeartbeat := make([]metadata.Container, 0, n)
	resources := newTestResources(n, np)

	for _, res := range resources {
		cluster.core.PutResource(res)
	}
	assert.Equal(t, int(n), cluster.core.Resources.GetResourceCount())

	for i, container := range containers {
		containerStats := &metapb.ContainerStats{
			ContainerID:   container.Meta.ID(),
			Capacity:      100,
			Available:     50,
			ResourceCount: 1,
		}
		assert.Error(t, cluster.HandleContainerHeartbeat(containerStats))
		assert.Nil(t, cluster.putContainerLocked(container))
		assert.Equal(t, i+1, cluster.GetContainerCount())
		assert.Equal(t, int64(0), container.GetLastHeartbeatTS().UnixNano())
		assert.NoError(t, cluster.HandleContainerHeartbeat(containerStats))

		s := cluster.GetContainer(container.Meta.ID())
		assert.NotEqual(t, int64(0), s.GetLastHeartbeatTS().UnixNano())
		assert.True(t, reflect.DeepEqual(containerStats, s.GetContainerStats()))
		containerMetasAfterHeartbeat = append(containerMetasAfterHeartbeat, s.Meta)
	}

	assert.Equal(t, int(n), cluster.GetContainerCount())

	for i, container := range containers {
		tmp, err := cluster.storage.GetContainer(container.Meta.ID())
		assert.NoError(t, err)
		assert.NotNil(t, tmp)
		assert.True(t, reflect.DeepEqual(tmp, containerMetasAfterHeartbeat[i]))
	}
}

func TestFilterUnhealthyContainer(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory, nil))

	containers := newTestContainers(3, "2.0.0")
	for _, container := range containers {
		containerStats := &metapb.ContainerStats{
			ContainerID:   container.Meta.ID(),
			Capacity:      100,
			Available:     50,
			ResourceCount: 1,
		}
		assert.NoError(t, cluster.putContainerLocked(container))
		assert.NoError(t, cluster.HandleContainerHeartbeat(containerStats))
		assert.NotNil(t, cluster.hotStat.GetRollingContainerStats(container.Meta.ID()))
	}

	for _, container := range containers {
		containerStats := &metapb.ContainerStats{
			ContainerID:   container.Meta.ID(),
			Capacity:      100,
			Available:     50,
			ResourceCount: 1,
		}
		newContainer := container.Clone(core.TombstoneContainer())
		assert.NoError(t, cluster.putContainerLocked(newContainer))
		assert.NoError(t, cluster.HandleContainerHeartbeat(containerStats))
		assert.Nil(t, cluster.hotStat.GetRollingContainerStats(container.Meta.ID()))
	}
}

func TestSetOfflineContainer(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory, nil))

	// Put 4 containers.
	for _, container := range newTestContainers(4, "2.0.0") {
		assert.NoError(t, cluster.PutContainer(container.Meta))
	}

	// container 1: up -> offline
	assert.NoError(t, cluster.RemoveContainer(1, false))
	container := cluster.GetContainer(1)
	assert.True(t, container.IsOffline())
	assert.False(t, container.IsPhysicallyDestroyed())

	// container 1: set physically to true success
	assert.NoError(t, cluster.RemoveContainer(1, true))
	container = cluster.GetContainer(1)
	assert.True(t, container.IsOffline())
	assert.True(t, container.IsPhysicallyDestroyed())

	// container 2:up -> offline & physically destroyed
	assert.NoError(t, cluster.RemoveContainer(2, true))
	// container 2: set physically destroyed to false failed
	assert.Error(t, cluster.RemoveContainer(2, false))
	assert.NoError(t, cluster.RemoveContainer(2, true))

	// container 3: up to offline
	assert.NoError(t, cluster.RemoveContainer(3, false))
	assert.NoError(t, cluster.RemoveContainer(3, false))

	cluster.checkContainers()
	// container 1,2,3 shuold be to tombstone
	for containerID := uint64(1); containerID <= 3; containerID++ {
		assert.True(t, cluster.GetContainer(containerID).IsTombstone())
	}
	// test bury container
	for containerID := uint64(0); containerID <= 4; containerID++ {
		container := cluster.GetContainer(containerID)
		if container == nil || container.IsUp() {
			assert.Error(t, cluster.buryContainer(containerID))
		} else {
			assert.NoError(t, cluster.buryContainer(containerID))
		}
	}
}

func TestReuseAddress(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory, nil))

	// Put 4 containers.
	for _, container := range newTestContainers(4, "2.0.0") {
		assert.NoError(t, cluster.PutContainer(container.Meta))
	}
	// container 1: up
	// container 2: offline
	assert.NoError(t, cluster.RemoveContainer(2, false))
	// container 3: offline and physically destroyed
	assert.NoError(t, cluster.RemoveContainer(3, true))
	// container 4: tombstone
	assert.NoError(t, cluster.RemoveContainer(4, true))
	assert.NoError(t, cluster.buryContainer(4))

	for id := uint64(1); id <= 4; id++ {
		container := cluster.GetContainer(id)
		containerID := container.Meta.ID() + 1000
		v, _ := container.Meta.Version()
		newContainer := &metadata.TestContainer{
			CID:         containerID,
			CAddr:       container.Meta.Addr(),
			CState:      metapb.ContainerState_UP,
			CVerion:     v,
			CDeployPath: fmt.Sprintf("test/container%d", containerID),
		}

		if container.IsPhysicallyDestroyed() || container.IsTombstone() {
			// try to start a new container with the same address with container which is physically destryed or tombstone should be success
			assert.NoError(t, cluster.PutContainer(newContainer))
		} else {
			assert.Error(t, cluster.PutContainer(newContainer))
		}
	}
}

func TestUpStore(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory, nil))
	// Put 3 stores.
	for _, container := range newTestContainers(3, "2.0.0") {
		assert.NoError(t, cluster.PutContainer(container.Meta))
	}

	// set store 1 offline
	assert.NoError(t, cluster.RemoveContainer(1, false))
	// up a offline store should be success.
	assert.NoError(t, cluster.UpContainer(1))

	// set store 2 offline and physically destroyed
	assert.NoError(t, cluster.RemoveContainer(2, true))
	assert.Error(t, cluster.UpContainer(2))

	// bury store 2
	cluster.checkContainers()
	// store is tombstone
	err = cluster.UpContainer(2)
	assert.True(t, strings.Contains(err.Error(), "tombstone"))

	// store 3 is up
	assert.NoError(t, cluster.UpContainer(3))

	// store 4 not exist
	err = cluster.UpContainer(4)
	assert.True(t, strings.Contains(err.Error(), "not found"))
}

func TestResourceHeartbeat(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory, nil))

	n, np := uint64(3), uint64(3)

	containers := newTestContainers(3, "2.0.0")
	resources := newTestResources(n, np)

	for _, container := range containers {
		assert.NoError(t, cluster.putContainerLocked(container))
	}

	for i, res := range resources {
		// resource does not exist.
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])
		checkResourcesKV(t, cluster.storage, resources[:i+1])

		// resource is the same, not updated.
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])
		checkResourcesKV(t, cluster.storage, resources[:i+1])
		origin := res
		// resource is updated.
		res = origin.Clone(core.WithIncVersion())
		resources[i] = res
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])
		checkResourcesKV(t, cluster.storage, resources[:i+1])

		// resource is stale (Version).
		stale := origin.Clone(core.WithIncConfVer())
		assert.Error(t, cluster.processResourceHeartbeat(stale))
		checkResources(t, cluster.core.Resources, resources[:i+1])
		checkResourcesKV(t, cluster.storage, resources[:i+1])

		// resource is updated.
		res = origin.Clone(
			core.WithIncVersion(),
			core.WithIncConfVer(),
		)
		resources[i] = res
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])
		checkResourcesKV(t, cluster.storage, resources[:i+1])

		// resource is stale (ConfVer).
		stale = origin.Clone(core.WithIncConfVer())
		assert.Error(t, cluster.processResourceHeartbeat(stale))
		checkResources(t, cluster.core.Resources, resources[:i+1])
		checkResourcesKV(t, cluster.storage, resources[:i+1])

		// Add a down peer.
		res = res.Clone(core.WithDownPeers([]metapb.ReplicaStats{
			{
				Replica:     res.Meta.Peers()[rand.Intn(len(res.Meta.Peers()))],
				DownSeconds: 42,
			},
		}))
		resources[i] = res
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])

		// Add a pending peer.
		res = res.Clone(core.WithPendingPeers([]metapb.Replica{res.Meta.Peers()[rand.Intn(len(res.Meta.Peers()))]}))
		resources[i] = res
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])

		// Clear down peers.
		res = res.Clone(core.WithDownPeers(nil))
		resources[i] = res
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])

		// Clear pending peers.
		res = res.Clone(core.WithPendingPeers(nil))
		resources[i] = res
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])

		// Remove peers.
		origin = res
		res = origin.Clone(core.SetPeers(res.Meta.Peers()[:1]))
		resources[i] = res
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])
		checkResourcesKV(t, cluster.storage, resources[:i+1])
		// Add peers.
		res = origin
		resources[i] = res
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])
		checkResourcesKV(t, cluster.storage, resources[:i+1])

		// Change leader.
		res = res.Clone(core.WithLeader(&res.Meta.Peers()[1]))
		resources[i] = res
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])

		// Change ApproximateSize.
		res = res.Clone(core.SetApproximateSize(144))
		resources[i] = res
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])

		// Change ApproximateKeys.
		res = res.Clone(core.SetApproximateKeys(144000))
		resources[i] = res
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])

		// Change bytes written.
		res = res.Clone(core.SetWrittenBytes(24000))
		resources[i] = res
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])

		// Change keys written.
		res = res.Clone(core.SetWrittenKeys(240))
		resources[i] = res
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])

		// Change bytes read.
		res = res.Clone(core.SetReadBytes(1080000))
		resources[i] = res
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])

		// Change keys read.
		res = res.Clone(core.SetReadKeys(1080))
		resources[i] = res
		assert.NoError(t, cluster.processResourceHeartbeat(res))
		checkResources(t, cluster.core.Resources, resources[:i+1])
	}

	resourceCounts := make(map[uint64]int)
	for _, res := range resources {
		for _, peer := range res.Meta.Peers() {
			resourceCounts[peer.ContainerID]++
		}
	}
	for id, count := range resourceCounts {
		assert.Equal(t, count, cluster.GetContainerResourceCount("", id))
	}

	for _, res := range cluster.GetResources() {
		checkResource(t, res, resources[res.Meta.ID()])
	}
	for _, res := range cluster.GetMetaResources() {
		assert.True(t, reflect.DeepEqual(res, resources[res.ID()].Meta))
	}

	for _, res := range resources {
		for _, container := range cluster.GetResourceContainers(res) {
			_, ok := res.GetContainerPeer(container.Meta.ID())
			assert.True(t, ok)
		}
		for _, container := range cluster.GetFollowerContainers(res) {
			peer, _ := res.GetContainerPeer(container.Meta.ID())
			assert.NotEqual(t, res.GetLeader().GetID(), peer.ID)
		}
	}

	for _, container := range cluster.core.Containers.GetContainers() {
		assert.Equal(t, container.GetLeaderCount(""), cluster.core.Resources.GetContainerLeaderCount("", container.Meta.ID()))
		assert.Equal(t, container.GetResourceCount(""), cluster.core.Resources.GetContainerResourceCount("", container.Meta.ID()))
		assert.Equal(t, container.GetLeaderSize(""), cluster.core.Resources.GetContainerLeaderResourceSize("", container.Meta.ID()))
		assert.Equal(t, container.GetResourceSize(""), cluster.core.Resources.GetContainerResourceSize("", container.Meta.ID()))
	}

	// Test with storage.
	if storage := cluster.storage; storage != nil {
		for _, res := range resources {
			tmp, err := storage.GetResource(res.Meta.ID())
			assert.NoError(t, err)
			assert.NotNil(t, tmp)
			assert.True(t, reflect.DeepEqual(tmp, res.Meta))
		}

		// Check overlap with stale version
		overlapResource := resources[n-1].Clone(
			core.WithStartKey([]byte("")),
			core.WithEndKey([]byte("")),
			core.WithNewResourceID(10000),
			core.WithDecVersion(),
		)
		assert.Error(t, cluster.processResourceHeartbeat(overlapResource))

		res, err := storage.GetResource(resources[n-1].Meta.ID())
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(res, resources[n-1].Meta))

		res, err = storage.GetResource(resources[n-2].Meta.ID())
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(res, resources[n-2].Meta))

		res, err = storage.GetResource(overlapResource.Meta.ID())
		assert.NoError(t, err)
		assert.Nil(t, res)

		// Check overlap
		overlapResource = resources[n-1].Clone(
			core.WithStartKey(resources[n-2].GetStartKey()),
			core.WithNewResourceID(resources[n-1].Meta.ID()+1),
		)
		assert.NoError(t, cluster.processResourceHeartbeat(overlapResource))

		res, err = storage.GetResource(resources[n-1].Meta.ID())
		assert.NoError(t, err)
		assert.Nil(t, res)

		res, err = storage.GetResource(resources[n-2].Meta.ID())
		assert.NoError(t, err)
		assert.Nil(t, res)

		res, err = storage.GetResource(overlapResource.Meta.ID())
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(res, overlapResource.Meta))
	}
}

func TestResourceFlowChanged(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory, nil))
	resources := []*core.CachedResource{core.NewTestCachedResource([]byte{}, []byte{})}
	processResources := func(resources []*core.CachedResource) {
		for _, r := range resources {
			cluster.processResourceHeartbeat(r)
		}
	}
	resources = core.SplitTestResources(resources)
	processResources(resources)
	// update resource
	res := resources[0]
	resources[0] = res.Clone(core.SetReadBytes(1000))
	processResources(resources)
	newResource := cluster.GetResource(res.Meta.ID())
	assert.Equal(t, uint64(1000), newResource.GetBytesRead())

	// do not trace the flow changes
	processResources([]*core.CachedResource{res})
	newResource = cluster.GetResource(res.Meta.ID())
	assert.Equal(t, uint64(0), res.GetBytesRead())
	assert.Equal(t, uint64(0), newResource.GetBytesRead())
}

func TestConcurrentResourceHeartbeat(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory, nil))

	resources := []*core.CachedResource{core.NewTestCachedResource([]byte{}, []byte{})}
	resources = core.SplitTestResources(resources)
	heartbeatResources(t, cluster, resources)

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
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory, nil))

	// 1: [nil, nil)
	resource1 := core.NewCachedResource(&metadata.TestResource{ResID: 1, ResEpoch: metapb.ResourceEpoch{Version: 1, ConfVer: 1}}, nil)
	assert.NoError(t, cluster.processResourceHeartbeat(resource1))
	checkResource(t, cluster.GetResourceByKey(0, []byte("foo")), resource1)

	// split 1 to 2: [nil, m) 1: [m, nil), sync 2 first.
	resource1 = resource1.Clone(
		core.WithStartKey([]byte("m")),
		core.WithIncVersion(),
	)
	resource2 := core.NewCachedResource(&metadata.TestResource{ResID: 2, End: []byte("m"), ResEpoch: metapb.ResourceEpoch{Version: 1, ConfVer: 1}}, nil)
	assert.NoError(t, cluster.processResourceHeartbeat(resource2))
	checkResource(t, cluster.GetResourceByKey(0, []byte("a")), resource2)
	// [m, nil) is missing before r1's heartbeat.
	assert.Nil(t, cluster.GetResourceByKey(0, []byte("z")))

	assert.NoError(t, cluster.processResourceHeartbeat(resource1))
	checkResource(t, cluster.GetResourceByKey(0, []byte("z")), resource1)

	// split 1 to 3: [m, q) 1: [q, nil), sync 1 first.
	resource1 = resource1.Clone(
		core.WithStartKey([]byte("q")),
		core.WithIncVersion(),
	)
	resource3 := core.NewCachedResource(&metadata.TestResource{ResID: 3, Start: []byte("m"), End: []byte("q"), ResEpoch: metapb.ResourceEpoch{Version: 1, ConfVer: 1}}, nil)
	assert.NoError(t, cluster.processResourceHeartbeat(resource1))
	checkResource(t, cluster.GetResourceByKey(0, []byte("z")), resource1)
	checkResource(t, cluster.GetResourceByKey(0, []byte("a")), resource2)
	// [m, q) is missing before r3's heartbeat.
	assert.Nil(t, cluster.GetResourceByKey(0, []byte("n")))
	assert.Nil(t, cluster.processResourceHeartbeat(resource3))
	checkResource(t, cluster.GetResourceByKey(0, []byte("n")), resource3)
}

func TestResourceSplitAndMerge(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory, nil))

	resources := []*core.CachedResource{core.NewTestCachedResource([]byte{}, []byte{})}

	// Byte will underflow/overflow if n > 7.
	n := 7

	// Split.
	for i := 0; i < n; i++ {
		resources = core.SplitTestResources(resources)
		heartbeatResources(t, cluster, resources)
	}

	// Merge.
	for i := 0; i < n; i++ {
		resources = core.MergeTestResources(resources)
		heartbeatResources(t, cluster, resources)
	}

	// Split twice and merge once.
	for i := 0; i < n*2; i++ {
		if (i+1)%3 == 0 {
			resources = core.MergeTestResources(resources)
		} else {
			resources = core.SplitTestResources(resources)
		}
		heartbeatResources(t, cluster, resources)
	}
}

func TestUpdateContainerPendingPeerCount(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	tc := newTestCluster(opt)
	containers := newTestContainers(5, "2.0.0")
	for _, s := range containers {
		assert.Nil(t, tc.putContainerLocked(s))
	}
	peers := []metapb.Replica{
		{
			ID:          2,
			ContainerID: 1,
		},
		{
			ID:          3,
			ContainerID: 2,
		},
		{
			ID:          3,
			ContainerID: 3,
		},
		{
			ID:          4,
			ContainerID: 4,
		},
	}
	origin := core.NewCachedResource(&metadata.TestResource{ResID: 1, ResPeers: peers[:3]}, &peers[0], core.WithPendingPeers(peers[1:3]))
	assert.NoError(t, tc.processResourceHeartbeat(origin))
	checkPendingPeerCount(t, []int{0, 1, 1, 0}, tc.RaftCluster)
	newResource := core.NewCachedResource(&metadata.TestResource{ResID: 1, ResPeers: peers[1:]}, &peers[1], core.WithPendingPeers(peers[3:4]))
	assert.NoError(t, tc.processResourceHeartbeat(newResource))
	checkPendingPeerCount(t, []int{0, 0, 0, 1}, tc.RaftCluster)
}

func TestContainers(t *testing.T) {
	n := uint64(10)
	cache := core.NewCachedContainers()
	containers := newTestContainers(n, "2.0.0")

	for i, container := range containers {
		id := container.Meta.ID()
		assert.Nil(t, cache.GetContainer(id))
		assert.NotNil(t, cache.PauseLeaderTransfer(id))
		cache.SetContainer(container)
		assert.True(t, reflect.DeepEqual(container, cache.GetContainer(id)))
		assert.Equal(t, i+1, cache.GetContainerCount())
		assert.Nil(t, cache.PauseLeaderTransfer(id))
		assert.False(t, cache.GetContainer(id).AllowLeaderTransfer())
		assert.NotNil(t, cache.PauseLeaderTransfer(id))
		cache.ResumeLeaderTransfer(id)
		assert.True(t, cache.GetContainer(id).AllowLeaderTransfer())
	}
	assert.Equal(t, int(n), cache.GetContainerCount())

	for _, container := range cache.GetContainers() {
		assert.True(t, reflect.DeepEqual(container, containers[container.Meta.ID()-1]))
	}
	for _, container := range cache.GetMetaContainers() {
		assert.True(t, reflect.DeepEqual(container, containers[container.ID()-1].Meta))
	}

	assert.Equal(t, int(n), cache.GetContainerCount())
}

func TestResources(t *testing.T) {
	n, np := uint64(10), uint64(3)
	resources := newTestResources(n, np)
	_, opts, err := newTestScheduleConfig()
	assert.NoError(t, err)
	tc := newTestRaftCluster(opts, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory, nil))
	cache := tc.core.Resources

	for i := uint64(0); i < n; i++ {
		res := resources[i]
		resKey := []byte{byte(i)}

		assert.Nil(t, cache.GetResource(i))
		assert.Nil(t, cache.SearchResource(0, resKey))
		checkResources(t, cache, resources[0:i])

		cache.AddResource(res)
		checkResource(t, cache.GetResource(i), res)
		checkResource(t, cache.SearchResource(0, resKey), res)
		checkResources(t, cache, resources[0:(i+1)])
		// previous resource
		if i == 0 {
			assert.Nil(t, cache.SearchPrevResource(0, resKey))
		} else {
			checkResource(t, cache.SearchPrevResource(0, resKey), resources[i-1])
		}
		// Update leader to peer np-1.
		newResource := res.Clone(core.WithLeader(&res.Meta.Peers()[np-1]))
		resources[i] = newResource
		cache.SetResource(newResource)
		checkResource(t, cache.GetResource(i), newResource)
		checkResource(t, cache.SearchResource(0, resKey), newResource)
		checkResources(t, cache, resources[0:(i+1)])

		cache.RemoveResource(res)
		assert.Nil(t, cache.GetResource(i))
		assert.Nil(t, cache.SearchResource(0, resKey))
		checkResources(t, cache, resources[0:i])

		// Reset leader to peer 0.
		newResource = res.Clone(core.WithLeader(&res.Meta.Peers()[0]))
		resources[i] = newResource
		cache.AddResource(newResource)
		checkResource(t, cache.GetResource(i), newResource)
		checkResources(t, cache, resources[0:(i+1)])
		checkResource(t, cache.SearchResource(0, resKey), newResource)
	}

	for i := uint64(0); i < n; i++ {
		res := tc.RandLeaderResource("", i, []core.KeyRange{core.NewKeyRange(0, "", "")}, opt.HealthResource(tc))
		assert.Equal(t, i, res.GetLeader().GetContainerID())

		res = tc.RandFollowerResource("", i, []core.KeyRange{core.NewKeyRange(0, "", "")}, opt.HealthResource(tc))
		assert.NotEqual(t, i, res.GetLeader().GetContainerID())
		_, ok := res.GetContainerPeer(i)
		assert.True(t, ok)
	}

	// check overlaps
	// clone it otherwise there are two items with the same key in the tree
	overlapResource := resources[n-1].Clone(core.WithStartKey(resources[n-2].GetStartKey()))
	cache.AddResource(overlapResource)
	assert.Nil(t, cache.GetResource(n-2))
	assert.NotNil(t, cache.GetResource(n-1))

	// All resources will be filtered out if they have pending peers.
	for i := uint64(0); i < n; i++ {
		for j := 0; j < cache.GetContainerLeaderCount("", i); j++ {
			res := tc.RandLeaderResource("", i, []core.KeyRange{core.NewKeyRange(0, "", "")}, opt.HealthResource(tc))
			newRes := res.Clone(core.WithPendingPeers(res.Meta.Peers()))
			cache.SetResource(newRes)
		}
		assert.Nil(t, tc.RandLeaderResource("", i, []core.KeyRange{core.NewKeyRange(0, "", "")}, opt.HealthResource(tc)))
	}
	for i := uint64(0); i < n; i++ {
		assert.Nil(t, tc.RandFollowerResource("", i, []core.KeyRange{core.NewKeyRange(0, "", "")}, opt.HealthResource(tc)))
	}
}

func TestCheckStaleResource(t *testing.T) {
	// (0, 0) v.s. (0, 0)
	resource := core.NewTestCachedResource([]byte{}, []byte{})
	origin := core.NewTestCachedResource([]byte{}, []byte{})
	assert.Nil(t, checkStaleResource(resource.Meta, origin.Meta))
	assert.Nil(t, checkStaleResource(origin.Meta, resource.Meta))

	// (1, 0) v.s. (0, 0)
	resource.Meta.(*metadata.TestResource).ResEpoch.Version++
	assert.Nil(t, checkStaleResource(origin.Meta, resource.Meta))
	assert.NotNil(t, checkStaleResource(resource.Meta, origin.Meta))

	// (1, 1) v.s. (0, 0)
	resource.Meta.(*metadata.TestResource).ResEpoch.Version++
	assert.Nil(t, checkStaleResource(origin.Meta, resource.Meta))
	assert.NotNil(t, checkStaleResource(resource.Meta, origin.Meta))

	// (0, 1) v.s. (0, 0)
	resource.Meta.(*metadata.TestResource).ResEpoch.Version++
	assert.Nil(t, checkStaleResource(origin.Meta, resource.Meta))
	assert.NotNil(t, checkStaleResource(resource.Meta, origin.Meta))
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
	rc := newTestRaftCluster(opt, storage, core.NewBasicCluster(metadata.TestResourceFactory, nil))
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

func newTestResourceMeta(resourceID uint64) metadata.Resource {
	return &metadata.TestResource{
		ResID:    resourceID,
		Start:    []byte(fmt.Sprintf("%20d", resourceID)),
		End:      []byte(fmt.Sprintf("%20d", resourceID+1)),
		ResEpoch: metapb.ResourceEpoch{Version: 1, ConfVer: 1},
	}
}

// Create n containers (0..n).
func newTestContainers(n uint64, version string) []*core.CachedContainer {
	containers := make([]*core.CachedContainer, 0, n)
	for i := uint64(1); i <= n; i++ {
		container := &metadata.TestContainer{
			CID:         i,
			CAddr:       fmt.Sprintf("127.0.0.1:%d", i),
			CState:      metapb.ContainerState_UP,
			CVerion:     version,
			CDeployPath: fmt.Sprintf("test/container%d", i),
		}
		containers = append(containers, core.NewCachedContainer(container))
	}
	return containers
}

// Create n resources (0..n) of n containers (0..n).
// Each resource contains np peers, the first peer is the leader.
func newTestResources(n, np uint64) []*core.CachedResource {
	resources := make([]*core.CachedResource, 0, n)
	for i := uint64(0); i < n; i++ {
		peers := make([]metapb.Replica, 0, np)
		for j := uint64(0); j < np; j++ {
			peer := metapb.Replica{
				ID: i*np + j,
			}
			peer.ContainerID = (i + j) % n
			peers = append(peers, peer)
		}
		res := &metadata.TestResource{
			ResID:    i,
			ResPeers: peers,
			Start:    []byte{byte(i)},
			End:      []byte{byte(i + 1)},
			ResEpoch: metapb.ResourceEpoch{ConfVer: 2, Version: 2},
		}
		resources = append(resources, core.NewCachedResource(res, &peers[0]))
	}
	return resources
}

func heartbeatResources(t *testing.T, cluster *RaftCluster, resources []*core.CachedResource) {
	// Heartbeat and check resource one by one.
	for _, r := range resources {
		assert.NoError(t, cluster.processResourceHeartbeat(r))

		checkResource(t, cluster.GetResource(r.Meta.ID()), r)
		checkResource(t, cluster.GetResourceByKey(0, r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkResource(t, cluster.GetResourceByKey(0, []byte{end - 1}), r)
		}
	}

	// Check all resources after handling all heartbeats.
	for _, r := range resources {
		checkResource(t, cluster.GetResource(r.Meta.ID()), r)
		checkResource(t, cluster.GetResourceByKey(0, r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkResource(t, cluster.GetResourceByKey(0, []byte{end - 1}), r)
			result := cluster.GetResourceByKey(0, []byte{end + 1})
			assert.NotEqual(t, r.Meta.ID(), result.Meta.ID())
		}
	}
}

func checkResource(t *testing.T, a *core.CachedResource, b *core.CachedResource) {
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

func checkResourcesKV(t *testing.T, s storage.Storage, resources []*core.CachedResource) {
	if s != nil {
		for _, res := range resources {
			meta, err := s.GetResource(res.Meta.ID())
			assert.NoError(t, err)
			assert.NotNil(t, meta)
			assert.True(t, reflect.DeepEqual(meta, res.Meta))
		}
	}
}

func checkResources(t *testing.T, cache *core.CachedResources, resources []*core.CachedResource) {
	resourceCount := make(map[uint64]int)
	leaderCount := make(map[uint64]int)
	followerCount := make(map[uint64]int)
	for _, res := range resources {
		for _, peer := range res.Meta.Peers() {
			resourceCount[peer.ContainerID]++
			if peer.ID == res.GetLeader().ID {
				leaderCount[peer.ContainerID]++
				checkResource(t, cache.GetLeader("", peer.ContainerID, res), res)
			} else {
				followerCount[peer.ContainerID]++
				checkResource(t, cache.GetFollower("", peer.ContainerID, res), res)
			}
		}
	}

	assert.Equal(t, len(resources), cache.GetResourceCount())
	for id, count := range resourceCount {
		assert.Equal(t, cache.GetContainerResourceCount("", id), count)
	}
	for id, count := range leaderCount {
		assert.Equal(t, cache.GetContainerLeaderCount("", id), count)
	}
	for id, count := range followerCount {
		assert.Equal(t, cache.GetContainerFollowerCount("", id), count)
	}

	for _, res := range cache.GetResources() {
		checkResource(t, res, resources[res.Meta.ID()])
	}
	for _, res := range cache.GetMetaResources() {
		assert.True(t, reflect.DeepEqual(res, resources[res.ID()].Meta))
	}
}

func checkPendingPeerCount(t *testing.T, expect []int, cluster *RaftCluster) {
	for i, e := range expect {
		s := cluster.core.Containers.GetContainer(uint64(i + 1))
		assert.Equal(t, e, s.GetPendingPeerCount())
	}
}

func checkStaleResource(origin, res metadata.Resource) error {
	o := origin.Epoch()
	e := res.Epoch()

	if e.GetVersion() < o.GetVersion() || e.GetConfVer() < o.GetConfVer() {
		return fmt.Errorf("resource is stale: resource %v origin %v", res, origin)
	}

	return nil
}
