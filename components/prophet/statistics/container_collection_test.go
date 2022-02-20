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

package statistics

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestContainerStatistics(t *testing.T) {
	opt := config.NewTestOptions()
	rep := opt.GetReplicationConfig().Clone()
	rep.LocationLabels = []string{"zone", "host"}
	opt.SetReplicationConfig(rep)

	metaContainers := []*metadata.TestContainer{
		{CID: 1, CAddr: "mock://server-1", CLabels: []metapb.Pair{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h1"}}},
		{CID: 2, CAddr: "mock://server-2", CLabels: []metapb.Pair{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h2"}}},
		{CID: 3, CAddr: "mock://server-3", CLabels: []metapb.Pair{{Key: "zone", Value: "z2"}, {Key: "host", Value: "h1"}}},
		{CID: 4, CAddr: "mock://server-4", CLabels: []metapb.Pair{{Key: "zone", Value: "z2"}, {Key: "host", Value: "h2"}}},
		{CID: 5, CAddr: "mock://server-5", CLabels: []metapb.Pair{{Key: "zone", Value: "z3"}, {Key: "host", Value: "h1"}}},
		{CID: 6, CAddr: "mock://server-6", CLabels: []metapb.Pair{{Key: "zone", Value: "z3"}, {Key: "host", Value: "h2"}}},
		{CID: 7, CAddr: "mock://server-7", CLabels: []metapb.Pair{{Key: "host", Value: "h1"}}},
		{CID: 8, CAddr: "mock://server-8", CLabels: []metapb.Pair{{Key: "host", Value: "h2"}}},
		{CID: 8, CAddr: "mock://server-9", CLabels: []metapb.Pair{{Key: "host", Value: "h3"}}, CState: metapb.ContainerState_Tombstone},
	}
	containersStats := NewContainersStats()
	containers := make([]*core.CachedContainer, 0, len(metaContainers))
	for _, m := range metaContainers {
		s := core.NewCachedContainer(m, core.SetLastHeartbeatTS(time.Now()))
		containersStats.GetOrCreateRollingContainerStats(m.CID)
		containers = append(containers, s)
	}

	container3 := containers[3].Clone(core.OfflineContainer(false))
	containers[3] = container3
	container4 := containers[4].Clone(core.SetLastHeartbeatTS(containers[4].GetLastHeartbeatTS().Add(-time.Hour)))
	containers[4] = container4
	containerStats := NewContainerStatisticsMap(opt)
	for _, container := range containers {
		containerStats.Observe(container, containersStats)
	}
	stats := containerStats.stats

	assert.Equal(t, stats.Up, 6)
	assert.Equal(t, stats.Down, 1)
	assert.Equal(t, stats.Offline, 1)
	assert.Equal(t, stats.ResourceCount, 0)
	assert.Equal(t, stats.Unhealthy, 0)
	assert.Equal(t, stats.Disconnect, 0)
	assert.Equal(t, stats.Tombstone, 1)
	assert.Equal(t, stats.LowSpace, 8)
	assert.Equal(t, stats.LabelCounter["zone:z1"], 2)
	assert.Equal(t, stats.LabelCounter["zone:z2"], 2)
	assert.Equal(t, stats.LabelCounter["zone:z3"], 2)
	assert.Equal(t, stats.LabelCounter["host:h1"], 4)
	assert.Equal(t, stats.LabelCounter["host:h2"], 4)
	assert.Equal(t, stats.LabelCounter["zone:unknown"], 2)
}
