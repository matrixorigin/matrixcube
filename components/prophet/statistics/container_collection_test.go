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
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestStoreStatistics(t *testing.T) {
	opt := config.NewTestOptions()
	rep := opt.GetReplicationConfig().Clone()
	rep.LocationLabels = []string{"zone", "host"}
	opt.SetReplicationConfig(rep)

	metaStores := []metapb.Store{
		{ID: 1, ClientAddress: "mock://server-1", Labels: []metapb.Label{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h1"}}},
		{ID: 2, ClientAddress: "mock://server-2", Labels: []metapb.Label{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h2"}}},
		{ID: 3, ClientAddress: "mock://server-3", Labels: []metapb.Label{{Key: "zone", Value: "z2"}, {Key: "host", Value: "h1"}}},
		{ID: 4, ClientAddress: "mock://server-4", Labels: []metapb.Label{{Key: "zone", Value: "z2"}, {Key: "host", Value: "h2"}}},
		{ID: 5, ClientAddress: "mock://server-5", Labels: []metapb.Label{{Key: "zone", Value: "z3"}, {Key: "host", Value: "h1"}}},
		{ID: 6, ClientAddress: "mock://server-6", Labels: []metapb.Label{{Key: "zone", Value: "z3"}, {Key: "host", Value: "h2"}}},
		{ID: 7, ClientAddress: "mock://server-7", Labels: []metapb.Label{{Key: "host", Value: "h1"}}},
		{ID: 8, ClientAddress: "mock://server-8", Labels: []metapb.Label{{Key: "host", Value: "h2"}}},
		{ID: 8, ClientAddress: "mock://server-9", Labels: []metapb.Label{{Key: "host", Value: "h3"}}, State: metapb.StoreState_StoreTombstone},
	}
	containersStats := NewStoresStats()
	containers := make([]*core.CachedStore, 0, len(metaStores))
	for _, m := range metaStores {
		s := core.NewCachedStore(m, core.SetLastHeartbeatTS(time.Now()))
		containersStats.GetOrCreateRollingStoreStats(m.GetID())
		containers = append(containers, s)
	}

	container3 := containers[3].Clone(core.OfflineStore(false))
	containers[3] = container3
	container4 := containers[4].Clone(core.SetLastHeartbeatTS(containers[4].GetLastHeartbeatTS().Add(-time.Hour)))
	containers[4] = container4
	containerStats := NewStoreStatisticsMap(opt)
	for _, container := range containers {
		containerStats.Observe(container, containersStats)
	}
	stats := containerStats.stats

	assert.Equal(t, stats.Up, 6)
	assert.Equal(t, stats.Down, 1)
	assert.Equal(t, stats.Offline, 1)
	assert.Equal(t, stats.ShardCount, 0)
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
