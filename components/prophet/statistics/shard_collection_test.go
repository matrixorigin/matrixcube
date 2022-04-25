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

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

type testShardStatistics struct {
	storage storage.Storage
	manager *placement.RuleManager
}

func (s *testShardStatistics) setup(t *testing.T) {
	s.storage = storage.NewTestStorage()
	var err error
	s.manager = placement.NewRuleManager(s.storage, nil, nil)
	err = s.manager.Initialize(3, []string{"zone", "rack", "host"})
	assert.NoError(t, err)
}

func TestShardStatistics(t *testing.T) {
	s := &testShardStatistics{}
	s.setup(t)

	opt := config.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	peers := []metapb.Replica{
		{ID: 5, StoreID: 1},
		{ID: 6, StoreID: 2},
		{ID: 4, StoreID: 3},
		{ID: 8, StoreID: 7, Role: metapb.ReplicaRole_Learner},
	}
	metaStores := []metapb.Store{
		{ID: 1, ClientAddress: "mock://server-1"},
		{ID: 2, ClientAddress: "mock://server-2"},
		{ID: 3, ClientAddress: "mock://server-3"},
		{ID: 7, ClientAddress: "mock://server-7"},
	}

	stores := make([]*core.CachedStore, 0, len(metaStores))
	for _, m := range metaStores {
		s := core.NewCachedStore(m)
		stores = append(stores, s)
	}

	downPeers := []metapb.ReplicaStats{
		{Replica: peers[0], DownSeconds: 3608},
		{Replica: peers[1], DownSeconds: 3608},
	}

	store3 := stores[3].Clone(core.OfflineStore(false))
	stores[3] = store3
	r1 := metapb.Shard{ID: 1, Replicas: peers, Start: []byte("aa"), End: []byte("bb")}
	r2 := metapb.Shard{ID: 2, Replicas: peers[0:2], Start: []byte("cc"), End: []byte("dd")}
	shard1 := core.NewCachedShard(r1, &peers[0])
	shard2 := core.NewCachedShard(r2, &peers[0])
	shardStats := NewShardStatistics(opt, s.manager)
	shardStats.Observe(shard1, stores)
	assert.Equal(t, 1, len(shardStats.stats[ExtraPeer]))
	assert.Equal(t, 1, len(shardStats.stats[LearnerPeer]))
	assert.Equal(t, 1, len(shardStats.stats[EmptyShard]))
	assert.Equal(t, 1, len(shardStats.offlineStats[ExtraPeer]))
	assert.Equal(t, 1, len(shardStats.offlineStats[LearnerPeer]))
	assert.Equal(t, 1, len(shardStats.offlineStats[EmptyShard]))
	assert.Equal(t, 1, len(shardStats.offlineStats[OfflinePeer]))

	shard1 = shard1.Clone(
		core.WithDownPeers(downPeers),
		core.WithPendingPeers(peers[0:1]),
		core.SetApproximateSize(144),
	)
	shardStats.Observe(shard1, stores)

	assert.Equal(t, len(shardStats.stats[ExtraPeer]), 1)
	assert.Equal(t, len(shardStats.stats[MissPeer]), 0)
	assert.Equal(t, len(shardStats.stats[DownPeer]), 1)
	assert.Equal(t, len(shardStats.stats[PendingPeer]), 1)
	assert.Equal(t, len(shardStats.stats[LearnerPeer]), 1)
	assert.Equal(t, len(shardStats.stats[EmptyShard]), 0)

	assert.Equal(t, 1, len(shardStats.offlineStats[ExtraPeer]))
	assert.Equal(t, 0, len(shardStats.offlineStats[MissPeer]))
	assert.Equal(t, 1, len(shardStats.offlineStats[DownPeer]))
	assert.Equal(t, 1, len(shardStats.offlineStats[PendingPeer]))
	assert.Equal(t, 1, len(shardStats.offlineStats[LearnerPeer]))
	assert.Equal(t, 0, len(shardStats.offlineStats[EmptyShard]))
	assert.Equal(t, 1, len(shardStats.offlineStats[OfflinePeer]))

	shard2 = shard2.Clone(core.WithDownPeers(downPeers[0:1]))
	shardStats.Observe(shard2, stores[0:2])
	assert.Equal(t, len(shardStats.stats[ExtraPeer]), 1)
	assert.Equal(t, len(shardStats.stats[MissPeer]), 1)
	assert.Equal(t, len(shardStats.stats[DownPeer]), 2)
	assert.Equal(t, len(shardStats.stats[PendingPeer]), 1)
	assert.Equal(t, len(shardStats.stats[LearnerPeer]), 1)
	assert.Equal(t, 1, len(shardStats.offlineStats[ExtraPeer]))
	assert.Equal(t, 0, len(shardStats.offlineStats[MissPeer]))
	assert.Equal(t, 1, len(shardStats.offlineStats[DownPeer]))
	assert.Equal(t, 1, len(shardStats.offlineStats[PendingPeer]))
	assert.Equal(t, 1, len(shardStats.offlineStats[LearnerPeer]))
	assert.Equal(t, 1, len(shardStats.offlineStats[OfflinePeer]))

	shard1 = shard1.Clone(core.WithRemoveStorePeer(7))
	shardStats.Observe(shard1, stores[0:3])
	assert.Equal(t, len(shardStats.stats[ExtraPeer]), 0)
	assert.Equal(t, len(shardStats.stats[MissPeer]), 1)
	assert.Equal(t, len(shardStats.stats[DownPeer]), 2)
	assert.Equal(t, len(shardStats.stats[PendingPeer]), 1)
	assert.Equal(t, len(shardStats.stats[LearnerPeer]), 0)
	assert.Equal(t, len(shardStats.stats[OfflinePeer]), 0)
	assert.Equal(t, 0, len(shardStats.offlineStats[ExtraPeer]))
	assert.Equal(t, 0, len(shardStats.offlineStats[MissPeer]))
	assert.Equal(t, 0, len(shardStats.offlineStats[DownPeer]))
	assert.Equal(t, 0, len(shardStats.offlineStats[PendingPeer]))
	assert.Equal(t, 0, len(shardStats.offlineStats[LearnerPeer]))
	assert.Equal(t, 0, len(shardStats.offlineStats[OfflinePeer]))

	store3 = stores[3].Clone(core.UpStore())
	stores[3] = store3
	shardStats.Observe(shard1, stores)
	assert.Equal(t, len(shardStats.stats[OfflinePeer]), 0)
}

func TestShardStatisticsWithPlacementRule(t *testing.T) {
	s := &testShardStatistics{}
	s.setup(t)

	opt := config.NewTestOptions()
	opt.SetPlacementRuleEnabled(true)
	peers := []metapb.Replica{
		{ID: 5, StoreID: 1},
		{ID: 6, StoreID: 2},
		{ID: 4, StoreID: 3},
		{ID: 8, StoreID: 7, Role: metapb.ReplicaRole_Learner},
	}
	metaStores := []metapb.Store{
		{ID: 1, ClientAddress: "mock://server-1"},
		{ID: 2, ClientAddress: "mock://server-2"},
		{ID: 3, ClientAddress: "mock://server-3"},
		{ID: 7, ClientAddress: "mock://server-7"},
	}

	stores := make([]*core.CachedStore, 0, len(metaStores))
	for _, m := range metaStores {
		s := core.NewCachedStore(m)
		stores = append(stores, s)
	}
	r2 := metapb.Shard{ID: 0, Replicas: peers[0:1], Start: []byte("aa"), End: []byte("bb")}
	r3 := metapb.Shard{ID: 1, Replicas: peers, Start: []byte("ee"), End: []byte("ff")}
	r4 := metapb.Shard{ID: 2, Replicas: peers[0:3], Start: []byte("gg"), End: []byte("hh")}

	shard2 := core.NewCachedShard(r2, &peers[0])
	shard3 := core.NewCachedShard(r3, &peers[0])
	shard4 := core.NewCachedShard(r4, &peers[0])
	shardStats := NewShardStatistics(opt, s.manager)
	// r2 didn't match the rules
	shardStats.Observe(shard2, stores)
	assert.Equal(t, len(shardStats.stats[MissPeer]), 1)
	shardStats.Observe(shard3, stores)
	// r3 didn't match the rules
	assert.Equal(t, len(shardStats.stats[ExtraPeer]), 1)
	shardStats.Observe(shard4, stores)
	// r4 match the rules
	assert.Equal(t, len(shardStats.stats[MissPeer]), 1)
	assert.Equal(t, len(shardStats.stats[ExtraPeer]), 1)
}

func TestShardLabelIsolationLevel(t *testing.T) {
	s := &testShardStatistics{}
	s.setup(t)

	locationLabels := []string{"zone", "rack", "host"}
	labelLevelStats := NewLabelStatistics()
	labelsSet := [][]map[string]string{
		{
			// isolated by rack
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z2", "rack": "r1", "host": "h2"},
			{"zone": "z2", "rack": "r2", "host": "h3"},
		},
		{
			// isolated by host when location labels is ["zone", "rack", "host"]
			// cannot be isolated when location labels is ["zone", "rack"]
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z2", "rack": "r2", "host": "h2"},
			{"zone": "z2", "rack": "r2", "host": "h3"},
		},
		{
			// isolated by zone
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z2", "rack": "r2", "host": "h2"},
			{"zone": "z3", "rack": "r2", "host": "h3"},
		},
		{
			// isolated by rack
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
			{"zone": "z1", "rack": "r3", "host": "h3"},
		},
		{
			// cannot be isolated
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
		},
		{
			// isolated by rack
			{"rack": "r1", "host": "h1"},
			{"rack": "r2", "host": "h2"},
			{"rack": "r3", "host": "h3"},
		},
		{
			// isolated by host
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
			{"zone": "z1", "host": "h3"},
		},
	}
	res := []string{"rack", "host", "zone", "rack", "none", "rack", "host"}
	counter := map[string]int{"none": 1, "host": 2, "rack": 3, "zone": 1}
	shardID := 1
	f := func(labels []map[string]string, res string, locationLabels []string) {
		metaStores := []metapb.Store{
			{ID: 1, ClientAddress: "mock://server-1"},
			{ID: 2, ClientAddress: "mock://server-2"},
			{ID: 3, ClientAddress: "mock://server-3"},
		}
		stores := make([]*core.CachedStore, 0, len(labels))
		for i, m := range metaStores {
			var newLabels []metapb.Label
			for k, v := range labels[i] {
				newLabels = append(newLabels, metapb.Label{Key: k, Value: v})
			}
			s := core.NewCachedStore(m, core.SetStoreLabels(newLabels))

			stores = append(stores, s)
		}
		shard := core.NewCachedShard(metapb.Shard{ID: uint64(shardID)}, nil)
		label := getShardLabelIsolation(stores, locationLabels)
		labelLevelStats.Observe(shard, stores, locationLabels)
		assert.Equal(t, res, label)
		shardID++
	}

	for i, labels := range labelsSet {
		f(labels, res[i], locationLabels)
	}
	for i, res := range counter {
		assert.Equal(t, res, labelLevelStats.labelCounter[i])
	}

	label := getShardLabelIsolation(nil, locationLabels)
	assert.Equal(t, nonIsolation, label)
	label = getShardLabelIsolation(nil, nil)
	assert.Equal(t, nonIsolation, label)
	store := core.NewCachedStore(metapb.Store{ID: 1, ClientAddress: "mock://server-1"}, core.SetStoreLabels([]metapb.Label{{Key: "foo", Value: "bar"}}))
	label = getShardLabelIsolation([]*core.CachedStore{store}, locationLabels)
	assert.Equal(t, "zone", label)

	shardID = 1
	res = []string{"rack", "none", "zone", "rack", "none", "rack", "none"}
	counter = map[string]int{"none": 3, "host": 0, "rack": 3, "zone": 1}
	locationLabels = []string{"zone", "rack"}

	for i, labels := range labelsSet {
		f(labels, res[i], locationLabels)
	}
	for i, res := range counter {
		assert.Equal(t, res, labelLevelStats.labelCounter[i])
	}
}
