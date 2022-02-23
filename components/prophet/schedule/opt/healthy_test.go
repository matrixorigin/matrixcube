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

package opt

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestIsShardHealthy(t *testing.T) {
	peers := func(ids ...uint64) []metapb.Replica {
		var peers []metapb.Replica
		for _, id := range ids {
			p := metapb.Replica{
				ID:      id,
				StoreID: id,
			}
			peers = append(peers, p)
		}
		return peers
	}

	resource := func(peers []metapb.Replica, opts ...core.ShardCreateOption) *core.CachedShard {
		return core.NewCachedShard(&metadata.Shard{Shard: metapb.Shard{Replicas: peers}}, &peers[0], opts...)
	}

	type testCase struct {
		resource *core.CachedShard
		// disable placement rules
		healthy1             bool
		healthyAllowPending1 bool
		replicated1          bool
		// enable placement rules
		healthy2             bool
		healthyAllowPending2 bool
		replicated2          bool
	}

	cases := []testCase{
		{resource(peers(1, 2, 3)), true, true, true, true, true, true},
		{resource(peers(1, 2, 3), core.WithPendingPeers(peers(1))), false, true, true, false, true, true},
		{resource(peers(1, 2, 3), core.WithLearners(peers(1))), false, false, false, true, true, false},
		{resource(peers(1, 2, 3), core.WithDownPeers([]metapb.ReplicaStats{{Replica: peers(1)[0]}})), false, false, true, false, false, true},
		{resource(peers(1, 2)), true, true, false, true, true, false},
		{resource(peers(1, 2, 3, 4), core.WithLearners(peers(1))), false, false, false, true, true, false},
	}

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.AddShardStore(1, 1)
	tc.AddShardStore(2, 1)
	tc.AddShardStore(3, 1)
	tc.AddShardStore(4, 1)
	for _, c := range cases {
		tc.SetEnablePlacementRules(false)
		assert.Equal(t, c.healthy1, IsShardHealthy(tc, c.resource))
		assert.Equal(t, c.healthyAllowPending1, IsHealthyAllowPending(tc, c.resource))
		assert.Equal(t, c.replicated1, IsShardReplicated(tc, c.resource))
		tc.SetEnablePlacementRules(true)
		assert.Equal(t, c.healthy2, IsShardHealthy(tc, c.resource))
		assert.Equal(t, c.healthyAllowPending2, IsHealthyAllowPending(tc, c.resource))
		assert.Equal(t, c.replicated2, IsShardReplicated(tc, c.resource))
	}
}
