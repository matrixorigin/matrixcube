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

package core

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestDistinctScore(t *testing.T) {
	labels := []string{"zone", "rack", "host"}
	zones := []string{"z1", "z2", "z3"}
	racks := []string{"r1", "r2", "r3"}
	hosts := []string{"h1", "h2", "h3"}

	var containers []*CachedStore
	for i, zone := range zones {
		for j, rack := range racks {
			for k, host := range hosts {
				containerID := uint64(i*len(racks)*len(hosts) + j*len(hosts) + k)
				containerLabels := map[string]string{
					"zone": zone,
					"rack": rack,
					"host": host,
				}
				container := NewTestStoreInfoWithLabel(containerID, 1, containerLabels)
				containers = append(containers, container)

				// Number of containers in different zones.
				numZones := i * len(racks) * len(hosts)
				// Number of containers in the same zone but in different racks.
				numRacks := j * len(hosts)
				// Number of containers in the same rack but in different hosts.
				numHosts := k
				score := (numZones*replicaBaseScore+numRacks)*replicaBaseScore + numHosts
				assert.Equal(t, float64(score), DistinctScore(labels, containers, container))
			}
		}
	}
	container := NewTestStoreInfoWithLabel(100, 1, nil)
	assert.Equal(t, float64(0), DistinctScore(labels, containers, container))
}

func TestCloneStore(t *testing.T) {
	meta := &metadata.Store{
		Store: metapb.Store{
			ID: 1, ClientAddr: "mock://s-1", Labels: []metapb.Pair{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h1"}},
		},
	}
	container := NewCachedStore(meta)
	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			if time.Since(start) > time.Second {
				break
			}
			container.Meta.State()
		}
	}()
	go func() {
		defer wg.Done()
		for {
			if time.Since(start) > time.Second {
				break
			}
			container.Clone(
				UpStore(),
				SetLastHeartbeatTS(time.Now()),
			)
		}
	}()
	wg.Wait()
}

func TestShardScore(t *testing.T) {
	stats := &metapb.StoreStats{}
	stats.Capacity = 512 * (1 << 20)  // 512 MB
	stats.Available = 100 * (1 << 20) // 100 MB
	stats.UsedSize = 0

	container := NewCachedStore(
		metadata.NewTestStore(1),
		SetStoreStats(stats),
		SetShardSize("", 1),
	)
	score := container.ShardScore("", "v1", 0.7, 0.9, 0, 0)
	// Shard score should never be NaN, or /container API would fail.
	assert.False(t, math.IsNaN(score))
}

func TestLowSpaceRatio(t *testing.T) {
	container := NewTestStoreInfoWithLabel(1, 20, nil)
	container.rawStats.Capacity = initialMinSpace << 4
	container.rawStats.Available = container.rawStats.Capacity >> 3

	assert.False(t, container.IsLowSpace(0.8))
	info := container.resourceInfo[""]
	info.count = 31
	container.resourceInfo[""] = info
	assert.True(t, container.IsLowSpace(0.8))
	container.rawStats.Available = container.rawStats.Capacity >> 2
	assert.False(t, container.IsLowSpace(0.8))
}
