package core

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestDistinctScore(t *testing.T) {
	labels := []string{"zone", "rack", "host"}
	zones := []string{"z1", "z2", "z3"}
	racks := []string{"r1", "r2", "r3"}
	hosts := []string{"h1", "h2", "h3"}

	var containers []*CachedContainer
	for i, zone := range zones {
		for j, rack := range racks {
			for k, host := range hosts {
				containerID := uint64(i*len(racks)*len(hosts) + j*len(hosts) + k)
				containerLabels := map[string]string{
					"zone": zone,
					"rack": rack,
					"host": host,
				}
				container := NewTestContainerInfoWithLabel(containerID, 1, containerLabels)
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
	container := NewTestContainerInfoWithLabel(100, 1, nil)
	assert.Equal(t, float64(0), DistinctScore(labels, containers, container))
}

func TestCloneContainer(t *testing.T) {
	meta := &metadata.TestContainer{CID: 1, CAddr: "mock://s-1", CLabels: []metapb.Pair{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h1"}}}
	container := NewCachedContainer(meta)
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
				UpContainer(),
				SetLastHeartbeatTS(time.Now()),
			)
		}
	}()
	wg.Wait()
}

func TestResourceScore(t *testing.T) {
	stats := &metapb.ContainerStats{}
	stats.Capacity = 512 * (1 << 20)  // 512 MB
	stats.Available = 100 * (1 << 20) // 100 MB
	stats.UsedSize = 0

	container := NewCachedContainer(
		&metadata.TestContainer{CID: 1},
		SetContainerStats(stats),
		SetResourceSize(0, 1),
	)
	score := container.ResourceScore(0, "v1", 0.7, 0.9, 0, 0)
	// Resource score should never be NaN, or /container API would fail.
	assert.False(t, math.IsNaN(score))
}

func TestLowSpaceRatio(t *testing.T) {
	container := NewTestContainerInfoWithLabel(1, 20, nil)
	container.rawStats.Capacity = initialMinSpace << 4
	container.rawStats.Available = container.rawStats.Capacity >> 3

	assert.False(t, container.IsLowSpace(0.8, []uint64{0}))
	info := container.resourceInfo[0]
	info.count = 31
	container.resourceInfo[0] = info
	assert.True(t, container.IsLowSpace(0.8, []uint64{0}))
	container.rawStats.Available = container.rawStats.Capacity >> 2
	assert.False(t, container.IsLowSpace(0.8, []uint64{0}))
}
