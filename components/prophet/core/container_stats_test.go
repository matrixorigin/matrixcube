package core

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/stretchr/testify/assert"
)

func TestContainerStats(t *testing.T) {
	G := uint64(1024 * 1024 * 1024)
	meta := &metadata.TestContainer{CID: 1, CState: metapb.ContainerState_UP}
	container := NewCachedContainer(meta, SetContainerStats(&rpcpb.ContainerStats{
		Capacity:  200 * G,
		UsedSize:  50 * G,
		Available: 150 * G,
	}))

	assert.Equal(t, 200*G, container.GetCapacity())
	assert.Equal(t, 50*G, container.GetUsedSize())
	assert.Equal(t, 150*G, container.GetAvailable())
	assert.Equal(t, 150*G, container.GetAvgAvailable())
	assert.Equal(t, uint64(0), container.GetAvailableDeviation())

	container = container.Clone(SetContainerStats(&rpcpb.ContainerStats{
		Capacity:  200 * G,
		UsedSize:  50 * G,
		Available: 160 * G,
	}))

	assert.Equal(t, 160*G, container.GetAvailable())
	assert.True(t, container.GetAvgAvailable() > 150*G)
	assert.True(t, container.GetAvgAvailable() < 160*G)
	assert.True(t, container.GetAvailableDeviation() > 0)
	assert.True(t, container.GetAvailableDeviation() < 10*G)
}
