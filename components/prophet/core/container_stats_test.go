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
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestContainerStats(t *testing.T) {
	G := uint64(1024 * 1024 * 1024)
	meta := &metadata.TestContainer{CID: 1, CState: metapb.ContainerState_UP}
	container := NewCachedContainer(meta, SetContainerStats(&metapb.ContainerStats{
		Capacity:  200 * G,
		UsedSize:  50 * G,
		Available: 150 * G,
	}))

	assert.Equal(t, 200*G, container.GetCapacity())
	assert.Equal(t, 50*G, container.GetUsedSize())
	assert.Equal(t, 150*G, container.GetAvailable())
	assert.Equal(t, 150*G, container.GetAvgAvailable())
	assert.Equal(t, uint64(0), container.GetAvailableDeviation())

	container = container.Clone(SetContainerStats(&metapb.ContainerStats{
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
