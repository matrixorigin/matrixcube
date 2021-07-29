// Copyright 2020 MatrixOrigin.
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

package raftstore

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/stretchr/testify/assert"
)

func TestIsEpochStale(t *testing.T) {
	assert.True(t, isEpochStale(metapb.ResourceEpoch{ConfVer: 1}, metapb.ResourceEpoch{ConfVer: 2}))
	assert.True(t, isEpochStale(metapb.ResourceEpoch{Version: 1}, metapb.ResourceEpoch{Version: 2}))

	assert.False(t, isEpochStale(metapb.ResourceEpoch{ConfVer: 2}, metapb.ResourceEpoch{ConfVer: 1}))
	assert.False(t, isEpochStale(metapb.ResourceEpoch{Version: 2}, metapb.ResourceEpoch{Version: 1}))
}

func TestFindPeer(t *testing.T) {
	shard := &bhmetapb.Shard{
		Peers: []metapb.Peer{
			{ID: 1, ContainerID: 10000},
			{ID: 2, ContainerID: 20000},
			{ID: 3, ContainerID: 30000},
		},
	}

	assert.NotNil(t, findPeer(shard, 10000))
	assert.NotNil(t, findPeer(shard, 20000))
	assert.NotNil(t, findPeer(shard, 30000))
	assert.Nil(t, findPeer(shard, 40000))
}

func TestRemovePeer(t *testing.T) {
	shard := &bhmetapb.Shard{
		Peers: []metapb.Peer{
			{ID: 1, ContainerID: 10000},
			{ID: 2, ContainerID: 20000},
			{ID: 3, ContainerID: 30000},
		},
	}

	v := removePeer(shard, 10000)
	assert.NotNil(t, v)
	assert.Equal(t, metapb.Peer{ID: 1, ContainerID: 10000}, *v)
	assert.Equal(t, 2, len(shard.Peers))

	assert.Nil(t, removePeer(shard, 10000))
	assert.Equal(t, 2, len(shard.Peers))
}

func TestRemovedPeers(t *testing.T) {
	old := bhmetapb.Shard{
		Peers: []metapb.Peer{
			{ID: 1, ContainerID: 10000},
			{ID: 2, ContainerID: 20000},
			{ID: 3, ContainerID: 30000},
		},
	}

	new := bhmetapb.Shard{
		Peers: []metapb.Peer{
			{ID: 1, ContainerID: 10000},
			{ID: 5, ContainerID: 50000},
			{ID: 4, ContainerID: 40000},
		},
	}

	ids := removedPeers(new, old)
	assert.Equal(t, 2, len(ids))
	assert.Equal(t, uint64(2), ids[0])
	assert.Equal(t, uint64(3), ids[1])

	old = bhmetapb.Shard{
		Peers: []metapb.Peer{
			{ID: 1, ContainerID: 10000},
			{ID: 2, ContainerID: 20000},
			{ID: 3, ContainerID: 30000},
		},
	}
	new = bhmetapb.Shard{
		Peers: []metapb.Peer{
			{ID: 6, ContainerID: 60000},
			{ID: 5, ContainerID: 50000},
			{ID: 4, ContainerID: 40000},
		},
	}

	ids = removedPeers(new, old)
	assert.Equal(t, 3, len(ids))
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(2), ids[1])
	assert.Equal(t, uint64(3), ids[2])

	ids = removedPeers(new, new)
	assert.Equal(t, 0, len(ids))
}
