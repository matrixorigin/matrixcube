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
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

// StoreCreateOption is used to create container.
type StoreCreateOption func(container *CachedStore)

// SetStoreAddress sets the address for the container.
func SetStoreAddress(address, shardAddress string) StoreCreateOption {
	return func(container *CachedStore) {
		meta := container.Meta.CloneValue()
		meta.SetAddrs(address, shardAddress)
		container.Meta = meta
	}
}

// SetStoreLabels sets the labels for the container.
func SetStoreLabels(labels []metapb.Pair) StoreCreateOption {
	return func(container *CachedStore) {
		meta := container.Meta.CloneValue()
		meta.SetLabels(labels)
		container.Meta = meta
	}
}

// SetStoreStartTime sets the start timestamp for the container.
func SetStoreStartTime(startTS int64) StoreCreateOption {
	return func(container *CachedStore) {
		meta := container.Meta.CloneValue()
		meta.SetStartTime(startTS)
		container.Meta = meta
	}
}

// SetStoreVersion sets the version for the container.
func SetStoreVersion(githash, version string) StoreCreateOption {
	return func(container *CachedStore) {
		meta := container.Meta.CloneValue()
		meta.SetVersionAndGitHash(version, githash)
		container.Meta = meta
	}
}

// SetStoreDeployPath sets the deploy path for the container.
func SetStoreDeployPath(deployPath string) StoreCreateOption {
	return func(container *CachedStore) {
		meta := container.Meta.CloneValue()
		meta.SetDeployPath(deployPath)
		container.Meta = meta
	}
}

// OfflineStore offline a container
func OfflineStore(physicallyDestroyed bool) StoreCreateOption {
	return func(container *CachedStore) {
		meta := container.Meta.CloneValue()
		meta.SetPhysicallyDestroyed(physicallyDestroyed)
		meta.SetState(metapb.StoreState_Offline)
		container.Meta = meta
	}
}

// UpStore up a container
func UpStore() StoreCreateOption {
	return func(container *CachedStore) {
		meta := container.Meta.CloneValue()
		meta.SetState(metapb.StoreState_UP)
		container.Meta = meta
	}
}

// TombstoneStore set a container to tombstone.
func TombstoneStore() StoreCreateOption {
	return func(container *CachedStore) {
		meta := container.Meta.CloneValue()
		meta.SetState(metapb.StoreState_StoreTombstone)
		container.Meta = meta
	}
}

// SetStoreState sets the state for the container.
func SetStoreState(state metapb.StoreState) StoreCreateOption {
	return func(container *CachedStore) {
		meta := container.Meta.CloneValue()
		meta.SetState(state)
		container.Meta = meta
	}
}

// PauseLeaderTransfer prevents the container from been selected as source or
// target container of TransferLeader.
func PauseLeaderTransfer() StoreCreateOption {
	return func(container *CachedStore) {
		container.pauseLeaderTransfer = true
	}
}

// ResumeLeaderTransfer cleans a container's pause state. The container can be selected
// as source or target of TransferLeader again.
func ResumeLeaderTransfer() StoreCreateOption {
	return func(container *CachedStore) {
		container.pauseLeaderTransfer = false
	}
}

// SetLeaderCount sets the leader count for the container.
func SetLeaderCount(groupKey string, leaderCount int) StoreCreateOption {
	return func(container *CachedStore) {
		info := container.leaderInfo[groupKey]
		info.count = leaderCount
		container.leaderInfo[groupKey] = info
	}
}

// SetShardCount sets the Shard count for the container.
func SetShardCount(groupKey string, resourceCount int) StoreCreateOption {
	return func(container *CachedStore) {
		info := container.resourceInfo[groupKey]
		info.count = resourceCount
		container.resourceInfo[groupKey] = info
	}
}

// SetPendingPeerCount sets the pending peer count for the container.
func SetPendingPeerCount(groupKey string, pendingPeerCount int) StoreCreateOption {
	return func(container *CachedStore) {
		container.pendingPeerCounts[groupKey] = pendingPeerCount
	}
}

// SetLeaderSize sets the leader size for the container.
func SetLeaderSize(groupKey string, leaderSize int64) StoreCreateOption {
	return func(container *CachedStore) {
		info := container.leaderInfo[groupKey]
		info.size = leaderSize
		container.leaderInfo[groupKey] = info
	}
}

// SetShardSize sets the Shard size for the container.
func SetShardSize(groupKey string, resourceSize int64) StoreCreateOption {
	return func(container *CachedStore) {
		info := container.resourceInfo[groupKey]
		info.size = resourceSize
		container.resourceInfo[groupKey] = info
	}
}

// SetLeaderWeight sets the leader weight for the container.
func SetLeaderWeight(leaderWeight float64) StoreCreateOption {
	return func(container *CachedStore) {
		container.leaderWeight = leaderWeight
	}
}

// SetShardWeight sets the Shard weight for the container.
func SetShardWeight(resourceWeight float64) StoreCreateOption {
	return func(container *CachedStore) {
		container.resourceWeight = resourceWeight
	}
}

// SetLastHeartbeatTS sets the time of last heartbeat for the container.
func SetLastHeartbeatTS(lastHeartbeatTS time.Time) StoreCreateOption {
	return func(container *CachedStore) {
		container.Meta.SetLastHeartbeat(lastHeartbeatTS.UnixNano())
	}
}

// SetLastPersistTime updates the time of last persistent.
func SetLastPersistTime(lastPersist time.Time) StoreCreateOption {
	return func(container *CachedStore) {
		container.lastPersistTime = lastPersist
	}
}

// SetStoreStats sets the statistics information for the container.
func SetStoreStats(stats *metapb.StoreStats) StoreCreateOption {
	return func(container *CachedStore) {
		container.containerStats.updateRawStats(stats)
	}
}

// SetNewStoreStats sets the raw statistics information for the container.
func SetNewStoreStats(stats *metapb.StoreStats) StoreCreateOption {
	return func(container *CachedStore) {
		// There is no clone in default container stats, we create new one to avoid to modify others.
		// And range cluster cannot use HMA because the last value is not cached
		container.containerStats = &containerStats{
			rawStats: stats,
		}
	}
}

// AttachAvailableFunc attaches a customize function for the container. The function f returns true if the container limit is not exceeded.
func AttachAvailableFunc(limitType limit.Type, f func() bool) StoreCreateOption {
	return func(container *CachedStore) {
		if container.available == nil {
			container.available = make(map[limit.Type]func() bool)
		}
		container.available[limitType] = f
	}
}
