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
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
)

// ContainerCreateOption is used to create container.
type ContainerCreateOption func(container *CachedContainer)

// SetContainerAddress sets the address for the container.
func SetContainerAddress(address, shardAddress string) ContainerCreateOption {
	return func(container *CachedContainer) {
		meta := container.Meta.Clone()
		meta.SetAddrs(address, shardAddress)
		container.Meta = meta
	}
}

// SetContainerLabels sets the labels for the container.
func SetContainerLabels(labels []metapb.Pair) ContainerCreateOption {
	return func(container *CachedContainer) {
		meta := container.Meta.Clone()
		meta.SetLabels(labels)
		container.Meta = meta
	}
}

// SetContainerStartTime sets the start timestamp for the container.
func SetContainerStartTime(startTS int64) ContainerCreateOption {
	return func(container *CachedContainer) {
		meta := container.Meta.Clone()
		meta.SetStartTimestamp(startTS)
		container.Meta = meta
	}
}

// SetContainerVersion sets the version for the container.
func SetContainerVersion(githash, version string) ContainerCreateOption {
	return func(container *CachedContainer) {
		meta := container.Meta.Clone()
		meta.SetVersion(version, githash)
		container.Meta = meta
	}
}

// SetContainerDeployPath sets the deploy path for the container.
func SetContainerDeployPath(deployPath string) ContainerCreateOption {
	return func(container *CachedContainer) {
		meta := container.Meta.Clone()
		meta.SetDeployPath(deployPath)
		container.Meta = meta
	}
}

// OfflineContainer offline a container
func OfflineContainer(physicallyDestroyed bool) ContainerCreateOption {
	return func(container *CachedContainer) {
		meta := container.Meta.Clone()
		meta.SetPhysicallyDestroyed(physicallyDestroyed)
		meta.SetState(metapb.ContainerState_Offline)
		container.Meta = meta
	}
}

// UpContainer up a container
func UpContainer() ContainerCreateOption {
	return func(container *CachedContainer) {
		meta := container.Meta.Clone()
		meta.SetState(metapb.ContainerState_UP)
		container.Meta = meta
	}
}

// TombstoneContainer set a container to tombstone.
func TombstoneContainer() ContainerCreateOption {
	return func(container *CachedContainer) {
		meta := container.Meta.Clone()
		meta.SetState(metapb.ContainerState_Tombstone)
		container.Meta = meta
	}
}

// SetContainerState sets the state for the container.
func SetContainerState(state metapb.ContainerState) ContainerCreateOption {
	return func(container *CachedContainer) {
		meta := container.Meta.Clone()
		meta.SetState(state)
		container.Meta = meta
	}
}

// PauseLeaderTransfer prevents the container from been selected as source or
// target container of TransferLeader.
func PauseLeaderTransfer() ContainerCreateOption {
	return func(container *CachedContainer) {
		container.pauseLeaderTransfer = true
	}
}

// ResumeLeaderTransfer cleans a container's pause state. The container can be selected
// as source or target of TransferLeader again.
func ResumeLeaderTransfer() ContainerCreateOption {
	return func(container *CachedContainer) {
		container.pauseLeaderTransfer = false
	}
}

// SetLeaderCount sets the leader count for the container.
func SetLeaderCount(group uint64, leaderCount int) ContainerCreateOption {
	return func(container *CachedContainer) {
		info := container.leaderInfo[group]
		info.count = leaderCount
		container.leaderInfo[group] = info
	}
}

// SetResourceCount sets the Resource count for the container.
func SetResourceCount(group uint64, resourceCount int) ContainerCreateOption {
	return func(container *CachedContainer) {
		info := container.resourceInfo[group]
		info.count = resourceCount
		container.resourceInfo[group] = info
	}
}

// SetPendingPeerCount sets the pending peer count for the container.
func SetPendingPeerCount(group uint64, pendingPeerCount int) ContainerCreateOption {
	return func(container *CachedContainer) {
		container.pendingPeerCounts[group] = pendingPeerCount
	}
}

// SetLeaderSize sets the leader size for the container.
func SetLeaderSize(group uint64, leaderSize int64) ContainerCreateOption {
	return func(container *CachedContainer) {
		info := container.leaderInfo[group]
		info.size = leaderSize
		container.leaderInfo[group] = info
	}
}

// SetResourceSize sets the Resource size for the container.
func SetResourceSize(group uint64, resourceSize int64) ContainerCreateOption {
	return func(container *CachedContainer) {
		info := container.resourceInfo[group]
		info.size = resourceSize
		container.resourceInfo[group] = info
	}
}

// SetLeaderWeight sets the leader weight for the container.
func SetLeaderWeight(leaderWeight float64) ContainerCreateOption {
	return func(container *CachedContainer) {
		container.leaderWeight = leaderWeight
	}
}

// SetResourceWeight sets the Resource weight for the container.
func SetResourceWeight(resourceWeight float64) ContainerCreateOption {
	return func(container *CachedContainer) {
		container.resourceWeight = resourceWeight
	}
}

// SetLastHeartbeatTS sets the time of last heartbeat for the container.
func SetLastHeartbeatTS(lastHeartbeatTS time.Time) ContainerCreateOption {
	return func(container *CachedContainer) {
		container.Meta.SetLastHeartbeat(lastHeartbeatTS.UnixNano())
	}
}

// SetLastPersistTime updates the time of last persistent.
func SetLastPersistTime(lastPersist time.Time) ContainerCreateOption {
	return func(container *CachedContainer) {
		container.lastPersistTime = lastPersist
	}
}

// SetContainerStats sets the statistics information for the container.
func SetContainerStats(stats *metapb.ContainerStats) ContainerCreateOption {
	return func(container *CachedContainer) {
		container.containerStats.updateRawStats(stats)
	}
}

// SetNewContainerStats sets the raw statistics information for the container.
func SetNewContainerStats(stats *metapb.ContainerStats) ContainerCreateOption {
	return func(container *CachedContainer) {
		// There is no clone in default container stats, we create new one to avoid to modify others.
		// And range cluster cannot use HMA because the last value is not cached
		container.containerStats = &containerStats{
			rawStats: stats,
		}
	}
}

// AttachAvailableFunc attaches a customize function for the container. The function f returns true if the container limit is not exceeded.
func AttachAvailableFunc(limitType limit.Type, f func() bool) ContainerCreateOption {
	return func(container *CachedContainer) {
		if container.available == nil {
			container.available = make(map[limit.Type]func() bool)
		}
		container.available[limitType] = f
	}
}
