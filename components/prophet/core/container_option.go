package core

import (
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
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
func SetLeaderCount(leaderCount int) ContainerCreateOption {
	return func(container *CachedContainer) {
		container.leaderCount = leaderCount
	}
}

// SetResourceCount sets the Resource count for the container.
func SetResourceCount(resourceCount int) ContainerCreateOption {
	return func(container *CachedContainer) {
		container.resourceCount = resourceCount
	}
}

// SetPendingPeerCount sets the pending peer count for the container.
func SetPendingPeerCount(pendingPeerCount int) ContainerCreateOption {
	return func(container *CachedContainer) {
		container.pendingPeerCount = pendingPeerCount
	}
}

// SetLeaderSize sets the leader size for the container.
func SetLeaderSize(leaderSize int64) ContainerCreateOption {
	return func(container *CachedContainer) {
		container.leaderSize = leaderSize
	}
}

// SetResourceSize sets the Resource size for the container.
func SetResourceSize(resourceSize int64) ContainerCreateOption {
	return func(container *CachedContainer) {
		container.resourceSize = resourceSize
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
func SetContainerStats(stats *rpcpb.ContainerStats) ContainerCreateOption {
	return func(container *CachedContainer) {
		container.containerStats.updateRawStats(stats)
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
