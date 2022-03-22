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

// StoreCreateOption is used to create cachedStore.
type StoreCreateOption func(cachedStore *CachedStore)

// SetStoreAddress sets the address for the cachedStore.
func SetStoreAddress(address, shardAddress string) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.Meta.SetAddrs(address, shardAddress)
	}
}

// SetStoreLabels sets the labels for the cachedStore.
func SetStoreLabels(labels []metapb.Label) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.Meta.SetLabels(labels)
	}
}

// SetStoreStartTime sets the start timestamp for the cachedStore.
func SetStoreStartTime(startTS int64) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.Meta.SetStartTime(startTS)
	}
}

// SetStoreVersion sets the version for the cachedStore.
func SetStoreVersion(commitID, version string) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.Meta.SetVersionAndCommitID(version, commitID)
	}
}

// SetStoreDeployPath sets the deploy-path for the cachedStore.
func SetStoreDeployPath(deployPath string) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.Meta.SetDeployPath(deployPath)
	}
}

// OfflineStore offline a cachedStore
func OfflineStore(physicallyDestroyed bool) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.Meta.SetDestroyed(physicallyDestroyed)
		cachedStore.Meta.SetState(metapb.StoreState_Down)
	}
}

// UpStore up a cachedStore
func UpStore() StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.Meta.SetState(metapb.StoreState_Up)
	}
}

// TombstoneStore set a cachedStore to tombstone.
func TombstoneStore() StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.Meta.SetState(metapb.StoreState_StoreTombstone)
	}
}

// PauseLeaderTransfer prevents the cachedStore from been selected as source or
// target cachedStore of TransferLeader.
func PauseLeaderTransfer() StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.pauseLeaderTransfer = true
	}
}

// ResumeLeaderTransfer cleans a cachedStore's pause state. The cachedStore can be selected
// as source or target of TransferLeader again.
func ResumeLeaderTransfer() StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.pauseLeaderTransfer = false
	}
}

// SetLeaderCount sets the leader count for the cachedStore.
func SetLeaderCount(groupKey string, leaderCount int) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		info := cachedStore.leaderInfo[groupKey]
		info.count = leaderCount
		cachedStore.leaderInfo[groupKey] = info
	}
}

// SetShardCount sets the Shard count for the cachedStore.
func SetShardCount(groupKey string, resourceCount int) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		info := cachedStore.shardInfo[groupKey]
		info.count = resourceCount
		cachedStore.shardInfo[groupKey] = info
	}
}

// SetPendingPeerCount sets the pending peer count for the cachedStore.
func SetPendingPeerCount(groupKey string, pendingPeerCount int) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.pendingPeerCounts[groupKey] = pendingPeerCount
	}
}

// SetLeaderSize sets the leader size for the cachedStore.
func SetLeaderSize(groupKey string, leaderSize int64) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		info := cachedStore.leaderInfo[groupKey]
		info.size = leaderSize
		cachedStore.leaderInfo[groupKey] = info
	}
}

// SetShardSize sets the Shard size for the cachedStore.
func SetShardSize(groupKey string, resourceSize int64) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		info := cachedStore.shardInfo[groupKey]
		info.size = resourceSize
		cachedStore.shardInfo[groupKey] = info
	}
}

// SetLeaderWeight sets the leader weight for the cachedStore.
func SetLeaderWeight(leaderWeight float64) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.leaderWeight = leaderWeight
	}
}

// SetShardWeight sets the Shard weight for the cachedStore.
func SetShardWeight(resourceWeight float64) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.shardWeight = resourceWeight
	}
}

// SetLastHeartbeatTS sets the time of last heartbeat for the cachedStore.
func SetLastHeartbeatTS(lastHeartbeatTS time.Time) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.Meta.SetLastHeartbeat(lastHeartbeatTS.UnixNano())
	}
}

// SetLastPersistTime updates the time of last persistent.
func SetLastPersistTime(lastPersist time.Time) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.lastPersistTime = lastPersist
	}
}

// SetStoreStats sets the statistics information for the cachedStore.
func SetStoreStats(stats *metapb.StoreStats) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		cachedStore.storeStats.updateRawStats(stats)
	}
}

// SetNewStoreStats sets the raw statistics information for the cachedStore.
func SetNewStoreStats(stats *metapb.StoreStats) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		// There is no clone in default cachedStore stats, we create new one to avoid modifying others.
		// And range cluster cannot use HMA because the last value is not cached
		cachedStore.storeStats = &storeStats{
			rawStats: stats,
		}
	}
}

// AttachAvailableFunc attaches a customize function for the cachedStore. The function f returns true if the cachedStore limit is not exceeded.
func AttachAvailableFunc(limitType limit.Type, f func() bool) StoreCreateOption {
	return func(cachedStore *CachedStore) {
		if cachedStore.available == nil {
			cachedStore.available = make(map[limit.Type]func() bool)
		}
		cachedStore.available[limitType] = f
	}
}
