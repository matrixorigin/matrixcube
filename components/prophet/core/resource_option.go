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
	"sort"

	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

// ShardOption is used to select resource.
type ShardOption func(res *CachedShard) bool

// ShardCreateOption used to create resource.
type ShardCreateOption func(res *CachedShard)

// WithState sets state for the resource.
func WithState(state metapb.ShardState) ShardCreateOption {
	return func(res *CachedShard) {
		res.Meta.SetState(state)
	}
}

// WithDownPeers sets the down peers for the resource.
func WithDownPeers(downReplicas []metapb.ReplicaStats) ShardCreateOption {
	return func(res *CachedShard) {
		res.downReplicas = append(downReplicas[:0:0], downReplicas...)
		sort.Sort(peerStatsSlice(res.downReplicas))
	}
}

// WithPendingPeers sets the pending peers for the resource.
func WithPendingPeers(pendingReplicas []metapb.Replica) ShardCreateOption {
	return func(res *CachedShard) {
		res.pendingReplicas = append(pendingReplicas[:0:0], pendingReplicas...)
		sort.Sort(peerSlice(res.pendingReplicas))
	}
}

// WithLearners sets the learners for the resource.
func WithLearners(learners []metapb.Replica) ShardCreateOption {
	return func(res *CachedShard) {
		peers := res.Meta.Peers()
		for i := range peers {
			for _, l := range learners {
				if peers[i].ID == l.ID {
					peers[i] = metapb.Replica{ID: l.ID, StoreID: l.StoreID, Role: metapb.ReplicaRole_Learner}
					break
				}
			}
		}
	}
}

// WithLeader sets the leader for the resource.
func WithLeader(leader *metapb.Replica) ShardCreateOption {
	return func(res *CachedShard) {
		res.leader = leader
	}
}

// WithStartKey sets the start key for the resource.
func WithStartKey(key []byte) ShardCreateOption {
	return func(res *CachedShard) {
		res.Meta.SetStartKey(key)
	}
}

// WithEndKey sets the end key for the resource.
func WithEndKey(key []byte) ShardCreateOption {
	return func(res *CachedShard) {
		res.Meta.SetEndKey(key)
	}
}

// WithNewShardID sets new id for the resource.
func WithNewShardID(id uint64) ShardCreateOption {
	return func(res *CachedShard) {
		res.Meta.SetID(id)
	}
}

// WithNewPeerIds sets new ids for peers.
func WithNewPeerIds(peerIDs ...uint64) ShardCreateOption {
	return func(res *CachedShard) {
		if len(peerIDs) != len(res.Meta.Peers()) {
			return
		}

		peers := res.Meta.Peers()
		for i := range peers {
			peers[i].ID = peerIDs[i]
		}
	}
}

// WithIncVersion increases the version of the resource.
func WithIncVersion() ShardCreateOption {
	return func(res *CachedShard) {
		e := res.Meta.Epoch()
		e.Version++
		res.Meta.SetEpoch(e)
	}
}

// WithDecVersion decreases the version of the resource.
func WithDecVersion() ShardCreateOption {
	return func(res *CachedShard) {
		e := res.Meta.Epoch()
		e.Version--
		res.Meta.SetEpoch(e)
	}
}

// WithIncConfVer increases the config version of the resource.
func WithIncConfVer() ShardCreateOption {
	return func(res *CachedShard) {
		e := res.Meta.Epoch()
		e.ConfVer++
		res.Meta.SetEpoch(e)
	}
}

// WithDecConfVer decreases the config version of the resource.
func WithDecConfVer() ShardCreateOption {
	return func(res *CachedShard) {
		e := res.Meta.Epoch()
		e.ConfVer--
		res.Meta.SetEpoch(e)
	}
}

// SetWrittenBytes sets the written bytes for the resource.
func SetWrittenBytes(v uint64) ShardCreateOption {
	return func(res *CachedShard) {
		res.stats.WrittenBytes = v
	}
}

// SetWrittenKeys sets the written keys for the resource.
func SetWrittenKeys(v uint64) ShardCreateOption {
	return func(res *CachedShard) {
		res.stats.WrittenKeys = v
	}
}

// WithRemoveStorePeer removes the specified peer for the resource.
func WithRemoveStorePeer(containerID uint64) ShardCreateOption {
	return func(res *CachedShard) {
		var peers []metapb.Replica
		for _, peer := range res.Meta.Peers() {
			if peer.StoreID != containerID {
				peers = append(peers, peer)
			}
		}
		res.Meta.SetPeers(peers)
	}
}

// SetReadBytes sets the read bytes for the resource.
func SetReadBytes(v uint64) ShardCreateOption {
	return func(res *CachedShard) {
		res.stats.ReadBytes = v
	}
}

// SetReadKeys sets the read keys for the resource.
func SetReadKeys(v uint64) ShardCreateOption {
	return func(res *CachedShard) {
		res.stats.ReadKeys = v
	}
}

// SetApproximateSize sets the approximate size for the resource.
func SetApproximateSize(v int64) ShardCreateOption {
	return func(res *CachedShard) {
		res.stats.ApproximateSize = uint64(v)
	}
}

// SetApproximateKeys sets the approximate keys for the resource.
func SetApproximateKeys(v int64) ShardCreateOption {
	return func(res *CachedShard) {
		res.stats.ApproximateKeys = uint64(v)
	}
}

// SetReportInterval sets the report interval for the resource.
func SetReportInterval(v uint64) ShardCreateOption {
	return func(res *CachedShard) {
		res.stats.Interval = &metapb.TimeInterval{Start: 0, End: v}
	}
}

// SetShardConfVer sets the config version for the resource.
func SetShardConfVer(confVer uint64) ShardCreateOption {
	return func(res *CachedShard) {
		e := res.Meta.Epoch()
		if e.Version == 0 {
			res.Meta.SetEpoch(metapb.ShardEpoch{ConfVer: confVer, Version: 1})
		} else {
			e.ConfVer = confVer
			res.Meta.SetEpoch(e)
		}
	}
}

// SetShardVersion sets the version for the resource.
func SetShardVersion(version uint64) ShardCreateOption {
	return func(res *CachedShard) {
		e := res.Meta.Epoch()
		if e.Version == 0 {
			res.Meta.SetEpoch(metapb.ShardEpoch{ConfVer: 1, Version: version})
		} else {
			e.Version = version
			res.Meta.SetEpoch(e)
		}
	}
}

// SetPeers sets the peers for the resource.
func SetPeers(peers []metapb.Replica) ShardCreateOption {
	return func(res *CachedShard) {
		res.Meta.SetPeers(peers)
	}
}

// WithAddPeer adds a peer for the resource.
func WithAddPeer(peer metapb.Replica) ShardCreateOption {
	return func(res *CachedShard) {
		peers := res.Meta.Peers()
		peers = append(peers, peer)
		res.Meta.SetPeers(peers)

		if metadata.IsLearner(peer) {
			res.learners = append(res.learners, peer)
		} else {
			res.voters = append(res.voters, peer)
		}
	}
}

// WithPromoteLearner promotes the learner.
func WithPromoteLearner(peerID uint64) ShardCreateOption {
	return func(res *CachedShard) {
		peers := res.Meta.Peers()
		for i := range res.Meta.Peers() {
			if peers[i].ID == peerID {
				peers[i].Role = metapb.ReplicaRole_Voter
			}
		}
	}
}

// WithReplacePeerStore replaces a peer's containerID with another ID.
func WithReplacePeerStore(oldStoreID, newStoreID uint64) ShardCreateOption {
	return func(res *CachedShard) {
		peers := res.Meta.Peers()

		for i := range peers {
			if peers[i].StoreID == oldStoreID {
				peers[i].StoreID = newStoreID
			}
		}
	}
}

// WithInterval sets the interval
func WithInterval(interval *metapb.TimeInterval) ShardCreateOption {
	return func(res *CachedShard) {
		res.stats.Interval = interval
	}
}
