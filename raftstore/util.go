// Copyright 2016 DeepFabric, Inc.
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
	"github.com/deepfabric/beehive/pb/bhmetapb"
	"github.com/deepfabric/prophet/pb/metapb"
)

const (
	invalidIndex = 0
)

// check whether epoch is staler than checkEpoch.
func isEpochStale(epoch metapb.ResourceEpoch, checkEpoch metapb.ResourceEpoch) bool {
	return epoch.Version < checkEpoch.Version ||
		epoch.ConfVer < checkEpoch.ConfVer
}

func findPeer(shard *bhmetapb.Shard, storeID uint64) *metapb.Peer {
	for _, peer := range shard.Peers {
		if peer.ContainerID == storeID {
			return &peer
		}
	}

	return nil
}

func removePeer(shard *bhmetapb.Shard, storeID uint64) *metapb.Peer {
	var removed *metapb.Peer
	var newPeers []metapb.Peer
	for _, peer := range shard.Peers {
		if peer.ContainerID != storeID {
			newPeers = append(newPeers, peer)
		} else {
			removed = &peer
		}
	}

	shard.Peers = newPeers
	return removed
}

func newPeer(peerID, storeID uint64) metapb.Peer {
	return metapb.Peer{
		ID:          peerID,
		ContainerID: storeID,
	}
}

func removedPeers(new, old bhmetapb.Shard) []uint64 {
	var ids []uint64

	for _, o := range old.Peers {
		c := 0
		for _, n := range new.Peers {
			if n.ID == o.ID {
				c++
				break
			}
		}

		if c == 0 {
			ids = append(ids, o.ID)
		}
	}

	return ids
}
