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
	"encoding/binary"
	"hash/crc64"

	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/fagongzi/goetty"
)

const (
	invalidIndex = 0
)

// check whether epoch is staler than checkEpoch.
func isEpochStale(epoch metapb.ShardEpoch, checkEpoch metapb.ShardEpoch) bool {
	return epoch.ShardVer < checkEpoch.ShardVer ||
		epoch.ConfVer < checkEpoch.ConfVer
}

func findPeer(shard *metapb.Shard, storeID uint64) *metapb.Peer {
	for _, peer := range shard.Peers {
		if peer.StoreID == storeID {
			return &peer
		}
	}

	return nil
}

func removePeer(shard *metapb.Shard, storeID uint64) {
	var newPeers []metapb.Peer
	for _, peer := range shard.Peers {
		if peer.StoreID != storeID {
			newPeers = append(newPeers, peer)
		}
	}

	shard.Peers = newPeers
}

func newPeer(peerID, storeID uint64) metapb.Peer {
	return metapb.Peer{
		ID:      peerID,
		StoreID: storeID,
	}
}

func removedPeers(new, old metapb.Shard) []uint64 {
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

var (
	tab = crc64.MakeTable(crc64.ECMA)
	mp  = goetty.NewSyncPool(8, 16, 2)
)

func noConvert(group uint64, key []byte, do func(uint64, []byte) metapb.Shard) metapb.Shard {
	return do(group, key)
}

func uint64Convert(group uint64, key []byte, do func(uint64, []byte) metapb.Shard) metapb.Shard {
	b := mp.Alloc(8)
	binary.BigEndian.PutUint64(b, crc64.Checksum(key, tab))
	value := do(group, b)
	mp.Free(b)
	return value
}
