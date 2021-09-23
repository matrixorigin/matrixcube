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
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
)

const (
	invalidIndex = 0
)

// check whether epoch is staler than checkEpoch. returns true means epoch < checkEpoch
func isEpochStale(epoch metapb.ResourceEpoch, checkEpoch metapb.ResourceEpoch) bool {
	return epoch.Version < checkEpoch.Version ||
		epoch.ConfVer < checkEpoch.ConfVer
}

func findReplica(shard *Shard, storeID uint64) *Replica {
	for idx := range shard.Replicas {
		if shard.Replicas[idx].ContainerID == storeID {
			return &shard.Replicas[idx]
		}
	}

	return nil
}

func removeReplica(shard *Shard, storeID uint64) *Replica {
	var removed *Replica
	var newReplicas []Replica
	for _, peer := range shard.Replicas {
		if peer.ContainerID == storeID {
			p := peer
			removed = &p
		} else {
			newReplicas = append(newReplicas, peer)
		}
	}

	shard.Replicas = newReplicas
	return removed
}

func removedReplicas(new, old Shard) []uint64 {
	var ids []uint64

	for _, o := range old.Replicas {
		c := 0
		for _, n := range new.Replicas {
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
