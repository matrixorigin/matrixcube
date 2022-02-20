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

package util

import (
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

// EmptyPeer returns a peer is a empty peer
func EmptyPeer(value metapb.Replica) bool {
	return value.ID == 0 && value.ContainerID == 0
}

// FindPeer find peer at the spec container
func FindPeer(peers []*metapb.Replica, containerID uint64) (metapb.Replica, bool) {
	for _, peer := range peers {
		if peer.ContainerID == containerID {
			return *peer, true
		}
	}

	return metapb.Replica{}, false
}
