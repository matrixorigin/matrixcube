package util

import (
	"github.com/deepfabric/prophet/pb/metapb"
)

// EmptyPeer returns a peer is a empty peer
func EmptyPeer(value metapb.Peer) bool {
	return value.ID == 0 && value.ContainerID == 0
}

// FindPeer find peer at the spec container
func FindPeer(peers []*metapb.Peer, containerID uint64) (metapb.Peer, bool) {
	for _, peer := range peers {
		if peer.ContainerID == containerID {
			return *peer, true
		}
	}

	return metapb.Peer{}, false
}
