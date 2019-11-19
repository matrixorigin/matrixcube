package prophet

// EmptyPeer returns a peer is a empty peer
func EmptyPeer(value Peer) bool {
	return value.ID == 0 && value.ContainerID == 0
}

// FindPeer find peer at the spec container
func FindPeer(peers []*Peer, containerID uint64) (Peer, bool) {
	for _, peer := range peers {
		if peer.ContainerID == containerID {
			return *peer, true
		}
	}

	return Peer{}, false
}
