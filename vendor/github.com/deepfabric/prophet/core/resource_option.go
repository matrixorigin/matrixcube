package core

import (
	"github.com/deepfabric/prophet/metadata"
	"github.com/deepfabric/prophet/pb/metapb"
	"github.com/deepfabric/prophet/pb/rpcpb"
)

// ResourceOption is used to select resource.
type ResourceOption func(res *CachedResource) bool

// ResourceCreateOption used to create resource.
type ResourceCreateOption func(res *CachedResource)

// WithDownPeers sets the down peers for the resource.
func WithDownPeers(downPeers []metapb.PeerStats) ResourceCreateOption {
	return func(res *CachedResource) {
		res.downPeers = downPeers
	}
}

// WithPendingPeers sets the pending peers for the resource.
func WithPendingPeers(pendingPeers []metapb.Peer) ResourceCreateOption {
	return func(res *CachedResource) {
		res.pendingPeers = pendingPeers
	}
}

// WithLearners sets the learners for the resource.
func WithLearners(learners []metapb.Peer) ResourceCreateOption {
	return func(res *CachedResource) {
		peers := res.Meta.Peers()
		for i := range peers {
			for _, l := range learners {
				if peers[i].ID == l.ID {
					peers[i] = metapb.Peer{ID: l.ID, ContainerID: l.ContainerID, Role: metapb.PeerRole_Learner}
					break
				}
			}
		}
	}
}

// WithLeader sets the leader for the resource.
func WithLeader(leader *metapb.Peer) ResourceCreateOption {
	return func(res *CachedResource) {
		res.leader = leader
	}
}

// WithStartKey sets the start key for the resource.
func WithStartKey(key []byte) ResourceCreateOption {
	return func(res *CachedResource) {
		res.Meta.SetStartKey(key)
	}
}

// WithEndKey sets the end key for the resource.
func WithEndKey(key []byte) ResourceCreateOption {
	return func(res *CachedResource) {
		res.Meta.SetEndKey(key)
	}
}

// WithNewResourceID sets new id for the resource.
func WithNewResourceID(id uint64) ResourceCreateOption {
	return func(res *CachedResource) {
		res.Meta.SetID(id)
	}
}

// WithNewPeerIds sets new ids for peers.
func WithNewPeerIds(peerIDs ...uint64) ResourceCreateOption {
	return func(res *CachedResource) {
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
func WithIncVersion() ResourceCreateOption {
	return func(res *CachedResource) {
		e := res.Meta.Epoch()
		e.Version++
		res.Meta.SetEpoch(e)
	}
}

// WithDecVersion decreases the version of the resource.
func WithDecVersion() ResourceCreateOption {
	return func(res *CachedResource) {
		e := res.Meta.Epoch()
		e.Version--
		res.Meta.SetEpoch(e)
	}
}

// WithIncConfVer increases the config version of the resource.
func WithIncConfVer() ResourceCreateOption {
	return func(res *CachedResource) {
		e := res.Meta.Epoch()
		e.ConfVer++
		res.Meta.SetEpoch(e)
	}
}

// WithDecConfVer decreases the config version of the resource.
func WithDecConfVer() ResourceCreateOption {
	return func(res *CachedResource) {
		e := res.Meta.Epoch()
		e.ConfVer--
		res.Meta.SetEpoch(e)
	}
}

// SetWrittenBytes sets the written bytes for the resource.
func SetWrittenBytes(v uint64) ResourceCreateOption {
	return func(res *CachedResource) {
		res.writtenBytes = v
	}
}

// SetWrittenKeys sets the written keys for the resource.
func SetWrittenKeys(v uint64) ResourceCreateOption {
	return func(res *CachedResource) {
		res.writtenKeys = v
	}
}

// WithRemoveContainerPeer removes the specified peer for the resource.
func WithRemoveContainerPeer(containerID uint64) ResourceCreateOption {
	return func(res *CachedResource) {
		var peers []metapb.Peer
		for _, peer := range res.Meta.Peers() {
			if peer.ContainerID != containerID {
				peers = append(peers, peer)
			}
		}
		res.Meta.SetPeers(peers)
	}
}

// SetReadBytes sets the read bytes for the resource.
func SetReadBytes(v uint64) ResourceCreateOption {
	return func(res *CachedResource) {
		res.readBytes = v
	}
}

// SetReadKeys sets the read keys for the resource.
func SetReadKeys(v uint64) ResourceCreateOption {
	return func(res *CachedResource) {
		res.readKeys = v
	}
}

// SetApproximateSize sets the approximate size for the resource.
func SetApproximateSize(v int64) ResourceCreateOption {
	return func(res *CachedResource) {
		res.approximateSize = v
	}
}

// SetApproximateKeys sets the approximate keys for the resource.
func SetApproximateKeys(v int64) ResourceCreateOption {
	return func(res *CachedResource) {
		res.approximateKeys = v
	}
}

// SetReportInterval sets the report interval for the resource.
func SetReportInterval(v uint64) ResourceCreateOption {
	return func(res *CachedResource) {
		res.interval = &rpcpb.TimeInterval{Start: 0, End: v}
	}
}

// SetResourceConfVer sets the config version for the resource.
func SetResourceConfVer(confVer uint64) ResourceCreateOption {
	return func(res *CachedResource) {
		e := res.Meta.Epoch()
		if e.Version == 0 {
			res.Meta.SetEpoch(metapb.ResourceEpoch{ConfVer: confVer, Version: 1})
		} else {
			e.ConfVer = confVer
			res.Meta.SetEpoch(e)
		}
	}
}

// SetResourceVersion sets the version for the resource.
func SetResourceVersion(version uint64) ResourceCreateOption {
	return func(res *CachedResource) {
		e := res.Meta.Epoch()
		if e.Version == 0 {
			res.Meta.SetEpoch(metapb.ResourceEpoch{ConfVer: 1, Version: version})
		} else {
			e.Version = version
			res.Meta.SetEpoch(e)
		}
	}
}

// SetPeers sets the peers for the resource.
func SetPeers(peers []metapb.Peer) ResourceCreateOption {
	return func(res *CachedResource) {
		res.Meta.SetPeers(peers)
	}
}

// WithAddPeer adds a peer for the resource.
func WithAddPeer(peer metapb.Peer) ResourceCreateOption {
	return func(res *CachedResource) {
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
func WithPromoteLearner(peerID uint64) ResourceCreateOption {
	return func(res *CachedResource) {
		peers := res.Meta.Peers()
		for i := range res.Meta.Peers() {
			if peers[i].ID == peerID {
				peers[i].Role = metapb.PeerRole_Voter
			}
		}
	}
}

// WithReplacePeerContainer replaces a peer's containerID with another ID.
func WithReplacePeerContainer(oldContainerID, newContainerID uint64) ResourceCreateOption {
	return func(res *CachedResource) {
		peers := res.Meta.Peers()

		for i := range peers {
			if peers[i].ContainerID == oldContainerID {
				peers[i].ContainerID = newContainerID
			}
		}
	}
}
