package metadata

import (
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
)

// Resource is an abstraction of data shard in a distributed system.
// Each Resource has multiple replication and is distributed on different nodes.
type Resource interface {
	// SetID update the resource id
	SetID(id uint64)
	// ID returns the resource id
	ID() uint64
	// Peers returns the repication peers
	Peers() []metapb.Peer
	// SetPeers update the repication peers
	SetPeers(peers []metapb.Peer)
	// Range resource range
	Range() ([]byte, []byte)
	// SetStartKey set startKey
	SetStartKey([]byte)
	// SetEndKey set startKey
	SetEndKey([]byte)
	// Epoch returns resource epoch
	Epoch() metapb.ResourceEpoch
	// SetEpoch set epoch
	SetEpoch(metapb.ResourceEpoch)
	// State resource state
	State() metapb.ResourceState
	// SetState set resource state
	SetState(metapb.ResourceState)
	// Unique is identifier of the resources, used for dynamic create resources.
	Unique() string
	// SetUnique set Unique
	SetUnique(string)
	// Clone returns the cloned value
	Clone() Resource
	// Marshal returns error if marshal failed
	Marshal() ([]byte, error)
	// Unmarshal returns error if unmarshal failed
	Unmarshal(data []byte) error
}

// Container is an abstraction of the node in a distributed system.
// Usually a container has many resoruces
type Container interface {
	// SetAddrs set addrs
	SetAddrs(addr, shardAddr string)
	// Addr returns address that used for client request
	Addr() string
	// ShardAddr returns address that used for communication between the resource replications
	ShardAddr() string
	// SetID update the container id
	SetID(id uint64)
	// ID returns the container id
	ID() uint64
	// Labels returns the lable tag of the container
	Labels() []metapb.Pair
	// SetLabels set labels
	SetLabels(labels []metapb.Pair)
	// The start timestamp of the current container
	StartTimestamp() int64
	// SetStartTimestamp set the start timestamp of the current container
	SetStartTimestamp(int64)
	// Version returns version and githash
	Version() (string, string)
	// SetVersion set version
	SetVersion(version string, githash string)
	// DeployPath returns the container deploy path
	DeployPath() string
	// SetDeployPath set deploy path
	SetDeployPath(string)
	// State returns the state of the container
	State() metapb.ContainerState
	// SetState set state
	SetState(metapb.ContainerState)
	// The last heartbeat timestamp of the container.
	LastHeartbeat() int64
	//SetLastHeartbeat set the last heartbeat timestamp of the container.
	SetLastHeartbeat(int64)

	// Clone returns the cloned value
	Clone() Container
	// ActionOnJoinCluster returns the cluster will do what when a new container join the cluster
	ActionOnJoinCluster() metapb.Action

	// Marshal returns error if marshal failed
	Marshal() ([]byte, error)
	// Unmarshal returns error if unmarshal failed
	Unmarshal(data []byte) error
}

// IsLearner judges whether the Peer's Role is Learner.
func IsLearner(peer metapb.Peer) bool {
	return peer.Role == metapb.PeerRole_Learner
}

// IsVoterOrIncomingVoter judges whether peer role will become Voter.
// The peer is not nil and the role is equal to IncomingVoter or Voter.
func IsVoterOrIncomingVoter(peer metapb.Peer) bool {
	switch peer.Role {
	case metapb.PeerRole_IncomingVoter, metapb.PeerRole_Voter:
		return true
	}
	return false
}

// IsLearnerOrDemotingVoter judges whether peer role will become Learner.
// The peer is not nil and the role is equal to DemotingVoter or Learner.
func IsLearnerOrDemotingVoter(peer metapb.Peer) bool {
	switch peer.Role {
	case metapb.PeerRole_DemotingVoter, metapb.PeerRole_Learner:
		return true
	}
	return false
}

// IsInJointState judges whether the Peer is in joint state.
func IsInJointState(peers ...metapb.Peer) bool {
	for _, peer := range peers {
		switch peer.Role {
		case metapb.PeerRole_IncomingVoter, metapb.PeerRole_DemotingVoter:
			return true
		default:
		}
	}
	return false
}

// CountInJointState count the peers are in joint state.
func CountInJointState(peers ...metapb.Peer) int {
	count := 0
	for _, peer := range peers {
		switch peer.Role {
		case metapb.PeerRole_IncomingVoter, metapb.PeerRole_DemotingVoter:
			count++
		default:
		}
	}
	return count
}
