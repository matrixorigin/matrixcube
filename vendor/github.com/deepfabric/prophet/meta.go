package prophet

// ResourceKind distinguishes different kinds of resources.
type ResourceKind int

const (
	// LeaderKind leader
	LeaderKind ResourceKind = iota
	// ReplicaKind replication of resource
	ReplicaKind
)

// State is the state
type State int

const (
	// UP is normal state
	UP State = iota
	// Down is the unavailable state
	Down
	// Tombstone is the destory state
	Tombstone
)

// Serializable serializable
type Serializable interface {
}

type codecSerializable interface {
	Serializable
	Prepare() error
	Init(adapter Adapter) error
}

// Peer is the resource peer
type Peer struct {
	ID          uint64 `json:"id"`
	ContainerID uint64 `json:"cid"`
}

// Clone returns a clone value
func (p *Peer) Clone() *Peer {
	return &Peer{
		ID:          p.ID,
		ContainerID: p.ContainerID,
	}
}

// PeerStats peer stats
type PeerStats struct {
	Peer        *Peer
	DownSeconds uint64
}

// Clone returns a clone value
func (ps *PeerStats) Clone() *PeerStats {
	return &PeerStats{
		Peer:        ps.Peer.Clone(),
		DownSeconds: ps.DownSeconds,
	}
}

// Resource is an abstraction of data shard in a distributed system.
// Each Resource has multiple replication and is distributed on different nodes.
type Resource interface {
	Serializable

	// SetID update the resource id
	SetID(id uint64)
	// ID returns the resource id
	ID() uint64
	// Peers returns the repication peers
	Peers() []*Peer
	// SetPeers update the repication peers
	SetPeers(peers []*Peer)
	// Stale returns true if the other resource is older than current resource
	Stale(other Resource) bool
	// Changed returns true if the other resource is newer than current resource
	Changed(other Resource) bool
	// Labels returns the label pairs that determine which the resources will be scheduled to which nodes
	Labels() []Pair
	// Clone returns the cloned value
	Clone() Resource

	// ScaleCompleted returns true if the current resource has been successfully scaled according to the specified container
	ScaleCompleted(uint64) bool

	// Marshal returns error if marshal failed
	Marshal() ([]byte, error)
	// Unmarshal returns error if unmarshal failed
	Unmarshal(data []byte) error

	// SupportRebalance support rebalance the resource
	SupportRebalance() bool
	// SupportTransferLeader support transfer leader
	SupportTransferLeader() bool
}

// Pair key value pair
type Pair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Container is an abstraction of the node in a distributed system.
// Usually a container has many resoruces
type Container interface {
	Serializable

	// ShardAddr returns address that used for communication between the resource replications
	ShardAddr() string
	// SetID update the container id
	SetID(id uint64)
	// ID returns the container id
	ID() uint64
	// Labels returns the lable tag of the container
	Labels() []Pair
	// State returns the state of the container
	State() State
	// Clone returns the cloned value
	Clone() Container
	// ActionOnJoinCluster returns the cluster will do what when a new container join the cluster
	ActionOnJoinCluster() Action

	// Marshal returns error if marshal failed
	Marshal() ([]byte, error)
	// Unmarshal returns error if unmarshal failed
	Unmarshal(data []byte) error
}

// Action the action on the cluster join the cluster
type Action int

const (
	// NoneAction none action
	NoneAction = Action(0)
	// ScaleOutAction all resources will received a scale operation when a new container join the cluster
	ScaleOutAction = Action(1)
)
