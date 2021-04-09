package opt

import (
	"github.com/deepfabric/prophet/config"
	"github.com/deepfabric/prophet/core"
	"github.com/deepfabric/prophet/metadata"
	"github.com/deepfabric/prophet/pb/rpcpb"
	"github.com/deepfabric/prophet/schedule/placement"
	"github.com/deepfabric/prophet/statistics"
)

const (
	// RejectLeader is the label property type that suggests a container should not
	// have any resource leaders.
	RejectLeader = "reject-leader"
)

// Cluster provides an overview of a cluster's resources distribution.
// TODO: This interface should be moved to a better place.
type Cluster interface {
	core.ResourceSetInformer
	core.ContainerSetInformer
	core.ContainerSetController
	statistics.ResourceStatInformer
	statistics.ContainerStatInformer

	GetOpts() *config.PersistOptions
	AllocID() (uint64, error)
	FitResource(*core.CachedResource) *placement.ResourceFit
	RemoveScheduler(name string) error
	AddSuspectResources(ids ...uint64)
	GetResourceFactory() func() metadata.Resource

	// just for test
	DisableJointConsensus()
	JointConsensusEnabled() bool
}

// HeartbeatStream is an interface.
type HeartbeatStream interface {
	Send(*rpcpb.ResourceHeartbeatRsp) error
}
