package statistics

import (
	"github.com/deepfabric/prophet/core"
)

// ResourceStatInformer provides access to a shared informer of statistics.
type ResourceStatInformer interface {
	IsResourceHot(res *core.CachedResource) bool
	// ResourceWriteStats return the containerID -> write stat of peers on this container
	ResourceWriteStats() map[uint64][]*HotPeerStat
	// ResourceReadStats return the containerID -> read stat of peers on this container
	ResourceReadStats() map[uint64][]*HotPeerStat
	RandHotResourceFromContainer(container uint64, kind FlowKind) *core.CachedResource
}
