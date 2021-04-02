package schedule

import (
	"github.com/deepfabric/prophet/core"
	"github.com/deepfabric/prophet/mock/mockcluster"
	"github.com/deepfabric/prophet/pb/metapb"
	"github.com/deepfabric/prophet/schedule/operator"
)

// ApplyOperatorStep applies operator step. Only for test purpose.
func ApplyOperatorStep(resource *core.CachedResource, op *operator.Operator) *core.CachedResource {
	_ = op.Start()
	if step := op.Check(resource); step != nil {
		switch s := step.(type) {
		case operator.TransferLeader:
			if p, ok := resource.GetContainerPeer(s.ToContainer); ok {
				resource = resource.Clone(core.WithLeader(&p))
			} else {
				resource = resource.Clone(core.WithLeader(nil))
			}
		case operator.AddPeer:
			if _, ok := resource.GetContainerPeer(s.ToContainer); ok {
				panic("Add peer that exists")
			}
			peer := metapb.Peer{
				ID:          s.PeerID,
				ContainerID: s.ToContainer,
			}
			resource = resource.Clone(core.WithAddPeer(peer))
		case operator.AddLightPeer:
			if _, ok := resource.GetContainerPeer(s.ToContainer); ok {
				panic("Add peer that exists")
			}
			peer := metapb.Peer{
				ID:          s.PeerID,
				ContainerID: s.ToContainer,
			}
			resource = resource.Clone(core.WithAddPeer(peer))
		case operator.RemovePeer:
			if _, ok := resource.GetContainerPeer(s.FromContainer); !ok {
				panic("Remove peer that doesn't exist")
			}
			if resource.GetLeader().GetContainerID() == s.FromContainer {
				panic("Cannot remove the leader peer")
			}
			resource = resource.Clone(core.WithRemoveContainerPeer(s.FromContainer))
		case operator.AddLearner:
			if _, ok := resource.GetContainerPeer(s.ToContainer); ok {
				panic("Add learner that exists")
			}
			peer := metapb.Peer{
				ID:          s.PeerID,
				ContainerID: s.ToContainer,
				Role:        metapb.PeerRole_Learner,
			}
			resource = resource.Clone(core.WithAddPeer(peer))
		case operator.AddLightLearner:
			if _, ok := resource.GetContainerPeer(s.ToContainer); ok {
				panic("Add learner that exists")
			}
			peer := metapb.Peer{
				ID:          s.PeerID,
				ContainerID: s.ToContainer,
				Role:        metapb.PeerRole_Learner,
			}
			resource = resource.Clone(core.WithAddPeer(peer))
		case operator.PromoteLearner:
			if _, ok := resource.GetContainerLearner(s.ToContainer); !ok {
				panic("Promote peer that doesn't exist")
			}
			peer := metapb.Peer{
				ID:          s.PeerID,
				ContainerID: s.ToContainer,
			}
			resource = resource.Clone(core.WithRemoveContainerPeer(s.ToContainer), core.WithAddPeer(peer))
		default:
			panic("Unknown operator step")
		}
	}
	return resource
}

// ApplyOperator applies operator. Only for test purpose.
func ApplyOperator(mc *mockcluster.Cluster, op *operator.Operator) {
	origin := mc.GetResource(op.ResourceID())
	resource := origin
	for !op.IsEnd() {
		resource = ApplyOperatorStep(resource, op)
	}
	mc.PutResource(resource)
	for id := range resource.GetContainerIDs() {
		mc.UpdateContainerStatus(id)
	}
	for id := range origin.GetContainerIDs() {
		mc.UpdateContainerStatus(id)
	}
}
