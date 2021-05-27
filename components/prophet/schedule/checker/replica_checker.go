package checker

import (
	"errors"
	"fmt"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/cache"
)

const (
	replicaCheckerName = "replica-checker"
)

const (
	offlineStatus = "offline"
	downStatus    = "down"
)

// ReplicaChecker ensures resource has the best replicas.
// Including the following:
// Replica number management.
// Unhealthy replica management, mainly used for disaster recovery of your storage application.
// Location management, mainly used for cross data center deployment.
type ReplicaChecker struct {
	cluster             opt.Cluster
	opts                *config.PersistOptions
	resourceWaitingList cache.Cache
}

// NewReplicaChecker creates a replica checker.
func NewReplicaChecker(cluster opt.Cluster, resourceWaitingList cache.Cache) *ReplicaChecker {
	return &ReplicaChecker{
		cluster:             cluster,
		opts:                cluster.GetOpts(),
		resourceWaitingList: resourceWaitingList,
	}
}

// GetType return ReplicaChecker's type
func (r *ReplicaChecker) GetType() string {
	return "replica-checker"
}

// FillReplicas make up all replica for a empty resource
func (r *ReplicaChecker) FillReplicas(res *core.CachedResource) error {
	if len(res.Meta.Peers()) > 0 {
		return fmt.Errorf("fill resource replicas only support empty resources")
	}

	if len(res.Meta.Peers()) >= r.opts.GetMaxReplicas() {
		return nil
	}

	rs := r.strategy(res)
	resourceContainers := r.cluster.GetResourceContainers(res)
	for i := 0; i < r.opts.GetMaxReplicas(); i++ {
		container := rs.SelectContainerToAdd(resourceContainers)
		if container == 0 {
			return errors.New("no container to add peer")
		}
		peers := res.Meta.Peers()
		peers = append(peers, metapb.Peer{ContainerID: container})
		res.Meta.SetPeers(peers)
	}
	return nil
}

// Check verifies a resource's replicas, creating an operator.Operator if need.
func (r *ReplicaChecker) Check(res *core.CachedResource) *operator.Operator {
	checkerCounter.WithLabelValues("replica_checker", "check").Inc()
	if op := r.checkDownPeer(res); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		op.SetPriorityLevel(core.HighPriority)
		return op
	}
	if op := r.checkOfflinePeer(res); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		op.SetPriorityLevel(core.HighPriority)
		return op
	}
	if op := r.checkMakeUpReplica(res); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		op.SetPriorityLevel(core.HighPriority)
		return op
	}
	if op := r.checkRemoveExtraReplica(res); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		return op
	}
	if op := r.checkLocationReplacement(res); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		return op
	}
	return nil
}

func (r *ReplicaChecker) checkDownPeer(res *core.CachedResource) *operator.Operator {
	if !r.opts.IsRemoveDownReplicaEnabled() {
		return nil
	}

	for _, stats := range res.GetDownPeers() {
		peer := stats.GetPeer()
		if peer.ID == 0 {
			continue
		}
		containerID := peer.ContainerID
		container := r.cluster.GetContainer(containerID)
		if container == nil {
			util.GetLogger().Warningf("lost the container %d, maybe you are recovering the Prophet cluster", containerID)
			return nil
		}
		if container.DownTime() < r.opts.GetMaxContainerDownTime() {
			continue
		}
		if stats.GetDownSeconds() < uint64(r.opts.GetMaxContainerDownTime().Seconds()) {
			continue
		}

		return r.fixPeer(res, containerID, downStatus)
	}
	return nil
}

func (r *ReplicaChecker) checkOfflinePeer(res *core.CachedResource) *operator.Operator {
	if !r.opts.IsReplaceOfflineReplicaEnabled() {
		return nil
	}

	// just skip learner
	if len(res.GetLearners()) != 0 {
		return nil
	}

	for _, peer := range res.Meta.Peers() {
		containerID := peer.ContainerID
		container := r.cluster.GetContainer(containerID)
		if container == nil {
			util.GetLogger().Warningf("lost the container %d, maybe you are recovering the Prophet cluster", containerID)
			return nil
		}
		if container.IsUp() {
			continue
		}

		return r.fixPeer(res, containerID, offlineStatus)
	}

	return nil
}

func (r *ReplicaChecker) checkMakeUpReplica(res *core.CachedResource) *operator.Operator {
	if !r.opts.IsMakeUpReplicaEnabled() {
		return nil
	}
	if len(res.Meta.Peers()) >= r.opts.GetMaxReplicas() {
		return nil
	}
	util.GetLogger().Debugf("resource %d has %d peers, fewer than max replicas",
		res.Meta.ID(),
		len(res.Meta.Peers()))
	resourceContainers := r.cluster.GetResourceContainers(res)
	target := r.strategy(res).SelectContainerToAdd(resourceContainers)
	if target == 0 {
		util.GetLogger().Debugf("no container to add replica for resource %d",
			res.Meta.ID())
		checkerCounter.WithLabelValues("replica_checker", "no-target-container").Inc()
		r.resourceWaitingList.Put(res.Meta.ID(), nil)
		return nil
	}
	newPeer := metapb.Peer{ContainerID: target}
	op, err := operator.CreateAddPeerOperator("make-up-replica", r.cluster, res, newPeer, operator.OpReplica)
	if err != nil {
		util.GetLogger().Debugf("create make-up-replica operator failed with %+v", err)
		return nil
	}
	return op
}

func (r *ReplicaChecker) checkRemoveExtraReplica(res *core.CachedResource) *operator.Operator {
	if !r.opts.IsRemoveExtraReplicaEnabled() {
		return nil
	}
	// when add learner peer, the number of peer will exceed max replicas for a while,
	// just comparing the the number of voters to avoid too many cancel add operator log.
	if len(res.GetVoters()) <= r.opts.GetMaxReplicas() {
		return nil
	}
	util.GetLogger().Debugf("resource %d has %d peers, more than max replicas",
		res.Meta.ID(),
		len(res.Meta.Peers()))
	resourceContainers := r.cluster.GetResourceContainers(res)
	old := r.strategy(res).SelectContainerToRemove(resourceContainers)
	if old == 0 {
		checkerCounter.WithLabelValues("replica_checker", "no-worst-peer").Inc()
		r.resourceWaitingList.Put(res.Meta.ID(), nil)
		return nil
	}
	op, err := operator.CreateRemovePeerOperator("remove-extra-replica", r.cluster, operator.OpReplica, res, old)
	if err != nil {
		checkerCounter.WithLabelValues("replica_checker", "create-operator-fail").Inc()
		return nil
	}
	return op
}

func (r *ReplicaChecker) checkLocationReplacement(res *core.CachedResource) *operator.Operator {
	if !r.opts.IsLocationReplacementEnabled() {
		return nil
	}

	strategy := r.strategy(res)
	resourceContainers := r.cluster.GetResourceContainers(res)
	oldContainer := strategy.SelectContainerToRemove(resourceContainers)
	if oldContainer == 0 {
		checkerCounter.WithLabelValues("replica_checker", "all-right").Inc()
		return nil
	}
	newContainer := strategy.SelectContainerToImprove(resourceContainers, oldContainer)
	if newContainer == 0 {
		util.GetLogger().Debugf("resource %d no better peer",
			res.Meta.ID())
		checkerCounter.WithLabelValues("replica_checker", "not-better").Inc()
		return nil
	}

	newPeer := metapb.Peer{ContainerID: newContainer}
	op, err := operator.CreateMovePeerOperator("move-to-better-location", r.cluster, res, operator.OpReplica, oldContainer, newPeer)
	if err != nil {
		checkerCounter.WithLabelValues("replica_checker", "create-operator-fail").Inc()
		return nil
	}
	return op
}

func (r *ReplicaChecker) fixPeer(res *core.CachedResource, containerID uint64, status string) *operator.Operator {
	// Check the number of replicas first.
	if len(res.GetVoters()) > r.opts.GetMaxReplicas() {
		removeExtra := fmt.Sprintf("remove-extra-%s-replica", status)
		op, err := operator.CreateRemovePeerOperator(removeExtra, r.cluster, operator.OpReplica, res, containerID)
		if err != nil {
			reason := fmt.Sprintf("%s-fail", removeExtra)
			checkerCounter.WithLabelValues("replica_checker", reason).Inc()
			return nil
		}
		return op
	}

	resourceContainers := r.cluster.GetResourceContainers(res)
	target := r.strategy(res).SelectContainerToReplace(resourceContainers, containerID)
	if target == 0 {
		reason := fmt.Sprintf("no-container-%s", status)
		checkerCounter.WithLabelValues("replica_checker", reason).Inc()
		r.resourceWaitingList.Put(res.Meta.ID(), nil)
		util.GetLogger().Debugf("resource %d no best container to add replica",
			res.Meta.ID())
		return nil
	}
	newPeer := metapb.Peer{ContainerID: target}
	replace := fmt.Sprintf("replace-%s-replica", status)
	op, err := operator.CreateMovePeerOperator(replace, r.cluster, res, operator.OpReplica, containerID, newPeer)
	if err != nil {
		reason := fmt.Sprintf("%s-fail", replace)
		checkerCounter.WithLabelValues("replica_checker", reason).Inc()
		return nil
	}
	return op
}

func (r *ReplicaChecker) strategy(res *core.CachedResource) *ReplicaStrategy {
	return &ReplicaStrategy{
		checkerName:    replicaCheckerName,
		cluster:        r.cluster,
		locationLabels: r.opts.GetLocationLabels(),
		isolationLevel: r.opts.GetIsolationLevel(),
		resource:       res,
	}
}
