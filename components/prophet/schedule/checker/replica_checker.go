// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
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

package checker

import (
	"fmt"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/util/cache"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"go.uber.org/zap"
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
func (r *ReplicaChecker) FillReplicas(res *core.CachedShard, leastPeers int) error {
	if len(res.Meta.Replicas()) > 0 {
		return fmt.Errorf("fill resource replicas only support empty resources")
	}

	if len(res.Meta.Replicas()) >= r.opts.GetMaxReplicas() {
		return nil
	}

	rs := r.strategy(res)
	resourceStores := r.cluster.GetShardStores(res)
	for i := 0; i < r.opts.GetMaxReplicas(); i++ {
		container := rs.SelectStoreToAdd(resourceStores)
		if container == 0 {
			break
		}

		peers := res.Meta.Replicas()
		peers = append(peers, metapb.Replica{StoreID: container})
		res.Meta.SetReplicas(peers)
	}

	if (leastPeers == 0 && len(res.Meta.Replicas()) == r.opts.GetMaxReplicas()) || // all peers matches
		(leastPeers > 0 && len(res.Meta.Replicas()) == leastPeers) { // least peers matches
		return nil
	}

	return nil
}

// Check verifies a resource's replicas, creating an operator.Operator if need.
func (r *ReplicaChecker) Check(res *core.CachedShard) *operator.Operator {
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

func (r *ReplicaChecker) checkDownPeer(res *core.CachedShard) *operator.Operator {
	if !r.opts.IsRemoveDownReplicaEnabled() {
		return nil
	}

	for _, stats := range res.GetDownPeers() {
		peer := stats.GetReplica()
		if peer.ID == 0 {
			continue
		}
		containerID := peer.StoreID
		container := r.cluster.GetStore(containerID)
		if container == nil {
			r.cluster.GetLogger().Warn("lost the container, maybe you are recovering the Prophet cluster",
				zap.Uint64("container", containerID))
			return nil
		}
		if container.DownTime() < r.opts.GetMaxStoreDownTime() {
			continue
		}
		if stats.GetDownSeconds() < uint64(r.opts.GetMaxStoreDownTime().Seconds()) {
			continue
		}

		return r.fixPeer(res, containerID, downStatus)
	}
	return nil
}

func (r *ReplicaChecker) checkOfflinePeer(res *core.CachedShard) *operator.Operator {
	if !r.opts.IsReplaceOfflineReplicaEnabled() {
		return nil
	}

	// just skip learner
	if len(res.GetLearners()) != 0 {
		return nil
	}

	for _, peer := range res.Meta.Replicas() {
		containerID := peer.StoreID
		container := r.cluster.GetStore(containerID)
		if container == nil {
			r.cluster.GetLogger().Warn("lost the container, maybe you are recovering the Prophet cluster",
				zap.Uint64("container", containerID))
			return nil
		}
		if container.IsUp() {
			continue
		}

		return r.fixPeer(res, containerID, offlineStatus)
	}

	return nil
}

func (r *ReplicaChecker) checkMakeUpReplica(res *core.CachedShard) *operator.Operator {
	if !r.opts.IsMakeUpReplicaEnabled() {
		return nil
	}
	if len(res.Meta.Replicas()) >= r.opts.GetMaxReplicas() {
		return nil
	}
	if res.IsDestroyState() {
		return nil
	}

	r.cluster.GetLogger().Debug("resource's peers fewer than max replicas",
		log.ResourceField(res.Meta.ID()),
		zap.Int("peers", len(res.Meta.Replicas())))
	resourceStores := r.cluster.GetShardStores(res)
	target := r.strategy(res).SelectStoreToAdd(resourceStores)
	if target == 0 {
		r.cluster.GetLogger().Debug("no container to add replica for resource",
			log.ResourceField(res.Meta.ID()))
		checkerCounter.WithLabelValues("replica_checker", "no-target-container").Inc()
		r.resourceWaitingList.Put(res.Meta.ID(), nil)
		return nil
	}
	newPeer := metapb.Replica{StoreID: target}
	op, err := operator.CreateAddPeerOperator("make-up-replica", r.cluster, res, newPeer, operator.OpReplica)
	if err != nil {
		r.cluster.GetLogger().Debug("fail to create make-up-replica operator",
			zap.Error(err))
		return nil
	}
	return op
}

func (r *ReplicaChecker) checkRemoveExtraReplica(res *core.CachedShard) *operator.Operator {
	if !r.opts.IsRemoveExtraReplicaEnabled() {
		return nil
	}
	// when add learner peer, the number of peer will exceed max replicas for a while,
	// just comparing the the number of voters to avoid too many cancel add operator log.
	if len(res.GetVoters()) <= r.opts.GetMaxReplicas() {
		return nil
	}
	r.cluster.GetLogger().Debug("resource's peers more than max replicas",
		log.ResourceField(res.Meta.ID()),
		zap.Int("peers", len(res.Meta.Replicas())))
	resourceStores := r.cluster.GetShardStores(res)
	old := r.strategy(res).SelectStoreToRemove(resourceStores)
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

func (r *ReplicaChecker) checkLocationReplacement(res *core.CachedShard) *operator.Operator {
	if !r.opts.IsLocationReplacementEnabled() {
		return nil
	}

	if res.IsDestroyState() {
		return nil
	}

	strategy := r.strategy(res)
	resourceStores := r.cluster.GetShardStores(res)
	oldStore := strategy.SelectStoreToRemove(resourceStores)
	if oldStore == 0 {
		checkerCounter.WithLabelValues("replica_checker", "all-right").Inc()
		return nil
	}
	newStore := strategy.SelectStoreToImprove(resourceStores, oldStore)
	if newStore == 0 {
		r.cluster.GetLogger().Debug("resource no better peer",
			log.ResourceField(res.Meta.ID()))
		checkerCounter.WithLabelValues("replica_checker", "not-better").Inc()
		return nil
	}

	newPeer := metapb.Replica{StoreID: newStore}
	op, err := operator.CreateMovePeerOperator("move-to-better-location", r.cluster, res, operator.OpReplica, oldStore, newPeer)
	if err != nil {
		checkerCounter.WithLabelValues("replica_checker", "create-operator-fail").Inc()
		return nil
	}
	return op
}

func (r *ReplicaChecker) fixPeer(res *core.CachedShard, containerID uint64, status string) *operator.Operator {
	// Check the number of replicas first.
	if len(res.GetVoters()) > r.opts.GetMaxReplicas() ||
		res.IsDestroyState() {
		removeExtra := fmt.Sprintf("remove-extra-%s-replica", status)
		op, err := operator.CreateRemovePeerOperator(removeExtra, r.cluster, operator.OpReplica, res, containerID)
		if err != nil {
			reason := fmt.Sprintf("%s-fail", removeExtra)
			checkerCounter.WithLabelValues("replica_checker", reason).Inc()
			return nil
		}
		return op
	}

	resourceStores := r.cluster.GetShardStores(res)
	target := r.strategy(res).SelectStoreToReplace(resourceStores, containerID)
	if target == 0 {
		reason := fmt.Sprintf("no-container-%s", status)
		checkerCounter.WithLabelValues("replica_checker", reason).Inc()
		r.resourceWaitingList.Put(res.Meta.ID(), nil)
		r.cluster.GetLogger().Debug("resource no best container to add replica",
			log.ResourceField(res.Meta.ID()))
		return nil
	}
	newPeer := metapb.Replica{StoreID: target}
	replace := fmt.Sprintf("replace-%s-replica", status)
	op, err := operator.CreateMovePeerOperator(replace, r.cluster, res, operator.OpReplica, containerID, newPeer)
	if err != nil {
		reason := fmt.Sprintf("%s-fail", replace)
		checkerCounter.WithLabelValues("replica_checker", reason).Inc()
		return nil
	}
	return op
}

func (r *ReplicaChecker) strategy(res *core.CachedShard) *ReplicaStrategy {
	return &ReplicaStrategy{
		checkerName:    replicaCheckerName,
		cluster:        r.cluster,
		locationLabels: r.opts.GetLocationLabels(),
		isolationLevel: r.opts.GetIsolationLevel(),
		resource:       res,
	}
}
