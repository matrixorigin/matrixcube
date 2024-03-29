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
	"errors"
	"fmt"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/filter"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/util/cache"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"go.uber.org/zap"
)

// RuleChecker fix/improve resource by placement rules.
type RuleChecker struct {
	cluster             opt.Cluster
	ruleManager         *placement.RuleManager
	name                string
	resourceWaitingList cache.Cache
}

// NewRuleChecker creates a checker instance.
func NewRuleChecker(cluster opt.Cluster, ruleManager *placement.RuleManager, resourceWaitingList cache.Cache) *RuleChecker {
	return &RuleChecker{
		cluster:             cluster,
		ruleManager:         ruleManager,
		name:                "rule-checker",
		resourceWaitingList: resourceWaitingList,
	}
}

// GetType returns RuleChecker's Type
func (c *RuleChecker) GetType() string {
	return "rule-checker"
}

// FillReplicas make up all replica for a empty resource
func (c *RuleChecker) FillReplicas(res *core.CachedShard, leastPeers int) error {
	if len(res.Meta.GetReplicas()) > 0 {
		return fmt.Errorf("fill resource replicas only support empty resources")
	}

	fit := c.ruleManager.FitShard(c.cluster, res)
	if len(fit.RuleFits) == 0 {
		return fmt.Errorf("fill resource replicas cann't matches no rules")
	}

	cnt := 0
	for _, rf := range fit.RuleFits {
		cnt += rf.Rule.Count
		rs := c.strategy(res, rf.Rule)
		ruleStores := c.getRuleFitStores(rf)

		for i := 0; i < rf.Rule.Count; i++ {
			container := rs.SelectStoreToAdd(ruleStores)
			if container == 0 {
				break
			}

			p := metapb.Replica{StoreID: container}
			switch rf.Rule.Role {
			case placement.Voter, placement.Follower, placement.Leader:
				p.Role = metapb.ReplicaRole_Voter
			default:
				p.Role = metapb.ReplicaRole_Learner
			}

			peers := res.Meta.GetReplicas()
			peers = append(peers, p)
			res.Meta.SetReplicas(peers)
		}
	}

	if (leastPeers == 0 && len(res.Meta.GetReplicas()) == cnt) || // all rule peers matches
		(leastPeers > 0 && len(res.Meta.GetReplicas()) == leastPeers) { // least peers matches
		return nil
	}

	return errors.New("no container to add peers")
}

// Check checks if the resource matches placement rules and returns Operator to
// fix it.
func (c *RuleChecker) Check(res *core.CachedShard) *operator.Operator {
	checkerCounter.WithLabelValues("rule_checker", "check").Inc()

	fit := c.cluster.FitShard(res)
	if len(fit.RuleFits) == 0 {
		checkerCounter.WithLabelValues("rule_checker", "fix-range").Inc()
		// If the resource matches no rules, the most possible reason is it spans across
		// multiple rules.
		return c.fixRange(res)
	}

	op, err := c.fixOrphanPeers(res, fit)
	if err == nil && op != nil {
		return op
	}
	if err != nil {
		c.cluster.GetLogger().Debug("fail to fix orphan peer",
			zap.Error(err))
	}

	for _, rf := range fit.RuleFits {
		op, err := c.fixRulePeer(res, fit, rf)
		if err != nil {
			c.cluster.GetLogger().Debug("fail to fix resource by rule",
				log.ResourceField(res.Meta.GetID()),
				zap.String("rule-group", rf.Rule.GroupID),
				zap.String("rule-id", rf.Rule.ID),
				zap.Error(err))
			continue
		}
		if op != nil {
			return op
		}
	}

	return nil
}

func (c *RuleChecker) fixRange(res *core.CachedShard) *operator.Operator {
	if res.IsDestroyState() {
		return nil
	}

	keys := c.ruleManager.GetSplitKeys(res.GetStartKey(), res.GetEndKey())
	if len(keys) == 0 {
		return nil
	}

	op, err := operator.CreateSplitShardOperator("rule-split-resource", res, 0, metapb.CheckPolicy_USEKEY, keys)
	if err != nil {
		c.cluster.GetLogger().Debug("fail to create split resource operator",
			zap.Error(err))
		return nil
	}

	return op
}

func (c *RuleChecker) fixRulePeer(res *core.CachedShard, fit *placement.ShardFit, rf *placement.RuleFit) (*operator.Operator, error) {
	// make up peers.
	if len(rf.Peers) < rf.Rule.Count &&
		!res.IsDestroyState() {
		return c.addRulePeer(res, rf)
	}
	// fix down/offline peers.
	for _, peer := range rf.Peers {
		if c.isDownPeer(res, peer) {
			checkerCounter.WithLabelValues("rule_checker", "replace-down").Inc()
			return c.replaceRulePeer(res, rf, peer, downStatus)
		}
		if c.isOfflinePeer(res, peer) {
			checkerCounter.WithLabelValues("rule_checker", "replace-offline").Inc()
			return c.replaceRulePeer(res, rf, peer, offlineStatus)
		}
	}
	// fix loose matched peers.
	for _, peer := range rf.PeersWithDifferentRole {
		op, err := c.fixLooseMatchPeer(res, fit, rf, peer)
		if err != nil {
			return nil, err
		}
		if op != nil {
			return op, nil
		}
	}
	return c.fixBetterLocation(res, rf)
}

func (c *RuleChecker) addRulePeer(res *core.CachedShard, rf *placement.RuleFit) (*operator.Operator, error) {
	checkerCounter.WithLabelValues("rule_checker", "add-rule-peer").Inc()
	ruleStores := c.getRuleFitStores(rf)
	container := c.strategy(res, rf.Rule).SelectStoreToAdd(ruleStores)
	if container == 0 {
		checkerCounter.WithLabelValues("rule_checker", "no-container-add").Inc()
		c.resourceWaitingList.Put(res.Meta.GetID(), nil)
		return nil, errors.New("no container to add peer")
	}
	peer := metapb.Replica{StoreID: container, Role: rf.Rule.Role.MetaPeerRole()}
	return operator.CreateAddPeerOperator("add-rule-peer", c.cluster, res, peer, operator.OpReplica)
}

func (c *RuleChecker) replaceRulePeer(res *core.CachedShard, rf *placement.RuleFit, peer metapb.Replica, status string) (*operator.Operator, error) {
	if res.IsDestroyState() {
		checkerCounter.WithLabelValues("rule_checker", "remove-"+status+"-peer").Inc()
		c.cluster.GetLogger().Debug("remove down replica",
			zap.Uint64("replica-id", peer.ID))
		return operator.CreateRemovePeerOperator("remove-"+status+"-peer", c.cluster, operator.OpReplica, res, peer.StoreID)
	}

	ruleStores := c.getRuleFitStores(rf)
	container := c.strategy(res, rf.Rule).SelectStoreToReplace(ruleStores, peer.StoreID)
	if container == 0 {
		checkerCounter.WithLabelValues("rule_checker", "no-container-replace").Inc()
		c.resourceWaitingList.Put(res.Meta.GetID(), nil)
		return nil, errors.New("no container to replace peer")
	}
	newPeer := metapb.Replica{StoreID: container, Role: rf.Rule.Role.MetaPeerRole()}
	return operator.CreateMovePeerOperator("replace-rule-"+status+"-peer",
		c.cluster, res, operator.OpReplica, peer.StoreID, newPeer)
}

func (c *RuleChecker) fixLooseMatchPeer(res *core.CachedShard, fit *placement.ShardFit, rf *placement.RuleFit, peer metapb.Replica) (*operator.Operator, error) {
	if res.IsDestroyState() {
		return nil, nil
	}

	if metadata.IsLearner(peer) && rf.Rule.Role != placement.Learner {
		checkerCounter.WithLabelValues("rule_checker", "fix-peer-role").Inc()
		return operator.CreatePromoteLearnerOperator("fix-peer-role", c.cluster, res, peer)
	}
	if res.GetLeader().GetID() != peer.GetID() && rf.Rule.Role == placement.Leader {
		checkerCounter.WithLabelValues("rule_checker", "fix-leader-role").Inc()
		if c.allowLeader(fit, peer) {
			return operator.CreateTransferLeaderOperator("fix-leader-role",
				c.cluster, res, res.GetLeader().StoreID, peer.StoreID, 0)
		}
		checkerCounter.WithLabelValues("rule_checker", "not-allow-leader")
		return nil, errors.New("peer cannot be leader")
	}
	if res.GetLeader().GetID() == peer.GetID() && rf.Rule.Role == placement.Follower {
		checkerCounter.WithLabelValues("rule_checker", "fix-follower-role").Inc()
		for _, p := range res.Meta.GetReplicas() {
			if c.allowLeader(fit, p) {
				return operator.CreateTransferLeaderOperator("fix-follower-role",
					c.cluster, res, peer.StoreID, p.StoreID, 0)
			}
		}
		checkerCounter.WithLabelValues("rule_checker", "no-new-leader").Inc()
		return nil, errors.New("no new leader")
	}
	return nil, nil
}

func (c *RuleChecker) allowLeader(fit *placement.ShardFit, peer metapb.Replica) bool {
	if metadata.IsLearner(peer) {
		return false
	}
	s := c.cluster.GetStore(peer.StoreID)
	if s == nil {
		return false
	}
	stateFilter := &filter.StoreStateFilter{ActionScope: "rule-checker", TransferLeader: true}
	if !stateFilter.Target(c.cluster.GetOpts(), s) {
		return false
	}
	for _, rf := range fit.RuleFits {
		if (rf.Rule.Role == placement.Leader || rf.Rule.Role == placement.Voter) &&
			placement.MatchLabelConstraints(s, rf.Rule.LabelConstraints) {
			return true
		}
	}
	return false
}

func (c *RuleChecker) fixBetterLocation(res *core.CachedShard, rf *placement.RuleFit) (*operator.Operator, error) {
	if res.IsDestroyState() {
		return nil, nil
	}

	if len(rf.Rule.LocationLabels) == 0 || rf.Rule.Count <= 1 {
		return nil, nil
	}

	strategy := c.strategy(res, rf.Rule)
	ruleStores := c.getRuleFitStores(rf)
	oldStore := strategy.SelectStoreToRemove(ruleStores)
	if oldStore == 0 {
		return nil, nil
	}
	newStore := strategy.SelectStoreToImprove(ruleStores, oldStore)
	if newStore == 0 {
		c.cluster.GetLogger().Debug("resource no replacement container",
			log.ResourceField(res.Meta.GetID()))
		return nil, nil
	}
	checkerCounter.WithLabelValues("rule_checker", "move-to-better-location").Inc()
	newPeer := metapb.Replica{StoreID: newStore, Role: rf.Rule.Role.MetaPeerRole()}
	return operator.CreateMovePeerOperator("move-to-better-location", c.cluster, res, operator.OpReplica, oldStore, newPeer)
}

func (c *RuleChecker) fixOrphanPeers(res *core.CachedShard, fit *placement.ShardFit) (*operator.Operator, error) {
	if len(fit.OrphanPeers) == 0 {
		return nil, nil
	}
	// remove orphan peers only when all rules are satisfied (count+role)
	for _, rf := range fit.RuleFits {
		if !rf.IsSatisfied() {
			checkerCounter.WithLabelValues("rule_checker", "skip-remove-orphan-peer").Inc()
			return nil, nil
		}
	}
	checkerCounter.WithLabelValues("rule_checker", "remove-orphan-peer").Inc()
	peer := fit.OrphanPeers[0]
	return operator.CreateRemovePeerOperator("remove-orphan-peer", c.cluster, operator.OpReplica, res, peer.StoreID)
}

func (c *RuleChecker) isDownPeer(res *core.CachedShard, peer metapb.Replica) bool {
	for _, stats := range res.GetDownPeers() {
		if stats.GetReplica().ID != peer.ID {
			continue
		}
		containerID := peer.StoreID
		container := c.cluster.GetStore(containerID)
		if container == nil {
			c.cluster.GetLogger().Warn("lost the container, maybe you are recovering the Prophet cluster",
				zap.Uint64("container", containerID))
			return false
		}
		if !res.IsDestroyState() &&
			container.DownTime() < c.cluster.GetOpts().GetMaxStoreDownTime() {
			continue
		}
		if !res.IsDestroyState() &&
			stats.GetDownSeconds() < uint64(c.cluster.GetOpts().GetMaxStoreDownTime().Seconds()) {
			continue
		}
		return true
	}
	return false
}

func (c *RuleChecker) isOfflinePeer(res *core.CachedShard, peer metapb.Replica) bool {
	container := c.cluster.GetStore(peer.StoreID)
	if container == nil {
		c.cluster.GetLogger().Warn("lost the container, maybe you are recovering the Prophet cluster",
			zap.Uint64("container", peer.StoreID))
		return false
	}
	return !container.IsUp()
}

func (c *RuleChecker) strategy(res *core.CachedShard, rule *placement.Rule) *ReplicaStrategy {
	return &ReplicaStrategy{
		checkerName:    c.name,
		cluster:        c.cluster,
		isolationLevel: rule.IsolationLevel,
		locationLabels: rule.LocationLabels,
		resource:       res,
		extraFilters:   []filter.Filter{filter.NewLabelConstaintFilter(c.name, rule.LabelConstraints)},
	}
}

func (c *RuleChecker) getRuleFitStores(rf *placement.RuleFit) []*core.CachedStore {
	var containers []*core.CachedStore
	for _, p := range rf.Peers {
		if s := c.cluster.GetStore(p.StoreID); s != nil {
			containers = append(containers, s)
		}
	}
	return containers
}
