package checker

import (
	"errors"
	"fmt"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/filter"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/cache"
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

// FillReplicas make up all replica for a empty resource
func (c *RuleChecker) FillReplicas(res *core.CachedResource) error {
	if len(res.Meta.Peers()) > 0 {
		return fmt.Errorf("fill resource replicas only support empty resources")
	}

	fit := c.cluster.FitResource(res)
	if len(fit.RuleFits) == 0 {
		return fmt.Errorf("fill resource replicas cann't matches no rules")
	}

	for _, rf := range fit.RuleFits {
		if rf.Rule.Role == placement.Voter {
			rs := c.strategy(res, rf.Rule)
			ruleContainers := c.getRuleFitContainers(rf)

			for i := 0; i < rf.Rule.Count; i++ {
				container := rs.SelectContainerToAdd(ruleContainers)
				if container == 0 {
					return errors.New("no container to add peer")
				}
				peers := res.Meta.Peers()
				peers = append(peers, metapb.Peer{ContainerID: container})
				res.Meta.SetPeers(peers)
			}
			return nil
		}
	}

	return fmt.Errorf("fill resource replicas cann't matches no voter rules")
}

// Check checks if the resource matches placement rules and returns Operator to
// fix it.
func (c *RuleChecker) Check(res *core.CachedResource) *operator.Operator {
	checkerCounter.WithLabelValues("rule_checker", "check").Inc()

	fit := c.cluster.FitResource(res)
	if len(fit.RuleFits) == 0 {
		checkerCounter.WithLabelValues("rule_checker", "fix-range").Inc()
		// If the resource matches no rules, the most possible reason is it spans across
		// multiple rules.
		return c.fixRange(res)
	}
	for _, rf := range fit.RuleFits {
		op, err := c.fixRulePeer(res, fit, rf)
		if err != nil {
			util.GetLogger().Debugf("res-%d rule %s/%s fix rule peer failed with %+v",
				res.Meta.ID(),
				rf.Rule.GroupID,
				rf.Rule.ID,
				err)
			continue
		}
		if op != nil {
			return op
		}
	}
	op, err := c.fixOrphanPeers(res, fit)
	if err != nil {
		util.GetLogger().Debugf("fix orphan peer failed with %+v", err)
		return nil
	}
	return op
}

func (c *RuleChecker) fixRange(res *core.CachedResource) *operator.Operator {
	keys := c.ruleManager.GetSplitKeys(res.GetStartKey(), res.GetEndKey())
	if len(keys) == 0 {
		return nil
	}

	op, err := operator.CreateSplitResourceOperator("rule-split-resource", res, 0, metapb.CheckPolicy_USEKEY, keys)
	if err != nil {
		util.GetLogger().Debugf("create split resource operator failed with %+v", err)
		return nil
	}

	return op
}

func (c *RuleChecker) fixRulePeer(res *core.CachedResource, fit *placement.ResourceFit, rf *placement.RuleFit) (*operator.Operator, error) {
	// make up peers.
	if len(rf.Peers) < rf.Rule.Count {
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

func (c *RuleChecker) addRulePeer(res *core.CachedResource, rf *placement.RuleFit) (*operator.Operator, error) {
	checkerCounter.WithLabelValues("rule_checker", "add-rule-peer").Inc()
	ruleContainers := c.getRuleFitContainers(rf)
	container := c.strategy(res, rf.Rule).SelectContainerToAdd(ruleContainers)
	if container == 0 {
		checkerCounter.WithLabelValues("rule_checker", "no-container-add").Inc()
		c.resourceWaitingList.Put(res.Meta.ID(), nil)
		return nil, errors.New("no container to add peer")
	}
	peer := metapb.Peer{ContainerID: container, Role: rf.Rule.Role.MetaPeerRole()}
	return operator.CreateAddPeerOperator("add-rule-peer", c.cluster, res, peer, operator.OpReplica)
}

func (c *RuleChecker) replaceRulePeer(res *core.CachedResource, rf *placement.RuleFit, peer metapb.Peer, status string) (*operator.Operator, error) {
	ruleContainers := c.getRuleFitContainers(rf)
	container := c.strategy(res, rf.Rule).SelectContainerToReplace(ruleContainers, peer.ContainerID)
	if container == 0 {
		checkerCounter.WithLabelValues("rule_checker", "no-container-replace").Inc()
		c.resourceWaitingList.Put(res.Meta.ID(), nil)
		return nil, errors.New("no container to replace peer")
	}
	newPeer := metapb.Peer{ContainerID: container, Role: rf.Rule.Role.MetaPeerRole()}
	return operator.CreateMovePeerOperator("replace-rule-"+status+"-peer",
		c.cluster, res, operator.OpReplica, peer.ContainerID, newPeer)
}

func (c *RuleChecker) fixLooseMatchPeer(res *core.CachedResource, fit *placement.ResourceFit, rf *placement.RuleFit, peer metapb.Peer) (*operator.Operator, error) {
	if metadata.IsLearner(peer) && rf.Rule.Role != placement.Learner {
		checkerCounter.WithLabelValues("rule_checker", "fix-peer-role").Inc()
		return operator.CreatePromoteLearnerOperator("fix-peer-role", c.cluster, res, peer)
	}
	if res.GetLeader().GetID() != peer.GetID() && rf.Rule.Role == placement.Leader {
		checkerCounter.WithLabelValues("rule_checker", "fix-leader-role").Inc()
		if c.allowLeader(fit, peer) {
			return operator.CreateTransferLeaderOperator("fix-leader-role",
				c.cluster, res, res.GetLeader().ContainerID, peer.ContainerID, 0)
		}
		checkerCounter.WithLabelValues("rule_checker", "not-allow-leader")
		return nil, errors.New("peer cannot be leader")
	}
	if res.GetLeader().GetID() == peer.GetID() && rf.Rule.Role == placement.Follower {
		checkerCounter.WithLabelValues("rule_checker", "fix-follower-role").Inc()
		for _, p := range res.Meta.Peers() {
			if c.allowLeader(fit, p) {
				return operator.CreateTransferLeaderOperator("fix-follower-role",
					c.cluster, res, peer.ContainerID, p.ContainerID, 0)
			}
		}
		checkerCounter.WithLabelValues("rule_checker", "no-new-leader").Inc()
		return nil, errors.New("no new leader")
	}
	return nil, nil
}

func (c *RuleChecker) allowLeader(fit *placement.ResourceFit, peer metapb.Peer) bool {
	if metadata.IsLearner(peer) {
		return false
	}
	s := c.cluster.GetContainer(peer.ContainerID)
	if s == nil {
		return false
	}
	stateFilter := &filter.ContainerStateFilter{ActionScope: "rule-checker", TransferLeader: true}
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

func (c *RuleChecker) fixBetterLocation(res *core.CachedResource, rf *placement.RuleFit) (*operator.Operator, error) {
	if len(rf.Rule.LocationLabels) == 0 || rf.Rule.Count <= 1 {
		return nil, nil
	}

	strategy := c.strategy(res, rf.Rule)
	ruleContainers := c.getRuleFitContainers(rf)
	oldContainer := strategy.SelectContainerToRemove(ruleContainers)
	if oldContainer == 0 {
		return nil, nil
	}
	newContainer := strategy.SelectContainerToImprove(ruleContainers, oldContainer)
	if newContainer == 0 {
		util.GetLogger().Debugf("resource %d no replacement container", res.Meta.ID())
		return nil, nil
	}
	checkerCounter.WithLabelValues("rule_checker", "move-to-better-location").Inc()
	newPeer := metapb.Peer{ContainerID: newContainer, Role: rf.Rule.Role.MetaPeerRole()}
	return operator.CreateMovePeerOperator("move-to-better-location", c.cluster, res, operator.OpReplica, oldContainer, newPeer)
}

func (c *RuleChecker) fixOrphanPeers(res *core.CachedResource, fit *placement.ResourceFit) (*operator.Operator, error) {
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
	return operator.CreateRemovePeerOperator("remove-orphan-peer", c.cluster, 0, res, peer.ContainerID)
}

func (c *RuleChecker) isDownPeer(res *core.CachedResource, peer metapb.Peer) bool {
	for _, stats := range res.GetDownPeers() {
		if stats.GetPeer().ID != peer.ID {
			continue
		}
		containerID := peer.ContainerID
		container := c.cluster.GetContainer(containerID)
		if container == nil {
			util.GetLogger().Warningf("lost the container %d, maybe you are recovering the Prophet cluster",
				containerID)
			return false
		}
		if container.DownTime() < c.cluster.GetOpts().GetMaxContainerDownTime() {
			continue
		}
		if stats.GetDownSeconds() < uint64(c.cluster.GetOpts().GetMaxContainerDownTime().Seconds()) {
			continue
		}
		return true
	}
	return false
}

func (c *RuleChecker) isOfflinePeer(res *core.CachedResource, peer metapb.Peer) bool {
	container := c.cluster.GetContainer(peer.ContainerID)
	if container == nil {
		util.GetLogger().Warningf("lost the container %d, maybe you are recovering the Prophet cluster",
			peer.ContainerID)
		return false
	}
	return !container.IsUp()
}

func (c *RuleChecker) strategy(res *core.CachedResource, rule *placement.Rule) *ReplicaStrategy {
	return &ReplicaStrategy{
		checkerName:    c.name,
		cluster:        c.cluster,
		isolationLevel: rule.IsolationLevel,
		locationLabels: rule.LocationLabels,
		resource:       res,
		extraFilters:   []filter.Filter{filter.NewLabelConstaintFilter(c.name, rule.LabelConstraints)},
	}
}

func (c *RuleChecker) getRuleFitContainers(rf *placement.RuleFit) []*core.CachedContainer {
	var containers []*core.CachedContainer
	for _, p := range rf.Peers {
		if s := c.cluster.GetContainer(p.ContainerID); s != nil {
			containers = append(containers, s)
		}
	}
	return containers
}
