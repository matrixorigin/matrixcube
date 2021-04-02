package schedule

import (
	"context"

	"github.com/deepfabric/prophet/config"
	"github.com/deepfabric/prophet/core"
	"github.com/deepfabric/prophet/schedule/checker"
	"github.com/deepfabric/prophet/schedule/operator"
	"github.com/deepfabric/prophet/schedule/opt"
	"github.com/deepfabric/prophet/schedule/placement"
	"github.com/deepfabric/prophet/util/cache"
)

// DefaultCacheSize is the default length of waiting list.
const DefaultCacheSize = 1000

// CheckerController is used to manage all checkers.
type CheckerController struct {
	cluster             opt.Cluster
	opts                *config.PersistOptions
	opController        *OperatorController
	learnerChecker      *checker.LearnerChecker
	replicaChecker      *checker.ReplicaChecker
	ruleChecker         *checker.RuleChecker
	mergeChecker        *checker.MergeChecker
	jointStateChecker   *checker.JointStateChecker
	resourceWaitingList cache.Cache
}

// NewCheckerController create a new CheckerController.
// TODO: isSupportMerge should be removed.
func NewCheckerController(ctx context.Context, cluster opt.Cluster, ruleManager *placement.RuleManager, opController *OperatorController) *CheckerController {
	resourceWaitingList := cache.NewDefaultCache(DefaultCacheSize)
	return &CheckerController{
		cluster:             cluster,
		opts:                cluster.GetOpts(),
		opController:        opController,
		learnerChecker:      checker.NewLearnerChecker(cluster),
		replicaChecker:      checker.NewReplicaChecker(cluster, resourceWaitingList),
		ruleChecker:         checker.NewRuleChecker(cluster, ruleManager, resourceWaitingList),
		mergeChecker:        checker.NewMergeChecker(ctx, cluster),
		jointStateChecker:   checker.NewJointStateChecker(cluster),
		resourceWaitingList: resourceWaitingList,
	}
}

// CheckResource will check the resource and add a new operator if needed.
func (c *CheckerController) CheckResource(res *core.CachedResource) []*operator.Operator {
	// If PD has restarted, it need to check learners added before and promote them.
	// Don't check isRaftLearnerEnabled cause it maybe disable learner feature but there are still some learners to promote.
	opController := c.opController

	if op := c.jointStateChecker.Check(res); op != nil {
		return []*operator.Operator{op}
	}

	if c.opts.IsPlacementRulesEnabled() {
		if op := c.ruleChecker.Check(res); op != nil {
			if opController.OperatorCount(operator.OpReplica) < c.opts.GetReplicaScheduleLimit() {
				return []*operator.Operator{op}
			}
			c.resourceWaitingList.Put(res.Meta.ID(), nil)
		}
	} else {
		if op := c.learnerChecker.Check(res); op != nil {
			return []*operator.Operator{op}
		}
		if op := c.replicaChecker.Check(res); op != nil {
			if opController.OperatorCount(operator.OpReplica) < c.opts.GetReplicaScheduleLimit() {
				return []*operator.Operator{op}
			}
			c.resourceWaitingList.Put(res.Meta.ID(), nil)
		}
	}

	if c.mergeChecker != nil && opController.OperatorCount(operator.OpMerge) < c.opts.GetMergeScheduleLimit() {
		if ops := c.mergeChecker.Check(res); ops != nil {
			// It makes sure that two operators can be added successfully altogether.
			return ops
		}
	}
	return nil
}

// GetMergeChecker returns the merge checker.
func (c *CheckerController) GetMergeChecker() *checker.MergeChecker {
	return c.mergeChecker
}

// GetWaitingResources returns the resources in the waiting list.
func (c *CheckerController) GetWaitingResources() []*cache.Item {
	return c.resourceWaitingList.Elems()
}

// AddWaitingResource returns the resources in the waiting list.
func (c *CheckerController) AddWaitingResource(res *core.CachedResource) {
	c.resourceWaitingList.Put(res.Meta.ID(), nil)
}

// RemoveWaitingResource removes the resource from the waiting list.
func (c *CheckerController) RemoveWaitingResource(id uint64) {
	c.resourceWaitingList.Remove(id)
}
