package checker

import (
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
)

// LearnerChecker ensures resource has a learner will be promoted.
type LearnerChecker struct {
	cluster opt.Cluster
}

// NewLearnerChecker creates a learner checker.
func NewLearnerChecker(cluster opt.Cluster) *LearnerChecker {
	return &LearnerChecker{
		cluster: cluster,
	}
}

// Check verifies a resource's role, creating an Operator if need.
func (l *LearnerChecker) Check(res *core.CachedResource) *operator.Operator {
	for _, p := range res.GetLearners() {
		op, err := operator.CreatePromoteLearnerOperator("promote-learner", l.cluster, res, p)
		if err != nil {
			util.GetLogger().Debugf("fail to create promote learner operator, error %+v",
				err)
			continue
		}
		return op
	}
	return nil
}
