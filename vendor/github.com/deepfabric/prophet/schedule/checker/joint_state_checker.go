package checker

import (
	"github.com/deepfabric/prophet/core"
	"github.com/deepfabric/prophet/metadata"
	"github.com/deepfabric/prophet/schedule/operator"
	"github.com/deepfabric/prophet/schedule/opt"
	"github.com/deepfabric/prophet/util"
)

// JointStateChecker ensures resource is in joint state will leave.
type JointStateChecker struct {
	cluster opt.Cluster
}

// NewJointStateChecker creates a joint state checker.
func NewJointStateChecker(cluster opt.Cluster) *JointStateChecker {
	return &JointStateChecker{
		cluster: cluster,
	}
}

// Check verifies a resource's role, creating an Operator if need.
func (c *JointStateChecker) Check(res *core.CachedResource) *operator.Operator {
	checkerCounter.WithLabelValues("joint_state_checker", "check").Inc()
	if !metadata.IsInJointState(res.Meta.Peers()...) {
		return nil
	}
	op, err := operator.CreateLeaveJointStateOperator("leave-joint-state",
		c.cluster, res)
	if err != nil {
		checkerCounter.WithLabelValues("joint_state_checker", "create-operator-fail").Inc()
		util.GetLogger().Debugf("fail to create leave joint state operator, error %+v", err)
		return nil
	} else if op != nil {
		checkerCounter.WithLabelValues("joint_state_checker", "new-operator").Inc()
		if op.Len() > 1 {
			checkerCounter.WithLabelValues("joint_state_checker", "transfer-leader").Inc()
		}
		op.SetPriorityLevel(core.HighPriority)
	}
	return op
}
