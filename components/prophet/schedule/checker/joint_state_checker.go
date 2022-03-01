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
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"go.uber.org/zap"
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
func (c *JointStateChecker) Check(res *core.CachedShard) *operator.Operator {
	checkerCounter.WithLabelValues("joint_state_checker", "check").Inc()
	if !metadata.IsInJointState(res.Meta.GetReplicas()...) {
		return nil
	}
	op, err := operator.CreateLeaveJointStateOperator("leave-joint-state",
		c.cluster, res)
	if err != nil {
		checkerCounter.WithLabelValues("joint_state_checker", "create-operator-fail").Inc()
		c.cluster.GetLogger().Debug("fail to create leave joint state operator",
			zap.Error(err))
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
