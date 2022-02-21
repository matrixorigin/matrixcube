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
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"go.uber.org/zap"
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
func (l *LearnerChecker) Check(res *core.CachedShard) *operator.Operator {
	for _, p := range res.GetLearners() {
		op, err := operator.CreatePromoteLearnerOperator("promote-learner", l.cluster, res, p)
		if err != nil {
			l.cluster.GetLogger().Debug("fail to create promote learner operator",
				zap.Error(err))
			continue
		}
		return op
	}
	return nil
}
