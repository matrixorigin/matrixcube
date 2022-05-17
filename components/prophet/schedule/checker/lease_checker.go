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
	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"go.uber.org/zap"
)

const (
	leaseCheckerName = "replica-lease-checker"
)

// LeaseChecker make sure the shard's lease replica and the leader lease is same replica
type LeaseChecker struct {
	cluster opt.Cluster
	opts    *config.PersistOptions
}

// NewLeaseChecker creates a replica lease checker.
func NewLeaseChecker(cluster opt.Cluster) *LeaseChecker {
	return &LeaseChecker{
		cluster: cluster,
		opts:    cluster.GetOpts(),
	}
}

// GetType return LeaseChecker's type
func (r *LeaseChecker) GetType() string {
	return leaseCheckerName
}

// Check verifies a shard's lease replica is equal leader replica
func (r *LeaseChecker) Check(res *core.CachedShard) *operator.Operator {
	checkerCounter.WithLabelValues("replica_lease_checker", "check").Inc()
	if res.IsDestroyState() {
		return nil
	}

	if len(res.Meta.GetReplicas()) != r.opts.GetMaxReplicas() {
		return nil
	}

	leader := res.GetLeader()
	if leader == nil {
		return nil
	}

	lease := res.GetLease()
	if lease.GetReplicaID() == leader.GetID() {
		return nil
	}

	epoch, err := r.cluster.NextShardEpoch(res.Meta.GetID())
	if err != nil {
		r.cluster.GetLogger().Debug("fail to create transfer-lease operator",
			zap.Error(err))
		return nil
	}

	op, err := operator.CreateTransferLeaseOperator("transfer-lease", r.cluster, res,
		epoch, leader.GetID(), operator.OpLease)
	if err != nil {
		r.cluster.GetLogger().Debug("fail to create transfer-lease operator",
			zap.Error(err))
		return nil
	}
	return op
}
