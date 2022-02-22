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

package opt

import (
	"github.com/matrixorigin/matrixcube/components/prophet/core"
)

// balanceEmptyShardThreshold is a threshold which allow balance the empty resource
// if the resource number is less than this threshold.
var balanceEmptyShardThreshold = 50

// IsShardHealthy checks if a resource is healthy for scheduling. It requires the
// resource does not have any down or pending peers. And when placement rules
// feature is disabled, it requires the resource does not have any learner peer.
func IsShardHealthy(cluster Cluster, res *core.CachedShard) bool {
	return IsHealthyAllowPending(cluster, res) && len(res.GetPendingPeers()) == 0
}

// IsHealthyAllowPending checks if a resource is healthy for scheduling.
// Differs from IsShardHealthy, it allows the resource to have pending peers.
func IsHealthyAllowPending(cluster Cluster, res *core.CachedShard) bool {
	if !cluster.GetOpts().IsPlacementRulesEnabled() && len(res.GetLearners()) > 0 {
		return false
	}
	return len(res.GetDownPeers()) == 0
}

// IsEmptyShardAllowBalance checks if a region is an empty region and can be balanced.
func IsEmptyShardAllowBalance(cluster Cluster, res *core.CachedShard) bool {
	return res.GetApproximateSize() > core.EmptyShardApproximateSize ||
		cluster.GetShardCount() < balanceEmptyShardThreshold
}

// HealthShard returns a function that checks if a resource is healthy for
// scheduling. It requires the resource does not have any down or pending peers,
// and does not have any learner peers when placement rules is disabled.
func HealthShard(cluster Cluster) func(*core.CachedShard) bool {
	return func(res *core.CachedShard) bool { return IsShardHealthy(cluster, res) }
}

// HealthAllowPending returns a function that checks if a resource is
// healthy for scheduling. Differs from HealthShard, it allows the resource
// to have pending peers.
func HealthAllowPending(cluster Cluster) func(*core.CachedShard) bool {
	return func(res *core.CachedShard) bool { return IsHealthyAllowPending(cluster, res) }
}

// AllowBalanceEmptyShard returns a function that checks if a resource is an empty resource
// and can be balanced.
func AllowBalanceEmptyShard(cluster Cluster) func(*core.CachedShard) bool {
	return func(res *core.CachedShard) bool { return IsEmptyShardAllowBalance(cluster, res) }
}

// IsShardReplicated checks if a resource is fully replicated. When placement
// rules is enabled, its peers should fit corresponding rules. When placement
// rules is disabled, it should have enough replicas and no any learner peer.
func IsShardReplicated(cluster Cluster, res *core.CachedShard) bool {
	if cluster.GetOpts().IsPlacementRulesEnabled() {
		return cluster.FitShard(res).IsSatisfied()
	}
	return len(res.GetLearners()) == 0 && len(res.Meta.Replicas()) == cluster.GetOpts().GetMaxReplicas()
}

// ReplicatedShard returns a function that checks if a resource is fully replicated.
func ReplicatedShard(cluster Cluster) func(*core.CachedShard) bool {
	return func(res *core.CachedShard) bool { return IsShardReplicated(cluster, res) }
}
