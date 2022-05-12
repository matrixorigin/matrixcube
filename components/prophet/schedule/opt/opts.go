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
	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"go.uber.org/zap"
)

const (
	// RejectLeader is the label property type that suggests a store should not
	// have any shard leaders.
	RejectLeader = "reject-leader"
)

// Cluster provides an overview of a cluster's shards distribution.
// TODO: This interface should be moved to a better place.
type Cluster interface {
	core.ShardSetInformer
	core.StoreSetInformer
	core.StoreSetController

	GetLogger() *zap.Logger
	GetOpts() *config.PersistOptions
	AllocID() (uint64, error)
	NextShardEpoch(uint64) (uint64, error)
	FitShard(*core.CachedShard) *placement.ShardFit
	RemoveScheduler(name string) error
	AddSuspectShards(ids ...uint64)

	// just for test
	DisableJointConsensus()
	JointConsensusEnabled() bool
}

// HeartbeatStream is an interface.
type HeartbeatStream interface {
	Send(*rpcpb.ShardHeartbeatRsp) error
}
