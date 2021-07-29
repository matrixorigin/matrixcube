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
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/statistics"
)

const (
	// RejectLeader is the label property type that suggests a container should not
	// have any resource leaders.
	RejectLeader = "reject-leader"
)

// Cluster provides an overview of a cluster's resources distribution.
// TODO: This interface should be moved to a better place.
type Cluster interface {
	core.ResourceSetInformer
	core.ContainerSetInformer
	core.ContainerSetController
	statistics.ResourceStatInformer
	statistics.ContainerStatInformer

	GetOpts() *config.PersistOptions
	AllocID() (uint64, error)
	FitResource(*core.CachedResource) *placement.ResourceFit
	RemoveScheduler(name string) error
	AddSuspectResources(ids ...uint64)
	GetResourceFactory() func() metadata.Resource

	// just for test
	DisableJointConsensus()
	JointConsensusEnabled() bool
}

// HeartbeatStream is an interface.
type HeartbeatStream interface {
	Send(*rpcpb.ResourceHeartbeatRsp) error
}
