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

package operator

import (
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

// OpInfluence records the influence of the cluster.
type OpInfluence struct {
	ContainersInfluence map[uint64]*ContainerInfluence
}

// GetContainerInfluence get containerInfluence of specific container.
func (m OpInfluence) GetContainerInfluence(id uint64) *ContainerInfluence {
	containerInfluence, ok := m.ContainersInfluence[id]
	if !ok {
		containerInfluence = &ContainerInfluence{
			InfluenceStats: map[string]InfluenceStats{},
		}
		m.ContainersInfluence[id] = containerInfluence
	}
	return containerInfluence
}

type InfluenceStats struct {
	ResourceSize  int64
	ResourceCount int64
	LeaderSize    int64
	LeaderCount   int64
}

// ContainerInfluence records influences that pending operators will make.
type ContainerInfluence struct {
	InfluenceStats map[string]InfluenceStats
	StepCost       map[limit.Type]int64
}

// ResourceProperty returns delta size of leader/resource by influence.
func (s ContainerInfluence) ResourceProperty(kind core.ScheduleKind, groupKey string) int64 {
	switch kind.ResourceKind {
	case metapb.ResourceKind_LeaderKind:
		switch kind.Policy {
		case core.ByCount:
			return s.InfluenceStats[groupKey].LeaderCount
		case core.BySize:
			return s.InfluenceStats[groupKey].LeaderSize
		default:
			return 0
		}
	case metapb.ResourceKind_ReplicaKind:
		return s.InfluenceStats[groupKey].ResourceSize
	default:
		return 0
	}
}

// GetStepCost returns the specific type step cost
func (s ContainerInfluence) GetStepCost(limitType limit.Type) int64 {
	if s.StepCost == nil {
		return 0
	}
	return s.StepCost[limitType]
}

func (s *ContainerInfluence) addStepCost(limitType limit.Type, cost int64) {
	if s.StepCost == nil {
		s.StepCost = make(map[limit.Type]int64)
	}
	s.StepCost[limitType] += cost
}

// AdjustStepCost adjusts the step cost of specific type container limit according to resource size
func (s *ContainerInfluence) AdjustStepCost(limitType limit.Type, resourceSize int64) {
	if resourceSize > limit.SmallResourceThreshold {
		s.addStepCost(limitType, limit.ResourceInfluence[limitType])
	} else if resourceSize <= limit.SmallResourceThreshold && resourceSize > limit.EmptyResourceApproximateSize {
		s.addStepCost(limitType, limit.SmallResourceInfluence[limitType])
	}
}
