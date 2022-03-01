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
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestPromoteLearner(t *testing.T) {
	cluster := mockcluster.NewCluster(config.NewTestOptions())
	lc := NewLearnerChecker(cluster)
	for id := uint64(1); id <= 10; id++ {
		cluster.PutStoreWithLabels(id)
	}

	resource := core.NewCachedShard(
		metapb.Shard{
			ID: 1,
			Replicas: []metapb.Replica{
				{ID: 101, StoreID: 1},
				{ID: 102, StoreID: 2},
				{ID: 103, StoreID: 3, Role: metapb.ReplicaRole_Learner},
			},
		}, &metapb.Replica{ID: 101, StoreID: 1})

	op := lc.Check(resource)

	assert.NotNil(t, op)
	assert.Equal(t, "promote-learner", op.Desc())
	v, ok := op.Step(0).(operator.PromoteLearner)
	assert.True(t, ok)
	assert.Equal(t, uint64(3), v.ToStore)

	p, ok := resource.GetPeer(103)
	assert.True(t, ok)
	resource = resource.Clone(core.WithPendingPeers([]metapb.Replica{p}))
	op = lc.Check(resource)
	assert.Nil(t, op)
}
