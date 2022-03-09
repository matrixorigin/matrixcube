// Copyright 2021 MatrixOrigin.
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

package raftstore

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestSetRules(t *testing.T) {
	defer leaktest.AfterTest(t)()

	gc := newReplicaGroupController()

	gc.setRules(nil)
	assert.Equal(t, 0, len(gc.rules))

	gc.setRules([]metapb.ScheduleGroupRule{{ID: 1, GroupID: 1}, {ID: 2, GroupID: 2}})
	assert.Equal(t, 2, len(gc.rules))
	assert.Equal(t, 1, len(gc.rules[1]))
	assert.Equal(t, 1, len(gc.rules[2]))
}

func TestGetShardGroupKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rules := []metapb.ScheduleGroupRule{{ID: 1, GroupByLabel: "l1"}}

	gc := newReplicaGroupController()
	gc.setRules(rules)

	shard := Shard{Labels: []metapb.Label{{Key: "l1", Value: "v1"}}}
	assert.Equal(t, util.EncodeGroupKey(0, rules, shard.Labels), gc.getShardGroupKey(shard))
}
