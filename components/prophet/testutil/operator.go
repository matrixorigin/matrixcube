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

package testutil

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/stretchr/testify/assert"
)

// CheckAddPeer checks if the operator is to add peer on specified container.
func CheckAddPeer(t *testing.T, op *operator.Operator, kind operator.OpKind, containerID uint64) {
	assert.NotNil(t, op)
	assert.Equal(t, 2, op.Len())
	assert.Equal(t, containerID, op.Step(0).(operator.AddLearner).ToStore)
	_, ok := op.Step(1).(operator.PromoteLearner)
	assert.True(t, ok)
	kind |= operator.OpShard
	assert.Equal(t, kind, op.Kind()&kind)
}

// CheckRemovePeer checks if the operator is to remove peer on specified container.
func CheckRemovePeer(t *testing.T, op *operator.Operator, containerID uint64) {
	assert.NotNil(t, op)
	if op.Len() == 1 {
		assert.Equal(t, containerID, op.Step(0).(operator.RemovePeer).FromStore)
	} else {
		assert.Equal(t, 2, op.Len())
		assert.Equal(t, containerID, op.Step(0).(operator.TransferLeader).FromStore)
		assert.Equal(t, containerID, op.Step(1).(operator.RemovePeer).FromStore)
	}
}

// CheckTransferLeader checks if the operator is to transfer leader between the specified source and target containers.
func CheckTransferLeader(t *testing.T, op *operator.Operator, kind operator.OpKind, sourceID, targetID uint64) {
	assert.NotNil(t, op)
	assert.Equal(t, 1, op.Len())
	assert.Equal(t, operator.TransferLeader{FromStore: sourceID, ToStore: targetID}, op.Step(0))
	kind |= operator.OpLeader
	assert.Equal(t, kind, op.Kind()&kind)
}

// CheckTransferLeaderFrom checks if the operator is to transfer leader out of the specified container.
func CheckTransferLeaderFrom(t *testing.T, op *operator.Operator, kind operator.OpKind, sourceID uint64) {
	assert.NotNil(t, op)
	assert.Equal(t, 1, op.Len())
	assert.Equal(t, sourceID, op.Step(0).(operator.TransferLeader).FromStore)
	kind |= operator.OpLeader
	assert.Equal(t, kind, op.Kind()&kind)
}

// CheckTransferPeer checks if the operator is to transfer peer between the specified source and target containers.
func CheckTransferPeer(t *testing.T, op *operator.Operator, kind operator.OpKind, sourceID, targetID uint64) {
	assert.NotNil(t, op)

	steps, _ := trimTransferLeaders(op)
	assert.Equal(t, 3, len(steps))
	assert.Equal(t, targetID, steps[0].(operator.AddLearner).ToStore)
	_, ok := steps[1].(operator.PromoteLearner)
	assert.True(t, ok)
	assert.Equal(t, sourceID, steps[2].(operator.RemovePeer).FromStore)
	kind |= operator.OpShard
	assert.Equal(t, kind, op.Kind()&kind)
}

// CheckTransferLearner checks if the operator is to transfer learner between the specified source and target containers.
func CheckTransferLearner(t *testing.T, op *operator.Operator, kind operator.OpKind, sourceID, targetID uint64) {
	assert.NotNil(t, op)

	steps, _ := trimTransferLeaders(op)
	assert.Equal(t, 2, len(steps))
	assert.Equal(t, steps[0].(operator.AddLearner).ToStore, targetID)
	assert.Equal(t, steps[1].(operator.RemovePeer).FromStore, sourceID)
	kind |= operator.OpShard
	assert.Equal(t, kind, op.Kind()&kind)
}

// CheckTransferPeerWithLeaderTransfer checks if the operator is to transfer
// peer between the specified source and target containers and it meanwhile
// transfers the leader out of source container.
func CheckTransferPeerWithLeaderTransfer(t *testing.T, op *operator.Operator, kind operator.OpKind, sourceID, targetID uint64) {
	assert.NotNil(t, op)

	steps, lastLeader := trimTransferLeaders(op)
	assert.Equal(t, 3, len(steps))
	assert.Equal(t, steps[0].(operator.AddLearner).ToStore, targetID)
	_, ok := steps[1].(operator.PromoteLearner)
	assert.True(t, ok)
	assert.Equal(t, steps[2].(operator.RemovePeer).FromStore, sourceID)
	assert.NotEqual(t, lastLeader, uint64(0))
	assert.NotEqual(t, lastLeader, sourceID)
	kind |= operator.OpShard
	assert.Equal(t, kind, op.Kind()&kind)
}

// CheckTransferPeerWithLeaderTransferFrom checks if the operator is to transfer
// peer out of the specified container and it meanwhile transfers the leader out of
// the container.
func CheckTransferPeerWithLeaderTransferFrom(t *testing.T, op *operator.Operator, kind operator.OpKind, sourceID uint64) {
	assert.NotNil(t, op)

	steps, lastLeader := trimTransferLeaders(op)
	_, ok := steps[0].(operator.AddLearner)
	assert.True(t, ok)
	_, ok = steps[1].(operator.PromoteLearner)
	assert.True(t, ok)

	assert.Equal(t, steps[2].(operator.RemovePeer).FromStore, sourceID)
	assert.NotEqual(t, lastLeader, uint64(0))
	assert.NotEqual(t, lastLeader, sourceID)
	kind |= operator.OpShard | operator.OpLeader
	assert.Equal(t, kind, op.Kind()&kind)
}

func trimTransferLeaders(op *operator.Operator) (steps []operator.OpStep, lastLeader uint64) {
	for i := 0; i < op.Len(); i++ {
		step := op.Step(i)
		if s, ok := step.(operator.TransferLeader); ok {
			lastLeader = s.ToStore
		} else {
			steps = append(steps, step)
		}
	}
	return
}
