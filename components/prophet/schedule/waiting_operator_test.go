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

package schedule

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestRandBuckets(t *testing.T) {
	rb := NewRandBuckets()
	addOperators(rb)
	for i := 0; i < 3; i++ {
		op := rb.GetOperator()
		assert.NotNil(t, op)
	}
	assert.Nil(t, rb.GetOperator())
}

func addOperators(wop WaitingOperator) {
	op := operator.NewOperator("testOperatorNormal", "test", uint64(1), metapb.ShardEpoch{}, operator.OpShard, []operator.OpStep{
		operator.RemovePeer{FromStore: uint64(1)},
	}...)
	wop.PutOperator(op)
	op = operator.NewOperator("testOperatorHigh", "test", uint64(2), metapb.ShardEpoch{}, operator.OpShard, []operator.OpStep{
		operator.RemovePeer{FromStore: uint64(2)},
	}...)
	op.SetPriorityLevel(core.HighPriority)
	wop.PutOperator(op)
	op = operator.NewOperator("testOperatorLow", "test", uint64(3), metapb.ShardEpoch{}, operator.OpShard, []operator.OpStep{
		operator.RemovePeer{FromStore: uint64(3)},
	}...)
	op.SetPriorityLevel(core.LowPriority)
	wop.PutOperator(op)
}

func TestListOperator(t *testing.T) {
	rb := NewRandBuckets()
	addOperators(rb)
	assert.Equal(t, 3, len(rb.ListOperator()))
}

func TestRandomBucketsWithMergeShard(t *testing.T) {
	rb := NewRandBuckets()
	descs := []string{"merge-resource", "admin-merge-resource", "random-merge"}
	for j := 0; j < 100; j++ {
		// adds operators
		desc := descs[j%3]
		op := operator.NewOperator(desc, "test", uint64(1), metapb.ShardEpoch{}, operator.OpShard|operator.OpMerge, []operator.OpStep{
			operator.MergeShard{
				FromShard: &metadata.ShardWithRWLock{
					Shard: metapb.Shard{
						ID:    1,
						Start: []byte{},
						End:   []byte{},
						Epoch: metapb.ShardEpoch{},
					},
				},
				ToShard: &metadata.ShardWithRWLock{
					Shard: metapb.Shard{
						ID:    2,
						Start: []byte{},
						End:   []byte{},
						Epoch: metapb.ShardEpoch{},
					},
				},
				IsPassive: false,
			},
		}...)
		rb.PutOperator(op)
		op = operator.NewOperator(desc, "test", uint64(2), metapb.ShardEpoch{}, operator.OpShard|operator.OpMerge, []operator.OpStep{
			operator.MergeShard{
				FromShard: &metadata.ShardWithRWLock{
					Shard: metapb.Shard{
						ID:    1,
						Start: []byte{},
						End:   []byte{},
						Epoch: metapb.ShardEpoch{},
					},
				},
				ToShard: &metadata.ShardWithRWLock{
					Shard: metapb.Shard{
						ID:    2,
						Start: []byte{},
						End:   []byte{},
						Epoch: metapb.ShardEpoch{},
					},
				},
				IsPassive: true,
			},
		}...)
		rb.PutOperator(op)
		op = operator.NewOperator("testOperatorHigh", "test", uint64(3), metapb.ShardEpoch{}, operator.OpShard, []operator.OpStep{
			operator.RemovePeer{FromStore: uint64(3)},
		}...)
		op.SetPriorityLevel(core.HighPriority)
		rb.PutOperator(op)

		for i := 0; i < 2; i++ {
			op := rb.GetOperator()
			assert.NotNil(t, op)
		}
		assert.Nil(t, rb.GetOperator())
	}
}
