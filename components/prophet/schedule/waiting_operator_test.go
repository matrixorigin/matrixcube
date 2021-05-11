package schedule

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
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
	op := operator.NewOperator("testOperatorNormal", "test", uint64(1), metapb.ResourceEpoch{}, operator.OpResource, []operator.OpStep{
		operator.RemovePeer{FromContainer: uint64(1)},
	}...)
	wop.PutOperator(op)
	op = operator.NewOperator("testOperatorHigh", "test", uint64(2), metapb.ResourceEpoch{}, operator.OpResource, []operator.OpStep{
		operator.RemovePeer{FromContainer: uint64(2)},
	}...)
	op.SetPriorityLevel(core.HighPriority)
	wop.PutOperator(op)
	op = operator.NewOperator("testOperatorLow", "test", uint64(3), metapb.ResourceEpoch{}, operator.OpResource, []operator.OpStep{
		operator.RemovePeer{FromContainer: uint64(3)},
	}...)
	op.SetPriorityLevel(core.LowPriority)
	wop.PutOperator(op)
}

func TestListOperator(t *testing.T) {
	rb := NewRandBuckets()
	addOperators(rb)
	assert.Equal(t, 3, len(rb.ListOperator()))
}

func TestRandomBucketsWithMergeResource(t *testing.T) {
	rb := NewRandBuckets()
	descs := []string{"merge-resource", "admin-merge-resource", "random-merge"}
	for j := 0; j < 100; j++ {
		// adds operators
		desc := descs[j%3]
		op := operator.NewOperator(desc, "test", uint64(1), metapb.ResourceEpoch{}, operator.OpResource|operator.OpMerge, []operator.OpStep{
			operator.MergeResource{
				FromResource: &metadata.TestResource{
					ResID:    1,
					Start:    []byte{},
					End:      []byte{},
					ResEpoch: metapb.ResourceEpoch{}},
				ToResource: &metadata.TestResource{
					ResID:    2,
					Start:    []byte{},
					End:      []byte{},
					ResEpoch: metapb.ResourceEpoch{}},
				IsPassive: false,
			},
		}...)
		rb.PutOperator(op)
		op = operator.NewOperator(desc, "test", uint64(2), metapb.ResourceEpoch{}, operator.OpResource|operator.OpMerge, []operator.OpStep{
			operator.MergeResource{
				FromResource: &metadata.TestResource{
					ResID:    1,
					Start:    []byte{},
					End:      []byte{},
					ResEpoch: metapb.ResourceEpoch{}},
				ToResource: &metadata.TestResource{
					ResID:    2,
					Start:    []byte{},
					End:      []byte{},
					ResEpoch: metapb.ResourceEpoch{}},
				IsPassive: true,
			},
		}...)
		rb.PutOperator(op)
		op = operator.NewOperator("testOperatorHigh", "test", uint64(3), metapb.ResourceEpoch{}, operator.OpResource, []operator.OpStep{
			operator.RemovePeer{FromContainer: uint64(3)},
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
