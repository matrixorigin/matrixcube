package checker

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/stretchr/testify/assert"
)

func TestLeaveJointState(t *testing.T) {
	cluster := mockcluster.NewCluster(config.NewTestOptions())
	jsc := NewJointStateChecker(cluster)
	for id := uint64(1); id <= 10; id++ {
		cluster.PutContainerWithLabels(id)
	}

	type testCase struct {
		Peers   []metapb.Peer // first is leader
		OpSteps []operator.OpStep
	}
	cases := []testCase{
		{
			[]metapb.Peer{
				{ID: 101, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 102, ContainerID: 2, Role: metapb.PeerRole_DemotingVoter},
				{ID: 103, ContainerID: 3, Role: metapb.PeerRole_IncomingVoter},
			},
			[]operator.OpStep{
				operator.ChangePeerV2Leave{
					PromoteLearners: []operator.PromoteLearner{{ToContainer: 3}},
					DemoteVoters:    []operator.DemoteVoter{{ToContainer: 2}},
				},
			},
		},
		{
			[]metapb.Peer{
				{ID: 101, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 102, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 103, ContainerID: 3, Role: metapb.PeerRole_IncomingVoter},
			},
			[]operator.OpStep{
				operator.ChangePeerV2Leave{
					PromoteLearners: []operator.PromoteLearner{{ToContainer: 3}},
					DemoteVoters:    []operator.DemoteVoter{},
				},
			},
		},
		{
			[]metapb.Peer{
				{ID: 101, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 102, ContainerID: 2, Role: metapb.PeerRole_DemotingVoter},
				{ID: 103, ContainerID: 3, Role: metapb.PeerRole_Voter},
			},
			[]operator.OpStep{
				operator.ChangePeerV2Leave{
					PromoteLearners: []operator.PromoteLearner{},
					DemoteVoters:    []operator.DemoteVoter{{ToContainer: 2}},
				},
			},
		},
		{
			[]metapb.Peer{
				{ID: 101, ContainerID: 1, Role: metapb.PeerRole_DemotingVoter},
				{ID: 102, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 103, ContainerID: 3, Role: metapb.PeerRole_Voter},
			},
			[]operator.OpStep{
				operator.TransferLeader{FromContainer: 1, ToContainer: 2},
				operator.ChangePeerV2Leave{
					PromoteLearners: []operator.PromoteLearner{},
					DemoteVoters:    []operator.DemoteVoter{{ToContainer: 1}},
				},
			},
		},
		{
			[]metapb.Peer{
				{ID: 101, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 102, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 103, ContainerID: 3, Role: metapb.PeerRole_Voter},
			},
			nil,
		},
		{
			[]metapb.Peer{
				{ID: 101, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 102, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 103, ContainerID: 3, Role: metapb.PeerRole_Learner},
			},
			nil,
		},
	}

	for _, tc := range cases {
		res := core.NewCachedResource(&metadata.TestResource{ResID: 1, ResPeers: tc.Peers}, &tc.Peers[0])
		op := jsc.Check(res)
		checkSteps(t, op, tc.OpSteps)
	}
}

func checkSteps(t *testing.T, op *operator.Operator, steps []operator.OpStep) {
	if len(steps) == 0 {
		assert.Nil(t, op)
		return
	}

	assert.NotNil(t, op)
	assert.Equal(t, "leave-joint-state", op.Desc())
	assert.Equal(t, len(steps), op.Len())

	for i := range steps {
		switch obtain := op.Step(i).(type) {
		case operator.ChangePeerV2Leave:
			expect := steps[i].(operator.ChangePeerV2Leave)
			assert.Equal(t, len(expect.PromoteLearners), len(obtain.PromoteLearners))
			assert.Equal(t, len(expect.DemoteVoters), len(obtain.DemoteVoters))
			for j, p := range expect.PromoteLearners {
				assert.Equal(t, expect.PromoteLearners[j].ToContainer, p.ToContainer)
			}
			for j, d := range expect.DemoteVoters {
				assert.Equal(t, expect.DemoteVoters[j].ToContainer, d.ToContainer)
			}
		case operator.TransferLeader:
			expect := steps[i].(operator.TransferLeader)
			assert.Equal(t, expect.FromContainer, obtain.FromContainer)
			assert.Equal(t, expect.ToContainer, obtain.ToContainer)
		default:
			assert.FailNow(t, "unknown operator step type")
		}
	}
}
