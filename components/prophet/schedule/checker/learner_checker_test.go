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

func TestPromoteLearner(t *testing.T) {
	cluster := mockcluster.NewCluster(config.NewTestOptions())
	lc := NewLearnerChecker(cluster)
	for id := uint64(1); id <= 10; id++ {
		cluster.PutContainerWithLabels(id)
	}

	resource := core.NewCachedResource(
		&metadata.TestResource{
			ResID: 1,
			ResPeers: []metapb.Peer{
				{ID: 101, ContainerID: 1},
				{ID: 102, ContainerID: 2},
				{ID: 103, ContainerID: 3, Role: metapb.PeerRole_Learner},
			},
		}, &metapb.Peer{ID: 101, ContainerID: 1})

	op := lc.Check(resource)

	assert.NotNil(t, op)
	assert.Equal(t, "promote-learner", op.Desc())
	v, ok := op.Step(0).(operator.PromoteLearner)
	assert.True(t, ok)
	assert.Equal(t, uint64(3), v.ToContainer)

	p, ok := resource.GetPeer(103)
	assert.True(t, ok)
	resource = resource.Clone(core.WithPendingPeers([]metapb.Peer{p}))
	op = lc.Check(resource)
	assert.Nil(t, op)
}
