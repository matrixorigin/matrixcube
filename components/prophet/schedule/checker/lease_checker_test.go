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

func TestTransferLease(t *testing.T) {
	cfg := config.NewTestOptions()
	cluster := mockcluster.NewCluster(cfg)
	lc := NewLeaseChecker(cluster)

	cluster.AddShardStore(1, 1)
	cluster.AddShardStore(2, 1)
	cluster.AddShardStore(3, 1)
	cluster.AddLeaderShard(1, 1, 2, 3)

	op := lc.Check(cluster.GetShard(1))
	assert.NotNil(t, op)
	assert.Equal(t, 1, op.Len())
	assert.Equal(t, uint64(1), op.Step(0).(operator.TransferLease).LeaseEpoch)
	assert.Equal(t, cluster.GetShard(1).Meta.Replicas[0].ID, op.Step(0).(operator.TransferLease).ToReplicaID)
}

func TestTransferLeaseWithLeaseAndLeaderIsSame(t *testing.T) {
	cfg := config.NewTestOptions()
	cluster := mockcluster.NewCluster(cfg)
	lc := NewLeaseChecker(cluster)

	cluster.AddShardStore(1, 1)
	cluster.AddShardStore(2, 1)
	cluster.AddShardStore(3, 1)
	cluster.AddLeaderShard(1, 1, 2, 3)

	shard := cluster.GetShard(1)
	op := lc.Check(shard.Clone(core.WithLease(&metapb.EpochLease{Epoch: 1, ReplicaID: shard.Meta.Replicas[0].ID})))
	assert.Nil(t, op)
}
