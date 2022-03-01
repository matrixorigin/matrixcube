package config

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestIssue85(t *testing.T) {
	cfg := NewConfig()
	cfg.Adjust(nil, false)
	cfg.ShardStateChangedHandler = func(res *metapb.Shard, from, to metapb.ShardState) {}

	pc := NewPersistOptions(cfg, nil)
	s := storage.NewTestStorage()
	assert.NoError(t, pc.Persist(s))
}
