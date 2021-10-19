package config

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/stretchr/testify/assert"
)

func TestIssue85(t *testing.T) {
	cfg := NewConfig()
	cfg.Adjust(nil, false)
	cfg.ResourceStateChangedHandler = func(res metadata.Resource, from, to metapb.ResourceState) {}

	pc := NewPersistOptions(cfg, nil)
	s := storage.NewTestStorage()
	assert.NoError(t, pc.Persist(s))
}
