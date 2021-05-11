package cluster

import (
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/stretchr/testify/assert"
)

type testContainerLimiter struct {
	opt *config.PersistOptions
}

func (s *testContainerLimiter) setup(t *testing.T) {
	// Create a server for testing
	s.opt = config.NewTestOptions()
}

func TestCollect(t *testing.T) {
	s := &testContainerLimiter{}
	s.setup(t)

	limiter := NewContainerLimiter(s.opt)

	limiter.Collect(&rpcpb.ContainerStats{})
	assert.Equal(t, int64(1), limiter.state.cst.total)
}

func TestContainerLimitScene(t *testing.T) {
	s := &testContainerLimiter{}
	s.setup(t)

	limiter := NewContainerLimiter(s.opt)
	assert.True(t, reflect.DeepEqual(limiter.scene[limit.AddPeer], limit.DefaultScene(limit.AddPeer)))
	assert.True(t, reflect.DeepEqual(limiter.scene[limit.RemovePeer], limit.DefaultScene(limit.RemovePeer)))
}

func TestReplaceContainerLimitScene(t *testing.T) {
	s := &testContainerLimiter{}
	s.setup(t)

	limiter := NewContainerLimiter(s.opt)

	sceneAddPeer := &limit.Scene{Idle: 4, Low: 3, Normal: 2, High: 1}
	limiter.ReplaceContainerLimitScene(sceneAddPeer, limit.AddPeer)

	assert.True(t, reflect.DeepEqual(limiter.scene[limit.AddPeer], sceneAddPeer))

	sceneRemovePeer := &limit.Scene{Idle: 5, Low: 4, Normal: 3, High: 2}
	limiter.ReplaceContainerLimitScene(sceneRemovePeer, limit.RemovePeer)
}
