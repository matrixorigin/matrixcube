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

package cluster

import (
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/pb/metapb"
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

	limiter := NewContainerLimiter(s.opt, nil)

	limiter.Collect(&metapb.ContainerStats{})
	assert.Equal(t, int64(1), limiter.state.cst.total)
}

func TestContainerLimitScene(t *testing.T) {
	s := &testContainerLimiter{}
	s.setup(t)

	limiter := NewContainerLimiter(s.opt, nil)
	assert.True(t, reflect.DeepEqual(limiter.scene[limit.AddPeer], limit.DefaultScene(limit.AddPeer)))
	assert.True(t, reflect.DeepEqual(limiter.scene[limit.RemovePeer], limit.DefaultScene(limit.RemovePeer)))
}

func TestReplaceContainerLimitScene(t *testing.T) {
	s := &testContainerLimiter{}
	s.setup(t)

	limiter := NewContainerLimiter(s.opt, nil)

	sceneAddPeer := &limit.Scene{Idle: 4, Low: 3, Normal: 2, High: 1}
	limiter.ReplaceContainerLimitScene(sceneAddPeer, limit.AddPeer)

	assert.True(t, reflect.DeepEqual(limiter.scene[limit.AddPeer], sceneAddPeer))

	sceneRemovePeer := &limit.Scene{Idle: 5, Low: 4, Normal: 3, High: 2}
	limiter.ReplaceContainerLimitScene(sceneRemovePeer, limit.RemovePeer)
}
