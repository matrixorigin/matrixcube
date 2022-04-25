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
	"sync"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"go.uber.org/zap"
)

// StoreLimiter adjust the store limit dynamically
type StoreLimiter struct {
	m       sync.RWMutex
	logger  *zap.Logger
	opt     *config.PersistOptions
	scene   map[limit.Type]*limit.Scene
	state   *State
	current LoadState
}

// NewStoreLimiter builds a store limiter object using the operator controller
func NewStoreLimiter(opt *config.PersistOptions, logger *zap.Logger) *StoreLimiter {
	defaultScene := map[limit.Type]*limit.Scene{
		limit.AddPeer:    limit.DefaultScene(limit.AddPeer),
		limit.RemovePeer: limit.DefaultScene(limit.RemovePeer),
	}

	return &StoreLimiter{
		opt:     opt,
		logger:  log.Adjust(logger),
		state:   NewState(),
		scene:   defaultScene,
		current: LoadStateNone,
	}
}

// Collect the store statistics and update the cluster state
func (s *StoreLimiter) Collect(stats *metapb.StoreStats) {
	s.m.Lock()
	defer s.m.Unlock()

	s.logger.Debug("collected statistics",
		zap.Any("stats", stats))
	s.state.Collect((*StatEntry)(stats))

	state := s.state.State()
	ratePeerAdd := s.calculateRate(limit.AddPeer, state)
	ratePeerRemove := s.calculateRate(limit.RemovePeer, state)

	if ratePeerAdd > 0 || ratePeerRemove > 0 {
		if ratePeerAdd > 0 {
			s.opt.SetAllStoresLimit(limit.AddPeer, ratePeerAdd)
			s.logger.Info("change store shard add limit for cluster",
				zap.String("state", state.String()),
				zap.Float64("rate-peer-add", ratePeerAdd))
		}
		if ratePeerRemove > 0 {
			s.opt.SetAllStoresLimit(limit.RemovePeer, ratePeerRemove)
			s.logger.Info("change store shard remove limit for cluster",
				zap.String("state", state.String()),
				zap.Float64("rate-peer-remove", ratePeerRemove))
		}
		s.current = state
		collectClusterStateCurrent(state)
	}
}

func collectClusterStateCurrent(state LoadState) {
	for i := LoadStateNone; i <= LoadStateHigh; i++ {
		if i == state {
			clusterStateCurrent.WithLabelValues(state.String()).Set(1)
			continue
		}
		clusterStateCurrent.WithLabelValues(i.String()).Set(0)
	}
}

func (s *StoreLimiter) calculateRate(limitType limit.Type, state LoadState) float64 {
	rate := float64(0)
	switch state {
	case LoadStateIdle:
		rate = float64(s.scene[limitType].Idle)
	case LoadStateLow:
		rate = float64(s.scene[limitType].Low)
	case LoadStateNormal:
		rate = float64(s.scene[limitType].Normal)
	case LoadStateHigh:
		rate = float64(s.scene[limitType].High)
	}
	return rate
}

// ReplaceStoreLimitScene replaces the store limit values for different scenes
func (s *StoreLimiter) ReplaceStoreLimitScene(scene *limit.Scene, limitType limit.Type) {
	s.m.Lock()
	defer s.m.Unlock()
	if s.scene == nil {
		s.scene = make(map[limit.Type]*limit.Scene)
	}
	s.scene[limitType] = scene
}

// StoreLimitScene returns the current limit for different scenes
func (s *StoreLimiter) StoreLimitScene(limitType limit.Type) *limit.Scene {
	s.m.RLock()
	defer s.m.RUnlock()
	return s.scene[limitType]
}
