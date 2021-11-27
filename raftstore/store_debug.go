// Copyright 2021 MatrixOrigin.
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

package raftstore

import (
	"go.uber.org/zap"
)

func (s *store) doLogDebugInfo() {
	if ce := s.logger.Check(zap.DebugLevel, ""); ce == nil {
		return
	}

	s.logReplicaTickInfo()
}

func (s *store) logReplicaTickInfo() {
	s.forEachReplica(func(r *replica) bool {
		r.logger.Debug("replica tick info",
			zap.Uint64("total", r.getTickTotalCount()),
			zap.Uint64("handled", r.getTickHandledCount()))
		return true
	})
}
