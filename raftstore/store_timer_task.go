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
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	putil "github.com/matrixorigin/matrixcube/components/prophet/util"
	"go.uber.org/zap"
)

func (s *store) startTimerTasks() {
	s.stopper.RunWorker(func() {
		last := time.Now()

		splitCheckTicker := time.NewTicker(s.cfg.Replication.ShardSplitCheckDuration.Duration)
		defer splitCheckTicker.Stop()

		stateCheckTicker := time.NewTicker(s.cfg.Replication.ShardStateCheckDuration.Duration)
		defer stateCheckTicker.Stop()

		shardLeaderheartbeatTicker := time.NewTicker(s.cfg.Replication.ShardHeartbeatDuration.Duration)
		defer shardLeaderheartbeatTicker.Stop()

		storeheartbeatTicker := time.NewTicker(s.cfg.Replication.StoreHeartbeatDuration.Duration)
		defer storeheartbeatTicker.Stop()

		compactLogCheckTicker := time.NewTicker(s.cfg.Replication.CompactLogCheckDuration.Duration)
		defer compactLogCheckTicker.Stop()

		for {
			select {
			case <-s.stopper.ShouldStop():
				s.logger.Info("timer based tasks stopped",
					s.storeField())
				return
			case <-compactLogCheckTicker.C:
				s.handleCompactLogTask()
			case <-splitCheckTicker.C:
				s.handleSplitCheckTask()
			case <-stateCheckTicker.C:
				s.handleShardStateCheckTask()
			case <-shardLeaderheartbeatTicker.C:
				s.handleShardHeartbeatTask()
			case <-storeheartbeatTicker.C:
				s.handleStoreHeartbeatTask(last)
				last = time.Now()
			}
		}
	})
}

func (s *store) handleShardStateCheckTask() {
	bm := roaring64.NewBitmap()
	s.forEachReplica(func(pr *replica) bool {
		bm.Add(pr.shardID)
		return true
	})

	if bm.GetCardinality() > 0 {
		rsp, err := s.pd.GetClient().CheckResourceState(bm)
		if err != nil {
			s.logger.Error("fail to check shards state, retry later",
				s.storeField(),
				zap.Error(err))
			return
		}

		bm := putil.MustUnmarshalBM64(rsp.Destroyed)
		// FIXME: removeData always true?
		for _, id := range bm.ToArray() {
			s.destroyReplica(id, true, true, "shard state check")
		}
	}
}

func (s *store) handleSplitCheckTask() {
	if s.cfg.Replication.DisableShardSplit {
		return
	}

	s.forEachReplica(func(pr *replica) bool {
		if pr.supportSplit() &&
			pr.isLeader() {
			pr.addAction(action{actionType: checkSplitAction, actionCallback: func(arg interface{}) {
				s.splitChecker.add(arg.(Shard))
			}})
		}

		return true
	})
}

func (s *store) handleShardHeartbeatTask() {
	s.forEachReplica(func(pr *replica) bool {
		if pr.isLeader() {
			pr.addAction(action{actionType: heartbeatAction})
		}
		return true
	})
}

func (s *store) handleCompactLogTask() {
	s.forEachReplica(func(pr *replica) bool {
		if pr.isLeader() {
			pr.addAction(action{actionType: checkCompactLogAction})
		}
		return true
	})
}

func (s *store) handleStoreHeartbeatTask(last time.Time) {
	req, err := s.getStoreHeartbeat(last)
	if err != nil {
		return
	}

	rsp, err := s.pd.GetClient().ContainerHeartbeat(req)
	if err != nil {
		s.logger.Error("fail to send store heartbeat",
			s.storeField(),
			zap.Error(err))
		return
	}
	if s.cfg.Customize.CustomStoreHeartbeatDataProcessor != nil {
		err := s.cfg.Customize.CustomStoreHeartbeatDataProcessor.HandleHeartbeatRsp(rsp.Data)
		if err != nil {
			s.logger.Error("fail to handle store heartbeat rsp data",
				s.storeField(),
				zap.Error(err))
		}
	}
}
