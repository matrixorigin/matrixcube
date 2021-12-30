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

		refreshScheduleGroupRuleTicker := time.NewTicker(time.Second * 30)
		defer refreshScheduleGroupRuleTicker.Stop()

		debugTicker := time.NewTicker(time.Second * 10)
		defer debugTicker.Stop()

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
			case <-refreshScheduleGroupRuleTicker.C:
				s.handleRefreshScheduleGroupRule()
			case <-debugTicker.C:
				s.doLogDebugInfo()
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
		for _, id := range bm.ToArray() {
			// FIXME: we don't known whether to remove data or not. Conservative retention data.
			s.destroyReplica(id, true, false, "shard state check")
		}

		bm = putil.MustUnmarshalBM64(rsp.Destroying)
		for _, id := range bm.ToArray() {
			if pr := s.getReplica(id, false); pr != nil {
				// There are a scenario that can trigger this process:
				// The Shard A has 3 replcias A1, A2, A3. The current replica A3 is running status,
				// and restart after a long time. During the time A3 was offline, A2 and A3 completed
				// the deletion process. So A3's log cannot continue to execute and will not trigger
				// the delete process.

				// We don't known which log index destroy at and whether to remove data or not.
				pr.startDestroyReplicaTask(0, false, "replicas state check")
			}
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

func (s *store) handleRefreshScheduleGroupRule() bool {
	rules, err := s.pd.GetClient().GetSchedulingRules()
	if err != nil {
		s.logger.Error("failed to load scheduling rules from prophet",
			zap.Error(err))
		return false
	}

	s.logger.Info("scheduling rules loadded",
		zap.Int("count", len(rules)))
	s.groupController.setRules(rules)
	return true
}
