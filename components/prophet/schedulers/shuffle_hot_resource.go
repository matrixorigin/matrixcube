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

package schedulers

import (
	"errors"
	"math/rand"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/filter"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"go.uber.org/zap"
)

const (
	// ShuffleHotShardName is shuffle hot resource scheduler name.
	ShuffleHotShardName = "shuffle-hot-resource-scheduler"
	// ShuffleHotShardType is shuffle hot resource scheduler type.
	ShuffleHotShardType = "shuffle-hot-resource"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(ShuffleHotShardType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*shuffleHotShardSchedulerConfig)
			if !ok {
				return errors.New("scheduler error configuration")
			}
			conf.Limit = uint64(1)
			if len(args) == 1 {
				limit, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					return err
				}
				conf.Limit = limit
			}
			conf.Name = ShuffleHotShardName
			return nil
		}
	})

	schedule.RegisterScheduler(ShuffleHotShardType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleHotShardSchedulerConfig{Limit: uint64(1)}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleHotShardScheduler(opController, conf), nil
	})
}

type shuffleHotShardSchedulerConfig struct {
	Name  string `json:"name"`
	Limit uint64 `json:"limit"`
}

// ShuffleHotShardScheduler mainly used to test.
// It will randomly pick a hot peer, and move the peer
// to a random container, and then transfer the leader to
// the hot peer.
type shuffleHotShardScheduler struct {
	*BaseScheduler
	stLoadInfos [resourceTypeLen]map[uint64]*containerLoadDetail
	r           *rand.Rand
	conf        *shuffleHotShardSchedulerConfig
	types       []rwType
}

// newShuffleHotShardScheduler creates an admin scheduler that random balance hot resources
func newShuffleHotShardScheduler(opController *schedule.OperatorController, conf *shuffleHotShardSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	ret := &shuffleHotShardScheduler{
		BaseScheduler: base,
		conf:          conf,
		types:         []rwType{read, write},
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.stLoadInfos[ty] = map[uint64]*containerLoadDetail{}
	}
	return ret
}

func (s *shuffleHotShardScheduler) GetName() string {
	return s.conf.Name
}

func (s *shuffleHotShardScheduler) GetType() string {
	return ShuffleHotShardType
}

func (s *shuffleHotShardScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *shuffleHotShardScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	hotRegionAllowed := s.OpController.OperatorCount(operator.OpHotShard) < s.conf.Limit
	regionAllowed := s.OpController.OperatorCount(operator.OpShard) < cluster.GetOpts().GetShardScheduleLimit()
	leaderAllowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !hotRegionAllowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpHotShard.String()).Inc()
	}
	if !regionAllowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpShard.String()).Inc()
	}
	if !leaderAllowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
	}
	return hotRegionAllowed && regionAllowed && leaderAllowed
}

func (s *shuffleHotShardScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	i := s.r.Int() % len(s.types)
	return s.dispatch(s.types[i], cluster)
}

func (s *shuffleHotShardScheduler) dispatch(typ rwType, cluster opt.Cluster) []*operator.Operator {
	storesLoads := cluster.GetStoresLoads()
	switch typ {
	case read:
		s.stLoadInfos[readLeader] = summaryStoresLoad(
			storesLoads,
			map[uint64]Influence{},
			cluster.ShardReadStats(),
			read, metapb.ShardKind_LeaderKind)
		return s.randomSchedule(cluster, s.stLoadInfos[readLeader])
	case write:
		s.stLoadInfos[writeLeader] = summaryStoresLoad(
			storesLoads,
			map[uint64]Influence{},
			cluster.ShardWriteStats(),
			write, metapb.ShardKind_LeaderKind)
		return s.randomSchedule(cluster, s.stLoadInfos[writeLeader])
	}
	return nil
}

func (s *shuffleHotShardScheduler) randomSchedule(cluster opt.Cluster, loadDetail map[uint64]*containerLoadDetail) []*operator.Operator {
	for _, detail := range loadDetail {
		if len(detail.HotPeers) < 1 {
			continue
		}
		i := s.r.Intn(len(detail.HotPeers))
		r := detail.HotPeers[i]
		// select src resource
		srcShard := cluster.GetShard(r.ShardID)
		if srcShard == nil || len(srcShard.GetDownPeers()) != 0 || len(srcShard.GetPendingPeers()) != 0 {
			continue
		}
		srcStoreID := srcShard.GetLeader().GetStoreID()
		srcStore := cluster.GetStore(srcStoreID)
		if srcStore == nil {
			cluster.GetLogger().Debug("source container not found",
				shuffleHotField,
				sourceField(srcStoreID))
		}

		filters := []filter.Filter{
			&filter.StoreStateFilter{ActionScope: s.GetName(), MoveShard: true},
			filter.NewExcludedFilter(s.GetName(), srcShard.GetStoreIDs(), srcShard.GetStoreIDs()),
			filter.NewPlacementSafeguard(s.GetName(), cluster, srcShard, srcStore),
		}
		containers := cluster.GetStores()
		destStoreIDs := make([]uint64, 0, len(containers))
		for _, container := range containers {
			if !filter.Target(cluster.GetOpts(), container, filters) {
				continue
			}
			destStoreIDs = append(destStoreIDs, container.Meta.GetID())
		}
		if len(destStoreIDs) == 0 {
			return nil
		}
		// random pick a dest container
		destStoreID := destStoreIDs[s.r.Intn(len(destStoreIDs))]
		if destStoreID == 0 {
			return nil
		}
		if _, ok := srcShard.GetStorePeer(srcStoreID); !ok {
			return nil
		}
		destPeer := metapb.Replica{StoreID: destStoreID}
		op, err := operator.CreateMoveLeaderOperator("random-move-hot-leader", cluster, srcShard, operator.OpShard|operator.OpLeader, srcStoreID, destPeer)
		if err != nil {
			cluster.GetLogger().Error("fail to create move leader operator",
				shuffleHotField,
				zap.Error(err))
			return nil
		}
		op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
		return []*operator.Operator{op}
	}
	schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
	return nil
}
