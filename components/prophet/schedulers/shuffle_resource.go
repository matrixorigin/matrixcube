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

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/filter"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

const (
	// ShuffleShardName is shuffle resource scheduler name.
	ShuffleShardName = "shuffle-resource-scheduler"
	// ShuffleShardType is shuffle resource scheduler type.
	ShuffleShardType = "shuffle-resource"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(ShuffleShardType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*shuffleShardSchedulerConfig)
			if !ok {
				return errors.New("scheduler error configuration")
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Roles = allRoles
			return nil
		}
	})
	schedule.RegisterScheduler(ShuffleShardType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleShardSchedulerConfig{storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleShardScheduler(opController, conf), nil
	})
}

type shuffleShardScheduler struct {
	*BaseScheduler
	conf    *shuffleShardSchedulerConfig
	filters []filter.Filter
}

// newShuffleShardScheduler creates an admin scheduler that shuffles resources
// between containers.
func newShuffleShardScheduler(opController *schedule.OperatorController, conf *shuffleShardSchedulerConfig) schedule.Scheduler {
	filters := []filter.Filter{
		&filter.StoreStateFilter{ActionScope: ShuffleShardName, MoveShard: true},
		filter.NewSpecialUseFilter(ShuffleShardName),
	}
	base := NewBaseScheduler(opController)
	conf.groupRanges = groupKeyRanges(conf.Ranges,
		opController.GetCluster().GetOpts().GetReplicationConfig().Groups)
	return &shuffleShardScheduler{
		BaseScheduler: base,
		conf:          conf,
		filters:       filters,
	}
}

func (s *shuffleShardScheduler) GetName() string {
	return ShuffleShardName
}

func (s *shuffleShardScheduler) GetType() string {
	return ShuffleShardType
}

func (s *shuffleShardScheduler) EncodeConfig() ([]byte, error) {
	return s.conf.EncodeConfig()
}

func (s *shuffleShardScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpShard) < cluster.GetOpts().GetShardScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpShard.String()).Inc()
	}
	return allowed
}

func (s *shuffleShardScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	res, oldPeer := s.scheduleRemovePeer(cluster)
	if res == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-resource").Inc()
		return nil
	}

	newPeer, ok := s.scheduleAddPeer(cluster, res, oldPeer)
	if !ok {
		schedulerCounter.WithLabelValues(s.GetName(), "no-new-peer").Inc()
		return nil
	}

	op, err := operator.CreateMovePeerOperator(ShuffleShardType, cluster, res, operator.OpAdmin, oldPeer.GetStoreID(), newPeer)
	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "create-operator-fail").Inc()
		return nil
	}
	op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
	op.SetPriorityLevel(core.HighPriority)
	return []*operator.Operator{op}
}

func (s *shuffleShardScheduler) scheduleRemovePeer(cluster opt.Cluster) (*core.CachedShard, metapb.Replica) {
	candidates := filter.NewCandidates(cluster.GetStores()).
		FilterSource(cluster.GetOpts(), s.filters...).
		Shuffle()

	for _, source := range candidates.Stores {
		for _, groupKey := range cluster.GetScheduleGroupKeys() {
			var res *core.CachedShard
			if s.conf.IsRoleAllow(roleFollower) {
				res = cluster.RandFollowerShard(groupKey, source.Meta.GetID(), s.conf.groupRanges[util.DecodeGroupKey(groupKey)], opt.HealthShard(cluster), opt.ReplicatedShard(cluster))
			}
			if res == nil && s.conf.IsRoleAllow(roleLeader) {
				res = cluster.RandLeaderShard(groupKey, source.Meta.GetID(), s.conf.groupRanges[util.DecodeGroupKey(groupKey)], opt.HealthShard(cluster), opt.ReplicatedShard(cluster))
			}
			if res == nil && s.conf.IsRoleAllow(roleLearner) {
				res = cluster.RandLearnerShard(groupKey, source.Meta.GetID(), s.conf.groupRanges[util.DecodeGroupKey(groupKey)], opt.HealthShard(cluster), opt.ReplicatedShard(cluster))
			}
			if res != nil {
				if p, ok := res.GetStorePeer(source.Meta.GetID()); ok {
					return res, p
				}

				return nil, metapb.Replica{}
			}
			schedulerCounter.WithLabelValues(s.GetName(), "no-resource").Inc()
		}
	}

	schedulerCounter.WithLabelValues(s.GetName(), "no-source-container").Inc()
	return nil, metapb.Replica{}
}

func (s *shuffleShardScheduler) scheduleAddPeer(cluster opt.Cluster, res *core.CachedShard, oldPeer metapb.Replica) (metapb.Replica, bool) {
	scoreGuard := filter.NewPlacementSafeguard(s.GetName(), cluster, res, cluster.GetStore(oldPeer.StoreID))
	excludedFilter := filter.NewExcludedFilter(s.GetName(), nil, res.GetStoreIDs())

	target := filter.NewCandidates(cluster.GetStores()).
		FilterTarget(cluster.GetOpts(), s.filters...).
		FilterTarget(cluster.GetOpts(), scoreGuard, excludedFilter).
		RandomPick()
	if target == nil {
		return metapb.Replica{}, false
	}
	return metapb.Replica{StoreID: target.Meta.GetID(), Role: oldPeer.GetRole()}, true
}
