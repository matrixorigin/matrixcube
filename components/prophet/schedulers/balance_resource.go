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
	"sort"
	"strconv"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/filter"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(BalanceShardType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*balanceShardSchedulerConfig)
			if !ok {
				return errors.New("scheduler not found")
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = BalanceShardName
			return nil
		}
	})
	schedule.RegisterScheduler(BalanceShardType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceShardSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newBalanceShardScheduler(opController, conf), nil
	})
}

const (
	// balanceShardRetryLimit is the limit to retry schedule for selected container.
	balanceShardRetryLimit = 10
	// BalanceShardName is balance resource scheduler name.
	BalanceShardName = "balance-resource-scheduler"
	// BalanceShardType is balance resource scheduler type.
	BalanceShardType = "balance-resource"
)

type balanceShardSchedulerConfig struct {
	Name        string                     `json:"name"`
	Ranges      []core.KeyRange            `json:"ranges"`
	groupRanges map[uint64][]core.KeyRange `json:"-"`
}

type balanceShardScheduler struct {
	*BaseScheduler
	conf         *balanceShardSchedulerConfig
	opController *schedule.OperatorController
	filters      []filter.Filter
	counter      *prometheus.CounterVec

	scheduleField zap.Field
}

// newBalanceShardScheduler creates a scheduler that tends to keep resources on
// each container balanced.
func newBalanceShardScheduler(opController *schedule.OperatorController, conf *balanceShardSchedulerConfig, opts ...BalanceShardCreateOption) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	conf.groupRanges = groupKeyRanges(conf.Ranges,
		opController.GetCluster().GetOpts().GetReplicationConfig().Groups)
	scheduler := &balanceShardScheduler{
		BaseScheduler: base,
		conf:          conf,
		opController:  opController,
		counter:       balanceShardCounter,
	}
	for _, setOption := range opts {
		setOption(scheduler)
	}
	scheduler.filters = []filter.Filter{
		&filter.StoreStateFilter{ActionScope: scheduler.GetName(), MoveShard: true},
		filter.NewSpecialUseFilter(scheduler.GetName()),
	}
	scheduler.scheduleField = zap.String("scheduler", scheduler.GetName())
	return scheduler
}

// BalanceShardCreateOption is used to create a scheduler with an option.
type BalanceShardCreateOption func(s *balanceShardScheduler)

// WithBalanceShardCounter sets the counter for the scheduler.
func WithBalanceShardCounter(counter *prometheus.CounterVec) BalanceShardCreateOption {
	return func(s *balanceShardScheduler) {
		s.counter = counter
	}
}

// WithBalanceShardName sets the name for the scheduler.
func WithBalanceShardName(name string) BalanceShardCreateOption {
	return func(s *balanceShardScheduler) {
		s.conf.Name = name
	}
}

func (s *balanceShardScheduler) GetName() string {
	return s.conf.Name
}

func (s *balanceShardScheduler) GetType() string {
	return BalanceShardType
}

func (s *balanceShardScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *balanceShardScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	allowed := s.opController.OperatorCount(operator.OpShard)-s.opController.OperatorCount(operator.OpMerge) < cluster.GetOpts().GetShardScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpShard.String()).Inc()
	}
	return allowed
}

func (s *balanceShardScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	containers := cluster.GetStores()
	if len(containers) <= cluster.GetOpts().GetMaxReplicas() {
		return nil
	}

	opts := cluster.GetOpts()
	containers = filter.SelectSourceStores(containers, s.filters, opts)
	for _, group := range cluster.GetScheduleGroupKeys() {
		ops := s.scheduleByGroup(group, cluster, containers)
		if len(ops) > 0 {
			return ops
		}
	}
	return nil
}

func (s *balanceShardScheduler) scheduleByGroup(groupKey string, cluster opt.Cluster, containers []*core.CachedStore) []*operator.Operator {
	opts := cluster.GetOpts()
	opInfluence := s.opController.GetOpInfluence(cluster)
	kind := core.NewScheduleKind(metapb.ShardKind_ReplicaKind, core.BySize)

	sort.Slice(containers, func(i, j int) bool {
		iOp := opInfluence.GetStoreInfluence(containers[i].Meta.ID()).ShardProperty(kind, groupKey)
		jOp := opInfluence.GetStoreInfluence(containers[j].Meta.ID()).ShardProperty(kind, groupKey)
		return containers[i].ShardScore(groupKey, opts.GetShardScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), iOp, -1) >
			containers[j].ShardScore(groupKey, opts.GetShardScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), jOp, -1)
	})

	groupID := util.DecodeGroupKey(groupKey)
	for _, source := range containers {
		sourceID := source.Meta.ID()

		for i := 0; i < balanceShardRetryLimit; i++ {
			// Priority pick the Shard that has a pending peer.
			// Pending Shard may means the disk is overload, remove the pending Shard firstly.
			res := cluster.RandPendingShard(groupKey, sourceID, s.conf.groupRanges[groupID], opt.HealthAllowPending(cluster), opt.ReplicatedShard(cluster), opt.AllowBalanceEmptyShard(cluster))
			if res == nil {
				// Then pick the Shard that has a follower in the source container.
				res = cluster.RandFollowerShard(groupKey, sourceID, s.conf.groupRanges[groupID], opt.HealthShard(cluster), opt.ReplicatedShard(cluster), opt.AllowBalanceEmptyShard(cluster))
			}
			if res == nil {
				// Then pick the Shard has the leader in the source container.
				res = cluster.RandLeaderShard(groupKey, sourceID, s.conf.groupRanges[groupID], opt.HealthShard(cluster), opt.ReplicatedShard(cluster), opt.AllowBalanceEmptyShard(cluster))
			}
			if res == nil {
				// Finally pick learner.
				res = cluster.RandLearnerShard(groupKey, sourceID, s.conf.groupRanges[groupID], opt.HealthShard(cluster), opt.ReplicatedShard(cluster), opt.AllowBalanceEmptyShard(cluster))
			}
			if res == nil {
				schedulerCounter.WithLabelValues(s.GetName(), "no-Shard").Inc()
				continue
			}

			if len(containers) > 1 {
				cluster.GetLogger().Debug("scheduler select resource",
					rebalanceShardField,
					s.scheduleField,
					resourceField(res.Meta.ID()))
			}

			// Skip hot resources.
			if cluster.IsShardHot(res) {
				cluster.GetLogger().Debug("skip hot resource",
					rebalanceShardField,
					s.scheduleField,
					resourceField(res.Meta.ID()))
				schedulerCounter.WithLabelValues(s.GetName(), "resource-hot").Inc()
				continue
			}
			// Check resource whether have leader
			if res.GetLeader() == nil {
				cluster.GetLogger().Debug("resource missing leader",
					rebalanceShardField,
					s.scheduleField,
					resourceField(res.Meta.ID()))
				schedulerCounter.WithLabelValues(s.GetName(), "no-leader").Inc()
				continue
			}
			// Skip destroyed res
			if res.IsDestroyState() {
				cluster.GetLogger().Debug("resource in destroy state",
					rebalanceShardField,
					s.scheduleField,
					resourceField(res.Meta.ID()))
				schedulerCounter.WithLabelValues(s.GetName(), "destroy").Inc()
				continue
			}

			oldPeer, _ := res.GetStorePeer(sourceID)
			if op := s.transferPeer(groupKey, cluster, res, oldPeer); op != nil {
				op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
				return []*operator.Operator{op}
			}
		}
	}
	return nil
}

// transferPeer selects the best container to create a new peer to replace the old peer.
func (s *balanceShardScheduler) transferPeer(group string, cluster opt.Cluster, res *core.CachedShard, oldPeer metapb.Replica) *operator.Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	sourceStoreID := oldPeer.GetStoreID()
	source := cluster.GetStore(sourceStoreID)
	if source == nil {
		cluster.GetLogger().Debug("source container not found",
			rebalanceShardField,
			s.scheduleField,
			zap.Uint64("container", sourceStoreID))

		return nil
	}

	filters := []filter.Filter{
		filter.NewExcludedFilter(s.GetName(), nil, res.GetStoreIDs()),
		filter.NewPlacementSafeguard(s.GetName(), cluster, res, source),
		filter.NewSpecialUseFilter(s.GetName()),
		&filter.StoreStateFilter{ActionScope: s.GetName(), MoveShard: true},
	}

	candidates := filter.NewCandidates(cluster.GetStores()).
		FilterTarget(cluster.GetOpts(), filters...).
		Sort(filter.ShardScoreComparer(group, cluster.GetOpts()))

	for _, target := range candidates.Stores {
		resID := res.Meta.ID()
		sourceID := source.Meta.ID()
		targetID := target.Meta.ID()
		cluster.GetLogger().Debug("check resource should balance",
			rebalanceShardField,
			s.scheduleField,
			resourceField(resID),
			sourceField(sourceID),
			targetField(targetID))

		opInfluence := s.opController.GetOpInfluence(cluster)
		kind := core.NewScheduleKind(metapb.ShardKind_ReplicaKind, core.BySize)
		shouldBalance, sourceScore, targetScore := shouldBalance(cluster, source, target, res, kind, opInfluence, s.GetName())
		if !shouldBalance {
			schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
			continue
		}

		newPeer := metapb.Replica{StoreID: target.Meta.ID(), Role: oldPeer.Role}
		op, err := operator.CreateMovePeerOperator(BalanceShardType, cluster, res, operator.OpShard, oldPeer.GetStoreID(), newPeer)
		if err != nil {
			cluster.GetLogger().Error("fail to create move peer operator",
				rebalanceShardField,
				s.scheduleField,
				resourceField(resID),
				sourceField(sourceID),
				targetField(targetID))
			schedulerCounter.WithLabelValues(s.GetName(), "create-operator-fail").Inc()
			return nil
		}
		sourceLabel := strconv.FormatUint(sourceID, 10)
		targetLabel := strconv.FormatUint(targetID, 10)
		op.Counters = append(op.Counters,
			balanceDirectionCounter.WithLabelValues(s.GetName(), sourceLabel, targetLabel),
		)
		op.FinishedCounters = append(op.FinishedCounters,
			s.counter.WithLabelValues("move-peer", sourceLabel+"-out"),
			s.counter.WithLabelValues("move-peer", targetLabel+"-in"),
		)
		op.AdditionalInfos["sourceScore"] = strconv.FormatFloat(sourceScore, 'f', 2, 64)
		op.AdditionalInfos["targetScore"] = strconv.FormatFloat(targetScore, 'f', 2, 64)
		return op
	}

	schedulerCounter.WithLabelValues(s.GetName(), "no-replacement").Inc()
	return nil
}
