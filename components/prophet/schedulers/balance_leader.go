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

const (
	// BalanceLeaderName is balance leader scheduler name.
	BalanceLeaderName = "balance-leader-scheduler"
	// BalanceLeaderType is balance leader scheduler type.
	BalanceLeaderType = "balance-leader"
	// balanceLeaderRetryLimit is the limit to retry schedule for selected source container and target container.
	balanceLeaderRetryLimit = 10
)

func init() {
	schedule.RegisterSliceDecoderBuilder(BalanceLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*balanceLeaderSchedulerConfig)
			if !ok {
				return errors.New("scheduler not found")
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = BalanceLeaderName
			return nil
		}
	})

	schedule.RegisterScheduler(BalanceLeaderType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceLeaderSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newBalanceLeaderScheduler(opController, conf), nil
	})
}

type balanceLeaderSchedulerConfig struct {
	Name        string                     `json:"name"`
	Ranges      []core.KeyRange            `json:"ranges"`
	groupRanges map[uint64][]core.KeyRange `json:"-"`
}

type balanceLeaderScheduler struct {
	*BaseScheduler
	conf          *balanceLeaderSchedulerConfig
	opController  *schedule.OperatorController
	filters       []filter.Filter
	counter       *prometheus.CounterVec
	scheduleField zap.Field
}

// newBalanceLeaderScheduler creates a scheduler that tends to keep leaders on
// each container balanced.
func newBalanceLeaderScheduler(opController *schedule.OperatorController, conf *balanceLeaderSchedulerConfig, options ...BalanceLeaderCreateOption) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	conf.groupRanges = groupKeyRanges(conf.Ranges, opController.GetCluster().GetOpts().GetReplicationConfig().Groups)
	s := &balanceLeaderScheduler{
		BaseScheduler: base,
		conf:          conf,
		opController:  opController,
		counter:       balanceLeaderCounter,
	}
	for _, option := range options {
		option(s)
	}
	s.filters = []filter.Filter{
		&filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true},
		filter.NewSpecialUseFilter(s.GetName()),
	}
	s.scheduleField = zap.String("scheduler", s.GetName())
	return s
}

// BalanceLeaderCreateOption is used to create a scheduler with an option.
type BalanceLeaderCreateOption func(s *balanceLeaderScheduler)

// WithBalanceLeaderCounter sets the counter for the scheduler.
func WithBalanceLeaderCounter(counter *prometheus.CounterVec) BalanceLeaderCreateOption {
	return func(s *balanceLeaderScheduler) {
		s.counter = counter
	}
}

// WithBalanceLeaderName sets the name for the scheduler.
func WithBalanceLeaderName(name string) BalanceLeaderCreateOption {
	return func(s *balanceLeaderScheduler) {
		s.conf.Name = name
	}
}

func (l *balanceLeaderScheduler) GetName() string {
	return l.conf.Name
}

func (l *balanceLeaderScheduler) GetType() string {
	return BalanceLeaderType
}

func (l *balanceLeaderScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(l.conf)
}

func (l *balanceLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	allowed := l.opController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(l.GetType(), operator.OpLeader.String()).Inc()
	}
	return allowed
}

func (l *balanceLeaderScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()

	containers := cluster.GetStores()
	if len(containers) <= 1 {
		return nil
	}

	leaderSchedulePolicy := l.opController.GetLeaderSchedulePolicy()
	opInfluence := l.opController.GetOpInfluence(cluster)
	sources := filter.SelectSourceStores(containers, l.filters, cluster.GetOpts())
	targets := filter.SelectTargetStores(containers, l.filters, cluster.GetOpts())
	kind := core.NewScheduleKind(metapb.ShardKind_LeaderKind, leaderSchedulePolicy)
	for _, groupKey := range cluster.GetScheduleGroupKeys() {
		sort.Slice(sources, func(i, j int) bool {
			iOp := opInfluence.GetStoreInfluence(sources[i].Meta.ID()).ShardProperty(kind, groupKey)
			jOp := opInfluence.GetStoreInfluence(sources[j].Meta.ID()).ShardProperty(kind, groupKey)
			return sources[i].LeaderScore(groupKey, leaderSchedulePolicy, iOp) >
				sources[j].LeaderScore(groupKey, leaderSchedulePolicy, jOp)
		})
		sort.Slice(targets, func(i, j int) bool {
			iOp := opInfluence.GetStoreInfluence(targets[i].Meta.ID()).ShardProperty(kind, groupKey)
			jOp := opInfluence.GetStoreInfluence(targets[j].Meta.ID()).ShardProperty(kind, groupKey)
			return targets[i].LeaderScore(groupKey, leaderSchedulePolicy, iOp) <
				targets[j].LeaderScore(groupKey, leaderSchedulePolicy, jOp)
		})

		for i := 0; i < len(sources) || i < len(targets); i++ {
			if i < len(sources) {
				source := sources[i]
				sourceID := source.Meta.ID()
				cluster.GetLogger().Debug("check resource leader out",
					rebalanceLeaderField,
					l.scheduleField,
					sourceField(sourceID))
				sourceStoreLabel := strconv.FormatUint(sourceID, 10)
				l.counter.WithLabelValues("high-score", sourceStoreLabel).Inc()
				for j := 0; j < balanceLeaderRetryLimit; j++ {
					if ops := l.transferLeaderOut(groupKey, cluster, source, opInfluence); len(ops) > 0 {
						ops[0].Counters = append(ops[0].Counters, l.counter.WithLabelValues("transfer-out", sourceStoreLabel))
						return ops
					}
				}
				cluster.GetLogger().Debug("no operator created for selected container",
					rebalanceLeaderField,
					l.scheduleField,
					sourceField(sourceID))
			}
			if i < len(targets) {
				target := targets[i]
				targetID := target.Meta.ID()
				cluster.GetLogger().Debug("check resource leader in",
					rebalanceLeaderField,
					l.scheduleField,
					targetField(targetID))
				targetStoreLabel := strconv.FormatUint(targetID, 10)
				l.counter.WithLabelValues("low-score", targetStoreLabel).Inc()

				for j := 0; j < balanceLeaderRetryLimit; j++ {
					if ops := l.transferLeaderIn(groupKey, cluster, target); len(ops) > 0 {
						ops[0].Counters = append(ops[0].Counters, l.counter.WithLabelValues("transfer-in", targetStoreLabel))
						return ops
					}
				}
				cluster.GetLogger().Debug("no operator created for selected container",
					rebalanceLeaderField,
					l.scheduleField,
					targetField(targetID))
			}
		}
	}

	return nil
}

// transferLeaderOut transfers leader from the source container.
// It randomly selects a health resource from the source container, then picks
// the best follower peer and transfers the leader.
func (l *balanceLeaderScheduler) transferLeaderOut(groupKey string, cluster opt.Cluster, source *core.CachedStore, opInfluence operator.OpInfluence) []*operator.Operator {
	sourceID := source.Meta.ID()
	resource := cluster.RandLeaderShard(groupKey, sourceID, l.conf.groupRanges[util.DecodeGroupKey(groupKey)], opt.HealthShard(cluster))
	if resource == nil {
		cluster.GetLogger().Debug("selected container has no leader, nothing to do",
			rebalanceLeaderField,
			l.scheduleField,
			sourceField(sourceID))
		schedulerCounter.WithLabelValues(l.GetName(), "no-leader-resource").Inc()
		return nil
	}
	targets := cluster.GetFollowerStores(resource)
	finalFilters := l.filters
	if leaderFilter := filter.NewPlacementLeaderSafeguard(l.GetName(), cluster, resource, source,
		l.opController.GetCluster().GetShardFactory()); leaderFilter != nil {
		finalFilters = append(l.filters, leaderFilter)
	}
	targets = filter.SelectTargetStores(targets, finalFilters, cluster.GetOpts())
	leaderSchedulePolicy := l.opController.GetLeaderSchedulePolicy()
	sort.Slice(targets, func(i, j int) bool {
		kind := core.NewScheduleKind(metapb.ShardKind_LeaderKind, leaderSchedulePolicy)
		iOp := opInfluence.GetStoreInfluence(targets[i].Meta.ID()).ShardProperty(kind, groupKey)
		jOp := opInfluence.GetStoreInfluence(targets[j].Meta.ID()).ShardProperty(kind, groupKey)
		return targets[i].LeaderScore(groupKey, leaderSchedulePolicy, iOp) < targets[j].LeaderScore(groupKey, leaderSchedulePolicy, jOp)
	})
	for _, target := range targets {
		if op := l.createOperator(cluster, resource, source, target); len(op) > 0 {
			return op
		}
	}

	cluster.GetLogger().Debug("no target container for transfer leader out",
		rebalanceLeaderField,
		l.scheduleField,
		sourceField(sourceID),
		resourceField(resource.Meta.ID()))
	schedulerCounter.WithLabelValues(l.GetName(), "no-target-container").Inc()
	return nil
}

// transferLeaderIn transfers leader to the target container.
// It randomly selects a health resource from the target container, then picks
// the worst follower peer and transfers the leader.
func (l *balanceLeaderScheduler) transferLeaderIn(groupKey string, cluster opt.Cluster, target *core.CachedStore) []*operator.Operator {
	targetID := target.Meta.ID()
	resource := cluster.RandFollowerShard(groupKey, targetID, l.conf.groupRanges[util.DecodeGroupKey(groupKey)], opt.HealthShard(cluster))
	if resource == nil {
		cluster.GetLogger().Debug("selected container has no folower, nothing to do",
			rebalanceLeaderField,
			l.scheduleField,
			targetField(targetID))
		schedulerCounter.WithLabelValues(l.GetName(), "no-follower-resource").Inc()
		return nil
	}
	leaderStoreID := resource.GetLeader().GetStoreID()
	source := cluster.GetStore(leaderStoreID)
	if source == nil {
		cluster.GetLogger().Debug("selected random follower resource has no leader, nothing to do",
			rebalanceLeaderField,
			l.scheduleField,
			targetField(leaderStoreID),
			resourceField(resource.Meta.ID()))
		schedulerCounter.WithLabelValues(l.GetName(), "no-leader").Inc()
		return nil
	}
	targets := []*core.CachedStore{
		target,
	}
	finalFilters := l.filters
	if leaderFilter := filter.NewPlacementLeaderSafeguard(l.GetName(), cluster, resource, source,
		l.opController.GetCluster().GetShardFactory()); leaderFilter != nil {
		finalFilters = append(l.filters, leaderFilter)
	}
	targets = filter.SelectTargetStores(targets, finalFilters, cluster.GetOpts())
	if len(targets) < 1 {
		cluster.GetLogger().Debug("selected random follower resource has no target container",
			rebalanceLeaderField,
			l.scheduleField,
			resourceField(resource.Meta.ID()))
		schedulerCounter.WithLabelValues(l.GetName(), "no-target-container").Inc()
		return nil
	}
	return l.createOperator(cluster, resource, source, targets[0])
}

// createOperator creates the operator according to the source and target container.
// If the resource is hot or the difference between the two containers is tolerable, then
// no new operator need to be created, otherwise create an operator that transfers
// the leader from the source container to the target container for the resource.
func (l *balanceLeaderScheduler) createOperator(cluster opt.Cluster, res *core.CachedShard, source, target *core.CachedStore) []*operator.Operator {
	if cluster.IsShardHot(res) {
		cluster.GetLogger().Debug("ignore hot resource",
			rebalanceLeaderField,
			l.scheduleField,
			resourceField(res.Meta.ID()))
		schedulerCounter.WithLabelValues(l.GetName(), "resource-hot").Inc()
		return nil
	}

	sourceID := source.Meta.ID()
	targetID := target.Meta.ID()

	opInfluence := l.opController.GetOpInfluence(cluster)
	kind := core.NewScheduleKind(metapb.ShardKind_LeaderKind, cluster.GetOpts().GetLeaderSchedulePolicy())
	shouldBalance, sourceScore, targetScore := shouldBalance(cluster, source, target, res, kind, opInfluence, l.GetName())
	if !shouldBalance {
		schedulerCounter.WithLabelValues(l.GetName(), "skip").Inc()
		return nil
	}

	op, err := operator.CreateTransferLeaderOperator(BalanceLeaderType, cluster, res, res.GetLeader().GetStoreID(), targetID, operator.OpLeader)
	if err != nil {
		cluster.GetLogger().Debug("fail to create balance leader operator",
			rebalanceLeaderField,
			l.scheduleField,
			zap.Error(err))
		return nil
	}
	sourceLabel := strconv.FormatUint(sourceID, 10)
	targetLabel := strconv.FormatUint(targetID, 10)
	op.Counters = append(op.Counters,
		schedulerCounter.WithLabelValues(l.GetName(), "new-operator"),
		balanceDirectionCounter.WithLabelValues(l.GetName(), sourceLabel, targetLabel),
	)
	op.FinishedCounters = append(op.FinishedCounters,
		l.counter.WithLabelValues("move-leader", sourceLabel+"-out"),
		l.counter.WithLabelValues("move-leader", targetLabel+"-in"),
	)
	op.AdditionalInfos["sourceScore"] = strconv.FormatFloat(sourceScore, 'f', 2, 64)
	op.AdditionalInfos["targetScore"] = strconv.FormatFloat(targetScore, 'f', 2, 64)
	return []*operator.Operator{op}
}
