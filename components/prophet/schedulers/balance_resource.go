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
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/filter"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(BalanceResourceType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*balanceResourceSchedulerConfig)
			if !ok {
				return errors.New("scheduler not found")
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = BalanceResourceName
			return nil
		}
	})
	schedule.RegisterScheduler(BalanceResourceType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceResourceSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newBalanceResourceScheduler(opController, conf), nil
	})
}

const (
	// balanceResourceRetryLimit is the limit to retry schedule for selected container.
	balanceResourceRetryLimit = 10
	// BalanceResourceName is balance resource scheduler name.
	BalanceResourceName = "balance-resource-scheduler"
	// BalanceResourceType is balance resource scheduler type.
	BalanceResourceType = "balance-resource"
)

type balanceResourceSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
}

type balanceResourceScheduler struct {
	*BaseScheduler
	conf         *balanceResourceSchedulerConfig
	opController *schedule.OperatorController
	filters      []filter.Filter
	counter      *prometheus.CounterVec

	scheduleField zap.Field
}

// newBalanceResourceScheduler creates a scheduler that tends to keep resources on
// each container balanced.
func newBalanceResourceScheduler(opController *schedule.OperatorController, conf *balanceResourceSchedulerConfig, opts ...BalanceResourceCreateOption) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	scheduler := &balanceResourceScheduler{
		BaseScheduler: base,
		conf:          conf,
		opController:  opController,
		counter:       balanceResourceCounter,
	}
	for _, setOption := range opts {
		setOption(scheduler)
	}
	scheduler.filters = []filter.Filter{
		&filter.ContainerStateFilter{ActionScope: scheduler.GetName(), MoveResource: true},
		filter.NewSpecialUseFilter(scheduler.GetName()),
	}
	scheduler.scheduleField = zap.String("scheduler", scheduler.GetName())
	return scheduler
}

// BalanceResourceCreateOption is used to create a scheduler with an option.
type BalanceResourceCreateOption func(s *balanceResourceScheduler)

// WithBalanceResourceCounter sets the counter for the scheduler.
func WithBalanceResourceCounter(counter *prometheus.CounterVec) BalanceResourceCreateOption {
	return func(s *balanceResourceScheduler) {
		s.counter = counter
	}
}

// WithBalanceResourceName sets the name for the scheduler.
func WithBalanceResourceName(name string) BalanceResourceCreateOption {
	return func(s *balanceResourceScheduler) {
		s.conf.Name = name
	}
}

func (s *balanceResourceScheduler) GetName() string {
	return s.conf.Name
}

func (s *balanceResourceScheduler) GetType() string {
	return BalanceResourceType
}

func (s *balanceResourceScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *balanceResourceScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	allowed := s.opController.OperatorCount(operator.OpResource)-s.opController.OperatorCount(operator.OpMerge) < cluster.GetOpts().GetResourceScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpResource.String()).Inc()
	}
	return allowed
}

func (s *balanceResourceScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	containers := cluster.GetContainers()
	opts := cluster.GetOpts()
	containers = filter.SelectSourceContainers(containers, s.filters, opts)
	opInfluence := s.opController.GetOpInfluence(cluster)
	kind := core.NewScheduleKind(metapb.ResourceKind_ReplicaKind, core.BySize)

	for _, group := range cluster.GetOpts().GetReplicationConfig().Groups {
		sort.Slice(containers, func(i, j int) bool {
			iOp := opInfluence.GetContainerInfluence(containers[i].Meta.ID()).ResourceProperty(kind)
			jOp := opInfluence.GetContainerInfluence(containers[j].Meta.ID()).ResourceProperty(kind)
			return containers[i].ResourceScore(group, opts.GetResourceScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), iOp, -1) >
				containers[j].ResourceScore(group, opts.GetResourceScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), jOp, -1)
		})
		for _, source := range containers {
			sourceID := source.Meta.ID()

			for i := 0; i < balanceResourceRetryLimit; i++ {
				// Priority pick the Resource that has a pending peer.
				// Pending Resource may means the disk is overload, remove the pending Resource firstly.
				res := cluster.RandPendingResource(sourceID, s.conf.Ranges, opt.HealthAllowPending(cluster), opt.ReplicatedResource(cluster), opt.AllowBalanceEmptyResource(cluster))
				if res == nil {
					// Then pick the Resource that has a follower in the source store.
					res = cluster.RandFollowerResource(sourceID, s.conf.Ranges, opt.HealthResource(cluster), opt.ReplicatedResource(cluster), opt.AllowBalanceEmptyResource(cluster))
				}
				if res == nil {
					// Then pick the Resource has the leader in the source store.
					res = cluster.RandLeaderResource(sourceID, s.conf.Ranges, opt.HealthResource(cluster), opt.ReplicatedResource(cluster), opt.AllowBalanceEmptyResource(cluster))
				}
				if res == nil {
					// Finally pick learner.
					res = cluster.RandLearnerResource(sourceID, s.conf.Ranges, opt.HealthResource(cluster), opt.ReplicatedResource(cluster), opt.AllowBalanceEmptyResource(cluster))
				}
				if res == nil {
					schedulerCounter.WithLabelValues(s.GetName(), "no-Resource").Inc()
					continue
				}
				cluster.GetLogger().Debug("scheduler select resource",
					rebalanceResourceField,
					s.scheduleField,
					resourceField(res.Meta.ID()))

				// Skip hot resources.
				if cluster.IsResourceHot(res) {
					cluster.GetLogger().Debug("skip hot resource",
						rebalanceResourceField,
						s.scheduleField,
						resourceField(res.Meta.ID()))
					schedulerCounter.WithLabelValues(s.GetName(), "resource-hot").Inc()
					continue
				}
				// Check resource whether have leader
				if res.GetLeader() == nil {
					cluster.GetLogger().Debug("resource missing leader",
						rebalanceResourceField,
						s.scheduleField,
						resourceField(res.Meta.ID()))
					schedulerCounter.WithLabelValues(s.GetName(), "no-leader").Inc()
					continue
				}

				oldPeer, _ := res.GetContainerPeer(sourceID)
				if op := s.transferPeer(group, cluster, res, oldPeer); op != nil {
					op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
					return []*operator.Operator{op}
				}
			}
		}
	}
	return nil
}

// transferPeer selects the best container to create a new peer to replace the old peer.
func (s *balanceResourceScheduler) transferPeer(group uint64, cluster opt.Cluster, res *core.CachedResource, oldPeer metapb.Replica) *operator.Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	sourceContainerID := oldPeer.GetContainerID()
	source := cluster.GetContainer(sourceContainerID)
	if source == nil {
		cluster.GetLogger().Debug("source container not found",
			rebalanceResourceField,
			s.scheduleField,
			zap.Uint64("container", sourceContainerID))

		return nil
	}

	filters := []filter.Filter{
		filter.NewExcludedFilter(s.GetName(), nil, res.GetContainerIDs()),
		filter.NewPlacementSafeguard(s.GetName(), cluster, res, source, s.opController.GetCluster().GetResourceFactory()),
		filter.NewSpecialUseFilter(s.GetName()),
		&filter.ContainerStateFilter{ActionScope: s.GetName(), MoveResource: true},
	}

	candidates := filter.NewCandidates(cluster.GetContainers()).
		FilterTarget(cluster.GetOpts(), filters...).
		Sort(filter.ResourceScoreComparer(group, cluster.GetOpts()))

	for _, target := range candidates.Containers {
		resID := res.Meta.ID()
		sourceID := source.Meta.ID()
		targetID := target.Meta.ID()
		cluster.GetLogger().Debug("check resource should balance",
			rebalanceResourceField,
			s.scheduleField,
			resourceField(resID),
			sourceField(sourceID),
			targetField(targetID))

		opInfluence := s.opController.GetOpInfluence(cluster)
		kind := core.NewScheduleKind(metapb.ResourceKind_ReplicaKind, core.BySize)
		shouldBalance, sourceScore, targetScore := shouldBalance(cluster, source, target, res, kind, opInfluence, s.GetName())
		if !shouldBalance {
			schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
			continue
		}

		newPeer := metapb.Replica{ContainerID: target.Meta.ID(), Role: oldPeer.Role}
		op, err := operator.CreateMovePeerOperator(BalanceResourceType, cluster, res, operator.OpResource, oldPeer.GetContainerID(), newPeer)
		if err != nil {
			cluster.GetLogger().Error("fail to create move peer operator",
				rebalanceResourceField,
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
