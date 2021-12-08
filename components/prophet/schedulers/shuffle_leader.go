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
	"go.uber.org/zap"
)

const (
	// ShuffleLeaderName is shuffle leader scheduler name.
	ShuffleLeaderName = "shuffle-leader-scheduler"
	// ShuffleLeaderType is shuffle leader scheduler type.
	ShuffleLeaderType = "shuffle-leader"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(ShuffleLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*shuffleLeaderSchedulerConfig)
			if !ok {
				return errors.New("scheduler error configuration")
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = ShuffleLeaderName
			return nil
		}
	})

	schedule.RegisterScheduler(ShuffleLeaderType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleLeaderSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleLeaderScheduler(opController, conf), nil
	})
}

type shuffleLeaderSchedulerConfig struct {
	Name        string                     `json:"name"`
	Ranges      []core.KeyRange            `json:"ranges"`
	groupRanges map[uint64][]core.KeyRange `json:"-"`
}

type shuffleLeaderScheduler struct {
	*BaseScheduler
	conf    *shuffleLeaderSchedulerConfig
	filters []filter.Filter
}

// newShuffleLeaderScheduler creates an admin scheduler that shuffles leaders
// between containers.
func newShuffleLeaderScheduler(opController *schedule.OperatorController, conf *shuffleLeaderSchedulerConfig) schedule.Scheduler {
	filters := []filter.Filter{
		&filter.ContainerStateFilter{ActionScope: conf.Name, TransferLeader: true},
		filter.NewSpecialUseFilter(conf.Name),
	}
	base := NewBaseScheduler(opController)
	conf.groupRanges = groupKeyRanges(conf.Ranges,
		opController.GetCluster().GetOpts().GetReplicationConfig().Groups)
	return &shuffleLeaderScheduler{
		BaseScheduler: base,
		conf:          conf,
		filters:       filters,
	}
}

func (s *shuffleLeaderScheduler) GetName() string {
	return s.conf.Name
}

func (s *shuffleLeaderScheduler) GetType() string {
	return ShuffleLeaderType
}

func (s *shuffleLeaderScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *shuffleLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
	}
	return allowed
}

func (s *shuffleLeaderScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	// We shuffle leaders between containers by:
	// 1. random select a valid container.
	// 2. transfer a leader to the container.
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	targetContainer := filter.NewCandidates(cluster.GetContainers()).
		FilterTarget(cluster.GetOpts(), s.filters...).
		RandomPick()
	if targetContainer == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-target-container").Inc()
		return nil
	}

	for _, group := range cluster.GetOpts().GetReplicationConfig().Groups {
		ops := s.scheduleByGroup(group, targetContainer, cluster)
		if len(ops) > 0 {
			return ops
		}
	}
	return nil
}

func (s *shuffleLeaderScheduler) scheduleByGroup(group uint64, targetContainer *core.CachedContainer, cluster opt.Cluster) []*operator.Operator {
	res := cluster.RandFollowerResource(group, targetContainer.Meta.ID(), s.conf.groupRanges[group], opt.HealthResource(cluster))
	if res == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-follower").Inc()
		return nil
	}
	op, err := operator.CreateTransferLeaderOperator(ShuffleLeaderType, cluster, res, res.GetLeader().GetContainerID(), targetContainer.Meta.ID(), operator.OpAdmin)
	if err != nil {
		cluster.GetLogger().Error("fail to create shuffle leader operator",
			shuffleLeaderField,
			zap.Error(err))
		return nil
	}
	op.SetPriorityLevel(core.HighPriority)
	op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
	return []*operator.Operator{op}
}
