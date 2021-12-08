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

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/checker"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/filter"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"go.uber.org/zap"
)

const (
	// RandomMergeName is random merge scheduler name.
	RandomMergeName = "random-merge-scheduler"
	// RandomMergeType is random merge scheduler type.
	RandomMergeType = "random-merge"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(RandomMergeType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*randomMergeSchedulerConfig)
			if !ok {
				return errors.New("scheduler error configuration")
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = RandomMergeName
			return nil
		}
	})
	schedule.RegisterScheduler(RandomMergeType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &randomMergeSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newRandomMergeScheduler(opController, conf), nil
	})
}

type randomMergeSchedulerConfig struct {
	Name        string                     `json:"name"`
	Ranges      []core.KeyRange            `json:"ranges"`
	groupRanges map[uint64][]core.KeyRange `json:"-"`
}

type randomMergeScheduler struct {
	*BaseScheduler
	conf *randomMergeSchedulerConfig
}

// newRandomMergeScheduler creates an admin scheduler that randomly picks two adjacent resources
// then merges them.
func newRandomMergeScheduler(opController *schedule.OperatorController, conf *randomMergeSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	conf.groupRanges = groupKeyRanges(conf.Ranges,
		opController.GetCluster().GetOpts().GetReplicationConfig().Groups)
	return &randomMergeScheduler{
		BaseScheduler: base,
		conf:          conf,
	}
}

func (s *randomMergeScheduler) GetName() string {
	return s.conf.Name
}

func (s *randomMergeScheduler) GetType() string {
	return RandomMergeType
}

func (s *randomMergeScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *randomMergeScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpMerge) < cluster.GetOpts().GetMergeScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpMerge.String()).Inc()
	}
	return allowed
}

func (s *randomMergeScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()

	container := filter.NewCandidates(cluster.GetContainers()).
		FilterSource(cluster.GetOpts(), &filter.ContainerStateFilter{ActionScope: s.conf.Name, MoveResource: true}).
		RandomPick()
	if container == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-source-container").Inc()
		return nil
	}

	for _, group := range cluster.GetOpts().GetReplicationConfig().Groups {
		ops := s.scheduleByGroup(group, container, cluster)
		if len(ops) > 0 {
			return ops
		}
	}
	return nil
}

func (s *randomMergeScheduler) scheduleByGroup(group uint64, container *core.CachedContainer, cluster opt.Cluster) []*operator.Operator {
	res := cluster.RandLeaderResource(group, container.Meta.ID(), s.conf.groupRanges[group], opt.HealthResource(cluster))
	if res == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-resource").Inc()
		return nil
	}

	other, target := cluster.GetAdjacentResources(res)
	if !cluster.GetOpts().IsOneWayMergeEnabled() && ((rand.Int()%2 == 0 && other != nil) || target == nil) {
		target = other
	}
	if target == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-target-container").Inc()
		return nil
	}

	if !s.allowMerge(cluster, res, target) {
		schedulerCounter.WithLabelValues(s.GetName(), "not-allowed").Inc()
		return nil
	}

	ops, err := operator.CreateMergeResourceOperator(RandomMergeType, cluster, res, target, operator.OpAdmin)
	if err != nil {
		cluster.GetLogger().Error("fail to create merge resource operator",
			randomMergeField,
			zap.Error(err))
		return nil
	}
	ops[0].Counters = append(ops[0].Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
	return ops
}

func (s *randomMergeScheduler) allowMerge(cluster opt.Cluster, res, target *core.CachedResource) bool {
	if !opt.IsResourceHealthy(cluster, res) || !opt.IsResourceHealthy(cluster, target) {
		return false
	}
	if !opt.IsResourceReplicated(cluster, res) || !opt.IsResourceReplicated(cluster, target) {
		return false
	}
	if cluster.IsResourceHot(res) || cluster.IsResourceHot(target) {
		return false
	}
	return checker.AllowMerge(cluster, res, target)
}
