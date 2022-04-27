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
	"go.uber.org/zap"
)

const (
	// LabelName is label scheduler name.
	LabelName = "label-scheduler"
	// LabelType is label scheduler type.
	LabelType = "label"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(LabelType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*labelSchedulerConfig)
			if !ok {
				return errors.New("scheduler error configuration")
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = LabelName
			return nil
		}
	})

	schedule.RegisterScheduler(LabelType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &labelSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newLabelScheduler(opController, conf), nil
	})
}

type labelSchedulerConfig struct {
	Name        string                     `json:"name"`
	Ranges      []core.KeyRange            `json:"ranges"`
	groupRanges map[uint64][]core.KeyRange `json:"-"`
}

type labelScheduler struct {
	*BaseScheduler
	conf *labelSchedulerConfig
}

// LabelScheduler is mainly based on the container's label information for scheduling.
// Now only used for reject leader schedule, that will move the leader out of
// the container with the specific label.
func newLabelScheduler(opController *schedule.OperatorController, conf *labelSchedulerConfig) schedule.Scheduler {
	conf.groupRanges = groupKeyRanges(conf.Ranges,
		opController.GetCluster().GetOpts().GetReplicationConfig().Groups)
	return &labelScheduler{
		BaseScheduler: NewBaseScheduler(opController),
		conf:          conf,
	}
}

func (s *labelScheduler) GetName() string {
	return s.conf.Name
}

func (s *labelScheduler) GetType() string {
	return LabelType
}

func (s *labelScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *labelScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
	}
	return allowed
}

func (s *labelScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	containers := cluster.GetStores()
	rejectLeaderStores := make(map[uint64]struct{})
	for _, s := range containers {
		if cluster.GetOpts().CheckLabelProperty(opt.RejectLeader, s.Meta.GetLabels()) {
			rejectLeaderStores[s.Meta.GetID()] = struct{}{}
		}
	}
	if len(rejectLeaderStores) == 0 {
		schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
		return nil
	}
	cluster.GetLogger().Debug("label scheduler reject leader container list",
		zap.Any("reject-containers", rejectLeaderStores))
	for id := range rejectLeaderStores {
		for _, groupKey := range cluster.GetScheduleGroupKeys() {
			if res := cluster.RandLeaderShard(groupKey, id, s.conf.groupRanges[util.DecodeGroupKey(groupKey)]); res != nil {
				cluster.GetLogger().Debug("label scheduler selects resource to transfer leader",
					shardField(res.Meta.GetID()))
				excludeStores := make(map[uint64]struct{})
				for _, p := range res.GetDownPeers() {
					excludeStores[p.GetReplica().StoreID] = struct{}{}
				}
				for _, p := range res.GetPendingPeers() {
					excludeStores[p.GetStoreID()] = struct{}{}
				}
				f := filter.NewExcludedFilter(s.GetName(), nil, excludeStores)

				target := filter.NewCandidates(cluster.GetFollowerStores(res)).
					FilterTarget(cluster.GetOpts(), &filter.StoreStateFilter{ActionScope: LabelName, TransferLeader: true}, f).
					RandomPick()
				if target == nil {
					cluster.GetLogger().Debug("label scheduler no target found for resource",
						shardField(res.Meta.GetID()))
					schedulerCounter.WithLabelValues(s.GetName(), "no-target").Inc()
					continue
				}

				op, err := operator.CreateTransferLeaderOperator("label-reject-leader", cluster, res, id, target.Meta.GetID(), operator.OpLeader)
				if err != nil {
					cluster.GetLogger().Debug("fail to create transfer label reject leader operator",
						zap.Error(err))
					return nil
				}
				op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
				return []*operator.Operator{op}
			}
		}
	}
	schedulerCounter.WithLabelValues(s.GetName(), "no-resource").Inc()
	return nil
}
