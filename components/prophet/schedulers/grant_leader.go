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
	"strconv"
	"sync"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
)

const (
	// GrantLeaderName is grant leader scheduler name.
	GrantLeaderName = "grant-leader-scheduler"
	// GrantLeaderType is grant leader scheduler type.
	GrantLeaderType = "grant-leader"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(GrantLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 1 {
				return errors.New("scheduler error configuration")
			}

			conf, ok := v.(*grantLeaderSchedulerConfig)
			if !ok {
				return errors.New("scheduler error configuration")
			}

			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return err
			}
			ranges, err := getKeyRanges(args[1:])
			if err != nil {
				return err
			}
			conf.ContainerIDWithRanges[id] = ranges
			return nil
		}
	})

	schedule.RegisterScheduler(GrantLeaderType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &grantLeaderSchedulerConfig{ContainerIDWithRanges: make(map[uint64][]core.KeyRange), storage: storage}
		conf.cluster = opController.GetCluster()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newGrantLeaderScheduler(opController, conf), nil
	})
}

type grantLeaderSchedulerConfig struct {
	mu                    sync.RWMutex
	storage               storage.Storage
	ContainerIDWithRanges map[uint64][]core.KeyRange `json:"container-id-ranges"`
	cluster               opt.Cluster
}

func (conf *grantLeaderSchedulerConfig) BuildWithArgs(args []string) error {
	if len(args) != 1 {
		return errors.New("scheduler error coniguration")
	}

	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return err
	}
	ranges, err := getKeyRanges(args[1:])
	if err != nil {
		return err
	}
	conf.mu.Lock()
	defer conf.mu.Unlock()
	conf.ContainerIDWithRanges[id] = ranges
	return nil
}

func (conf *grantLeaderSchedulerConfig) Clone() *grantLeaderSchedulerConfig {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return &grantLeaderSchedulerConfig{
		ContainerIDWithRanges: conf.ContainerIDWithRanges,
	}
}

func (conf *grantLeaderSchedulerConfig) Persist() error {
	name := conf.getSchedulerName()
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(name, data)
}

func (conf *grantLeaderSchedulerConfig) getSchedulerName() string {
	return GrantLeaderName
}

// grantLeaderScheduler transfers all leaders to peers in the container.
type grantLeaderScheduler struct {
	*BaseScheduler
	conf *grantLeaderSchedulerConfig
}

// newGrantLeaderScheduler creates an admin scheduler that transfers all leaders
// to a container.
func newGrantLeaderScheduler(opController *schedule.OperatorController, conf *grantLeaderSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	return &grantLeaderScheduler{
		BaseScheduler: base,
		conf:          conf,
	}
}

func (s *grantLeaderScheduler) GetName() string {
	return GrantLeaderName
}

func (s *grantLeaderScheduler) GetType() string {
	return GrantLeaderType
}

func (s *grantLeaderScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *grantLeaderScheduler) Prepare(cluster opt.Cluster) error {
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	var res error
	for id := range s.conf.ContainerIDWithRanges {
		if err := cluster.PauseLeaderTransfer(id); err != nil {
			res = err
		}
	}
	return res
}

func (s *grantLeaderScheduler) Cleanup(cluster opt.Cluster) {
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	for id := range s.conf.ContainerIDWithRanges {
		cluster.ResumeLeaderTransfer(id)
	}
}

func (s *grantLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
	}
	return allowed
}

func (s *grantLeaderScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	var ops []*operator.Operator
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	for id, ranges := range s.conf.ContainerIDWithRanges {
		res := cluster.RandFollowerResource(id, ranges, opt.HealthResource(cluster))
		if res == nil {
			schedulerCounter.WithLabelValues(s.GetName(), "no-follower").Inc()
			continue
		}

		op, err := operator.CreateForceTransferLeaderOperator(GrantLeaderType, cluster, res, res.GetLeader().GetContainerID(), id, operator.OpLeader)
		if err != nil {
			util.GetLogger().Debugf("create grant leader operator failed with %+v", err)
			continue
		}
		op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
		op.SetPriorityLevel(core.HighPriority)
		ops = append(ops, op)
	}

	return ops
}
