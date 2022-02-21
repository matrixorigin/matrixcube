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
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/filter"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"go.uber.org/zap"
)

const (
	// EvictLeaderName is evict leader scheduler name.
	EvictLeaderName = "evict-leader-scheduler"
	// EvictLeaderType is evict leader scheduler type.
	EvictLeaderType = "evict-leader"
	// EvictLeaderBatchSize is the number of operators to to transfer
	// leaders by one scheduling
	EvictLeaderBatchSize    = 3
	lastStoreDeleteInfo = "The last container has been deleted"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(EvictLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 1 {
				return errors.New("scheduler error configuration")
			}
			conf, ok := v.(*evictLeaderSchedulerConfig)
			if !ok {
				return errors.New("scheduler not found")
			}

			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errors.New("scheduler error configuration")
			}
			ranges, err := getKeyRanges(args[1:])
			if err != nil {
				return err
			}
			conf.StoreIDWithRanges[id] = ranges
			return nil

		}
	})

	schedule.RegisterScheduler(EvictLeaderType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &evictLeaderSchedulerConfig{StoreIDWithRanges: make(map[uint64][]core.KeyRange), storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.cluster = opController.GetCluster()
		return newEvictLeaderScheduler(opController, conf), nil
	})
}

type evictLeaderSchedulerConfig struct {
	mu                         sync.RWMutex
	storage                    storage.Storage
	StoreIDWithRanges      map[uint64][]core.KeyRange `json:"container-id-ranges"`
	cluster                    opt.Cluster
	groupStoreIDWithRanges map[uint64]map[uint64][]core.KeyRange
}

func (conf *evictLeaderSchedulerConfig) BuildWithArgs(args []string) error {
	if len(args) != 1 {
		return errors.New("scheduler error coniguration")
	}

	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return errors.New("scheduler error coniguration")
	}
	ranges, err := getKeyRanges(args[1:])
	if err != nil {
		return err
	}
	conf.mu.Lock()
	defer conf.mu.Unlock()
	conf.StoreIDWithRanges[id] = ranges
	return nil
}

func (conf *evictLeaderSchedulerConfig) Clone() *evictLeaderSchedulerConfig {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return &evictLeaderSchedulerConfig{
		StoreIDWithRanges: conf.StoreIDWithRanges,
	}
}

func (conf *evictLeaderSchedulerConfig) Persist() error {
	name := conf.getSchedulerName()
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(name, data)
}

func (conf *evictLeaderSchedulerConfig) getSchedulerName() string {
	return EvictLeaderName
}

type evictLeaderScheduler struct {
	*BaseScheduler
	conf *evictLeaderSchedulerConfig
}

// newEvictLeaderScheduler creates an admin scheduler that transfers all leaders
// out of a container.
func newEvictLeaderScheduler(opController *schedule.OperatorController, conf *evictLeaderSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	conf.groupStoreIDWithRanges = make(map[uint64]map[uint64][]core.KeyRange)
	for _, group := range conf.cluster.GetOpts().GetReplicationConfig().Groups {
		ms := make(map[uint64][]core.KeyRange)
		for cid, rs := range conf.StoreIDWithRanges {
			ms[cid] = groupKeyRanges(rs, []uint64{group})[group]
		}
		conf.groupStoreIDWithRanges[group] = ms
	}
	return &evictLeaderScheduler{
		BaseScheduler: base,
		conf:          conf,
	}
}

func (s *evictLeaderScheduler) GetName() string {
	return EvictLeaderName
}

func (s *evictLeaderScheduler) GetType() string {
	return EvictLeaderType
}

func (s *evictLeaderScheduler) EncodeConfig() ([]byte, error) {
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	return schedule.EncodeConfig(s.conf)
}

func (s *evictLeaderScheduler) Prepare(cluster opt.Cluster) error {
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	var res error
	for id := range s.conf.StoreIDWithRanges {
		if err := cluster.PauseLeaderTransfer(id); err != nil {
			res = err
		}
	}
	return res
}

func (s *evictLeaderScheduler) Cleanup(cluster opt.Cluster) {
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	for id := range s.conf.StoreIDWithRanges {
		cluster.ResumeLeaderTransfer(id)
	}
}

func (s *evictLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
	}
	return allowed
}

func (s *evictLeaderScheduler) scheduleOnce(cluster opt.Cluster) []*operator.Operator {
	var ops []*operator.Operator
	for group, ms := range s.conf.groupStoreIDWithRanges {
		prefix := util.EncodeGroupKey(group, nil, nil)
		for containerID, ranges := range ms {
			groupKeys := cluster.GetScheduleGroupKeysWithPrefix(prefix)
			for _, groupKey := range groupKeys {
				res := cluster.RandLeaderShard(groupKey, containerID, ranges, opt.HealthShard(cluster))
				if res == nil {
					schedulerCounter.WithLabelValues(s.GetName(), "no-leader").Inc()
					continue
				}
				target := filter.NewCandidates(cluster.GetFollowerStores(res)).
					FilterTarget(cluster.GetOpts(), &filter.StoreStateFilter{ActionScope: EvictLeaderName, TransferLeader: true}).
					RandomPick()
				if target == nil {
					schedulerCounter.WithLabelValues(s.GetName(), "no-target-container").Inc()
					continue
				}
				op, err := operator.CreateTransferLeaderOperator(EvictLeaderType, cluster, res, res.GetLeader().GetStoreID(), target.Meta.ID(), operator.OpLeader)
				if err != nil {
					cluster.GetLogger().Debug("fail to create evict leader operator",
						zap.Error(err))
					continue
				}
				op.SetPriorityLevel(core.HighPriority)
				op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
				ops = append(ops, op)
			}
		}
	}
	return ops
}

func (s *evictLeaderScheduler) uniqueAppend(dst []*operator.Operator, src ...*operator.Operator) []*operator.Operator {
	resIDs := make(map[uint64]struct{})
	for i := range dst {
		resIDs[dst[i].ShardID()] = struct{}{}
	}
	for i := range src {
		if _, ok := resIDs[src[i].ShardID()]; ok {
			continue
		}
		resIDs[src[i].ShardID()] = struct{}{}
		dst = append(dst, src[i])
	}
	return dst
}

func (s *evictLeaderScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	var ops []*operator.Operator
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()

	for i := 0; i < EvictLeaderBatchSize; i++ {
		once := s.scheduleOnce(cluster)
		// no more resources
		if len(once) == 0 {
			break
		}
		ops = s.uniqueAppend(ops, once...)
		// the batch has been fulfilled
		if len(ops) > EvictLeaderBatchSize {
			break
		}
	}

	return ops
}
