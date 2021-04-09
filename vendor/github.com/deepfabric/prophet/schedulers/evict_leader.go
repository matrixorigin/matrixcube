package schedulers

import (
	"errors"
	"strconv"
	"sync"

	"github.com/deepfabric/prophet/core"
	"github.com/deepfabric/prophet/schedule"
	"github.com/deepfabric/prophet/schedule/filter"
	"github.com/deepfabric/prophet/schedule/operator"
	"github.com/deepfabric/prophet/schedule/opt"
	"github.com/deepfabric/prophet/storage"
	"github.com/deepfabric/prophet/util"
)

const (
	// EvictLeaderName is evict leader scheduler name.
	EvictLeaderName = "evict-leader-scheduler"
	// EvictLeaderType is evict leader scheduler type.
	EvictLeaderType = "evict-leader"
	// EvictLeaderBatchSize is the number of operators to to transfer
	// leaders by one scheduling
	EvictLeaderBatchSize    = 3
	lastContainerDeleteInfo = "The last container has been deleted"
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
			conf.ContainerIDWithRanges[id] = ranges
			return nil

		}
	})

	schedule.RegisterScheduler(EvictLeaderType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &evictLeaderSchedulerConfig{ContainerIDWithRanges: make(map[uint64][]core.KeyRange), storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.cluster = opController.GetCluster()
		return newEvictLeaderScheduler(opController, conf), nil
	})
}

type evictLeaderSchedulerConfig struct {
	mu                    sync.RWMutex
	storage               storage.Storage
	ContainerIDWithRanges map[uint64][]core.KeyRange `json:"container-id-ranges"`
	cluster               opt.Cluster
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
	conf.ContainerIDWithRanges[id] = ranges
	return nil
}

func (conf *evictLeaderSchedulerConfig) Clone() *evictLeaderSchedulerConfig {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return &evictLeaderSchedulerConfig{
		ContainerIDWithRanges: conf.ContainerIDWithRanges,
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

func (conf *evictLeaderSchedulerConfig) getRanges(id uint64) []string {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	var res []string
	ranges := conf.ContainerIDWithRanges[id]
	for index := range ranges {
		res = append(res, (string)(ranges[index].StartKey), (string)(ranges[index].EndKey))
	}
	return res
}

func (conf *evictLeaderSchedulerConfig) mayBeRemoveContainerFromConfig(id uint64) (succ bool, last bool) {
	conf.mu.Lock()
	defer conf.mu.Unlock()
	_, exists := conf.ContainerIDWithRanges[id]
	succ, last = false, false
	if exists {
		delete(conf.ContainerIDWithRanges, id)
		conf.cluster.ResumeLeaderTransfer(id)
		succ = true
		last = len(conf.ContainerIDWithRanges) == 0
	}
	return succ, last
}

type evictLeaderScheduler struct {
	*BaseScheduler
	conf *evictLeaderSchedulerConfig
}

// newEvictLeaderScheduler creates an admin scheduler that transfers all leaders
// out of a container.
func newEvictLeaderScheduler(opController *schedule.OperatorController, conf *evictLeaderSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)
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
	for id := range s.conf.ContainerIDWithRanges {
		if err := cluster.PauseLeaderTransfer(id); err != nil {
			res = err
		}
	}
	return res
}

func (s *evictLeaderScheduler) Cleanup(cluster opt.Cluster) {
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	for id := range s.conf.ContainerIDWithRanges {
		cluster.ResumeLeaderTransfer(id)
	}
}

func (s *evictLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
}

func (s *evictLeaderScheduler) scheduleOnce(cluster opt.Cluster) []*operator.Operator {
	var ops []*operator.Operator
	for id, ranges := range s.conf.ContainerIDWithRanges {
		res := cluster.RandLeaderResource(id, ranges, opt.HealthResource(cluster))
		if res == nil {
			schedulerCounter.WithLabelValues(s.GetName(), "no-leader").Inc()
			continue
		}

		target := filter.NewCandidates(cluster.GetFollowerContainers(res)).
			FilterTarget(cluster.GetOpts(), &filter.ContainerStateFilter{ActionScope: EvictLeaderName, TransferLeader: true}).
			RandomPick()
		if target == nil {
			schedulerCounter.WithLabelValues(s.GetName(), "no-target-container").Inc()
			continue
		}
		op, err := operator.CreateTransferLeaderOperator(EvictLeaderType, cluster, res, res.GetLeader().GetContainerID(), target.Meta.ID(), operator.OpLeader)
		if err != nil {
			util.GetLogger().Debugf("create evict leader operator failed with %+v",
				err)
			continue
		}
		op.SetPriorityLevel(core.HighPriority)
		op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
		ops = append(ops, op)
	}
	return ops
}

func (s *evictLeaderScheduler) uniqueAppend(dst []*operator.Operator, src ...*operator.Operator) []*operator.Operator {
	resIDs := make(map[uint64]struct{})
	for i := range dst {
		resIDs[dst[i].ResourceID()] = struct{}{}
	}
	for i := range src {
		if _, ok := resIDs[src[i].ResourceID()]; ok {
			continue
		}
		resIDs[src[i].ResourceID()] = struct{}{}
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
