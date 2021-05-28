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
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
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
	res := cluster.RandFollowerResource(targetContainer.Meta.ID(), s.conf.Ranges, opt.HealthResource(cluster))
	if res == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-follower").Inc()
		return nil
	}
	op, err := operator.CreateTransferLeaderOperator(ShuffleLeaderType, cluster, res, res.GetLeader().GetContainerID(), targetContainer.Meta.ID(), operator.OpAdmin)
	if err != nil {
		util.GetLogger().Debugf("create shuffle leader operator failed with %+v",
			err)
		return nil
	}
	op.SetPriorityLevel(core.HighPriority)
	op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
	return []*operator.Operator{op}
}
