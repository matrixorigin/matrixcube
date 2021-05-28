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
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
}

type labelScheduler struct {
	*BaseScheduler
	conf *labelSchedulerConfig
}

// LabelScheduler is mainly based on the container's label information for scheduling.
// Now only used for reject leader schedule, that will move the leader out of
// the container with the specific label.
func newLabelScheduler(opController *schedule.OperatorController, conf *labelSchedulerConfig) schedule.Scheduler {
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
	containers := cluster.GetContainers()
	rejectLeaderContainers := make(map[uint64]struct{})
	for _, s := range containers {
		if cluster.GetOpts().CheckLabelProperty(opt.RejectLeader, s.Meta.Labels()) {
			rejectLeaderContainers[s.Meta.ID()] = struct{}{}
		}
	}
	if len(rejectLeaderContainers) == 0 {
		schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
		return nil
	}
	util.GetLogger().Debugf("label scheduler reject leader container list %+v",
		rejectLeaderContainers)
	for id := range rejectLeaderContainers {
		if res := cluster.RandLeaderResource(id, s.conf.Ranges); res != nil {
			util.GetLogger().Debugf("label scheduler selects resource %d to transfer leader",
				res.Meta.ID())
			excludeContainers := make(map[uint64]struct{})
			for _, p := range res.GetDownPeers() {
				excludeContainers[p.GetPeer().ContainerID] = struct{}{}
			}
			for _, p := range res.GetPendingPeers() {
				excludeContainers[p.GetContainerID()] = struct{}{}
			}
			f := filter.NewExcludedFilter(s.GetName(), nil, excludeContainers)

			target := filter.NewCandidates(cluster.GetFollowerContainers(res)).
				FilterTarget(cluster.GetOpts(), &filter.ContainerStateFilter{ActionScope: LabelName, TransferLeader: true}, f).
				RandomPick()
			if target == nil {
				util.GetLogger().Debugf("label scheduler no target found for resource %d",
					res.Meta.ID())
				schedulerCounter.WithLabelValues(s.GetName(), "no-target").Inc()
				continue
			}

			op, err := operator.CreateTransferLeaderOperator("label-reject-leader", cluster, res, id, target.Meta.ID(), operator.OpLeader)
			if err != nil {
				util.GetLogger().Debugf("create transfer label reject leader operator failed with %+v",
					err)
				return nil
			}
			op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
			return []*operator.Operator{op}
		}
	}
	schedulerCounter.WithLabelValues(s.GetName(), "no-resource").Inc()
	return nil
}
