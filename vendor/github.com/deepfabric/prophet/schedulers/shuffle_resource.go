package schedulers

import (
	"errors"

	"github.com/deepfabric/prophet/core"
	"github.com/deepfabric/prophet/pb/metapb"
	"github.com/deepfabric/prophet/schedule"
	"github.com/deepfabric/prophet/schedule/filter"
	"github.com/deepfabric/prophet/schedule/operator"
	"github.com/deepfabric/prophet/schedule/opt"
	"github.com/deepfabric/prophet/storage"
)

const (
	// ShuffleResourceName is shuffle resource scheduler name.
	ShuffleResourceName = "shuffle-resource-scheduler"
	// ShuffleResourceType is shuffle resource scheduler type.
	ShuffleResourceType = "shuffle-resource"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(ShuffleResourceType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*shuffleResourceSchedulerConfig)
			if !ok {
				return errors.New("scheduler error configuration")
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Roles = allRoles
			return nil
		}
	})
	schedule.RegisterScheduler(ShuffleResourceType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleResourceSchedulerConfig{storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleResourceScheduler(opController, conf), nil
	})
}

type shuffleResourceScheduler struct {
	*BaseScheduler
	conf    *shuffleResourceSchedulerConfig
	filters []filter.Filter
}

// newShuffleResourceScheduler creates an admin scheduler that shuffles resources
// between containers.
func newShuffleResourceScheduler(opController *schedule.OperatorController, conf *shuffleResourceSchedulerConfig) schedule.Scheduler {
	filters := []filter.Filter{
		&filter.ContainerStateFilter{ActionScope: ShuffleResourceName, MoveResource: true},
		filter.NewSpecialUseFilter(ShuffleResourceName),
	}
	base := NewBaseScheduler(opController)
	return &shuffleResourceScheduler{
		BaseScheduler: base,
		conf:          conf,
		filters:       filters,
	}
}

func (s *shuffleResourceScheduler) GetName() string {
	return ShuffleResourceName
}

func (s *shuffleResourceScheduler) GetType() string {
	return ShuffleResourceType
}

func (s *shuffleResourceScheduler) EncodeConfig() ([]byte, error) {
	return s.conf.EncodeConfig()
}

func (s *shuffleResourceScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.OpController.OperatorCount(operator.OpResource) < cluster.GetOpts().GetResourceScheduleLimit()
}

func (s *shuffleResourceScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	res, oldPeer := s.scheduleRemovePeer(cluster)
	if res == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-resource").Inc()
		return nil
	}

	newPeer, ok := s.scheduleAddPeer(cluster, res, oldPeer)
	if !ok {
		schedulerCounter.WithLabelValues(s.GetName(), "no-new-peer").Inc()
		return nil
	}

	op, err := operator.CreateMovePeerOperator(ShuffleResourceType, cluster, res, operator.OpAdmin, oldPeer.GetContainerID(), newPeer)
	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "create-operator-fail").Inc()
		return nil
	}
	op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
	op.SetPriorityLevel(core.HighPriority)
	return []*operator.Operator{op}
}

func (s *shuffleResourceScheduler) scheduleRemovePeer(cluster opt.Cluster) (*core.CachedResource, metapb.Peer) {
	candidates := filter.NewCandidates(cluster.GetContainers()).
		FilterSource(cluster.GetOpts(), s.filters...).
		Shuffle()

	for _, source := range candidates.Containers {
		var res *core.CachedResource
		if s.conf.IsRoleAllow(roleFollower) {
			res = cluster.RandFollowerResource(source.Meta.ID(), s.conf.GetRanges(), opt.HealthResource(cluster), opt.ReplicatedResource(cluster))
		}
		if res == nil && s.conf.IsRoleAllow(roleLeader) {
			res = cluster.RandLeaderResource(source.Meta.ID(), s.conf.GetRanges(), opt.HealthResource(cluster), opt.ReplicatedResource(cluster))
		}
		if res == nil && s.conf.IsRoleAllow(roleLearner) {
			res = cluster.RandLearnerResource(source.Meta.ID(), s.conf.GetRanges(), opt.HealthResource(cluster), opt.ReplicatedResource(cluster))
		}
		if res != nil {
			if p, ok := res.GetContainerPeer(source.Meta.ID()); ok {
				return res, p
			}

			return nil, metapb.Peer{}
		}
		schedulerCounter.WithLabelValues(s.GetName(), "no-resource").Inc()
	}

	schedulerCounter.WithLabelValues(s.GetName(), "no-source-container").Inc()
	return nil, metapb.Peer{}
}

func (s *shuffleResourceScheduler) scheduleAddPeer(cluster opt.Cluster, res *core.CachedResource, oldPeer metapb.Peer) (metapb.Peer, bool) {
	scoreGuard := filter.NewPlacementSafeguard(s.GetName(), cluster, res, cluster.GetContainer(oldPeer.ContainerID), s.OpController.GetCluster().GetResourceFactory())
	excludedFilter := filter.NewExcludedFilter(s.GetName(), nil, res.GetContainerIDs())

	target := filter.NewCandidates(cluster.GetContainers()).
		FilterTarget(cluster.GetOpts(), s.filters...).
		FilterTarget(cluster.GetOpts(), scoreGuard, excludedFilter).
		RandomPick()
	if target == nil {
		return metapb.Peer{}, false
	}
	return metapb.Peer{ContainerID: target.Meta.ID(), Role: oldPeer.GetRole()}, true
}
