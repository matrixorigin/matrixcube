package schedulers

import (
	"errors"
	"math/rand"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/filter"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
)

const (
	// ShuffleHotResourceName is shuffle hot resource scheduler name.
	ShuffleHotResourceName = "shuffle-hot-resource-scheduler"
	// ShuffleHotResourceType is shuffle hot resource scheduler type.
	ShuffleHotResourceType = "shuffle-hot-resource"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(ShuffleHotResourceType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*shuffleHotResourceSchedulerConfig)
			if !ok {
				return errors.New("scheduler error configuration")
			}
			conf.Limit = uint64(1)
			if len(args) == 1 {
				limit, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					return err
				}
				conf.Limit = limit
			}
			conf.Name = ShuffleHotResourceName
			return nil
		}
	})

	schedule.RegisterScheduler(ShuffleHotResourceType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleHotResourceSchedulerConfig{Limit: uint64(1)}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleHotResourceScheduler(opController, conf), nil
	})
}

type shuffleHotResourceSchedulerConfig struct {
	Name  string `json:"name"`
	Limit uint64 `json:"limit"`
}

// ShuffleHotResourceScheduler mainly used to test.
// It will randomly pick a hot peer, and move the peer
// to a random container, and then transfer the leader to
// the hot peer.
type shuffleHotResourceScheduler struct {
	*BaseScheduler
	stLoadInfos [resourceTypeLen]map[uint64]*containerLoadDetail
	r           *rand.Rand
	conf        *shuffleHotResourceSchedulerConfig
	types       []rwType
}

// newShuffleHotResourceScheduler creates an admin scheduler that random balance hot resources
func newShuffleHotResourceScheduler(opController *schedule.OperatorController, conf *shuffleHotResourceSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	ret := &shuffleHotResourceScheduler{
		BaseScheduler: base,
		conf:          conf,
		types:         []rwType{read, write},
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.stLoadInfos[ty] = map[uint64]*containerLoadDetail{}
	}
	return ret
}

func (s *shuffleHotResourceScheduler) GetName() string {
	return s.conf.Name
}

func (s *shuffleHotResourceScheduler) GetType() string {
	return ShuffleHotResourceType
}

func (s *shuffleHotResourceScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *shuffleHotResourceScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.OpController.OperatorCount(operator.OpHotResource) < s.conf.Limit &&
		s.OpController.OperatorCount(operator.OpResource) < cluster.GetOpts().GetResourceScheduleLimit() &&
		s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
}

func (s *shuffleHotResourceScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	i := s.r.Int() % len(s.types)
	return s.dispatch(s.types[i], cluster)
}

func (s *shuffleHotResourceScheduler) dispatch(typ rwType, cluster opt.Cluster) []*operator.Operator {
	containersStats := cluster.GetContainersStats()
	minHotDegree := cluster.GetOpts().GetHotResourceCacheHitsThreshold()
	switch typ {
	case read:
		s.stLoadInfos[readLeader] = summaryContainersLoad(
			containersStats.GetContainersBytesReadStat(),
			containersStats.GetContainersKeysReadStat(),
			map[uint64]Influence{},
			cluster.ResourceReadStats(),
			minHotDegree,
			read, metapb.ResourceKind_LeaderKind)
		return s.randomSchedule(cluster, s.stLoadInfos[readLeader])
	case write:
		s.stLoadInfos[writeLeader] = summaryContainersLoad(
			containersStats.GetContainersBytesWriteStat(),
			containersStats.GetContainersKeysWriteStat(),
			map[uint64]Influence{},
			cluster.ResourceWriteStats(),
			minHotDegree,
			write, metapb.ResourceKind_LeaderKind)
		return s.randomSchedule(cluster, s.stLoadInfos[writeLeader])
	}
	return nil
}

func (s *shuffleHotResourceScheduler) randomSchedule(cluster opt.Cluster, loadDetail map[uint64]*containerLoadDetail) []*operator.Operator {
	for _, detail := range loadDetail {
		if len(detail.HotPeers) < 1 {
			continue
		}
		i := s.r.Intn(len(detail.HotPeers))
		r := detail.HotPeers[i]
		// select src resource
		srcResource := cluster.GetResource(r.ResourceID)
		if srcResource == nil || len(srcResource.GetDownPeers()) != 0 || len(srcResource.GetPendingPeers()) != 0 {
			continue
		}
		srcContainerID := srcResource.GetLeader().GetContainerID()
		srcContainer := cluster.GetContainer(srcContainerID)
		if srcContainer == nil {
			util.GetLogger().Errorf("get the source container %d failed with not found",
				srcContainerID)
		}

		filters := []filter.Filter{
			&filter.ContainerStateFilter{ActionScope: s.GetName(), MoveResource: true},
			filter.NewExcludedFilter(s.GetName(), srcResource.GetContainerIDs(), srcResource.GetContainerIDs()),
			filter.NewPlacementSafeguard(s.GetName(), cluster, srcResource, srcContainer, s.OpController.GetCluster().GetResourceFactory()),
		}
		containers := cluster.GetContainers()
		destContainerIDs := make([]uint64, 0, len(containers))
		for _, container := range containers {
			if !filter.Target(cluster.GetOpts(), container, filters) {
				continue
			}
			destContainerIDs = append(destContainerIDs, container.Meta.ID())
		}
		if len(destContainerIDs) == 0 {
			return nil
		}
		// random pick a dest container
		destContainerID := destContainerIDs[s.r.Intn(len(destContainerIDs))]
		if destContainerID == 0 {
			return nil
		}
		if _, ok := srcResource.GetContainerPeer(srcContainerID); !ok {
			return nil
		}
		destPeer := metapb.Peer{ContainerID: destContainerID}
		op, err := operator.CreateMoveLeaderOperator("random-move-hot-leader", cluster, srcResource, operator.OpResource|operator.OpLeader, srcContainerID, destPeer)
		if err != nil {
			util.GetLogger().Debugf("create move leader operator failed with %+v",
				err)
			return nil
		}
		op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
		return []*operator.Operator{op}
	}
	schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
	return nil
}
