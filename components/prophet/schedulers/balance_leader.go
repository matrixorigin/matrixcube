package schedulers

import (
	"errors"
	"sort"
	"strconv"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/filter"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// BalanceLeaderName is balance leader scheduler name.
	BalanceLeaderName = "balance-leader-scheduler"
	// BalanceLeaderType is balance leader scheduler type.
	BalanceLeaderType = "balance-leader"
	// balanceLeaderRetryLimit is the limit to retry schedule for selected source container and target container.
	balanceLeaderRetryLimit = 10
)

func init() {
	schedule.RegisterSliceDecoderBuilder(BalanceLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*balanceLeaderSchedulerConfig)
			if !ok {
				return errors.New("scheduler not found")
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = BalanceLeaderName
			return nil
		}
	})

	schedule.RegisterScheduler(BalanceLeaderType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceLeaderSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newBalanceLeaderScheduler(opController, conf), nil
	})
}

type balanceLeaderSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
}

type balanceLeaderScheduler struct {
	*BaseScheduler
	conf         *balanceLeaderSchedulerConfig
	opController *schedule.OperatorController
	filters      []filter.Filter
	counter      *prometheus.CounterVec
}

// newBalanceLeaderScheduler creates a scheduler that tends to keep leaders on
// each container balanced.
func newBalanceLeaderScheduler(opController *schedule.OperatorController, conf *balanceLeaderSchedulerConfig, options ...BalanceLeaderCreateOption) schedule.Scheduler {
	base := NewBaseScheduler(opController)

	s := &balanceLeaderScheduler{
		BaseScheduler: base,
		conf:          conf,
		opController:  opController,
		counter:       balanceLeaderCounter,
	}
	for _, option := range options {
		option(s)
	}
	s.filters = []filter.Filter{
		&filter.ContainerStateFilter{ActionScope: s.GetName(), TransferLeader: true},
		filter.NewSpecialUseFilter(s.GetName()),
	}
	return s
}

// BalanceLeaderCreateOption is used to create a scheduler with an option.
type BalanceLeaderCreateOption func(s *balanceLeaderScheduler)

// WithBalanceLeaderCounter sets the counter for the scheduler.
func WithBalanceLeaderCounter(counter *prometheus.CounterVec) BalanceLeaderCreateOption {
	return func(s *balanceLeaderScheduler) {
		s.counter = counter
	}
}

// WithBalanceLeaderName sets the name for the scheduler.
func WithBalanceLeaderName(name string) BalanceLeaderCreateOption {
	return func(s *balanceLeaderScheduler) {
		s.conf.Name = name
	}
}

func (l *balanceLeaderScheduler) GetName() string {
	return l.conf.Name
}

func (l *balanceLeaderScheduler) GetType() string {
	return BalanceLeaderType
}

func (l *balanceLeaderScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(l.conf)
}

func (l *balanceLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return l.opController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
}

func (l *balanceLeaderScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()

	leaderSchedulePolicy := l.opController.GetLeaderSchedulePolicy()
	containers := cluster.GetContainers()
	sources := filter.SelectSourceContainers(containers, l.filters, cluster.GetOpts())
	targets := filter.SelectTargetContainers(containers, l.filters, cluster.GetOpts())
	opInfluence := l.opController.GetOpInfluence(cluster)
	kind := core.NewScheduleKind(metapb.ResourceKind_LeaderKind, leaderSchedulePolicy)
	sort.Slice(sources, func(i, j int) bool {
		iOp := opInfluence.GetContainerInfluence(sources[i].Meta.ID()).ResourceProperty(kind)
		jOp := opInfluence.GetContainerInfluence(sources[j].Meta.ID()).ResourceProperty(kind)
		return sources[i].LeaderScore(leaderSchedulePolicy, iOp) >
			sources[j].LeaderScore(leaderSchedulePolicy, jOp)
	})
	sort.Slice(targets, func(i, j int) bool {
		iOp := opInfluence.GetContainerInfluence(targets[i].Meta.ID()).ResourceProperty(kind)
		jOp := opInfluence.GetContainerInfluence(targets[j].Meta.ID()).ResourceProperty(kind)
		return targets[i].LeaderScore(leaderSchedulePolicy, iOp) <
			targets[j].LeaderScore(leaderSchedulePolicy, jOp)
	})

	for i := 0; i < len(sources) || i < len(targets); i++ {
		if i < len(sources) {
			source := sources[i]
			sourceID := source.Meta.ID()
			util.GetLogger().Debugf("container leader score, scheduler %s, source container %d",
				l.GetName(),
				sourceID)
			sourceContainerLabel := strconv.FormatUint(sourceID, 10)
			l.counter.WithLabelValues("high-score", sourceContainerLabel).Inc()
			for j := 0; j < balanceLeaderRetryLimit; j++ {
				if ops := l.transferLeaderOut(cluster, source, opInfluence); len(ops) > 0 {
					ops[0].Counters = append(ops[0].Counters, l.counter.WithLabelValues("transfer-out", sourceContainerLabel))
					return ops
				}
			}
			// util.GetLogger().Debugf("no operator created for selected containers, scheduler %s, source container %d",
			// 	l.GetName(),
			// 	sourceID)
		}
		if i < len(targets) {
			target := targets[i]
			targetID := target.Meta.ID()
			// util.GetLogger().Debugf("container leader score, scheduler %s, target container %d",
			// 	l.GetName(),
			// 	targetID)
			targetContainerLabel := strconv.FormatUint(targetID, 10)
			l.counter.WithLabelValues("low-score", targetContainerLabel).Inc()

			for j := 0; j < balanceLeaderRetryLimit; j++ {
				if ops := l.transferLeaderIn(cluster, target); len(ops) > 0 {
					ops[0].Counters = append(ops[0].Counters, l.counter.WithLabelValues("transfer-in", targetContainerLabel))
					return ops
				}
			}
			// util.GetLogger().Debugf("no operator created for selected containers, scheduler %s, target container %d",
			// 	l.GetName(),
			// 	targetID)
		}
	}
	return nil
}

// transferLeaderOut transfers leader from the source container.
// It randomly selects a health resource from the source container, then picks
// the best follower peer and transfers the leader.
func (l *balanceLeaderScheduler) transferLeaderOut(cluster opt.Cluster, source *core.CachedContainer, opInfluence operator.OpInfluence) []*operator.Operator {
	sourceID := source.Meta.ID()
	resource := cluster.RandLeaderResource(sourceID, l.conf.Ranges, opt.HealthResource(cluster))
	if resource == nil {
		// util.GetLogger().Debugf("container %d has no leader, scheduler %s",
		// 	sourceID,
		// 	l.GetName())
		schedulerCounter.WithLabelValues(l.GetName(), "no-leader-resource").Inc()
		return nil
	}
	targets := cluster.GetFollowerContainers(resource)
	finalFilters := l.filters
	if leaderFilter := filter.NewPlacementLeaderSafeguard(l.GetName(), cluster, resource, source,
		l.opController.GetCluster().GetResourceFactory()); leaderFilter != nil {
		finalFilters = append(l.filters, leaderFilter)
	}
	targets = filter.SelectTargetContainers(targets, finalFilters, cluster.GetOpts())
	leaderSchedulePolicy := l.opController.GetLeaderSchedulePolicy()
	sort.Slice(targets, func(i, j int) bool {
		kind := core.NewScheduleKind(metapb.ResourceKind_LeaderKind, leaderSchedulePolicy)
		iOp := opInfluence.GetContainerInfluence(targets[i].Meta.ID()).ResourceProperty(kind)
		jOp := opInfluence.GetContainerInfluence(targets[j].Meta.ID()).ResourceProperty(kind)
		return targets[i].LeaderScore(leaderSchedulePolicy, iOp) < targets[j].LeaderScore(leaderSchedulePolicy, jOp)
	})
	for _, target := range targets {
		if op := l.createOperator(cluster, resource, source, target); len(op) > 0 {
			return op
		}
	}
	util.GetLogger().Debugf("resource %d has no target container, scheduler %s",
		resource.Meta.ID(),
		l.GetName())
	schedulerCounter.WithLabelValues(l.GetName(), "no-target-container").Inc()
	return nil
}

// transferLeaderIn transfers leader to the target container.
// It randomly selects a health resource from the target container, then picks
// the worst follower peer and transfers the leader.
func (l *balanceLeaderScheduler) transferLeaderIn(cluster opt.Cluster, target *core.CachedContainer) []*operator.Operator {
	targetID := target.Meta.ID()
	resource := cluster.RandFollowerResource(targetID, l.conf.Ranges, opt.HealthResource(cluster))
	if resource == nil {
		// util.GetLogger().Debugf("container %d has no follower, scheduler %s",
		// 	targetID,
		// 	l.GetName())
		schedulerCounter.WithLabelValues(l.GetName(), "no-follower-resource").Inc()
		return nil
	}
	leaderContainerID := resource.GetLeader().GetContainerID()
	source := cluster.GetContainer(leaderContainerID)
	if source == nil {
		util.GetLogger().Debugf("resource %d has no leader or leader container %d cannot be found, scheduler %s",
			resource.Meta.ID(),
			leaderContainerID,
			l.GetName())
		schedulerCounter.WithLabelValues(l.GetName(), "no-leader").Inc()
		return nil
	}
	targets := []*core.CachedContainer{
		target,
	}
	finalFilters := l.filters
	if leaderFilter := filter.NewPlacementLeaderSafeguard(l.GetName(), cluster, resource, source,
		l.opController.GetCluster().GetResourceFactory()); leaderFilter != nil {
		finalFilters = append(l.filters, leaderFilter)
	}
	targets = filter.SelectTargetContainers(targets, finalFilters, cluster.GetOpts())
	if len(targets) < 1 {
		util.GetLogger().Debugf("resource %d has no target container, scheduler %s",
			resource.Meta.ID(),
			l.GetName())
		schedulerCounter.WithLabelValues(l.GetName(), "no-target-container").Inc()
		return nil
	}
	return l.createOperator(cluster, resource, source, targets[0])
}

// createOperator creates the operator according to the source and target container.
// If the resource is hot or the difference between the two containers is tolerable, then
// no new operator need to be created, otherwise create an operator that transfers
// the leader from the source container to the target container for the resource.
func (l *balanceLeaderScheduler) createOperator(cluster opt.Cluster, res *core.CachedResource, source, target *core.CachedContainer) []*operator.Operator {
	if cluster.IsResourceHot(res) {
		util.GetLogger().Debugf("resource %d is hot resource, ignore it, scheduler %s",
			res.Meta.ID(),
			l.GetName())
		schedulerCounter.WithLabelValues(l.GetName(), "resource-hot").Inc()
		return nil
	}

	sourceID := source.Meta.ID()
	targetID := target.Meta.ID()

	opInfluence := l.opController.GetOpInfluence(cluster)
	kind := core.NewScheduleKind(metapb.ResourceKind_LeaderKind, cluster.GetOpts().GetLeaderSchedulePolicy())
	shouldBalance, sourceScore, targetScore := shouldBalance(cluster, source, target, res, kind, opInfluence, l.GetName())
	if !shouldBalance {
		schedulerCounter.WithLabelValues(l.GetName(), "skip").Inc()
		return nil
	}

	op, err := operator.CreateTransferLeaderOperator(BalanceLeaderType, cluster, res, res.GetLeader().GetContainerID(), targetID, operator.OpLeader)
	if err != nil {
		util.GetLogger().Debugf("create balance leader operator failed with %+v",
			err)
		return nil
	}
	sourceLabel := strconv.FormatUint(sourceID, 10)
	targetLabel := strconv.FormatUint(targetID, 10)
	op.Counters = append(op.Counters,
		schedulerCounter.WithLabelValues(l.GetName(), "new-operator"),
		balanceDirectionCounter.WithLabelValues(l.GetName(), sourceLabel, targetLabel),
	)
	op.FinishedCounters = append(op.FinishedCounters,
		l.counter.WithLabelValues("move-leader", sourceLabel+"-out"),
		l.counter.WithLabelValues("move-leader", targetLabel+"-in"),
	)
	op.AdditionalInfos["sourceScore"] = strconv.FormatFloat(sourceScore, 'f', 2, 64)
	op.AdditionalInfos["targetScore"] = strconv.FormatFloat(targetScore, 'f', 2, 64)
	return []*operator.Operator{op}
}
