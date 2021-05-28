package schedulers

import (
	"errors"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
)

func init() {
	// args: [start-key, end-key, range-name].
	schedule.RegisterSliceDecoderBuilder(ScatterRangeType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 3 {
				return errors.New("scheduler error configuration")
			}
			if len(args[2]) == 0 {
				return errors.New("scheduler error configuration")
			}
			conf, ok := v.(*scatterRangeSchedulerConfig)
			if !ok {
				return errors.New("scheduler error configuration")
			}
			conf.StartKey = args[0]
			conf.EndKey = args[1]
			conf.RangeName = args[2]
			return nil
		}
	})

	schedule.RegisterScheduler(ScatterRangeType, func(opController *schedule.OperatorController, storage storage.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &scatterRangeSchedulerConfig{
			storage: storage,
		}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		rangeName := conf.RangeName
		if len(rangeName) == 0 {
			return nil, errors.New("scheduler error configuration")
		}
		return newScatterRangeScheduler(opController, conf), nil
	})
}

const (
	// ScatterRangeType is scatter range scheduler type
	ScatterRangeType = "scatter-range"
	// ScatterRangeName is scatter range scheduler name
	ScatterRangeName = "scatter-range"
)

type scatterRangeSchedulerConfig struct {
	mu        sync.RWMutex
	storage   storage.Storage
	RangeName string `json:"range-name"`
	StartKey  string `json:"start-key"`
	EndKey    string `json:"end-key"`
}

func (conf *scatterRangeSchedulerConfig) BuildWithArgs(args []string) error {
	if len(args) != 3 {
		return errors.New("scheduler error configuration")
	}
	conf.mu.Lock()
	defer conf.mu.Unlock()

	conf.RangeName = args[0]
	conf.StartKey = args[1]
	conf.EndKey = args[2]
	return nil
}

func (conf *scatterRangeSchedulerConfig) Clone() *scatterRangeSchedulerConfig {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return &scatterRangeSchedulerConfig{
		StartKey:  conf.StartKey,
		EndKey:    conf.EndKey,
		RangeName: conf.RangeName,
	}
}

func (conf *scatterRangeSchedulerConfig) Persist() error {
	name := conf.getSchedulerName()
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(name, data)
}

func (conf *scatterRangeSchedulerConfig) GetRangeName() string {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return conf.RangeName
}

func (conf *scatterRangeSchedulerConfig) GetStartKey() []byte {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return []byte(conf.StartKey)
}

func (conf *scatterRangeSchedulerConfig) GetEndKey() []byte {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return []byte(conf.EndKey)
}

func (conf *scatterRangeSchedulerConfig) getSchedulerName() string {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return fmt.Sprintf("scatter-range-%s", conf.RangeName)
}

type scatterRangeScheduler struct {
	*BaseScheduler
	name            string
	config          *scatterRangeSchedulerConfig
	balanceLeader   schedule.Scheduler
	balanceResource schedule.Scheduler
}

// newScatterRangeScheduler creates a scheduler that balances the distribution of leaders and resources that in the specified key range.
func newScatterRangeScheduler(opController *schedule.OperatorController, config *scatterRangeSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)

	name := config.getSchedulerName()
	scheduler := &scatterRangeScheduler{
		BaseScheduler: base,
		config:        config,
		name:          name,
		balanceLeader: newBalanceLeaderScheduler(
			opController,
			&balanceLeaderSchedulerConfig{Ranges: []core.KeyRange{core.NewKeyRange("", "")}},
			WithBalanceLeaderName("scatter-range-leader"),
			WithBalanceLeaderCounter(scatterRangeLeaderCounter),
		),
		balanceResource: newBalanceResourceScheduler(
			opController,
			&balanceResourceSchedulerConfig{Ranges: []core.KeyRange{core.NewKeyRange("", "")}},
			WithBalanceResourceName("scatter-range-resource"),
			WithBalanceResourceCounter(scatterRangeResourceCounter),
		),
	}
	return scheduler
}

func (l *scatterRangeScheduler) GetName() string {
	return l.name
}

func (l *scatterRangeScheduler) GetType() string {
	return ScatterRangeType
}

func (l *scatterRangeScheduler) EncodeConfig() ([]byte, error) {
	l.config.mu.RLock()
	defer l.config.mu.RUnlock()
	return schedule.EncodeConfig(l.config)
}

func (l *scatterRangeScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return l.allowBalanceLeader(cluster) || l.allowBalanceResource(cluster)
}

func (l *scatterRangeScheduler) allowBalanceLeader(cluster opt.Cluster) bool {
	allowed := l.OpController.OperatorCount(operator.OpRange) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(l.GetType(), operator.OpLeader.String()).Inc()
	}
	return allowed
}

func (l *scatterRangeScheduler) allowBalanceResource(cluster opt.Cluster) bool {
	allowed := l.OpController.OperatorCount(operator.OpRange) < cluster.GetOpts().GetResourceScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(l.GetType(), operator.OpResource.String()).Inc()
	}
	return allowed
}

func (l *scatterRangeScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()
	// isolate a new cluster according to the key range
	c := schedule.GenRangeCluster(cluster, l.config.GetStartKey(), l.config.GetEndKey())
	c.SetTolerantSizeRatio(2)
	if l.allowBalanceLeader(cluster) {
		ops := l.balanceLeader.Schedule(c)
		if len(ops) > 0 {
			ops[0].SetDesc(fmt.Sprintf("scatter-range-leader-%s", l.config.RangeName))
			ops[0].AttachKind(operator.OpRange)
			ops[0].Counters = append(ops[0].Counters,
				schedulerCounter.WithLabelValues(l.GetName(), "new-operator"),
				schedulerCounter.WithLabelValues(l.GetName(), "new-leader-operator"))
			return ops
		}
		schedulerCounter.WithLabelValues(l.GetName(), "no-need-balance-leader").Inc()
	}
	if l.allowBalanceResource(cluster) {
		ops := l.balanceResource.Schedule(c)
		if len(ops) > 0 {
			ops[0].SetDesc(fmt.Sprintf("scatter-range-resource-%s", l.config.RangeName))
			ops[0].AttachKind(operator.OpRange)
			ops[0].Counters = append(ops[0].Counters,
				schedulerCounter.WithLabelValues(l.GetName(), "new-operator"),
				schedulerCounter.WithLabelValues(l.GetName(), "new-resource-operator"),
			)
			return ops
		}
		schedulerCounter.WithLabelValues(l.GetName(), "no-need-balance-resource").Inc()
	}

	return nil
}
