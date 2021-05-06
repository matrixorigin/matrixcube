package schedulers

import (
	"sync"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util/slice"
)

const (
	roleLeader   = string(placement.Leader)
	roleFollower = string(placement.Follower)
	roleLearner  = string(placement.Learner)
)

var allRoles = []string{roleLeader, roleFollower, roleLearner}

type shuffleResourceSchedulerConfig struct {
	sync.RWMutex
	storage storage.Storage

	Ranges []core.KeyRange `json:"ranges"`
	Roles  []string        `json:"roles"` // can include `leader`, `follower`, `learner`.
}

func (conf *shuffleResourceSchedulerConfig) EncodeConfig() ([]byte, error) {
	conf.RLock()
	defer conf.RUnlock()
	return schedule.EncodeConfig(conf)
}

func (conf *shuffleResourceSchedulerConfig) GetRoles() []string {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Roles
}

func (conf *shuffleResourceSchedulerConfig) GetRanges() []core.KeyRange {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Ranges
}

func (conf *shuffleResourceSchedulerConfig) IsRoleAllow(role string) bool {
	conf.RLock()
	defer conf.RUnlock()
	return slice.AnyOf(conf.Roles, func(i int) bool { return conf.Roles[i] == role })
}

func (conf *shuffleResourceSchedulerConfig) persist() error {
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(ShuffleResourceName, data)
}
