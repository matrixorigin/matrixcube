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
