// Copyright 2021 MatrixOrigin.
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

package raftstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDoDynamicallyCreate(t *testing.T) {
	c := NewSingleTestClusterStore(t)
	s := c.GetStore(0).(*store)
	s.DataStorageByGroup(1).GetInitialStates()
	assert.True(t, s.doDynamicallyCreate(Shard{ID: 100, Group: 1, Replicas: []Replica{{ID: 200, ContainerID: s.Meta().ID, InitialMember: true}}}))
	assert.NotNil(t, s.getReplica(100, false))
}

func TestDoDynamicallyCreateWithNoReplicaOnCurrentStore(t *testing.T) {
	c := NewSingleTestClusterStore(t)
	s := c.GetStore(0).(*store)
	assert.False(t, s.doDynamicallyCreate(Shard{ID: 100, Group: 1, Replicas: []Replica{{ID: 200, ContainerID: s.Meta().ID + 1, InitialMember: true}}}))
}

func TestDoDynamicallyCreateWithExists(t *testing.T) {
	c := NewSingleTestClusterStore(t)
	s := c.GetStore(0).(*store)
	s.addReplica(newTestReplica(Shard{ID: 1}, Replica{ID: 100}, s))
	assert.False(t, s.doDynamicallyCreate(Shard{ID: 1, Group: 1, Replicas: []Replica{{ID: 200, ContainerID: s.Meta().ID, InitialMember: true}}}))
}
