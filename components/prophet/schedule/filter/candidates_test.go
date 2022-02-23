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

package filter

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

// A dummy comparer for testing.
func idComparer(a, b *core.CachedStore) int {
	if a.Meta.GetID() > b.Meta.GetID() {
		return 1
	}
	if a.Meta.GetID() < b.Meta.GetID() {
		return -1
	}
	return 0
}

// Another dummy comparer for testing.
func idComparer2(a, b *core.CachedStore) int {
	if a.Meta.GetID()/10 > b.Meta.GetID()/10 {
		return 1
	}
	if a.Meta.GetID()/10 < b.Meta.GetID()/10 {
		return -1
	}
	return 0
}

type idFilter func(uint64) bool

func (f idFilter) Scope() string { return "idFilter" }
func (f idFilter) Type() string  { return "idFilter" }
func (f idFilter) Source(opt *config.PersistOptions, container *core.CachedStore) bool {
	return f(container.Meta.GetID())
}
func (f idFilter) Target(opt *config.PersistOptions, container *core.CachedStore) bool {
	return f(container.Meta.GetID())
}

func TestCandidates(t *testing.T) {
	cs := newCandidates(1, 2, 3, 4, 5)
	cs.FilterSource(nil, idFilter(func(id uint64) bool { return id > 2 }))
	checkCandidates(t, cs, 3, 4, 5)
	cs.FilterTarget(nil, idFilter(func(id uint64) bool { return id%2 == 1 }))
	checkCandidates(t, cs, 3, 5)
	cs.FilterTarget(nil, idFilter(func(id uint64) bool { return id > 100 }))
	checkCandidates(t, cs)
	container := cs.PickFirst()
	assert.Nil(t, container)
	container = cs.RandomPick()
	assert.Nil(t, container)

	cs = newCandidates(1, 3, 5, 7, 6, 2, 4)
	cs.Sort(idComparer)
	checkCandidates(t, cs, 1, 2, 3, 4, 5, 6, 7)
	container = cs.PickFirst()
	assert.Equal(t, uint64(1), container.Meta.GetID())
	cs.Reverse()
	checkCandidates(t, cs, 7, 6, 5, 4, 3, 2, 1)
	container = cs.PickFirst()
	assert.Equal(t, uint64(7), container.Meta.GetID())
	cs.Shuffle()
	cs.Sort(idComparer)
	checkCandidates(t, cs, 1, 2, 3, 4, 5, 6, 7)
	container = cs.RandomPick()
	assert.True(t, container.Meta.GetID() > 0 && container.Meta.GetID() < 8)

	cs = newCandidates(10, 15, 23, 20, 33, 32, 31)
	cs.Sort(idComparer).Reverse().Top(idComparer2)
	checkCandidates(t, cs, 33, 32, 31)
}

func newCandidates(ids ...uint64) *StoreCandidates {
	var containers []*core.CachedStore
	for _, id := range ids {
		containers = append(containers, core.NewCachedStore(&metapb.Store{ID: id}))
	}
	return NewCandidates(containers)
}

func checkCandidates(t *testing.T, candidates *StoreCandidates, ids ...uint64) {
	assert.Equal(t, len(ids), len(candidates.Stores))
	for i, s := range candidates.Stores {
		assert.Equal(t, ids[i], s.Meta.GetID())
	}
}
