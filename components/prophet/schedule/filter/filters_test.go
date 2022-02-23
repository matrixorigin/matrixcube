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
	"reflect"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestDistinctScoreFilter(t *testing.T) {
	labels := []string{"zone", "rack", "host"}
	allStores := []*core.CachedStore{
		core.NewTestStoreInfoWithLabel(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"}),
		core.NewTestStoreInfoWithLabel(2, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"}),
		core.NewTestStoreInfoWithLabel(3, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"}),
		core.NewTestStoreInfoWithLabel(4, 1, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"}),
		core.NewTestStoreInfoWithLabel(5, 1, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"}),
		core.NewTestStoreInfoWithLabel(6, 1, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"}),
	}

	testCases := []struct {
		containers   []uint64
		source       uint64
		target       uint64
		safeGuardRes bool
		improverRes  bool
	}{
		{[]uint64{1, 2, 3}, 1, 4, true, true},
		{[]uint64{1, 3, 4}, 1, 2, true, false},
		{[]uint64{1, 4, 6}, 4, 2, false, false},
	}
	for _, tc := range testCases {
		var containers []*core.CachedStore
		for _, id := range tc.containers {
			containers = append(containers, allStores[id-1])
		}
		ls := NewLocationSafeguard("", labels, containers, allStores[tc.source-1])
		li := NewLocationImprover("", labels, containers, allStores[tc.source-1])
		assert.Equal(t, tc.safeGuardRes, ls.Target(config.NewTestOptions(), allStores[tc.target-1]))
		assert.Equal(t, tc.improverRes, li.Target(config.NewTestOptions(), allStores[tc.target-1]))
	}
}

func TestLabelConstraintsFilter(t *testing.T) {
	opt := config.NewTestOptions()
	testCluster := mockcluster.NewCluster(opt)
	container := core.NewTestStoreInfoWithLabel(1, 1, map[string]string{"id": "1"})

	testCases := []struct {
		key    string
		op     string
		values []string
		res    bool
	}{
		{"id", "in", []string{"1"}, true},
		{"id", "in", []string{"2"}, false},
		{"id", "in", []string{"1", "2"}, true},
		{"id", "notIn", []string{"2", "3"}, true},
		{"id", "notIn", []string{"1", "2"}, false},
		{"id", "exists", []string{}, true},
		{"_id", "exists", []string{}, false},
		{"id", "notExists", []string{}, false},
		{"_id", "notExists", []string{}, true},
	}
	for _, tc := range testCases {
		filter := NewLabelConstaintFilter("", []placement.LabelConstraint{{Key: tc.key, Op: placement.LabelConstraintOp(tc.op), Values: tc.values}})
		assert.Equal(t, tc.res, filter.Source(testCluster.GetOpts(), container))
	}
}

func TestRuleFitFilter(t *testing.T) {
	opt := config.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	testCluster := mockcluster.NewCluster(opt)
	testCluster.SetLocationLabels([]string{"zone"})
	testCluster.SetEnablePlacementRules(true)
	resource := core.NewCachedShard(&metapb.Shard{
		Replicas: []metapb.Replica{
			{StoreID: 1, ID: 1},
			{StoreID: 3, ID: 3},
			{StoreID: 5, ID: 5},
		}}, &metapb.Replica{StoreID: 1, ID: 1})

	testCases := []struct {
		StoreID       uint64
		resourceCount int
		labels        map[string]string
		sourceRes     bool
		targetRes     bool
	}{
		{1, 1, map[string]string{"zone": "z1"}, true, true},
		{2, 1, map[string]string{"zone": "z1"}, true, true},
		{3, 1, map[string]string{"zone": "z2"}, true, false},
		{4, 1, map[string]string{"zone": "z2"}, true, false},
		{5, 1, map[string]string{"zone": "z3"}, true, false},
		{6, 1, map[string]string{"zone": "z4"}, true, true},
	}
	// Init cluster
	for _, tc := range testCases {
		testCluster.AddLabelsStore(tc.StoreID, tc.resourceCount, tc.labels)
	}
	for _, tc := range testCases {
		filter := newRuleFitFilter("", testCluster, resource, 1)
		assert.Equal(t, tc.sourceRes, filter.Source(testCluster.GetOpts(), testCluster.GetStore(tc.StoreID)))
		assert.Equal(t, tc.targetRes, filter.Target(testCluster.GetOpts(), testCluster.GetStore(tc.StoreID)))
	}
}

func TestStoreStateFilter(t *testing.T) {
	filters := []Filter{
		&StoreStateFilter{TransferLeader: true},
		&StoreStateFilter{MoveShard: true},
		&StoreStateFilter{TransferLeader: true, MoveShard: true},
		&StoreStateFilter{MoveShard: true, AllowTemporaryStates: true},
	}
	opt := config.NewTestOptions()
	container := core.NewTestStoreInfoWithLabel(1, 0, map[string]string{})

	type testCase struct {
		filterIdx int
		sourceRes bool
		targetRes bool
	}

	check := func(container *core.CachedStore, testCases []testCase) {
		for _, tc := range testCases {
			assert.Equal(t, tc.sourceRes, filters[tc.filterIdx].Source(opt, container))
			assert.Equal(t, tc.targetRes, filters[tc.filterIdx].Target(opt, container))
		}
	}

	container = container.Clone(core.SetLastHeartbeatTS(time.Now()))
	testCases := []testCase{
		{2, true, true},
	}
	check(container, testCases)

	// Disconn
	container = container.Clone(core.SetLastHeartbeatTS(time.Now().Add(-5 * time.Minute)))
	testCases = []testCase{
		{0, false, false},
		{1, true, false},
		{2, false, false},
		{3, true, true},
	}
	check(container, testCases)

	// Busy
	container = container.Clone(core.SetLastHeartbeatTS(time.Now())).
		Clone(core.SetStoreStats(&metapb.StoreStats{IsBusy: true}))
	testCases = []testCase{
		{0, true, false},
		{1, false, false},
		{2, false, false},
		{3, true, true},
	}
	check(container, testCases)
}

func TestIsolationFilter(t *testing.T) {
	opt := config.NewTestOptions()
	testCluster := mockcluster.NewCluster(opt)
	testCluster.SetLocationLabels([]string{"zone", "rack", "host"})
	allStores := []struct {
		StoreID       uint64
		resourceCount int
		labels        map[string]string
	}{
		{1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"}},
		{2, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"}},
		{3, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"}},
		{4, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"}},
		{5, 1, map[string]string{"zone": "z1", "rack": "r3", "host": "h1"}},
		{6, 1, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"}},
		{7, 1, map[string]string{"zone": "z3", "rack": "r3", "host": "h1"}},
	}
	for _, container := range allStores {
		testCluster.AddLabelsStore(container.StoreID, container.resourceCount, container.labels)
	}

	testCases := []struct {
		resource       *core.CachedShard
		isolationLevel string
		sourceRes      []bool
		targetRes      []bool
	}{
		{
			core.NewCachedShard(&metapb.Shard{Replicas: []metapb.Replica{
				{ID: 1, StoreID: 1},
				{ID: 2, StoreID: 6},
			}}, &metapb.Replica{StoreID: 1, ID: 1}),
			"zone",
			[]bool{true, true, true, true, true, true, true},
			[]bool{false, false, false, false, false, false, true},
		},
		{
			core.NewCachedShard(&metapb.Shard{Replicas: []metapb.Replica{
				{ID: 1, StoreID: 1},
				{ID: 2, StoreID: 4},
				{ID: 3, StoreID: 7},
			}}, &metapb.Replica{StoreID: 1, ID: 1}),
			"rack",
			[]bool{true, true, true, true, true, true, true},
			[]bool{false, false, false, false, true, true, false},
		},
		{
			core.NewCachedShard(&metapb.Shard{Replicas: []metapb.Replica{
				{ID: 1, StoreID: 1},
				{ID: 2, StoreID: 4},
				{ID: 3, StoreID: 6},
			}}, &metapb.Replica{StoreID: 1, ID: 1}),
			"host",
			[]bool{true, true, true, true, true, true, true},
			[]bool{false, false, true, false, true, false, true},
		},
	}

	for _, tc := range testCases {
		filter := NewIsolationFilter("", tc.isolationLevel, testCluster.GetLocationLabels(), testCluster.GetShardStores(tc.resource))
		for idx, container := range allStores {
			assert.Equal(t, tc.sourceRes[idx], filter.Source(testCluster.GetOpts(), testCluster.GetStore(container.StoreID)))
			assert.Equal(t, tc.targetRes[idx], filter.Target(testCluster.GetOpts(), testCluster.GetStore(container.StoreID)))
		}
	}
}

func TestPlacementGuard(t *testing.T) {
	opt := config.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	testCluster := mockcluster.NewCluster(opt)
	testCluster.SetLocationLabels([]string{"zone"})
	testCluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	testCluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1"})
	testCluster.AddLabelsStore(3, 1, map[string]string{"zone": "z2"})
	testCluster.AddLabelsStore(4, 1, map[string]string{"zone": "z2"})
	testCluster.AddLabelsStore(5, 1, map[string]string{"zone": "z3"})
	resource := core.NewCachedShard(&metapb.Shard{
		Replicas: []metapb.Replica{
			{StoreID: 1, ID: 1},
			{StoreID: 3, ID: 3},
			{StoreID: 5, ID: 5},
		}}, &metapb.Replica{StoreID: 1, ID: 1})
	container := testCluster.GetStore(1)

	obtained := reflect.ValueOf(NewPlacementSafeguard("", testCluster, resource, container))
	expected := reflect.ValueOf(NewLocationSafeguard("", []string{"zone"}, testCluster.GetShardStores(resource), container))
	assert.True(t, obtained.Type().AssignableTo(expected.Type()))

	testCluster.SetEnablePlacementRules(true)
	obtained = reflect.ValueOf(NewPlacementSafeguard("", testCluster, resource, container))
	expected = reflect.ValueOf(newRuleFitFilter("", testCluster, resource, 1))
	assert.True(t, obtained.Type().AssignableTo(expected.Type()))
}
