package filter

import (
	"reflect"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/stretchr/testify/assert"
)

func TestDistinctScoreFilter(t *testing.T) {
	labels := []string{"zone", "rack", "host"}
	allContainers := []*core.CachedContainer{
		core.NewTestContainerInfoWithLabel(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"}),
		core.NewTestContainerInfoWithLabel(2, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"}),
		core.NewTestContainerInfoWithLabel(3, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"}),
		core.NewTestContainerInfoWithLabel(4, 1, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"}),
		core.NewTestContainerInfoWithLabel(5, 1, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"}),
		core.NewTestContainerInfoWithLabel(6, 1, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"}),
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
		var containers []*core.CachedContainer
		for _, id := range tc.containers {
			containers = append(containers, allContainers[id-1])
		}
		ls := NewLocationSafeguard("", labels, containers, allContainers[tc.source-1])
		li := NewLocationImprover("", labels, containers, allContainers[tc.source-1])
		assert.Equal(t, tc.safeGuardRes, ls.Target(config.NewTestOptions(), allContainers[tc.target-1]))
		assert.Equal(t, tc.improverRes, li.Target(config.NewTestOptions(), allContainers[tc.target-1]))
	}
}

func TestLabelConstraintsFilter(t *testing.T) {
	opt := config.NewTestOptions()
	testCluster := mockcluster.NewCluster(opt)
	container := core.NewTestContainerInfoWithLabel(1, 1, map[string]string{"id": "1"})

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
	resource := core.NewCachedResource(&metadata.TestResource{ResPeers: []metapb.Peer{
		{ContainerID: 1, ID: 1},
		{ContainerID: 3, ID: 3},
		{ContainerID: 5, ID: 5},
	}}, &metapb.Peer{ContainerID: 1, ID: 1})

	testCases := []struct {
		ContainerID   uint64
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
		testCluster.AddLabelsContainer(tc.ContainerID, tc.resourceCount, tc.labels)
	}
	for _, tc := range testCases {
		filter := newRuleFitFilter("", testCluster, resource, 1, metadata.TestResourceFactory)
		assert.Equal(t, tc.sourceRes, filter.Source(testCluster.GetOpts(), testCluster.GetContainer(tc.ContainerID)))
		assert.Equal(t, tc.targetRes, filter.Target(testCluster.GetOpts(), testCluster.GetContainer(tc.ContainerID)))
	}
}

func TestContainerStateFilter(t *testing.T) {
	filters := []Filter{
		&ContainerStateFilter{TransferLeader: true},
		&ContainerStateFilter{MoveResource: true},
		&ContainerStateFilter{TransferLeader: true, MoveResource: true},
		&ContainerStateFilter{MoveResource: true, AllowTemporaryStates: true},
	}
	opt := config.NewTestOptions()
	container := core.NewTestContainerInfoWithLabel(1, 0, map[string]string{})

	type testCase struct {
		filterIdx int
		sourceRes bool
		targetRes bool
	}

	check := func(container *core.CachedContainer, testCases []testCase) {
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
		Clone(core.SetContainerStats(&metapb.ContainerStats{IsBusy: true}))
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
	allContainers := []struct {
		ContainerID   uint64
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
	for _, container := range allContainers {
		testCluster.AddLabelsContainer(container.ContainerID, container.resourceCount, container.labels)
	}

	testCases := []struct {
		resource       *core.CachedResource
		isolationLevel string
		sourceRes      []bool
		targetRes      []bool
	}{
		{
			core.NewCachedResource(&metadata.TestResource{ResPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1},
				{ID: 2, ContainerID: 6},
			}}, &metapb.Peer{ContainerID: 1, ID: 1}),
			"zone",
			[]bool{true, true, true, true, true, true, true},
			[]bool{false, false, false, false, false, false, true},
		},
		{
			core.NewCachedResource(&metadata.TestResource{ResPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1},
				{ID: 2, ContainerID: 4},
				{ID: 3, ContainerID: 7},
			}}, &metapb.Peer{ContainerID: 1, ID: 1}),
			"rack",
			[]bool{true, true, true, true, true, true, true},
			[]bool{false, false, false, false, true, true, false},
		},
		{
			core.NewCachedResource(&metadata.TestResource{ResPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1},
				{ID: 2, ContainerID: 4},
				{ID: 3, ContainerID: 6},
			}}, &metapb.Peer{ContainerID: 1, ID: 1}),
			"host",
			[]bool{true, true, true, true, true, true, true},
			[]bool{false, false, true, false, true, false, true},
		},
	}

	for _, tc := range testCases {
		filter := NewIsolationFilter("", tc.isolationLevel, testCluster.GetLocationLabels(), testCluster.GetResourceContainers(tc.resource))
		for idx, container := range allContainers {
			assert.Equal(t, tc.sourceRes[idx], filter.Source(testCluster.GetOpts(), testCluster.GetContainer(container.ContainerID)))
			assert.Equal(t, tc.targetRes[idx], filter.Target(testCluster.GetOpts(), testCluster.GetContainer(container.ContainerID)))
		}
	}
}

func TestPlacementGuard(t *testing.T) {
	opt := config.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	testCluster := mockcluster.NewCluster(opt)
	testCluster.SetLocationLabels([]string{"zone"})
	testCluster.AddLabelsContainer(1, 1, map[string]string{"zone": "z1"})
	testCluster.AddLabelsContainer(2, 1, map[string]string{"zone": "z1"})
	testCluster.AddLabelsContainer(3, 1, map[string]string{"zone": "z2"})
	testCluster.AddLabelsContainer(4, 1, map[string]string{"zone": "z2"})
	testCluster.AddLabelsContainer(5, 1, map[string]string{"zone": "z3"})
	resource := core.NewCachedResource(&metadata.TestResource{ResPeers: []metapb.Peer{
		{ContainerID: 1, ID: 1},
		{ContainerID: 3, ID: 3},
		{ContainerID: 5, ID: 5},
	}}, &metapb.Peer{ContainerID: 1, ID: 1})
	container := testCluster.GetContainer(1)

	obtained := reflect.ValueOf(NewPlacementSafeguard("", testCluster, resource, container, metadata.TestResourceFactory))
	expected := reflect.ValueOf(NewLocationSafeguard("", []string{"zone"}, testCluster.GetResourceContainers(resource), container))
	assert.True(t, obtained.Type().AssignableTo(expected.Type()))

	testCluster.SetEnablePlacementRules(true)
	obtained = reflect.ValueOf(NewPlacementSafeguard("", testCluster, resource, container, metadata.TestResourceFactory))
	expected = reflect.ValueOf(newRuleFitFilter("", testCluster, resource, 1, metadata.TestResourceFactory))
	assert.True(t, obtained.Type().AssignableTo(expected.Type()))
}
