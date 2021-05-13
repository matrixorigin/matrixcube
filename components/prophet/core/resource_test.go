package core

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func newTestCachedResourceWithID(id uint64) *CachedResource {
	res := &CachedResource{
		Meta: &metadata.TestResource{ResID: id},
	}
	res.stats.ApproximateSize = id
	res.stats.ApproximateKeys = id
	return res
}

func TestResourceMap(t *testing.T) {
	var empty *resourceMap
	assert.Equal(t, 0, empty.Len(), "TestResourceMap failed")
	assert.Nil(t, empty.Get(1), "TestResourceMap failed")

	rm := newResourceMap()
	checkResourceMap(t, "TestResourceMap failed", rm)
	rm.Put(newTestCachedResourceWithID(1))
	checkResourceMap(t, "TestResourceMap failed", rm, 1)

	rm.Put(newTestCachedResourceWithID(2))
	rm.Put(newTestCachedResourceWithID(3))
	checkResourceMap(t, "TestResourceMap failed", rm, 1, 2, 3)

	rm.Put(newTestCachedResourceWithID(3))
	rm.Delete(4)
	checkResourceMap(t, "TestResourceMap failed", rm, 1, 2, 3)

	rm.Delete(3)
	rm.Delete(1)
	checkResourceMap(t, "TestResourceMap failed", rm, 2)

	rm.Put(newTestCachedResourceWithID(3))
	checkResourceMap(t, "TestResourceMap failed", rm, 2, 3)
}

func TestResourceKey(t *testing.T) {
	cases := []struct {
		key    string
		expect string
	}{
		{`"t\x80\x00\x00\x00\x00\x00\x00\xff!_r\x80\x00\x00\x00\x00\xff\x02\u007fY\x00\x00\x00\x00\x00\xfa"`,
			`7480000000000000FF215F728000000000FF027F590000000000FA`},
		{"\"\\x80\\x00\\x00\\x00\\x00\\x00\\x00\\xff\\x05\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\xf8\"",
			`80000000000000FF0500000000000000F8`},
	}
	for _, c := range cases {
		got, err := strconv.Unquote(c.key)
		assert.NoError(t, err, "TestResourceKey failed")

		// start key changed
		origin := NewCachedResource(&metadata.TestResource{End: []byte(got)}, nil)
		res := NewCachedResource(&metadata.TestResource{Start: []byte(got), End: []byte(got)}, nil)

		s := DiffResourceKeyInfo(origin, res)
		assert.True(t, regexp.MustCompile("^.*StartKey Changed.*$").MatchString(s), "TestResourceKey failed")
		assert.True(t, strings.Contains(s, c.expect), "TestResourceKey failed")

		// end key changed
		origin = NewCachedResource(&metadata.TestResource{Start: []byte(got)}, nil)
		res = NewCachedResource(&metadata.TestResource{Start: []byte(got), End: []byte(got)}, nil)
		s = DiffResourceKeyInfo(origin, res)
		assert.True(t, regexp.MustCompile(".*EndKey Changed.*").MatchString(s), "TestResourceKey failed")
		assert.True(t, strings.Contains(s, c.expect), "TestResourceKey failed")
	}
}

func TestSetResource(t *testing.T) {
	resources := NewCachedResources(func() metadata.Resource {
		return &metadata.TestResource{}
	})
	for i := 0; i < 100; i++ {
		peer1 := metapb.Peer{ContainerID: uint64(i%5 + 1), ID: uint64(i*5 + 1)}
		peer2 := metapb.Peer{ContainerID: uint64((i+1)%5 + 1), ID: uint64(i*5 + 2)}
		peer3 := metapb.Peer{ContainerID: uint64((i+2)%5 + 1), ID: uint64(i*5 + 3)}
		res := NewCachedResource(&metadata.TestResource{
			ResID:    uint64(i + 1),
			ResPeers: []metapb.Peer{peer1, peer2, peer3},
			Start:    []byte(fmt.Sprintf("%20d", i*10)),
			End:      []byte(fmt.Sprintf("%20d", (i+1)*10)),
		}, &peer1)
		resources.SetResource(res)
	}

	peer1 := metapb.Peer{ContainerID: uint64(4), ID: uint64(101)}
	peer2 := metapb.Peer{ContainerID: uint64(5), ID: uint64(102)}
	peer3 := metapb.Peer{ContainerID: uint64(1), ID: uint64(103)}
	res := NewCachedResource(&metadata.TestResource{
		ResID:    uint64(21),
		ResPeers: []metapb.Peer{peer1, peer2, peer3},
		Start:    []byte(fmt.Sprintf("%20d", 184)),
		End:      []byte(fmt.Sprintf("%20d", 211)),
	}, &peer1)
	res.learners = append(res.learners, peer2)
	res.pendingPeers = append(res.pendingPeers, peer3)

	resources.SetResource(res)
	checkResources(t, resources, "TestSetResource failed")
	assert.Equal(t, 97, resources.tree.length(), "TestSetResource failed")
	assert.Equal(t, 97, len(resources.GetResources()), "TestSetResource failed")

	resources.SetResource(res)
	peer1 = metapb.Peer{ContainerID: uint64(2), ID: uint64(101)}
	peer2 = metapb.Peer{ContainerID: uint64(3), ID: uint64(102)}
	peer3 = metapb.Peer{ContainerID: uint64(1), ID: uint64(103)}
	res = NewCachedResource(&metadata.TestResource{
		ResID:    uint64(21),
		ResPeers: []metapb.Peer{peer1, peer2, peer3},
		Start:    []byte(fmt.Sprintf("%20d", 184)),
		End:      []byte(fmt.Sprintf("%20d", 211)),
	}, &peer1)
	res.learners = append(res.learners, peer2)
	res.pendingPeers = append(res.pendingPeers, peer3)

	resources.SetResource(res)
	checkResources(t, resources, "TestSetResource failed")
	assert.Equal(t, 97, resources.tree.length(), "TestSetResource failed")
	assert.Equal(t, 97, len(resources.GetResources()), "TestSetResource failed")

	// Test remove overlaps.
	res = res.Clone(WithStartKey([]byte(fmt.Sprintf("%20d", 175))), WithNewResourceID(201))
	assert.NotNil(t, resources.GetResource(21), "TestSetResource failed")
	assert.NotNil(t, resources.GetResource(18), "TestSetResource failed")
	resources.SetResource(res)
	checkResources(t, resources, "TestSetResource failed")
	assert.Equal(t, 96, resources.tree.length(), "TestSetResource failed")
	assert.Equal(t, 96, len(resources.GetResources()), "TestSetResource failed")
	assert.NotNil(t, resources.GetResource(201), "TestSetResource failed")
	assert.Nil(t, resources.GetResource(21), "TestSetResource failed")
	assert.Nil(t, resources.GetResource(18), "TestSetResource failed")

	// Test update keys and size of resource.
	res = res.Clone()
	res.stats.ApproximateKeys = 20
	res.stats.ApproximateSize = 30
	resources.SetResource(res)
	checkResources(t, resources, "TestSetResource failed")
	assert.Equal(t, 96, resources.tree.length(), "TestSetResource failed")
	assert.Equal(t, 96, len(resources.GetResources()), "TestSetResource failed")
	assert.NotNil(t, resources.GetResource(201), "TestSetResource failed")
	assert.Equal(t, int64(20), resources.resources.totalKeys, "TestSetResource failed")
	assert.Equal(t, int64(30), resources.resources.totalSize, "TestSetResource failed")
}

func TestShouldRemoveFromSubTree(t *testing.T) {
	resources := NewCachedResources(func() metadata.Resource {
		return &metadata.TestResource{}
	})
	peer1 := metapb.Peer{ContainerID: uint64(1), ID: uint64(1)}
	peer2 := metapb.Peer{ContainerID: uint64(2), ID: uint64(2)}
	peer3 := metapb.Peer{ContainerID: uint64(3), ID: uint64(3)}
	peer4 := metapb.Peer{ContainerID: uint64(3), ID: uint64(3)}
	res := NewCachedResource(&metadata.TestResource{
		ResID:    uint64(1),
		ResPeers: []metapb.Peer{peer1, peer2, peer4},
		Start:    []byte(fmt.Sprintf("%20d", 10)),
		End:      []byte(fmt.Sprintf("%20d", 20)),
	}, &peer1)

	origin := NewCachedResource(&metadata.TestResource{
		ResID:    uint64(2),
		ResPeers: []metapb.Peer{peer1, peer2, peer3},
		Start:    []byte(fmt.Sprintf("%20d", 10)),
		End:      []byte(fmt.Sprintf("%20d", 20)),
	}, &peer1)
	assert.False(t, resources.shouldRemoveFromSubTree(res, origin))

	res.leader = &peer2
	assert.True(t, resources.shouldRemoveFromSubTree(res, origin))

	res.leader = &peer1
	res.pendingPeers = append(res.pendingPeers, peer4)
	assert.True(t, resources.shouldRemoveFromSubTree(res, origin))

	res.pendingPeers = nil
	res.learners = append(res.learners, peer2)
	assert.True(t, resources.shouldRemoveFromSubTree(res, origin))

	origin.learners = append(origin.learners, peer3, peer2)
	res.learners = append(res.learners, peer4)
	assert.False(t, resources.shouldRemoveFromSubTree(res, origin))

	res.voters[2].ContainerID = 4
	assert.True(t, resources.shouldRemoveFromSubTree(res, origin))
}

func checkResourceMap(t *testing.T, msg string, rm *resourceMap, ids ...uint64) {
	// Check Get.
	for _, id := range ids {
		assert.Equal(t, id, rm.Get(id).Meta.ID(), msg)
	}

	// Check Len.
	assert.Equal(t, len(ids), rm.Len(), msg)

	// Check id set.
	set1 := make(map[uint64]struct{})
	for _, r := range rm.m {
		set1[r.Meta.ID()] = struct{}{}
	}
	for _, id := range ids {
		_, ok := set1[id]
		assert.True(t, ok, msg)
	}

	// Check resource size.
	var total int64
	for _, id := range ids {
		total += int64(id)
	}
	assert.Equal(t, rm.totalSize, total, msg)
}

func checkResources(t *testing.T, resources *CachedResources, msg string) {
	leaderMap := make(map[uint64]uint64)
	followerMap := make(map[uint64]uint64)
	learnerMap := make(map[uint64]uint64)
	pendingPeerMap := make(map[uint64]uint64)
	for _, item := range resources.GetResources() {
		if leaderCount, ok := leaderMap[item.leader.ContainerID]; ok {
			leaderMap[item.leader.ContainerID] = leaderCount + 1
		} else {
			leaderMap[item.leader.ContainerID] = 1
		}
		for _, follower := range item.GetFollowers() {
			if followerCount, ok := followerMap[follower.ContainerID]; ok {
				followerMap[follower.ContainerID] = followerCount + 1
			} else {
				followerMap[follower.ContainerID] = 1
			}
		}
		for _, learner := range item.GetLearners() {
			if learnerCount, ok := learnerMap[learner.ContainerID]; ok {
				learnerMap[learner.ContainerID] = learnerCount + 1
			} else {
				learnerMap[learner.ContainerID] = 1
			}
		}
		for _, pendingPeer := range item.GetPendingPeers() {
			if pendingPeerCount, ok := pendingPeerMap[pendingPeer.ContainerID]; ok {
				pendingPeerMap[pendingPeer.ContainerID] = pendingPeerCount + 1
			} else {
				pendingPeerMap[pendingPeer.ContainerID] = 1
			}
		}
	}
	for key, value := range resources.leaders {
		assert.Equal(t, int(leaderMap[key]), value.length(), msg)
	}
	for key, value := range resources.followers {
		assert.Equal(t, int(followerMap[key]), value.length(), msg)
	}
	for key, value := range resources.learners {
		assert.Equal(t, int(learnerMap[key]), value.length(), msg)
	}
	for key, value := range resources.pendingPeers {
		assert.Equal(t, int(pendingPeerMap[key]), value.length(), msg)
	}
}
