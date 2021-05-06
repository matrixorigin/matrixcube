package opt

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestIsResourceHealthy(t *testing.T) {
	peers := func(ids ...uint64) []metapb.Peer {
		var peers []metapb.Peer
		for _, id := range ids {
			p := metapb.Peer{
				ID:          id,
				ContainerID: id,
			}
			peers = append(peers, p)
		}
		return peers
	}

	resource := func(peers []metapb.Peer, opts ...core.ResourceCreateOption) *core.CachedResource {
		return core.NewCachedResource(&metadata.TestResource{ResPeers: peers}, &peers[0], opts...)
	}

	type testCase struct {
		resource *core.CachedResource
		// disable placement rules
		healthy1             bool
		healthyAllowPending1 bool
		replicated1          bool
		// enable placement rules
		healthy2             bool
		healthyAllowPending2 bool
		replicated2          bool
	}

	cases := []testCase{
		{resource(peers(1, 2, 3)), true, true, true, true, true, true},
		{resource(peers(1, 2, 3), core.WithPendingPeers(peers(1))), false, true, true, false, true, true},
		{resource(peers(1, 2, 3), core.WithLearners(peers(1))), false, false, false, true, true, false},
		{resource(peers(1, 2, 3), core.WithDownPeers([]metapb.PeerStats{{Peer: peers(1)[0]}})), false, false, true, false, false, true},
		{resource(peers(1, 2)), true, true, false, true, true, false},
		{resource(peers(1, 2, 3, 4), core.WithLearners(peers(1))), false, false, false, true, true, false},
	}

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.AddResourceContainer(1, 1)
	tc.AddResourceContainer(2, 1)
	tc.AddResourceContainer(3, 1)
	tc.AddResourceContainer(4, 1)
	for _, c := range cases {
		tc.SetEnablePlacementRules(false)
		assert.Equal(t, c.healthy1, IsResourceHealthy(tc, c.resource))
		assert.Equal(t, c.healthyAllowPending1, IsHealthyAllowPending(tc, c.resource))
		assert.Equal(t, c.replicated1, IsResourceReplicated(tc, c.resource))
		tc.SetEnablePlacementRules(true)
		assert.Equal(t, c.healthy2, IsResourceHealthy(tc, c.resource))
		assert.Equal(t, c.healthyAllowPending2, IsHealthyAllowPending(tc, c.resource))
		assert.Equal(t, c.replicated2, IsResourceReplicated(tc, c.resource))
	}
}
