package opt

import (
	"github.com/matrixorigin/matrixcube/components/prophet/core"
)

// IsResourceHealthy checks if a resource is healthy for scheduling. It requires the
// resource does not have any down or pending peers. And when placement rules
// feature is disabled, it requires the resource does not have any learner peer.
func IsResourceHealthy(cluster Cluster, res *core.CachedResource) bool {
	return IsHealthyAllowPending(cluster, res) && len(res.GetPendingPeers()) == 0
}

// IsHealthyAllowPending checks if a resource is healthy for scheduling.
// Differs from IsResourceHealthy, it allows the resource to have pending peers.
func IsHealthyAllowPending(cluster Cluster, res *core.CachedResource) bool {
	if !cluster.GetOpts().IsPlacementRulesEnabled() && len(res.GetLearners()) > 0 {
		return false
	}
	return len(res.GetDownPeers()) == 0
}

// HealthResource returns a function that checks if a resource is healthy for
// scheduling. It requires the resource does not have any down or pending peers,
// and does not have any learner peers when placement rules is disabled.
func HealthResource(cluster Cluster) func(*core.CachedResource) bool {
	return func(res *core.CachedResource) bool { return IsResourceHealthy(cluster, res) }
}

// HealthAllowPending returns a function that checks if a resource is
// healthy for scheduling. Differs from HealthResource, it allows the resource
// to have pending peers.
func HealthAllowPending(cluster Cluster) func(*core.CachedResource) bool {
	return func(res *core.CachedResource) bool { return IsHealthyAllowPending(cluster, res) }
}

// IsResourceReplicated checks if a resource is fully replicated. When placement
// rules is enabled, its peers should fit corresponding rules. When placement
// rules is disabled, it should have enough replicas and no any learner peer.
func IsResourceReplicated(cluster Cluster, res *core.CachedResource) bool {
	if cluster.GetOpts().IsPlacementRulesEnabled() {
		return cluster.FitResource(res).IsSatisfied()
	}
	return len(res.GetLearners()) == 0 && len(res.Meta.Peers()) == cluster.GetOpts().GetMaxReplicas()
}

// ReplicatedResource returns a function that checks if a resource is fully replicated.
func ReplicatedResource(cluster Cluster) func(*core.CachedResource) bool {
	return func(res *core.CachedResource) bool { return IsResourceReplicated(cluster, res) }
}
