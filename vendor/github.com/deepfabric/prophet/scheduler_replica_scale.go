package prophet

import (
	"sort"
)

type replicaScaleChecker struct {
	enable bool
	rt     *Runtime
}

func newReplicaScaleChecker(rt *Runtime, enable bool) *replicaScaleChecker {
	return &replicaScaleChecker{
		enable: enable,
		rt:     rt,
	}
}

// Check return the Operator to add or remove replica
func (r *replicaScaleChecker) Check(target *ResourceRuntime) Operator {
	if !r.enable {
		return nil
	}

	cts := r.rt.Containers()
	sort.Slice(cts, func(i, j int) bool {
		return cts[i].meta.ID() < cts[j].meta.ID()
	})

	for _, ct := range cts {
		if ct.meta.ActionOnJoinCluster() == ScaleOutAction &&
			!target.meta.ScaleCompleted(ct.meta.ID()) {
			return newScalePeerOp(target.meta.ID(), ct.meta.ID())
		}
	}

	return nil
}
