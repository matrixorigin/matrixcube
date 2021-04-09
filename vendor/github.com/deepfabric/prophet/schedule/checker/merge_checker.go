package checker

import (
	"bytes"
	"context"
	"time"

	"github.com/deepfabric/prophet/config"
	"github.com/deepfabric/prophet/core"
	"github.com/deepfabric/prophet/schedule/operator"
	"github.com/deepfabric/prophet/schedule/opt"
	"github.com/deepfabric/prophet/schedule/placement"
	"github.com/deepfabric/prophet/util"
)

const maxTargetResourceSize = 500

// MergeChecker ensures resource to merge with adjacent resource when size is small
type MergeChecker struct {
	cluster    opt.Cluster
	opts       *config.PersistOptions
	splitCache *util.TTLUint64
	startTime  time.Time // it's used to judge whether server recently start.
}

// NewMergeChecker creates a merge checker.
func NewMergeChecker(ctx context.Context, cluster opt.Cluster) *MergeChecker {
	opts := cluster.GetOpts()
	splitCache := util.NewIDTTL(ctx, time.Minute, opts.GetSplitMergeInterval())
	return &MergeChecker{
		cluster:    cluster,
		opts:       opts,
		splitCache: splitCache,
		startTime:  time.Now(),
	}
}

// RecordResourceSplit put the recently split resource into cache. MergeChecker
// will skip check it for a while.
func (m *MergeChecker) RecordResourceSplit(resourceIDs []uint64) {
	for _, resID := range resourceIDs {
		m.splitCache.PutWithTTL(resID, nil, m.opts.GetSplitMergeInterval())
	}
}

// Check verifies a resource's replicas, creating an Operator if need.
func (m *MergeChecker) Check(res *core.CachedResource) []*operator.Operator {
	expireTime := m.startTime.Add(m.opts.GetSplitMergeInterval())
	if time.Now().Before(expireTime) {
		checkerCounter.WithLabelValues("merge_checker", "recently-start").Inc()
		return nil
	}

	if m.splitCache.Exists(res.Meta.ID()) {
		checkerCounter.WithLabelValues("merge_checker", "recently-split").Inc()
		return nil
	}

	checkerCounter.WithLabelValues("merge_checker", "check").Inc()

	// when pd just started, it will load resource meta from etcd
	// but the size for these loaded resource info is 0
	// pd don't know the real size of one resource until the first heartbeat of the resource
	// thus here when size is 0, just skip.
	if res.GetApproximateSize() == 0 {
		checkerCounter.WithLabelValues("merge_checker", "skip").Inc()
		return nil
	}

	// resource is not small enough
	if res.GetApproximateSize() > int64(m.opts.GetMaxMergeResourceSize()) ||
		res.GetApproximateKeys() > int64(m.opts.GetMaxMergeResourceKeys()) {
		checkerCounter.WithLabelValues("merge_checker", "no-need").Inc()
		return nil
	}

	// skip resource has down peers or pending peers or learner peers
	if !opt.IsResourceHealthy(m.cluster, res) {
		checkerCounter.WithLabelValues("merge_checker", "special-peer").Inc()
		return nil
	}

	if !opt.IsResourceReplicated(m.cluster, res) {
		checkerCounter.WithLabelValues("merge_checker", "abnormal-replica").Inc()
		return nil
	}

	// skip hot resource
	if m.cluster.IsResourceHot(res) {
		checkerCounter.WithLabelValues("merge_checker", "hot-resource").Inc()
		return nil
	}

	prev, next := m.cluster.GetAdjacentResources(res)

	var target *core.CachedResource
	if m.checkTarget(res, next) {
		target = next
	}
	if !m.opts.IsOneWayMergeEnabled() && m.checkTarget(res, prev) { // allow a resource can be merged by two ways.
		if target == nil || prev.GetApproximateSize() < next.GetApproximateSize() { // pick smaller
			target = prev
		}
	}

	if target == nil {
		checkerCounter.WithLabelValues("merge_checker", "no-target").Inc()
		return nil
	}

	if target.GetApproximateSize() > maxTargetResourceSize {
		checkerCounter.WithLabelValues("merge_checker", "target-too-large").Inc()
		return nil
	}

	util.GetLogger().Debugf("try to merge resource, from %+v to %+v",
		core.ResourceToHexMeta(res.Meta),
		core.ResourceToHexMeta(target.Meta))

	ops, err := operator.CreateMergeResourceOperator("merge-resource", m.cluster, res, target, operator.OpMerge)
	if err != nil {
		util.GetLogger().Warningf("create merge resource operator failed with %+v", err)
		return nil
	}
	checkerCounter.WithLabelValues("merge_checker", "new-operator").Inc()
	if res.GetApproximateSize() > target.GetApproximateSize() ||
		res.GetApproximateKeys() > target.GetApproximateKeys() {
		checkerCounter.WithLabelValues("merge_checker", "larger-source").Inc()
	}
	return ops
}

func (m *MergeChecker) checkTarget(res, adjacent *core.CachedResource) bool {
	return adjacent != nil && !m.cluster.IsResourceHot(adjacent) && AllowMerge(m.cluster, res, adjacent) &&
		opt.IsResourceHealthy(m.cluster, adjacent) && opt.IsResourceReplicated(m.cluster, adjacent)
}

// AllowMerge returns true if two resources can be merged according to the key type.
func AllowMerge(cluster opt.Cluster, res *core.CachedResource, adjacent *core.CachedResource) bool {
	var start, end []byte
	if bytes.Equal(res.GetEndKey(), adjacent.GetStartKey()) && len(res.GetEndKey()) != 0 {
		start, end = res.GetStartKey(), adjacent.GetEndKey()
	} else if bytes.Equal(adjacent.GetEndKey(), res.GetStartKey()) && len(adjacent.GetEndKey()) != 0 {
		start, end = adjacent.GetStartKey(), res.GetEndKey()
	} else {
		return false
	}
	if cluster.GetOpts().IsPlacementRulesEnabled() {
		type withRuleManager interface {
			GetRuleManager() *placement.RuleManager
		}
		cl, ok := cluster.(withRuleManager)
		if !ok || len(cl.GetRuleManager().GetSplitKeys(start, end)) > 0 {
			return false
		}
	}

	return true
}
