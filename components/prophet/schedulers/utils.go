package schedulers

import (
	"math"
	"net/url"
	"strconv"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/statistics"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/montanaflynn/stats"
)

const (
	// KB kb
	KB = 1024
	// MB mb
	MB = 1024 * KB
)

const (
	// adjustRatio is used to adjust TolerantSizeRatio according to resource count.
	adjustRatio             float64 = 0.005
	leaderTolerantSizeRatio float64 = 5.0
	minTolerantSizeRatio    float64 = 1.0
)

func shouldBalance(cluster opt.Cluster, source, target *core.CachedContainer, res *core.CachedResource, kind core.ScheduleKind, opInfluence operator.OpInfluence, scheduleName string) (shouldBalance bool, sourceScore float64, targetScore float64) {
	// The reason we use max(resourceSize, averageResourceSize) to check is:
	// 1. prevent moving small resources between containers with close scores, leading to unnecessary balance.
	// 2. prevent moving huge resources, leading to over balance.
	sourceID := source.Meta.ID()
	targetID := target.Meta.ID()
	tolerantResource := getTolerantResource(cluster, res, kind)
	sourceInfluence := opInfluence.GetContainerInfluence(sourceID).ResourceProperty(kind)
	targetInfluence := opInfluence.GetContainerInfluence(targetID).ResourceProperty(kind)
	sourceDelta, targetDelta := sourceInfluence-tolerantResource, targetInfluence+tolerantResource
	opts := cluster.GetOpts()
	switch kind.ResourceKind {
	case metapb.ResourceKind_LeaderKind:
		sourceScore = source.LeaderScore(kind.Policy, sourceDelta)
		targetScore = target.LeaderScore(kind.Policy, targetDelta)
	case metapb.ResourceKind_ReplicaKind:
		sourceScore = source.ResourceScore(opts.GetResourceScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), sourceDelta, -1)
		targetScore = target.ResourceScore(opts.GetResourceScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), targetDelta, 1)
	}
	if opts.IsDebugMetricsEnabled() {
		opInfluenceStatus.WithLabelValues(scheduleName, strconv.FormatUint(sourceID, 10), "source").Set(float64(sourceInfluence))
		opInfluenceStatus.WithLabelValues(scheduleName, strconv.FormatUint(targetID, 10), "target").Set(float64(targetInfluence))
		tolerantResourceStatus.WithLabelValues(scheduleName, strconv.FormatUint(sourceID, 10), strconv.FormatUint(targetID, 10)).Set(float64(tolerantResource))
	}
	// Make sure after move, source score is still greater than target score.
	shouldBalance = sourceScore > targetScore

	if !shouldBalance {
		util.GetLogger().Debugf("skip balance %s, scheduler %s, resource %d, source container %d, target container %d, source-size %d, source-score %d, source-influence %d, target-size %d, target-score %d, target-influence %d, average-resource-size %d, tolerant-resource %d",
			kind.ResourceKind.String(),
			scheduleName,
			res.Meta.ID(),
			sourceID,
			targetID,
			source.GetResourceSize(),
			sourceScore,
			sourceInfluence,
			target.GetResourceSize(),
			targetScore,
			targetInfluence,
			cluster.GetAverageResourceSize(),
			tolerantResource)
	}
	return shouldBalance, sourceScore, targetScore
}

func getTolerantResource(cluster opt.Cluster, res *core.CachedResource, kind core.ScheduleKind) int64 {
	if kind.ResourceKind == metapb.ResourceKind_LeaderKind && kind.Policy == core.ByCount {
		tolerantSizeRatio := cluster.GetOpts().GetTolerantSizeRatio()
		if tolerantSizeRatio == 0 {
			tolerantSizeRatio = leaderTolerantSizeRatio
		}
		leaderCount := int64(1.0 * tolerantSizeRatio)
		return leaderCount
	}

	resourceSize := res.GetApproximateSize()
	if resourceSize < cluster.GetAverageResourceSize() {
		resourceSize = cluster.GetAverageResourceSize()
	}
	resourceSize = int64(float64(resourceSize) * adjustTolerantRatio(cluster))
	return resourceSize
}

func adjustTolerantRatio(cluster opt.Cluster) float64 {
	tolerantSizeRatio := cluster.GetOpts().GetTolerantSizeRatio()
	if tolerantSizeRatio == 0 {
		var maxResourceCount float64
		containers := cluster.GetContainers()
		for _, container := range containers {
			resourceCount := float64(cluster.GetContainerResourceCount(container.Meta.ID()))
			if maxResourceCount < resourceCount {
				maxResourceCount = resourceCount
			}
		}
		tolerantSizeRatio = maxResourceCount * adjustRatio
		if tolerantSizeRatio < minTolerantSizeRatio {
			tolerantSizeRatio = minTolerantSizeRatio
		}
	}
	return tolerantSizeRatio
}

func adjustBalanceLimit(cluster opt.Cluster, kind metapb.ResourceKind) uint64 {
	containers := cluster.GetContainers()
	counts := make([]float64, 0, len(containers))
	for _, s := range containers {
		if s.IsUp() {
			counts = append(counts, float64(s.ResourceCount(kind)))
		}
	}
	limit, _ := stats.StandardDeviation(counts)
	return typeutil.MaxUint64(1, uint64(limit))
}

func getKeyRanges(args []string) ([]core.KeyRange, error) {
	var ranges []core.KeyRange
	for len(args) > 1 {
		startKey, err := url.QueryUnescape(args[0])
		if err != nil {
			return nil, err
		}
		endKey, err := url.QueryUnescape(args[1])
		if err != nil {
			return nil, err
		}
		args = args[2:]
		ranges = append(ranges, core.NewKeyRange(startKey, endKey))
	}
	if len(ranges) == 0 {
		return []core.KeyRange{core.NewKeyRange("", "")}, nil
	}
	return ranges, nil
}

// Influence records operator influence.
type Influence struct {
	ByteRate float64
	KeyRate  float64
	Count    float64
}

func (infl Influence) add(rhs *Influence, w float64) Influence {
	infl.ByteRate += rhs.ByteRate * w
	infl.KeyRate += rhs.KeyRate * w
	infl.Count += rhs.Count * w
	return infl
}

// TODO: merge it into OperatorInfluence.
type pendingInfluence struct {
	op       *operator.Operator
	from, to uint64
	origin   Influence
}

func newPendingInfluence(op *operator.Operator, from, to uint64, infl Influence) *pendingInfluence {
	return &pendingInfluence{
		op:     op,
		from:   from,
		to:     to,
		origin: infl,
	}
}

// summaryPendingInfluence calculate the summary pending Influence for each container and return containerID -> Influence
// It makes each key/byte rate or count become (1+w) times to the origin value while f is the function to provide w(weight)
func summaryPendingInfluence(pendings map[*pendingInfluence]struct{}, f func(*operator.Operator) float64) map[uint64]Influence {
	ret := map[uint64]Influence{}
	for p := range pendings {
		w := f(p.op)
		if w == 0 {
			delete(pendings, p)
		}
		ret[p.to] = ret[p.to].add(&p.origin, w)
		ret[p.from] = ret[p.from].add(&p.origin, -w)
	}
	return ret
}

type containerLoad struct {
	ByteRate float64
	KeyRate  float64
	Count    float64
}

func (load *containerLoad) ToLoadPred(infl Influence) *containerLoadPred {
	future := *load
	future.ByteRate += infl.ByteRate
	future.KeyRate += infl.KeyRate
	future.Count += infl.Count
	return &containerLoadPred{
		Current: *load,
		Future:  future,
	}
}

func stLdByteRate(ld *containerLoad) float64 {
	return ld.ByteRate
}

func stLdKeyRate(ld *containerLoad) float64 {
	return ld.KeyRate
}

func stLdCount(ld *containerLoad) float64 {
	return ld.Count
}

type containerLoadCmp func(ld1, ld2 *containerLoad) int

func negLoadCmp(cmp containerLoadCmp) containerLoadCmp {
	return func(ld1, ld2 *containerLoad) int {
		return -cmp(ld1, ld2)
	}
}

func sliceLoadCmp(cmps ...containerLoadCmp) containerLoadCmp {
	return func(ld1, ld2 *containerLoad) int {
		for _, cmp := range cmps {
			if r := cmp(ld1, ld2); r != 0 {
				return r
			}
		}
		return 0
	}
}

func stLdRankCmp(dim func(ld *containerLoad) float64, rank func(value float64) int64) containerLoadCmp {
	return func(ld1, ld2 *containerLoad) int {
		return rankCmp(dim(ld1), dim(ld2), rank)
	}
}

func rankCmp(a, b float64, rank func(value float64) int64) int {
	aRk, bRk := rank(a), rank(b)
	if aRk < bRk {
		return -1
	} else if aRk > bRk {
		return 1
	}
	return 0
}

// container load prediction
type containerLoadPred struct {
	Current containerLoad
	Future  containerLoad
	Expect  containerLoad
}

func (lp *containerLoadPred) min() *containerLoad {
	return minLoad(&lp.Current, &lp.Future)
}

func (lp *containerLoadPred) max() *containerLoad {
	return maxLoad(&lp.Current, &lp.Future)
}

func (lp *containerLoadPred) diff() *containerLoad {
	mx, mn := lp.max(), lp.min()
	return &containerLoad{
		ByteRate: mx.ByteRate - mn.ByteRate,
		KeyRate:  mx.KeyRate - mn.KeyRate,
		Count:    mx.Count - mn.Count,
	}
}

type containerLPCmp func(lp1, lp2 *containerLoadPred) int

func sliceLPCmp(cmps ...containerLPCmp) containerLPCmp {
	return func(lp1, lp2 *containerLoadPred) int {
		for _, cmp := range cmps {
			if r := cmp(lp1, lp2); r != 0 {
				return r
			}
		}
		return 0
	}
}

func minLPCmp(ldCmp containerLoadCmp) containerLPCmp {
	return func(lp1, lp2 *containerLoadPred) int {
		return ldCmp(lp1.min(), lp2.min())
	}
}

func maxLPCmp(ldCmp containerLoadCmp) containerLPCmp {
	return func(lp1, lp2 *containerLoadPred) int {
		return ldCmp(lp1.max(), lp2.max())
	}
}

func diffCmp(ldCmp containerLoadCmp) containerLPCmp {
	return func(lp1, lp2 *containerLoadPred) int {
		return ldCmp(lp1.diff(), lp2.diff())
	}
}

func minLoad(a, b *containerLoad) *containerLoad {
	return &containerLoad{
		ByteRate: math.Min(a.ByteRate, b.ByteRate),
		KeyRate:  math.Min(a.KeyRate, b.KeyRate),
		Count:    math.Min(a.Count, b.Count),
	}
}

func maxLoad(a, b *containerLoad) *containerLoad {
	return &containerLoad{
		ByteRate: math.Max(a.ByteRate, b.ByteRate),
		KeyRate:  math.Max(a.KeyRate, b.KeyRate),
		Count:    math.Max(a.Count, b.Count),
	}
}

type containerLoadDetail struct {
	LoadPred *containerLoadPred
	HotPeers []*statistics.HotPeerStat
}

func (li *containerLoadDetail) toHotPeersStat() *statistics.HotPeersStat {
	peers := make([]statistics.HotPeerStat, 0, len(li.HotPeers))
	var totalBytesRate, totalKeysRate float64
	for _, peer := range li.HotPeers {
		if peer.HotDegree > 0 {
			peers = append(peers, *peer.Clone())
			totalBytesRate += peer.ByteRate
			totalKeysRate += peer.KeyRate
		}
	}
	return &statistics.HotPeersStat{
		TotalBytesRate: math.Round(totalBytesRate),
		TotalKeysRate:  math.Round(totalKeysRate),
		Count:          len(peers),
		Stats:          peers,
	}
}
