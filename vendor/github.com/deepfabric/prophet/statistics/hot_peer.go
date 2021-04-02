package statistics

import (
	"math"
	"time"

	"github.com/deepfabric/prophet/util/movingaverage"
)

const (
	byteDim int = iota
	keyDim
	dimLen
)

// HotPeerStat records each hot peer's statistics
type HotPeerStat struct {
	ContainerID uint64 `json:"container_id"`
	ResourceID  uint64 `json:"resource_id"`

	// HotDegree records the times for the resource considered as hot spot during each HandleResourceHeartbeat
	HotDegree int `json:"hot_degree"`
	// AntiCount used to eliminate some noise when remove resource in cache
	AntiCount int `json:"anti_count"`

	Kind     FlowKind `json:"-"`
	ByteRate float64  `json:"flow_bytes"`
	KeyRate  float64  `json:"flow_keys"`

	// rolling statistics, recording some recently added records.
	rollingByteRate *movingaverage.TimeMedian
	rollingKeyRate  *movingaverage.TimeMedian

	// LastUpdateTime used to calculate average write
	LastUpdateTime time.Time `json:"last_update_time"`

	needDelete bool
	isLeader   bool
	isNew      bool
}

// ID returns resource ID. Implementing TopNItem.
func (stat *HotPeerStat) ID() uint64 {
	return stat.ResourceID
}

// Less compares two HotPeerStat.Implementing TopNItem.
func (stat *HotPeerStat) Less(k int, than TopNItem) bool {
	rhs := than.(*HotPeerStat)
	switch k {
	case keyDim:
		return stat.GetKeyRate() < rhs.GetKeyRate()
	case byteDim:
		fallthrough
	default:
		return stat.GetByteRate() < rhs.GetByteRate()
	}
}

// IsNeedDelete to delete the item in cache.
func (stat *HotPeerStat) IsNeedDelete() bool {
	return stat.needDelete
}

// IsLeader indicates the item belong to the leader.
func (stat *HotPeerStat) IsLeader() bool {
	return stat.isLeader
}

// IsNew indicates the item is first update in the cache of the resource.
func (stat *HotPeerStat) IsNew() bool {
	return stat.isNew
}

// GetByteRate returns denoised BytesRate if possible.
func (stat *HotPeerStat) GetByteRate() float64 {
	if stat.rollingByteRate == nil {
		return math.Round(stat.ByteRate)
	}
	return math.Round(stat.rollingByteRate.Get())
}

// GetKeyRate returns denoised KeysRate if possible.
func (stat *HotPeerStat) GetKeyRate() float64 {
	if stat.rollingKeyRate == nil {
		return math.Round(stat.KeyRate)
	}
	return math.Round(stat.rollingKeyRate.Get())
}

// Clone clones the HotPeerStat
func (stat *HotPeerStat) Clone() *HotPeerStat {
	ret := *stat
	ret.ByteRate = stat.GetByteRate()
	ret.rollingByteRate = nil
	ret.KeyRate = stat.GetKeyRate()
	ret.rollingKeyRate = nil
	return &ret
}
