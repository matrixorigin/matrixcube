package statistics

// HotStat contains cluster's hotspot statistics.
type HotStat struct {
	*HotCache
	*ContainersStats
}

// NewHotStat creates the container to hold cluster's hotspot statistics.
func NewHotStat() *HotStat {
	return &HotStat{
		HotCache:        NewHotCache(),
		ContainersStats: NewContainersStats(),
	}
}
