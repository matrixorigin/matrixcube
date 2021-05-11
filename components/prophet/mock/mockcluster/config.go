package mockcluster

import (
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
)

// SetMaxMergeResourceSize updates the MaxMergeResourceSize configuration.
func (mc *Cluster) SetMaxMergeResourceSize(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.MaxMergeResourceSize = uint64(v) })
}

// SetMaxMergeResourceKeys updates the MaxMergeResourceKeys configuration.
func (mc *Cluster) SetMaxMergeResourceKeys(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.MaxMergeResourceKeys = uint64(v) })
}

// SetSplitMergeInterval updates the SplitMergeInterval configuration.
func (mc *Cluster) SetSplitMergeInterval(v time.Duration) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.SplitMergeInterval = typeutil.NewDuration(v) })
}

// SetEnableOneWayMerge updates the EnableOneWayMerge configuration.
func (mc *Cluster) SetEnableOneWayMerge(v bool) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.EnableOneWayMerge = v })
}

// SetMaxSnapshotCount updates the MaxSnapshotCount configuration.
func (mc *Cluster) SetMaxSnapshotCount(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.MaxSnapshotCount = uint64(v) })
}

// SetEnableMakeUpReplica updates the EnableMakeUpReplica configuration.
func (mc *Cluster) SetEnableMakeUpReplica(v bool) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.EnableMakeUpReplica = v })
}

// SetEnableRemoveExtraReplica updates the EnableRemoveExtraReplica configuration.
func (mc *Cluster) SetEnableRemoveExtraReplica(v bool) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.EnableRemoveExtraReplica = v })
}

// SetEnableLocationReplacement updates the EnableLocationReplacement configuration.
func (mc *Cluster) SetEnableLocationReplacement(v bool) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.EnableLocationReplacement = v })
}

// SetEnableRemoveDownReplica updates the EnableRemoveDownReplica configuration.
func (mc *Cluster) SetEnableRemoveDownReplica(v bool) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.EnableRemoveDownReplica = v })
}

// SetEnableReplaceOfflineReplica updates the EnableReplaceOfflineReplica configuration.
func (mc *Cluster) SetEnableReplaceOfflineReplica(v bool) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.EnableReplaceOfflineReplica = v })
}

// SetLeaderSchedulePolicy updates the LeaderSchedulePolicy configuration.
func (mc *Cluster) SetLeaderSchedulePolicy(v string) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.LeaderSchedulePolicy = v })
}

// SetTolerantSizeRatio updates the TolerantSizeRatio configuration.
func (mc *Cluster) SetTolerantSizeRatio(v float64) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.TolerantSizeRatio = v })
}

// SetResourceScoreFormulaVersion updates the ResourceScoreFormulaVersion configuration.
func (mc *Cluster) SetResourceScoreFormulaVersion(v string) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.ResourceScoreFormulaVersion = v })
}

// SetLeaderScheduleLimit updates the LeaderScheduleLimit configuration.
func (mc *Cluster) SetLeaderScheduleLimit(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.LeaderScheduleLimit = uint64(v) })
}

// SetResourceScheduleLimit updates the ResourceScheduleLimit configuration.
func (mc *Cluster) SetResourceScheduleLimit(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.ResourceScheduleLimit = uint64(v) })
}

// SetMergeScheduleLimit updates the MergeScheduleLimit configuration.
func (mc *Cluster) SetMergeScheduleLimit(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.MergeScheduleLimit = uint64(v) })
}

// SetHotResourceScheduleLimit updates the HotResourceScheduleLimit configuration.
func (mc *Cluster) SetHotResourceScheduleLimit(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.HotResourceScheduleLimit = uint64(v) })
}

// SetHotResourceCacheHitsThreshold updates the HotResourceCacheHitsThreshold configuration.
func (mc *Cluster) SetHotResourceCacheHitsThreshold(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.HotResourceCacheHitsThreshold = uint64(v) })
}

// SetEnablePlacementRules updates the EnablePlacementRules configuration.
func (mc *Cluster) SetEnablePlacementRules(v bool) {
	mc.updateReplicationConfig(func(r *config.ReplicationConfig) { r.EnablePlacementRules = v })
	if v {
		mc.initRuleManager()
	}
}

// SetMaxReplicas updates the maxReplicas configuration.
func (mc *Cluster) SetMaxReplicas(v int) {
	mc.updateReplicationConfig(func(r *config.ReplicationConfig) { r.MaxReplicas = uint64(v) })
}

// SetLocationLabels updates the LocationLabels configuration.
func (mc *Cluster) SetLocationLabels(v []string) {
	mc.updateReplicationConfig(func(r *config.ReplicationConfig) { r.LocationLabels = v })
}

func (mc *Cluster) updateScheduleConfig(f func(*config.ScheduleConfig)) {
	s := mc.GetScheduleConfig().Clone()
	f(s)
	mc.SetScheduleConfig(s)
}

func (mc *Cluster) updateReplicationConfig(f func(*config.ReplicationConfig)) {
	r := mc.GetReplicationConfig().Clone()
	f(r)
	mc.SetReplicationConfig(r)
}
