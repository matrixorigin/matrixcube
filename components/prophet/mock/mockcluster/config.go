// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mockcluster

import (
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
)

// SetMaxMergeShardSize updates the MaxMergeShardSize configuration.
func (mc *Cluster) SetMaxMergeShardSize(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.MaxMergeShardSize = uint64(v) })
}

// SetMaxMergeShardKeys updates the MaxMergeShardKeys configuration.
func (mc *Cluster) SetMaxMergeShardKeys(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.MaxMergeShardKeys = uint64(v) })
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

// SetShardScoreFormulaVersion updates the ShardScoreFormulaVersion configuration.
func (mc *Cluster) SetShardScoreFormulaVersion(v string) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.ShardScoreFormulaVersion = v })
}

// SetLeaderScheduleLimit updates the LeaderScheduleLimit configuration.
func (mc *Cluster) SetLeaderScheduleLimit(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.LeaderScheduleLimit = uint64(v) })
}

// SetShardScheduleLimit updates the ShardScheduleLimit configuration.
func (mc *Cluster) SetShardScheduleLimit(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.ShardScheduleLimit = uint64(v) })
}

// SetMergeScheduleLimit updates the MergeScheduleLimit configuration.
func (mc *Cluster) SetMergeScheduleLimit(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.MergeScheduleLimit = uint64(v) })
}

// SetHotShardScheduleLimit updates the HotShardScheduleLimit configuration.
func (mc *Cluster) SetHotShardScheduleLimit(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.HotShardScheduleLimit = uint64(v) })
}

// SetHotShardCacheHitsThreshold updates the HotShardCacheHitsThreshold configuration.
func (mc *Cluster) SetHotShardCacheHitsThreshold(v int) {
	mc.updateScheduleConfig(func(s *config.ScheduleConfig) { s.HotShardCacheHitsThreshold = uint64(v) })
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
