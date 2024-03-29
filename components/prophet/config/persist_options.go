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

package config

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coreos/go-semver/semver"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util/cache"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"go.uber.org/zap"
)

// PersistOptions wraps all configurations that need to persist to storage and
// allows to access them safely.
type PersistOptions struct {
	logger         *zap.Logger
	ttl            *cache.TTLString
	schedule       atomic.Value
	replication    atomic.Value
	labelProperty  atomic.Value
	clusterVersion unsafe.Pointer
}

// NewPersistOptions creates a new PersistOptions instance.
func NewPersistOptions(cfg *Config, logger *zap.Logger) *PersistOptions {
	o := &PersistOptions{}
	o.schedule.Store(&cfg.Schedule)
	o.replication.Store(&cfg.Replication)
	o.labelProperty.Store(cfg.LabelProperty)
	o.logger = log.Adjust(logger)
	o.ttl = nil
	return o
}

// GetScheduleConfig returns scheduling configurations.
func (o *PersistOptions) GetScheduleConfig() *ScheduleConfig {
	return o.schedule.Load().(*ScheduleConfig)
}

// SetScheduleConfig sets the PD scheduling configuration.
func (o *PersistOptions) SetScheduleConfig(cfg *ScheduleConfig) {
	o.schedule.Store(cfg)
}

// GetReplicationConfig returns replication configurations.
func (o *PersistOptions) GetReplicationConfig() *ReplicationConfig {
	return o.replication.Load().(*ReplicationConfig)
}

// SetReplicationConfig sets the PD replication configuration.
func (o *PersistOptions) SetReplicationConfig(cfg *ReplicationConfig) {
	o.replication.Store(cfg)
}

// GetLabelPropertyConfig returns the label property.
func (o *PersistOptions) GetLabelPropertyConfig() LabelPropertyConfig {
	return o.labelProperty.Load().(LabelPropertyConfig)
}

// SetLabelPropertyConfig sets the label property configuration.
func (o *PersistOptions) SetLabelPropertyConfig(cfg LabelPropertyConfig) {
	o.labelProperty.Store(cfg)
}

// GetClusterVersion returns the cluster version.
func (o *PersistOptions) GetClusterVersion() *semver.Version {
	return (*semver.Version)(atomic.LoadPointer(&o.clusterVersion))
}

// SetClusterVersion sets the cluster version.
func (o *PersistOptions) SetClusterVersion(v *semver.Version) {
	atomic.StorePointer(&o.clusterVersion, unsafe.Pointer(v))
}

// CASClusterVersion sets the cluster version.
func (o *PersistOptions) CASClusterVersion(old, new *semver.Version) bool {
	return atomic.CompareAndSwapPointer(&o.clusterVersion, unsafe.Pointer(old), unsafe.Pointer(new))
}

// GetLocationLabels returns the location labels for each resource.
func (o *PersistOptions) GetLocationLabels() []string {
	return o.GetReplicationConfig().LocationLabels
}

// GetIsolationLevel returns the isolation label for each resource.
func (o *PersistOptions) GetIsolationLevel() string {
	return o.GetReplicationConfig().IsolationLevel
}

// IsPlacementRulesEnabled returns if the placement rules is enabled.
func (o *PersistOptions) IsPlacementRulesEnabled() bool {
	return o.GetReplicationConfig().EnablePlacementRules
}

// SetPlacementRuleEnabled set PlacementRuleEnabled
func (o *PersistOptions) SetPlacementRuleEnabled(enabled bool) {
	v := o.GetReplicationConfig().Clone()
	v.EnablePlacementRules = enabled
	o.SetReplicationConfig(v)
}

// GetStrictlyMatchLabel returns whether check label strict.
func (o *PersistOptions) GetStrictlyMatchLabel() bool {
	return o.GetReplicationConfig().StrictlyMatchLabel
}

// GetMaxReplicas returns the number of replicas for each resource.
func (o *PersistOptions) GetMaxReplicas() int {
	return int(o.GetReplicationConfig().MaxReplicas)
}

// SetMaxReplicas sets the number of replicas for each resource.
func (o *PersistOptions) SetMaxReplicas(replicas int) {
	v := o.GetReplicationConfig().Clone()
	v.MaxReplicas = uint64(replicas)
	o.SetReplicationConfig(v)
}

const (
	maxSnapshotCountKey            = "schedule.max-snapshot-count"
	maxMergeShardSizeKey           = "schedule.max-merge-resource-size"
	maxPendingPeerCountKey         = "schedule.max-pending-peer-count"
	maxMergeShardKeysKey           = "schedule.max-merge-resource-keys"
	leaderScheduleLimitKey         = "schedule.leader-schedule-limit"
	resourceScheduleLimitKey       = "schedule.resource-schedule-limit"
	replicaRescheduleLimitKey      = "schedule.replica-schedule-limit"
	mergeScheduleLimitKey          = "schedule.merge-schedule-limit"
	hotShardScheduleLimitKey       = "schedule.hot-resource-schedule-limit"
	schedulerMaxWaitingOperatorKey = "schedule.scheduler-max-waiting-operator"
	enableLocationReplacement      = "schedule.enable-location-replacement"
)

var supportedTTLConfigs = []string{
	maxSnapshotCountKey,
	maxMergeShardSizeKey,
	maxPendingPeerCountKey,
	maxMergeShardKeysKey,
	leaderScheduleLimitKey,
	resourceScheduleLimitKey,
	replicaRescheduleLimitKey,
	mergeScheduleLimitKey,
	hotShardScheduleLimitKey,
	schedulerMaxWaitingOperatorKey,
	enableLocationReplacement,
	"default-add-peer",
	"default-remove-peer",
}

// IsSupportedTTLConfig checks whether a key is a supported config item with ttl
func IsSupportedTTLConfig(key string) bool {
	for _, supportedConfig := range supportedTTLConfigs {
		if key == supportedConfig {
			return true
		}
	}
	return strings.HasPrefix(key, "add-peer-") || strings.HasPrefix(key, "remove-peer-")
}

// GetMaxSnapshotCount returns the number of the max snapshot which is allowed to send.
func (o *PersistOptions) GetMaxSnapshotCount() uint64 {
	return o.getTTLUintOr(maxSnapshotCountKey, o.GetScheduleConfig().MaxSnapshotCount)
}

// GetMaxPendingPeerCount returns the number of the max pending peers.
func (o *PersistOptions) GetMaxPendingPeerCount() uint64 {
	return o.getTTLUintOr(maxPendingPeerCountKey, o.GetScheduleConfig().MaxPendingPeerCount)
}

// GetMaxMergeShardSize returns the max resource size.
func (o *PersistOptions) GetMaxMergeShardSize() uint64 {
	return o.getTTLUintOr(maxMergeShardSizeKey, o.GetScheduleConfig().MaxMergeShardSize)
}

// GetMaxMergeShardKeys returns the max number of keys.
func (o *PersistOptions) GetMaxMergeShardKeys() uint64 {
	return o.getTTLUintOr(maxMergeShardKeysKey, o.GetScheduleConfig().MaxMergeShardKeys)
}

// GetSplitMergeInterval returns the interval between finishing split and starting to merge.
func (o *PersistOptions) GetSplitMergeInterval() time.Duration {
	return o.GetScheduleConfig().SplitMergeInterval.Duration
}

// SetSplitMergeInterval to set the interval between finishing split and starting to merge. It's only used to test.
func (o *PersistOptions) SetSplitMergeInterval(splitMergeInterval time.Duration) {
	v := o.GetScheduleConfig().Clone()
	v.SplitMergeInterval = typeutil.Duration{Duration: splitMergeInterval}
	o.SetScheduleConfig(v)
}

// SetStoreLimit sets a container limit for a given type and rate.
func (o *PersistOptions) SetStoreLimit(containerID uint64, typ limit.Type, ratePerMin float64) {
	v := o.GetScheduleConfig().Clone()
	var sc StoreLimitConfig
	var rate float64
	switch typ {
	case limit.AddPeer:
		if _, ok := v.StoreLimit[containerID]; !ok {
			rate = DefaultStoreLimit.GetDefaultStoreLimit(limit.RemovePeer)
		} else {
			rate = v.StoreLimit[containerID].RemovePeer
		}
		sc = StoreLimitConfig{AddPeer: ratePerMin, RemovePeer: rate}
	case limit.RemovePeer:
		if _, ok := v.StoreLimit[containerID]; !ok {
			rate = DefaultStoreLimit.GetDefaultStoreLimit(limit.AddPeer)
		} else {
			rate = v.StoreLimit[containerID].AddPeer
		}
		sc = StoreLimitConfig{AddPeer: rate, RemovePeer: ratePerMin}
	}
	v.StoreLimit[containerID] = sc
	o.SetScheduleConfig(v)
}

// SetAllStoresLimit sets all container limit for a given type and rate.
func (o *PersistOptions) SetAllStoresLimit(typ limit.Type, ratePerMin float64) {
	v := o.GetScheduleConfig().Clone()
	switch typ {
	case limit.AddPeer:
		DefaultStoreLimit.SetDefaultStoreLimit(limit.AddPeer, ratePerMin)
		for containerID := range v.StoreLimit {
			sc := StoreLimitConfig{AddPeer: ratePerMin, RemovePeer: v.StoreLimit[containerID].RemovePeer}
			v.StoreLimit[containerID] = sc
		}
	case limit.RemovePeer:
		DefaultStoreLimit.SetDefaultStoreLimit(limit.RemovePeer, ratePerMin)
		for containerID := range v.StoreLimit {
			sc := StoreLimitConfig{AddPeer: v.StoreLimit[containerID].AddPeer, RemovePeer: ratePerMin}
			v.StoreLimit[containerID] = sc
		}
	}

	o.SetScheduleConfig(v)
}

// IsOneWayMergeEnabled returns if a resource can only be merged into the next resource of it.
func (o *PersistOptions) IsOneWayMergeEnabled() bool {
	return o.GetScheduleConfig().EnableOneWayMerge
}

// IsCrossTableMergeEnabled returns if across table merge is enabled.
func (o *PersistOptions) IsCrossTableMergeEnabled() bool {
	return o.GetScheduleConfig().EnableCrossTableMerge
}

// GetPatrolShardInterval returns the interval of patrolling resource.
func (o *PersistOptions) GetPatrolShardInterval() time.Duration {
	return o.GetScheduleConfig().PatrolShardInterval.Duration
}

// GetMaxStoreDownTime returns the max down time of a container.
func (o *PersistOptions) GetMaxStoreDownTime() time.Duration {
	return o.GetScheduleConfig().MaxStoreDownTime.Duration
}

// GetLeaderScheduleLimit returns the limit for leader schedule.
func (o *PersistOptions) GetLeaderScheduleLimit() uint64 {
	return o.getTTLUintOr(leaderScheduleLimitKey, o.GetScheduleConfig().LeaderScheduleLimit)
}

// GetShardScheduleLimit returns the limit for resource schedule.
func (o *PersistOptions) GetShardScheduleLimit() uint64 {
	return o.getTTLUintOr(resourceScheduleLimitKey, o.GetScheduleConfig().ShardScheduleLimit)
}

// GetReplicaScheduleLimit returns the limit for replica schedule.
func (o *PersistOptions) GetReplicaScheduleLimit() uint64 {
	return o.getTTLUintOr(replicaRescheduleLimitKey, o.GetScheduleConfig().ReplicaScheduleLimit)
}

// GetMergeScheduleLimit returns the limit for merge schedule.
func (o *PersistOptions) GetMergeScheduleLimit() uint64 {
	return o.getTTLUintOr(mergeScheduleLimitKey, o.GetScheduleConfig().MergeScheduleLimit)
}

// GetHotShardScheduleLimit returns the limit for hot resource schedule.
func (o *PersistOptions) GetHotShardScheduleLimit() uint64 {
	return o.getTTLUintOr(hotShardScheduleLimitKey, o.GetScheduleConfig().HotShardScheduleLimit)
}

// GetStoreLimit returns the limit of a container.
func (o *PersistOptions) GetStoreLimit(containerID uint64) (returnSC StoreLimitConfig) {
	defer func() {
		returnSC.RemovePeer = o.getTTLFloatOr(fmt.Sprintf("remove-peer-%v", containerID), returnSC.RemovePeer)
		returnSC.AddPeer = o.getTTLFloatOr(fmt.Sprintf("add-peer-%v", containerID), returnSC.AddPeer)
	}()
	if limit, ok := o.GetScheduleConfig().StoreLimit[containerID]; ok {
		return limit
	}
	cfg := o.GetScheduleConfig().Clone()
	sc := StoreLimitConfig{
		AddPeer:    DefaultStoreLimit.GetDefaultStoreLimit(limit.AddPeer),
		RemovePeer: DefaultStoreLimit.GetDefaultStoreLimit(limit.RemovePeer),
	}
	v, ok1, err := o.getTTLFloat("default-add-peer")
	if err != nil {
		o.logger.Warn("failed to parse default-add-peer from PersistOptions's ttl storage")
	}
	canSetAddPeer := ok1 && err == nil
	if canSetAddPeer {
		returnSC.AddPeer = v
	}

	v, ok2, err := o.getTTLFloat("default-remove-peer")
	if err != nil {
		o.logger.Warn("failed to parse default-remove-peer from PersistOptions's ttl storage")
	}
	canSetRemovePeer := ok2 && err == nil
	if canSetRemovePeer {
		returnSC.RemovePeer = v
	}

	if canSetAddPeer || canSetRemovePeer {
		return returnSC
	}
	cfg.StoreLimit[containerID] = sc
	o.SetScheduleConfig(cfg)
	return o.GetScheduleConfig().StoreLimit[containerID]
}

// GetStoreLimitByType returns the limit of a container with a given type.
func (o *PersistOptions) GetStoreLimitByType(containerID uint64, typ limit.Type) (returned float64) {
	defer func() {
		if typ == limit.RemovePeer {
			returned = o.getTTLFloatOr(fmt.Sprintf("remove-peer-%v", containerID), returned)
		} else if typ == limit.AddPeer {
			returned = o.getTTLFloatOr(fmt.Sprintf("add-peer-%v", containerID), returned)
		}
	}()
	l := o.GetStoreLimit(containerID)
	switch typ {
	case limit.AddPeer:
		return l.AddPeer
	case limit.RemovePeer:
		return l.RemovePeer
	default:
		panic("no such limit type")
	}
}

// GetAllStoresLimit returns the limit of all containers.
func (o *PersistOptions) GetAllStoresLimit() map[uint64]StoreLimitConfig {
	return o.GetScheduleConfig().StoreLimit
}

// GetStoreLimitMode returns the limit mode of container.
func (o *PersistOptions) GetStoreLimitMode() string {
	return o.GetScheduleConfig().StoreLimitMode
}

// GetTolerantSizeRatio gets the tolerant size ratio.
func (o *PersistOptions) GetTolerantSizeRatio() float64 {
	return o.GetScheduleConfig().TolerantSizeRatio
}

// GetLowSpaceRatio returns the low space ratio.
func (o *PersistOptions) GetLowSpaceRatio() float64 {
	return o.GetScheduleConfig().LowSpaceRatio
}

// GetHighSpaceRatio returns the high space ratio.
func (o *PersistOptions) GetHighSpaceRatio() float64 {
	return o.GetScheduleConfig().HighSpaceRatio
}

// GetShardScoreFormulaVersion returns the formula version config.
func (o *PersistOptions) GetShardScoreFormulaVersion() string {
	return o.GetScheduleConfig().ShardScoreFormulaVersion
}

// GetSchedulerMaxWaitingOperator returns the number of the max waiting operators.
func (o *PersistOptions) GetSchedulerMaxWaitingOperator() uint64 {
	return o.getTTLUintOr(schedulerMaxWaitingOperatorKey, o.GetScheduleConfig().SchedulerMaxWaitingOperator)
}

// GetLeaderSchedulePolicy is to get leader schedule policy.
func (o *PersistOptions) GetLeaderSchedulePolicy() core.SchedulePolicy {
	return core.StringToSchedulePolicy(o.GetScheduleConfig().LeaderSchedulePolicy)
}

// IsRemoveDownReplicaEnabled returns if remove down replica is enabled.
func (o *PersistOptions) IsRemoveDownReplicaEnabled() bool {
	return o.GetScheduleConfig().EnableRemoveDownReplica
}

// IsReplaceOfflineReplicaEnabled returns if replace offline replica is enabled.
func (o *PersistOptions) IsReplaceOfflineReplicaEnabled() bool {
	return o.GetScheduleConfig().EnableReplaceOfflineReplica
}

// IsMakeUpReplicaEnabled returns if make up replica is enabled.
func (o *PersistOptions) IsMakeUpReplicaEnabled() bool {
	return o.GetScheduleConfig().EnableMakeUpReplica
}

// IsRemoveExtraReplicaEnabled returns if remove extra replica is enabled.
func (o *PersistOptions) IsRemoveExtraReplicaEnabled() bool {
	return o.GetScheduleConfig().EnableRemoveExtraReplica
}

// IsLocationReplacementEnabled returns if location replace is enabled.
func (o *PersistOptions) IsLocationReplacementEnabled() bool {
	if v, ok := o.getTTLData(enableLocationReplacement); ok {
		result, err := strconv.ParseBool(v)
		if err == nil {
			return result
		}
		o.logger.Warn("failed to parse " + enableLocationReplacement + " from PersistOptions's ttl storage")
	}
	return o.GetScheduleConfig().EnableLocationReplacement
}

// IsDebugMetricsEnabled returns if debug metrics is enabled.
func (o *PersistOptions) IsDebugMetricsEnabled() bool {
	return o.GetScheduleConfig().EnableDebugMetrics
}

// IsUseJointConsensus returns if using joint consensus as a operator step is enabled.
func (o *PersistOptions) IsUseJointConsensus() bool {
	return o.GetScheduleConfig().EnableJointConsensus
}

// SetEnableJointConsensus sets whether to enable joint-consensus. It's only used to test.
func (o *PersistOptions) SetEnableJointConsensus(enableJointConsensus bool) {
	v := o.GetScheduleConfig().Clone()
	v.EnableJointConsensus = enableJointConsensus
	o.SetScheduleConfig(v)
}

// GetHotShardCacheHitsThreshold is a threshold to decide if a resource is hot.
func (o *PersistOptions) GetHotShardCacheHitsThreshold() int {
	return int(o.GetScheduleConfig().HotShardCacheHitsThreshold)
}

// GetSchedulers gets the scheduler configurations.
func (o *PersistOptions) GetSchedulers() SchedulerConfigs {
	return o.GetScheduleConfig().Schedulers
}

// AddSchedulerCfg adds the scheduler configurations.
func (o *PersistOptions) AddSchedulerCfg(tp string, args []string) {
	v := o.GetScheduleConfig().Clone()
	for i, schedulerCfg := range v.Schedulers {
		// comparing args is to cover the case that there are schedulers in same type but not with same name
		// such as two schedulers of type "evict-leader",
		// one name is "evict-leader-scheduler-1" and the other is "evict-leader-scheduler-2"
		if reflect.DeepEqual(schedulerCfg, SchedulerConfig{Type: tp, Args: args, Disable: false}) {
			return
		}

		if reflect.DeepEqual(schedulerCfg, SchedulerConfig{Type: tp, Args: args, Disable: true}) {
			schedulerCfg.Disable = false
			v.Schedulers[i] = schedulerCfg
			o.SetScheduleConfig(v)
			return
		}
	}
	v.Schedulers = append(v.Schedulers, SchedulerConfig{Type: tp, Args: args, Disable: false})
	o.SetScheduleConfig(v)
}

// SetLabelProperty sets the label property.
func (o *PersistOptions) SetLabelProperty(typ, labelKey, labelValue string) {
	cfg := o.GetLabelPropertyConfig().Clone()
	for _, l := range cfg[typ] {
		if l.Key == labelKey && l.Value == labelValue {
			return
		}
	}
	cfg[typ] = append(cfg[typ], StoreLabel{Key: labelKey, Value: labelValue})
	o.labelProperty.Store(cfg)
}

// DeleteLabelProperty deletes the label property.
func (o *PersistOptions) DeleteLabelProperty(typ, labelKey, labelValue string) {
	cfg := o.GetLabelPropertyConfig().Clone()
	oldLabels := cfg[typ]
	cfg[typ] = []StoreLabel{}
	for _, l := range oldLabels {
		if l.Key == labelKey && l.Value == labelValue {
			continue
		}
		cfg[typ] = append(cfg[typ], l)
	}
	if len(cfg[typ]) == 0 {
		delete(cfg, typ)
	}
	o.labelProperty.Store(cfg)
}

// Persist saves the configuration to the storage.
func (o *PersistOptions) Persist(storage storage.Storage) error {
	cfg := &Config{
		Schedule:      *o.GetScheduleConfig(),
		Replication:   *o.GetReplicationConfig(),
		LabelProperty: o.GetLabelPropertyConfig(),
	}
	return storage.SaveConfig(cfg)
}

// CheckLabelProperty checks the label property.
func (o *PersistOptions) CheckLabelProperty(typ string, labels []metapb.Label) bool {
	pc := o.labelProperty.Load().(LabelPropertyConfig)
	for _, cfg := range pc[typ] {
		for _, l := range labels {
			if l.Key == cfg.Key && l.Value == cfg.Value {
				return true
			}
		}
	}
	return false
}

func (o *PersistOptions) getTTLUint(key string) (uint64, bool, error) {
	stringForm, ok := o.getTTLData(key)
	if !ok {
		return 0, false, nil
	}
	r, err := strconv.ParseUint(stringForm, 10, 64)
	return r, true, err
}

func (o *PersistOptions) getTTLUintOr(key string, defaultValue uint64) uint64 {
	if v, ok, err := o.getTTLUint(key); ok {
		if err == nil {
			return v
		}
		o.logger.Warn("failed to parse " + key + " from PersistOptions's ttl storage")
	}
	return defaultValue
}

func (o *PersistOptions) getTTLFloat(key string) (float64, bool, error) {
	stringForm, ok := o.getTTLData(key)
	if !ok {
		return 0, false, nil
	}
	r, err := strconv.ParseFloat(stringForm, 64)
	return r, true, err
}

func (o *PersistOptions) getTTLFloatOr(key string, defaultValue float64) float64 {
	if v, ok, err := o.getTTLFloat(key); ok {
		if err == nil {
			return v
		}
		o.logger.Warn("failed to parse " + key + " from PersistOptions's ttl storage")
	}
	return defaultValue
}

func (o *PersistOptions) getTTLData(key string) (string, bool) {
	if o.ttl == nil {
		return "", false
	}
	if result, ok := o.ttl.Get(key); ok {
		return result.(string), ok
	}
	return "", false
}
