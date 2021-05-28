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
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/cache"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
)

// PersistOptions wraps all configurations that need to persist to storage and
// allows to access them safely.
type PersistOptions struct {
	ttl            *cache.TTLString
	schedule       atomic.Value
	replication    atomic.Value
	labelProperty  atomic.Value
	clusterVersion unsafe.Pointer
}

// NewPersistOptions creates a new PersistOptions instance.
func NewPersistOptions(cfg *Config) *PersistOptions {
	o := &PersistOptions{}
	o.schedule.Store(&cfg.Schedule)
	o.replication.Store(&cfg.Replication)
	o.labelProperty.Store(cfg.LabelProperty)
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
	maxMergeResourceSizeKey        = "schedule.max-merge-resource-size"
	maxPendingPeerCountKey         = "schedule.max-pending-peer-count"
	maxMergeResourceKeysKey        = "schedule.max-merge-resource-keys"
	leaderScheduleLimitKey         = "schedule.leader-schedule-limit"
	resourceScheduleLimitKey       = "schedule.resource-schedule-limit"
	replicaRescheduleLimitKey      = "schedule.replica-schedule-limit"
	mergeScheduleLimitKey          = "schedule.merge-schedule-limit"
	hotResourceScheduleLimitKey    = "schedule.hot-resource-schedule-limit"
	schedulerMaxWaitingOperatorKey = "schedule.scheduler-max-waiting-operator"
	enableLocationReplacement      = "schedule.enable-location-replacement"
)

var supportedTTLConfigs = []string{
	maxSnapshotCountKey,
	maxMergeResourceSizeKey,
	maxPendingPeerCountKey,
	maxMergeResourceKeysKey,
	leaderScheduleLimitKey,
	resourceScheduleLimitKey,
	replicaRescheduleLimitKey,
	mergeScheduleLimitKey,
	hotResourceScheduleLimitKey,
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

// GetMaxMergeResourceSize returns the max resource size.
func (o *PersistOptions) GetMaxMergeResourceSize() uint64 {
	return o.getTTLUintOr(maxMergeResourceSizeKey, o.GetScheduleConfig().MaxMergeResourceSize)
}

// GetMaxMergeResourceKeys returns the max number of keys.
func (o *PersistOptions) GetMaxMergeResourceKeys() uint64 {
	return o.getTTLUintOr(maxMergeResourceKeysKey, o.GetScheduleConfig().MaxMergeResourceKeys)
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

// SetContainerLimit sets a container limit for a given type and rate.
func (o *PersistOptions) SetContainerLimit(containerID uint64, typ limit.Type, ratePerMin float64) {
	v := o.GetScheduleConfig().Clone()
	var sc ContainerLimitConfig
	var rate float64
	switch typ {
	case limit.AddPeer:
		if _, ok := v.ContainerLimit[containerID]; !ok {
			rate = DefaultContainerLimit.GetDefaultContainerLimit(limit.RemovePeer)
		} else {
			rate = v.ContainerLimit[containerID].RemovePeer
		}
		sc = ContainerLimitConfig{AddPeer: ratePerMin, RemovePeer: rate}
	case limit.RemovePeer:
		if _, ok := v.ContainerLimit[containerID]; !ok {
			rate = DefaultContainerLimit.GetDefaultContainerLimit(limit.AddPeer)
		} else {
			rate = v.ContainerLimit[containerID].AddPeer
		}
		sc = ContainerLimitConfig{AddPeer: rate, RemovePeer: ratePerMin}
	}
	v.ContainerLimit[containerID] = sc
	o.SetScheduleConfig(v)
}

// SetAllContainersLimit sets all container limit for a given type and rate.
func (o *PersistOptions) SetAllContainersLimit(typ limit.Type, ratePerMin float64) {
	v := o.GetScheduleConfig().Clone()
	switch typ {
	case limit.AddPeer:
		DefaultContainerLimit.SetDefaultContainerLimit(limit.AddPeer, ratePerMin)
		for containerID := range v.ContainerLimit {
			sc := ContainerLimitConfig{AddPeer: ratePerMin, RemovePeer: v.ContainerLimit[containerID].RemovePeer}
			v.ContainerLimit[containerID] = sc
		}
	case limit.RemovePeer:
		DefaultContainerLimit.SetDefaultContainerLimit(limit.RemovePeer, ratePerMin)
		for containerID := range v.ContainerLimit {
			sc := ContainerLimitConfig{AddPeer: v.ContainerLimit[containerID].AddPeer, RemovePeer: ratePerMin}
			v.ContainerLimit[containerID] = sc
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

// GetPatrolResourceInterval returns the interval of patrolling resource.
func (o *PersistOptions) GetPatrolResourceInterval() time.Duration {
	return o.GetScheduleConfig().PatrolResourceInterval.Duration
}

// GetMaxContainerDownTime returns the max down time of a container.
func (o *PersistOptions) GetMaxContainerDownTime() time.Duration {
	return o.GetScheduleConfig().MaxContainerDownTime.Duration
}

// GetLeaderScheduleLimit returns the limit for leader schedule.
func (o *PersistOptions) GetLeaderScheduleLimit() uint64 {
	return o.getTTLUintOr(leaderScheduleLimitKey, o.GetScheduleConfig().LeaderScheduleLimit)
}

// GetResourceScheduleLimit returns the limit for resource schedule.
func (o *PersistOptions) GetResourceScheduleLimit() uint64 {
	return o.getTTLUintOr(resourceScheduleLimitKey, o.GetScheduleConfig().ResourceScheduleLimit)
}

// GetReplicaScheduleLimit returns the limit for replica schedule.
func (o *PersistOptions) GetReplicaScheduleLimit() uint64 {
	return o.getTTLUintOr(replicaRescheduleLimitKey, o.GetScheduleConfig().ReplicaScheduleLimit)
}

// GetMergeScheduleLimit returns the limit for merge schedule.
func (o *PersistOptions) GetMergeScheduleLimit() uint64 {
	return o.getTTLUintOr(mergeScheduleLimitKey, o.GetScheduleConfig().MergeScheduleLimit)
}

// GetHotResourceScheduleLimit returns the limit for hot resource schedule.
func (o *PersistOptions) GetHotResourceScheduleLimit() uint64 {
	return o.getTTLUintOr(hotResourceScheduleLimitKey, o.GetScheduleConfig().HotResourceScheduleLimit)
}

// GetContainerLimit returns the limit of a container.
func (o *PersistOptions) GetContainerLimit(containerID uint64) (returnSC ContainerLimitConfig) {
	defer func() {
		returnSC.RemovePeer = o.getTTLFloatOr(fmt.Sprintf("remove-peer-%v", containerID), returnSC.RemovePeer)
		returnSC.AddPeer = o.getTTLFloatOr(fmt.Sprintf("add-peer-%v", containerID), returnSC.AddPeer)
	}()
	if limit, ok := o.GetScheduleConfig().ContainerLimit[containerID]; ok {
		return limit
	}
	cfg := o.GetScheduleConfig().Clone()
	sc := ContainerLimitConfig{
		AddPeer:    DefaultContainerLimit.GetDefaultContainerLimit(limit.AddPeer),
		RemovePeer: DefaultContainerLimit.GetDefaultContainerLimit(limit.RemovePeer),
	}
	v, ok1, err := o.getTTLFloat("default-add-peer")
	if err != nil {
		util.GetLogger().Warning("failed to parse default-add-peer from PersistOptions's ttl storage")
	}
	canSetAddPeer := ok1 && err == nil
	if canSetAddPeer {
		returnSC.AddPeer = v
	}

	v, ok2, err := o.getTTLFloat("default-remove-peer")
	if err != nil {
		util.GetLogger().Warning("failed to parse default-remove-peer from PersistOptions's ttl storage")
	}
	canSetRemovePeer := ok2 && err == nil
	if canSetRemovePeer {
		returnSC.RemovePeer = v
	}

	if canSetAddPeer || canSetRemovePeer {
		return returnSC
	}
	cfg.ContainerLimit[containerID] = sc
	o.SetScheduleConfig(cfg)
	return o.GetScheduleConfig().ContainerLimit[containerID]
}

// GetContainerLimitByType returns the limit of a container with a given type.
func (o *PersistOptions) GetContainerLimitByType(containerID uint64, typ limit.Type) (returned float64) {
	defer func() {
		if typ == limit.RemovePeer {
			returned = o.getTTLFloatOr(fmt.Sprintf("remove-peer-%v", containerID), returned)
		} else if typ == limit.AddPeer {
			returned = o.getTTLFloatOr(fmt.Sprintf("add-peer-%v", containerID), returned)
		}
	}()
	l := o.GetContainerLimit(containerID)
	switch typ {
	case limit.AddPeer:
		return l.AddPeer
	case limit.RemovePeer:
		return l.RemovePeer
	default:
		panic("no such limit type")
	}
}

// GetAllContainersLimit returns the limit of all containers.
func (o *PersistOptions) GetAllContainersLimit() map[uint64]ContainerLimitConfig {
	return o.GetScheduleConfig().ContainerLimit
}

// GetContainerLimitMode returns the limit mode of container.
func (o *PersistOptions) GetContainerLimitMode() string {
	return o.GetScheduleConfig().ContainerLimitMode
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

// GetResourceScoreFormulaVersion returns the formula version config.
func (o *PersistOptions) GetResourceScoreFormulaVersion() string {
	return o.GetScheduleConfig().ResourceScoreFormulaVersion
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
		util.GetLogger().Warning("failed to parse " + enableLocationReplacement + " from PersistOptions's ttl storage")
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

// GetHotResourceCacheHitsThreshold is a threshold to decide if a resource is hot.
func (o *PersistOptions) GetHotResourceCacheHitsThreshold() int {
	return int(o.GetScheduleConfig().HotResourceCacheHitsThreshold)
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
	cfg[typ] = append(cfg[typ], ContainerLabel{Key: labelKey, Value: labelValue})
	o.labelProperty.Store(cfg)
}

// DeleteLabelProperty deletes the label property.
func (o *PersistOptions) DeleteLabelProperty(typ, labelKey, labelValue string) {
	cfg := o.GetLabelPropertyConfig().Clone()
	oldLabels := cfg[typ]
	cfg[typ] = []ContainerLabel{}
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
func (o *PersistOptions) CheckLabelProperty(typ string, labels []metapb.Pair) bool {
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
		util.GetLogger().Warning("failed to parse " + key + " from PersistOptions's ttl storage")
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
		util.GetLogger().Warning("failed to parse " + key + " from PersistOptions's ttl storage")
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
