package core

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
)

const (
	// Interval to save container meta (including heartbeat ts) to etcd.
	containerPersistInterval = 5 * time.Minute
	mb                       = 1 << 20 // megabyte
	gb                       = 1 << 30 // 1GB size
	initialMaxResourceCounts = 30      // exclude storage Threshold Filter when resource less than 30
	initialMinSpace          = 1 << 33 // 2^3=8GB
)

// CachedContainer is the container runtime info cached in the cache
type CachedContainer struct {
	Meta metadata.Container
	*containerStats

	pauseLeaderTransfer bool // not allow to be used as source or target of transfer leader
	leaderCount         int
	resourceCount       int
	leaderSize          int64
	resourceSize        int64
	pendingPeerCount    int
	lastPersistTime     time.Time
	leaderWeight        float64
	resourceWeight      float64
	available           map[limit.Type]func() bool
}

// NewCachedContainer creates CachedContainer with meta data.
func NewCachedContainer(meta metadata.Container, opts ...ContainerCreateOption) *CachedContainer {
	container := &CachedContainer{
		Meta:           meta,
		containerStats: newContainerStats(),
		leaderWeight:   1.0,
		resourceWeight: 1.0,
	}
	for _, opt := range opts {
		opt(container)
	}
	return container
}

// Clone creates a copy of current CachedContainer.
func (cr *CachedContainer) Clone(opts ...ContainerCreateOption) *CachedContainer {
	container := &CachedContainer{
		Meta:                cr.Meta.Clone(),
		containerStats:      cr.containerStats,
		pauseLeaderTransfer: cr.pauseLeaderTransfer,
		leaderCount:         cr.leaderCount,
		resourceCount:       cr.resourceCount,
		leaderSize:          cr.leaderSize,
		resourceSize:        cr.resourceSize,
		pendingPeerCount:    cr.pendingPeerCount,
		lastPersistTime:     cr.lastPersistTime,
		leaderWeight:        cr.leaderWeight,
		resourceWeight:      cr.resourceWeight,
		available:           cr.available,
	}

	for _, opt := range opts {
		opt(container)
	}
	return container
}

// ShallowClone creates a copy of current CachedContainer, but not clone 'Meta'.
func (cr *CachedContainer) ShallowClone(opts ...ContainerCreateOption) *CachedContainer {
	container := &CachedContainer{
		Meta:                cr.Meta,
		containerStats:      cr.containerStats,
		pauseLeaderTransfer: cr.pauseLeaderTransfer,
		leaderCount:         cr.leaderCount,
		resourceCount:       cr.resourceCount,
		leaderSize:          cr.leaderSize,
		resourceSize:        cr.resourceSize,
		pendingPeerCount:    cr.pendingPeerCount,
		lastPersistTime:     cr.lastPersistTime,
		leaderWeight:        cr.leaderWeight,
		resourceWeight:      cr.resourceWeight,
		available:           cr.available,
	}

	for _, opt := range opts {
		opt(container)
	}
	return container
}

// AllowLeaderTransfer returns if the container is allowed to be selected
// as source or target of transfer leader.
func (cr *CachedContainer) AllowLeaderTransfer() bool {
	return !cr.pauseLeaderTransfer
}

// IsAvailable returns if the container bucket of limitation is available
func (cr *CachedContainer) IsAvailable(limitType limit.Type) bool {
	if cr.available != nil && cr.available[limitType] != nil {
		return cr.available[limitType]()
	}
	return true
}

// IsUp checks if the container's state is Up.
func (cr *CachedContainer) IsUp() bool {
	return cr.GetState() == metapb.ContainerState_UP
}

// IsOffline checks if the container's state is Offline.
func (cr *CachedContainer) IsOffline() bool {
	return cr.GetState() == metapb.ContainerState_Offline
}

// IsTombstone checks if the container's state is Tombstone.
func (cr *CachedContainer) IsTombstone() bool {
	return cr.GetState() == metapb.ContainerState_Tombstone
}

// IsPhysicallyDestroyed checks if the store's physically destroyed.
func (cr *CachedContainer) IsPhysicallyDestroyed() bool {
	return cr.Meta.PhysicallyDestroyed()
}

// DownTime returns the time elapsed since last heartbeat.
func (cr *CachedContainer) DownTime() time.Duration {
	return time.Since(cr.GetLastHeartbeatTS())
}

// GetState returns the state of the container.
func (cr *CachedContainer) GetState() metapb.ContainerState {
	return cr.Meta.State()
}

// GetLeaderCount returns the leader count of the container.
func (cr *CachedContainer) GetLeaderCount() int {
	return cr.leaderCount
}

// GetResourceCount returns the Resource count of the container.
func (cr *CachedContainer) GetResourceCount() int {
	return cr.resourceCount
}

// GetLeaderSize returns the leader size of the container.
func (cr *CachedContainer) GetLeaderSize() int64 {
	return cr.leaderSize
}

// GetResourceSize returns the Resource size of the container.
func (cr *CachedContainer) GetResourceSize() int64 {
	return cr.resourceSize
}

// GetPendingPeerCount returns the pending peer count of the container.
func (cr *CachedContainer) GetPendingPeerCount() int {
	return cr.pendingPeerCount
}

// GetLeaderWeight returns the leader weight of the container.
func (cr *CachedContainer) GetLeaderWeight() float64 {
	return cr.leaderWeight
}

// GetResourceWeight returns the Resource weight of the container.
func (cr *CachedContainer) GetResourceWeight() float64 {
	return cr.resourceWeight
}

// GetLastHeartbeatTS returns the last heartbeat timestamp of the container.
func (cr *CachedContainer) GetLastHeartbeatTS() time.Time {
	return time.Unix(0, cr.Meta.LastHeartbeat())
}

// NeedPersist returns if it needs to save to etcd.
func (cr *CachedContainer) NeedPersist() bool {
	return cr.GetLastHeartbeatTS().Sub(cr.lastPersistTime) > containerPersistInterval
}

const minWeight = 1e-6
const maxScore = 1024 * 1024 * 1024

// LeaderScore returns the container's leader score.
func (cr *CachedContainer) LeaderScore(policy SchedulePolicy, delta int64) float64 {
	switch policy {
	case BySize:
		return float64(cr.GetLeaderSize()+delta) / math.Max(cr.GetLeaderWeight(), minWeight)
	case ByCount:
		return float64(int64(cr.GetLeaderCount())+delta) / math.Max(cr.GetLeaderWeight(), minWeight)
	default:
		return 0
	}
}

// ResourceScore returns the container's resource score.
// Deviation It is used to control the direction of the deviation considered
// when calculating the resource score. It is set to -1 when it is the source
// container of balance, 1 when it is the target, and 0 in the rest of cases.
func (cr *CachedContainer) ResourceScore(version string, highSpaceRatio, lowSpaceRatio float64, delta int64, deviation int) float64 {
	switch version {
	case "v2":
		return cr.resourceScoreV2(delta, deviation, lowSpaceRatio)
	case "v1":
		fallthrough
	default:
		return cr.resourceScoreV1(highSpaceRatio, lowSpaceRatio, delta)
	}
}

func (cr *CachedContainer) resourceScoreV1(highSpaceRatio, lowSpaceRatio float64, delta int64) float64 {
	var score float64
	var amplification float64
	available := float64(cr.GetAvailable()) / mb
	used := float64(cr.GetUsedSize()) / mb
	capacity := float64(cr.GetCapacity()) / mb

	if cr.GetResourceSize() == 0 || used == 0 {
		amplification = 1
	} else {
		// because of db compression, resource size is larger than actual used size
		amplification = float64(cr.GetResourceSize()) / used
	}

	// highSpaceBound is the lower bound of the high space stage.
	highSpaceBound := (1 - highSpaceRatio) * capacity
	// lowSpaceBound is the upper bound of the low space stage.
	lowSpaceBound := (1 - lowSpaceRatio) * capacity
	if available-float64(delta)/amplification >= highSpaceBound {
		score = float64(cr.GetResourceSize() + delta)
	} else if available-float64(delta)/amplification <= lowSpaceBound {
		score = maxScore - (available - float64(delta)/amplification)
	} else {
		// to make the score function continuous, we use linear function y = k * x + b as transition period
		// from above we know that there are two points must on the function image
		// note that it is possible that other irrelative files occupy a lot of storage, so capacity == available + used + irrelative
		// and we regarded irrelative as a fixed value.
		// Then amp = size / used = size / (capacity - irrelative - available)
		//
		// When available == highSpaceBound,
		// we can conclude that size = (capacity - irrelative - highSpaceBound) * amp = (used + available - highSpaceBound) * amp
		// Similarly, when available == lowSpaceBound,
		// we can conclude that size = (capacity - irrelative - lowSpaceBound) * amp = (used + available - lowSpaceBound) * amp
		// These are the two fixed points' x-coordinates, and y-coordinates which can be easily obtained from the above two functions.
		x1, y1 := (used+available-highSpaceBound)*amplification, (used+available-highSpaceBound)*amplification
		x2, y2 := (used+available-lowSpaceBound)*amplification, maxScore-lowSpaceBound

		k := (y2 - y1) / (x2 - x1)
		b := y1 - k*x1
		score = k*float64(cr.GetResourceSize()+delta) + b
	}

	return score / math.Max(cr.GetResourceWeight(), minWeight)
}

func (cr *CachedContainer) resourceScoreV2(delta int64, deviation int, lowSpaceRatio float64) float64 {
	A := float64(float64(cr.GetAvgAvailable())-float64(deviation)*float64(cr.GetAvailableDeviation())) / gb
	C := float64(cr.GetCapacity()) / gb
	R := float64(cr.GetResourceSize() + delta)
	var (
		K, M float64 = 1, 256 // Experience value to control the weight of the available influence on score
		F    float64 = 50     // Experience value to prevent some nodes from running out of disk space prematurely.
		B            = 1e7
	)
	F = math.Max(F, C*(1-lowSpaceRatio))
	var score float64
	if A >= C || C < 1 {
		score = R
	} else if A > F {
		// As the amount of data increases (available becomes smaller), the weight of resource size on total score
		// increases. Ideally, all nodes converge at the position where remaining space is F (default 20GiB).
		score = (K + M*(math.Log(C)-math.Log(A-F+1))/(C-A+F-1)) * R
	} else {
		// When remaining space is less then F, the score is mainly determined by available space.
		// store's score will increase rapidly after it has few space. and it will reach similar score when they has no space
		score = (K+M*math.Log(C)/C)*R + B*(F-A)/F
	}
	return score / math.Max(cr.GetResourceWeight(), minWeight)
}

// StorageSize returns container's used storage size reported from your storage.
func (cr *CachedContainer) StorageSize() uint64 {
	return cr.GetUsedSize()
}

// AvailableRatio is container's freeSpace/capacity.
func (cr *CachedContainer) AvailableRatio() float64 {
	if cr.GetCapacity() == 0 {
		return 0
	}
	return float64(cr.GetAvailable()) / float64(cr.GetCapacity())
}

// IsLowSpace checks if the container is lack of space.
func (cr *CachedContainer) IsLowSpace(lowSpaceRatio float64) bool {
	if cr.GetContainerStats() == nil {
		return false
	}
	// issue #3444
	if cr.resourceCount < initialMaxResourceCounts && cr.GetAvailable() > initialMinSpace {
		return false
	}
	return cr.AvailableRatio() < 1-lowSpaceRatio
}

// ResourceCount returns count of leader/resource-replica in the container.
func (cr *CachedContainer) ResourceCount(kind metapb.ResourceKind) uint64 {
	switch kind {
	case metapb.ResourceKind_LeaderKind:
		return uint64(cr.GetLeaderCount())
	case metapb.ResourceKind_ReplicaKind:
		return uint64(cr.GetResourceCount())
	default:
		return 0
	}
}

// ResourceSize returns size of leader/resource-replica in the container
func (cr *CachedContainer) ResourceSize(kind metapb.ResourceKind) int64 {
	switch kind {
	case metapb.ResourceKind_LeaderKind:
		return cr.GetLeaderSize()
	case metapb.ResourceKind_ReplicaKind:
		return cr.GetResourceSize()
	default:
		return 0
	}
}

// ResourceWeight returns weight of leader/resource-replica in the score
func (cr *CachedContainer) ResourceWeight(kind metapb.ResourceKind) float64 {
	switch kind {
	case metapb.ResourceKind_LeaderKind:
		leaderWeight := cr.GetLeaderWeight()
		if leaderWeight <= 0 {
			return minWeight
		}
		return leaderWeight
	case metapb.ResourceKind_ReplicaKind:
		resourceWeight := cr.GetResourceWeight()
		if resourceWeight <= 0 {
			return minWeight
		}
		return resourceWeight
	default:
		return 0
	}
}

// GetStartTime returns the start timestamp.
func (cr *CachedContainer) GetStartTime() time.Time {
	return time.Unix(cr.Meta.StartTimestamp(), 0)
}

// GetUptime returns the uptime.
func (cr *CachedContainer) GetUptime() time.Duration {
	uptime := cr.GetLastHeartbeatTS().Sub(cr.GetStartTime())
	if uptime > 0 {
		return uptime
	}
	return 0
}

var (
	// If a container's last heartbeat is containerDisconnectDuration ago, the container will
	// be marked as disconnected state. The value should be greater than storage application's
	// container heartbeat interval (default 10s).
	containerDisconnectDuration = 20 * time.Second
	containerUnhealthyDuration  = 10 * time.Minute
)

// IsDisconnected checks if a container is disconnected, which means Prophet misses
// storage application's container heartbeat for a short time, maybe caused by process restart or
// temporary network failure.
func (cr *CachedContainer) IsDisconnected() bool {
	return cr.DownTime() > containerDisconnectDuration
}

// IsUnhealthy checks if a container is unhealthy.
func (cr *CachedContainer) IsUnhealthy() bool {
	return cr.DownTime() > containerUnhealthyDuration
}

// GetLabelValue returns a label's value (if exists).
func (cr *CachedContainer) GetLabelValue(key string) string {
	for _, label := range cr.Meta.Labels() {
		if strings.EqualFold(label.GetKey(), key) {
			return label.GetValue()
		}
	}
	return ""
}

// CompareLocation compares 2 containers' labels and returns at which level their
// locations are different. It returns -1 if they are at the same location.
func (cr *CachedContainer) CompareLocation(other *CachedContainer, labels []string) int {
	for i, key := range labels {
		v1, v2 := cr.GetLabelValue(key), other.GetLabelValue(key)
		// If label is not set, the container is considered at the same location
		// with any other container.
		if v1 != "" && v2 != "" && !strings.EqualFold(v1, v2) {
			return i
		}
	}
	return -1
}

const replicaBaseScore = 100

// DistinctScore returns the score that the other is distinct from the containers.
// A higher score means the other container is more different from the existed containers.
func DistinctScore(labels []string, containers []*CachedContainer, other *CachedContainer) float64 {
	var score float64
	for _, s := range containers {
		if s.Meta.ID() == other.Meta.ID() {
			continue
		}
		if index := s.CompareLocation(other, labels); index != -1 {
			score += math.Pow(replicaBaseScore, float64(len(labels)-index-1))
		}
	}
	return score
}

// MergeLabels merges the passed in labels with origins, overriding duplicated
// ones.
func (cr *CachedContainer) MergeLabels(labels []metapb.Pair) []metapb.Pair {
	containerLabels := cr.Meta.Labels()
L:
	for _, newLabel := range labels {
		for _, label := range containerLabels {
			if strings.EqualFold(label.Key, newLabel.Key) {
				label.Value = newLabel.Value
				continue L
			}
		}
		containerLabels = append(containerLabels, newLabel)
	}
	res := containerLabels[:0]
	for _, l := range containerLabels {
		if l.Value != "" {
			res = append(res, l)
		}
	}
	return res
}

// CachedContainers contains information about all container.
type CachedContainers struct {
	containers map[uint64]*CachedContainer
}

// NewCachedContainers create a CachedContainer with map of containerID to CachedContainer
func NewCachedContainers() *CachedContainers {
	return &CachedContainers{
		containers: make(map[uint64]*CachedContainer),
	}
}

// GetContainer returns a copy of the CachedContainer with the specified containerID.
func (s *CachedContainers) GetContainer(containerID uint64) *CachedContainer {
	container, ok := s.containers[containerID]
	if !ok {
		return nil
	}
	return container
}

// TakeContainer returns the point of the origin CachedContainer with the specified containerID.
func (s *CachedContainers) TakeContainer(containerID uint64) *CachedContainer {
	container, ok := s.containers[containerID]
	if !ok {
		return nil
	}
	return container
}

// SetContainer sets a CachedContainer with containerID.
func (s *CachedContainers) SetContainer(container *CachedContainer) {
	s.containers[container.Meta.ID()] = container
}

// PauseLeaderTransfer pauses a CachedContainer with containerID.
func (s *CachedContainers) PauseLeaderTransfer(containerID uint64) error {
	container, ok := s.containers[containerID]
	if !ok {
		return fmt.Errorf("container %d not found", containerID)
	}
	if !container.AllowLeaderTransfer() {
		return fmt.Errorf("container %d pause transfer leader", containerID)
	}
	s.containers[containerID] = container.Clone(PauseLeaderTransfer())
	return nil
}

// ResumeLeaderTransfer cleans a container's pause state. The container can be selected
// as source or target of TransferLeader again.
func (s *CachedContainers) ResumeLeaderTransfer(containerID uint64) {
	container, ok := s.containers[containerID]
	if !ok {
		util.GetLogger().Fatalf("try to clean a container %d pause state, but it is not found",
			containerID)
	}
	s.containers[containerID] = container.Clone(ResumeLeaderTransfer())
}

// AttachAvailableFunc attaches f to a specific container.
func (s *CachedContainers) AttachAvailableFunc(containerID uint64, limitType limit.Type, f func() bool) {
	if container, ok := s.containers[containerID]; ok {
		s.containers[containerID] = container.Clone(AttachAvailableFunc(limitType, f))
	}
}

// GetContainers gets a complete set of CachedContainer.
func (s *CachedContainers) GetContainers() []*CachedContainer {
	containers := make([]*CachedContainer, 0, len(s.containers))
	for _, container := range s.containers {
		containers = append(containers, container)
	}
	return containers
}

// GetMetaContainers gets a complete set of metadata.Container
func (s *CachedContainers) GetMetaContainers() []metadata.Container {
	metas := make([]metadata.Container, 0, len(s.containers))
	for _, container := range s.containers {
		metas = append(metas, container.Meta)
	}
	return metas
}

// DeleteContainer deletes tombstone record form container
func (s *CachedContainers) DeleteContainer(container *CachedContainer) {
	delete(s.containers, container.Meta.ID())
}

// GetContainerCount returns the total count of CachedContainer.
func (s *CachedContainers) GetContainerCount() int {
	return len(s.containers)
}

// SetLeaderCount sets the leader count to a CachedContainer.
func (s *CachedContainers) SetLeaderCount(containerID uint64, leaderCount int) {
	if container, ok := s.containers[containerID]; ok {
		s.containers[containerID] = container.Clone(SetLeaderCount(leaderCount))
	}
}

// SetResourceCount sets the resource count to a CachedContainer.
func (s *CachedContainers) SetResourceCount(containerID uint64, resourceCount int) {
	if container, ok := s.containers[containerID]; ok {
		s.containers[containerID] = container.Clone(SetResourceCount(resourceCount))
	}
}

// SetPendingPeerCount sets the pending count to a CachedContainer.
func (s *CachedContainers) SetPendingPeerCount(containerID uint64, pendingPeerCount int) {
	if container, ok := s.containers[containerID]; ok {
		s.containers[containerID] = container.Clone(SetPendingPeerCount(pendingPeerCount))
	}
}

// SetLeaderSize sets the leader size to a CachedContainer.
func (s *CachedContainers) SetLeaderSize(containerID uint64, leaderSize int64) {
	if container, ok := s.containers[containerID]; ok {
		s.containers[containerID] = container.Clone(SetLeaderSize(leaderSize))
	}
}

// SetResourceSize sets the resource size to a CachedContainer.
func (s *CachedContainers) SetResourceSize(containerID uint64, resourceSize int64) {
	if container, ok := s.containers[containerID]; ok {
		s.containers[containerID] = container.Clone(SetResourceSize(resourceSize))
	}
}

// UpdateContainerStatus updates the information of the container.
func (s *CachedContainers) UpdateContainerStatus(containerID uint64, leaderCount int, resourceCount int, pendingPeerCount int, leaderSize int64, resourceSize int64) {
	if container, ok := s.containers[containerID]; ok {
		newContainer := container.ShallowClone(SetLeaderCount(leaderCount),
			SetResourceCount(resourceCount),
			SetPendingPeerCount(pendingPeerCount),
			SetLeaderSize(leaderSize),
			SetResourceSize(resourceSize))
		s.SetContainer(newContainer)
	}
}
