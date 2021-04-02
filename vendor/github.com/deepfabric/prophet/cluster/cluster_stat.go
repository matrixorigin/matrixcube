package cluster

import (
	"strings"
	"sync"
	"time"

	"github.com/deepfabric/prophet/pb/rpcpb"
	"github.com/deepfabric/prophet/util"
	"github.com/deepfabric/prophet/util/movingaverage"
	"github.com/deepfabric/prophet/util/slice"
)

// Cluster State Statistics
//
// The target of cluster state statistics is to statistic the load state
// of a cluster given a time duration. The basic idea is to collect all
// the load information from every container at the same time duration and calculates
// the load for the whole cluster.
//
// Now we just support CPU as the measurement of the load. The CPU information
// is reported by each container with a heartbeat message which sending to PD every
// interval(10s). There is no synchronization between each container, so the containers
// could not send heartbeat messages at the same time, and the collected
// information has time shift.
//
// The diagram below demonstrates the time shift. "|" indicates the latest
// heartbeat.
//
// S1 ------------------------------|---------------------->
// S2 ---------------------------|------------------------->
// S3 ---------------------------------|------------------->
//
// The max time shift between 2 containers is 2*interval which is 20s here, and
// this is also the max time shift for the whole cluster. We assume that the
// time of starting to heartbeat is randomized, so the average time shift of
// the cluster is 10s. This is acceptable for statistics.
//
// Implementation
//
// Keep a 5min history statistics for each container, the history is saved in a
// circle array which evicting the oldest entry in a FIFO strategy. All the
// containers' histories combines into the cluster's history. So we can calculate
// any load value within 5 minutes. The algorithm for calculate is simple,
// Iterate each container's history from the latest entry with the same step and
// calculate the average CPU usage for the cluster.
//
// For example.
// To calculate the average load of the cluster within 3 minutes, start from the
// tail of circle array(which containers the history), and backward 18 steps to
// collect all the statistics that being accessed, then calculates the average
// CPU usage for this container. The average of all the containers CPU usage is the
// CPU usage of the whole cluster.
//

// LoadState indicates the load of a cluster or container
type LoadState int

// LoadStates that supported, None means no state determined
const (
	LoadStateNone LoadState = iota
	LoadStateIdle
	LoadStateLow
	LoadStateNormal
	LoadStateHigh
)

// String representation of LoadState
func (s LoadState) String() string {
	switch s {
	case LoadStateIdle:
		return "idle"
	case LoadStateLow:
		return "low"
	case LoadStateNormal:
		return "normal"
	case LoadStateHigh:
		return "high"
	}
	return "none"
}

// ThreadsCollected filters the threads to take into
// the calculation of CPU usage.
var ThreadsCollected = []string{"grpc-server-"}

// NumberOfEntries is the max number of StatEntry that preserved,
// it is the history of a container's heartbeats. The interval of container
// heartbeats from TiKV is 10s, so we can preserve 30 entries per
// container which is about 5 minutes.
const NumberOfEntries = 30

// StaleEntriesTimeout is the time before an entry is deleted as stale.
// It is about 30 entries * 10s
const StaleEntriesTimeout = 300 * time.Second

// StatEntry is an entry of container statistics
type StatEntry rpcpb.ContainerStats

// CPUEntries saves a history of container statistics
type CPUEntries struct {
	cpu     movingaverage.MovingAvg
	updated time.Time
}

// NewCPUEntries returns the StateEntries with a fixed size
func NewCPUEntries(size int) *CPUEntries {
	return &CPUEntries{
		cpu: movingaverage.NewMedianFilter(size),
	}
}

// Append a StatEntry, it accepts an optional threads as a filter of CPU usage
func (s *CPUEntries) Append(stat *StatEntry, threads ...string) bool {
	usages := stat.CpuUsages
	// all gRPC fields are optional, so we must check the empty value
	if usages == nil {
		return false
	}

	cpu := float64(0)
	appended := 0
	for _, usage := range usages {
		name := usage.GetKey()
		value := usage.GetValue()
		if threads != nil && slice.NoneOf(threads, func(i int) bool {
			return strings.HasPrefix(name, threads[i])
		}) {
			continue
		}
		cpu += float64(value)
		appended++
	}
	if appended > 0 {
		s.cpu.Add(cpu / float64(appended))
		s.updated = time.Now()
		return true
	}
	return false
}

// CPU returns the cpu usage
func (s *CPUEntries) CPU() float64 {
	return s.cpu.Get()
}

// StatEntries saves the StatEntries for each container in the cluster
type StatEntries struct {
	m     sync.RWMutex
	stats map[uint64]*CPUEntries
	size  int   // size of entries to keep for each container
	total int64 // total of StatEntry appended
	ttl   time.Duration
}

// NewStatEntries returns a statistics object for the cluster
func NewStatEntries(size int) *StatEntries {
	return &StatEntries{
		stats: make(map[uint64]*CPUEntries),
		size:  size,
		ttl:   StaleEntriesTimeout,
	}
}

// Append an container StatEntry
func (cst *StatEntries) Append(stat *StatEntry) bool {
	cst.m.Lock()
	defer cst.m.Unlock()

	cst.total++

	// append the entry
	containerID := stat.ContainerID
	entries, ok := cst.stats[containerID]
	if !ok {
		entries = NewCPUEntries(cst.size)
		cst.stats[containerID] = entries
	}

	return entries.Append(stat, ThreadsCollected...)
}

func contains(slice []uint64, value uint64) bool {
	for i := range slice {
		if slice[i] == value {
			return true
		}
	}
	return false
}

// CPU returns the cpu usage of the cluster
func (cst *StatEntries) CPU(excludes ...uint64) float64 {
	cst.m.Lock()
	defer cst.m.Unlock()

	// no entries have been collected
	if cst.total == 0 {
		return 0
	}

	sum := 0.0
	for sid, stat := range cst.stats {
		if contains(excludes, sid) {
			continue
		}
		if time.Since(stat.updated) > cst.ttl {
			delete(cst.stats, sid)
			continue
		}
		sum += stat.CPU()
	}
	if len(cst.stats) == 0 {
		return 0.0
	}
	return sum / float64(len(cst.stats))
}

// State collects information from container heartbeat
// and calculates the load state of the cluster
type State struct {
	cst *StatEntries
}

// NewState return the LoadState object which collects
// information from container heartbeats and gives the current state of
// the cluster
func NewState() *State {
	return &State{
		cst: NewStatEntries(NumberOfEntries),
	}
}

// State returns the state of the cluster, excludes is the list of container ID
// to be excluded
func (cs *State) State(excludes ...uint64) LoadState {
	// Return LoadStateNone if there is not enough heartbeats
	// collected.
	if cs.cst.total < NumberOfEntries {
		return LoadStateNone
	}

	// The CPU usage in fact is collected from grpc-server, so it is not the
	// CPU usage for the whole TiKV process. The boundaries are empirical
	// values.
	// TODO we may get a more accurate state with the information of the number // of the CPU cores
	cpu := cs.cst.CPU(excludes...)
	util.GetLogger().Debugf("calculated cpu %+v", cpu)
	clusterStateCPUGauge.Set(cpu)
	switch {
	case cpu < 5:
		return LoadStateIdle
	case cpu >= 5 && cpu < 10:
		return LoadStateLow
	case cpu >= 10 && cpu < 30:
		return LoadStateNormal
	case cpu >= 30:
		return LoadStateHigh
	}
	return LoadStateNone
}

// Collect statistics from container heartbeat
func (cs *State) Collect(stat *StatEntry) {
	cs.cst.Append(stat)
}
