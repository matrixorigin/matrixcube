package cluster

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/stretchr/testify/assert"
)

func cpu(usage int64) []rpcpb.RecordPair {
	n := 10
	name := "cpu"
	pairs := make([]rpcpb.RecordPair, n)
	for i := 0; i < n; i++ {
		pairs[i] = rpcpb.RecordPair{
			Key:   fmt.Sprintf("%s:%d", name, i),
			Value: uint64(usage),
		}
	}
	return pairs
}

func TestCPUEntriesAppend(t *testing.T) {
	N := 10

	checkAppend := func(appended bool, usage int64, threads ...string) {
		entries := NewCPUEntries(N)
		assert.NotEmpty(t, entries)
		for i := 0; i < N; i++ {
			entry := &StatEntry{
				CpuUsages: cpu(usage),
			}
			assert.Equal(t, appended, entries.Append(entry, threads...))
		}
		assert.Equal(t, float64(usage), entries.cpu.Get())
	}

	checkAppend(true, 20)
	checkAppend(true, 20, "cpu")
	checkAppend(false, 0, "cup")
}

func TestCPUEntriesCPU(t *testing.T) {
	N := 10
	entries := NewCPUEntries(N)
	assert.NotEmpty(t, entries)

	usages := cpu(20)
	for i := 0; i < N; i++ {
		entry := &StatEntry{
			CpuUsages: usages,
		}
		entries.Append(entry)
	}
	assert.Equal(t, float64(20), entries.CPU())
}

func TestStatEntriesAppend(t *testing.T) {
	N := 10
	cst := NewStatEntries(N)
	assert.NotNil(t, cst)
	ThreadsCollected = []string{"cpu:"}

	// fill 2*N entries, 2 entries for each container
	for i := 0; i < 2*N; i++ {
		entry := &StatEntry{
			ContainerID: uint64(i % N),
			CpuUsages:   cpu(20),
		}
		assert.True(t, cst.Append(entry))
	}

	// use i as the container ID
	for i := 0; i < N; i++ {
		assert.Equal(t, float64(20), cst.stats[uint64(i)].CPU())
	}
}

func TestStatEntriesCPU(t *testing.T) {
	N := 10
	cst := NewStatEntries(N)
	assert.NotNil(t, cst)

	// the average cpu usage is 20%
	usages := cpu(20)
	ThreadsCollected = []string{"cpu:"}

	// 2 entries per container
	for i := 0; i < 2*N; i++ {
		entry := &StatEntry{
			ContainerID: uint64(i % N),
			CpuUsages:   usages,
		}
		assert.True(t, cst.Append(entry))
	}

	assert.Equal(t, int64(2*N), cst.total)
	// the cpu usage of the whole cluster is 20%
	assert.Equal(t, float64(20), cst.CPU())
}

func TestStatEntriesCPUStale(t *testing.T) {
	N := 10
	cst := NewStatEntries(N)
	// make all entries stale immediately
	cst.ttl = 0

	usages := cpu(20)
	ThreadsCollected = []string{"cpu:"}
	for i := 0; i < 2*N; i++ {
		entry := &StatEntry{
			ContainerID: uint64(i % N),
			CpuUsages:   usages,
		}
		cst.Append(entry)
	}
	assert.Equal(t, float64(0), cst.CPU())
}

func TestStatEntriesState(t *testing.T) {
	Load := func(usage int64) *State {
		cst := NewStatEntries(10)
		assert.NotNil(t, cst)

		usages := cpu(usage)
		ThreadsCollected = []string{"cpu:"}

		for i := 0; i < NumberOfEntries; i++ {
			entry := &StatEntry{
				ContainerID: 0,
				CpuUsages:   usages,
			}
			cst.Append(entry)
		}
		return &State{cst}
	}
	assert.Equal(t, Load(0).State(), LoadStateIdle)
	assert.Equal(t, Load(5).State(), LoadStateLow)
	assert.Equal(t, Load(10).State(), LoadStateNormal)
	assert.Equal(t, Load(30).State(), LoadStateHigh)
}
