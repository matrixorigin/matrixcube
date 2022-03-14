package config

import (
	"sync"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

type jobRegister struct {
	sync.RWMutex
	jobProcessors map[metapb.JobType]JobProcessor
}

// ShardsAware resources aware
type ShardsAware interface {
	// ForeachWaittingCreateShards do every waitting resources
	ForeachWaittingCreateShards(do func(res metapb.Shard))
	// ForeachShards foreach resource by group
	ForeachShards(group uint64, fn func(res metapb.Shard))
	// GetShard returns resource runtime info
	GetShard(resourceID uint64) *core.CachedShard
}

// JobProcessor job processor
type JobProcessor interface {
	// Start create the job
	Start(metapb.Job, storage.JobStorage, ShardsAware)
	// Stop stop the job, the job will restart at other node
	Stop(metapb.Job, storage.JobStorage, ShardsAware)
	// Remove remove job, the job will never start again
	Remove(metapb.Job, storage.JobStorage, ShardsAware)
	// Execute execute the data on job and returns the result
	Execute([]byte, storage.JobStorage, ShardsAware) ([]byte, error)
}

// RegisterJobProcessor register job processor
func (c *Config) RegisterJobProcessor(jobType metapb.JobType, processor JobProcessor) {
	c.jobRegister.Lock()
	defer c.jobRegister.Unlock()

	if c.jobRegister.jobProcessors == nil {
		c.jobRegister.jobProcessors = make(map[metapb.JobType]JobProcessor)
	}

	c.jobRegister.jobProcessors[jobType] = processor
}

// GetJobProcessor returns the job handler
func (c *Config) GetJobProcessor(jobType metapb.JobType) JobProcessor {
	c.jobRegister.RLock()
	defer c.jobRegister.RUnlock()

	if c.jobRegister.jobProcessors == nil {
		c.jobRegister.jobProcessors = make(map[metapb.JobType]JobProcessor)
	}

	return c.jobRegister.jobProcessors[jobType]
}
