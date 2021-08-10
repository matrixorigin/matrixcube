package config

import (
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
)

// ResourcesAware resources aware
type ResourcesAware interface {
	// ForeachWaittingCreateResources do every waitting resources
	ForeachWaittingCreateResources(do func(res metadata.Resource))
	// ForeachResources foreach resource by group
	ForeachResources(group uint64, fn func(res metadata.Resource))
	// GetResource returns resource runtime info
	GetResource(resourceID uint64) *core.CachedResource
}

// JobProcessor job processor
type JobProcessor interface {
	// Start create the job
	Start(metapb.Job, storage.JobStorage, ResourcesAware)
	// Stop stop the job, the job will restart at other node
	Stop(metapb.Job, storage.JobStorage, ResourcesAware)
	// Remove remove job, the job will never start again
	Remove(metapb.Job, storage.JobStorage, ResourcesAware)
	// Execute execute the data on job and returns the result
	Execute([]byte, storage.JobStorage, ResourcesAware) ([]byte, error)
}

// RegisterJobProcessor register job processor
func (c *Config) RegisterJobProcessor(jobType metapb.JobType, processor JobProcessor) {
	c.jobMu.Lock()
	defer c.jobMu.Unlock()

	if c.jobMu.jobProcessors == nil {
		c.jobMu.jobProcessors = make(map[metapb.JobType]JobProcessor)
	}

	c.jobMu.jobProcessors[jobType] = processor
}

// GetJobProcessor returns the job handler
func (c *Config) GetJobProcessor(jobType metapb.JobType) JobProcessor {
	c.jobMu.RLock()
	defer c.jobMu.RUnlock()

	if c.jobMu.jobProcessors == nil {
		c.jobMu.jobProcessors = make(map[metapb.JobType]JobProcessor)
	}

	return c.jobMu.jobProcessors[jobType]
}
