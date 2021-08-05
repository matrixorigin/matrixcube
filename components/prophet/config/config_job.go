package config

import (
	"sync"

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

var (
	jobMu         sync.RWMutex
	jobProcessors = make(map[metapb.JobType]JobProcessor)
)

// RegisterJobProcessor register job processor
func RegisterJobProcessor(jobType metapb.JobType, processor JobProcessor) {
	jobMu.Lock()
	defer jobMu.Unlock()

	jobProcessors[jobType] = processor
}

// GetJobProcessor returns the job handler
func GetJobProcessor(jobType metapb.JobType) JobProcessor {
	jobMu.RLock()
	defer jobMu.RUnlock()

	return jobProcessors[jobType]
}
