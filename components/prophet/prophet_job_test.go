// Copyright 2020 MatrixOrigin.
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

package prophet

import (
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/stretchr/testify/assert"
)

type testJobProcessor struct {
	sync.Mutex
	starts  map[metapb.JobType]metapb.Job
	stops   map[metapb.JobType]metapb.Job
	removes map[metapb.JobType]metapb.Job
}

func newTestJobProcessor() *testJobProcessor {
	return &testJobProcessor{
		starts:  make(map[metapb.JobType]metapb.Job),
		stops:   make(map[metapb.JobType]metapb.Job),
		removes: make(map[metapb.JobType]metapb.Job),
	}
}

func (p *testJobProcessor) Start(job metapb.Job, s storage.JobStorage, aware config.ResourcesAware) {
	p.Lock()
	defer p.Unlock()

	p.starts[job.Type] = job
	delete(p.stops, job.Type)
}

func (p *testJobProcessor) Stop(job metapb.Job, s storage.JobStorage, aware config.ResourcesAware) {
	p.Lock()
	defer p.Unlock()

	p.stops[job.Type] = job
	delete(p.starts, job.Type)
}

func (p *testJobProcessor) Remove(job metapb.Job, s storage.JobStorage, aware config.ResourcesAware) {
	p.Lock()
	defer p.Unlock()

	p.removes[job.Type] = job
}

func (p *testJobProcessor) Execute([]byte, storage.JobStorage, config.ResourcesAware) ([]byte, error) {
	return nil, nil
}

func TestStartAndStopAndRemoveJobs(t *testing.T) {
	cluster := newTestClusterProphet(t, 3, nil)
	defer func() {
		for _, p := range cluster {
			p.Stop()
		}
	}()

	jp := newTestJobProcessor()
	config.RegisterJobProcessor(metapb.JobType(1), jp)
	config.RegisterJobProcessor(metapb.JobType(2), jp)
	config.RegisterJobProcessor(metapb.JobType(3), jp)

	p := cluster[0].(*defaultProphet)
	rc := p.GetRaftCluster()

	assert.NoError(t, p.handleCreateJob(rc,
		&rpcpb.Request{Type: rpcpb.TypeCreateJobReq,
			CreateJob: rpcpb.CreateJobReq{Job: metapb.Job{Type: metapb.JobType(1), Content: []byte("job1")}}},
		&rpcpb.Response{Type: rpcpb.TypeCreateJobRsp}))
	assert.NoError(t, p.handleCreateJob(rc,
		&rpcpb.Request{Type: rpcpb.TypeCreateJobReq,
			CreateJob: rpcpb.CreateJobReq{Job: metapb.Job{Type: metapb.JobType(2), Content: []byte("job2")}}},
		&rpcpb.Response{Type: rpcpb.TypeCreateJobRsp}))
	assert.NoError(t, p.handleCreateJob(rc,
		&rpcpb.Request{Type: rpcpb.TypeCreateJobReq,
			CreateJob: rpcpb.CreateJobReq{Job: metapb.Job{Type: metapb.JobType(3), Content: []byte("job3")}}},
		&rpcpb.Response{Type: rpcpb.TypeCreateJobRsp}))

	assert.Equal(t, 3, len(jp.starts))
	assert.Equal(t, 0, len(jp.stops))

	p.stopJobs()
	time.Sleep(time.Second)
	assert.Equal(t, 3, len(jp.stops))
	assert.Equal(t, 0, len(jp.starts))

	jp = newTestJobProcessor()
	config.RegisterJobProcessor(metapb.JobType(1), jp)
	config.RegisterJobProcessor(metapb.JobType(2), jp)
	config.RegisterJobProcessor(metapb.JobType(3), jp)
	p.startJobs()
	time.Sleep(time.Second)
	assert.Equal(t, 3, len(jp.starts))
	assert.Equal(t, 0, len(jp.stops))

	jp = newTestJobProcessor()
	config.RegisterJobProcessor(metapb.JobType(1), jp)
	config.RegisterJobProcessor(metapb.JobType(2), jp)
	config.RegisterJobProcessor(metapb.JobType(3), jp)
	assert.NoError(t, p.handleRemoveJob(rc,
		&rpcpb.Request{Type: rpcpb.TypeCreateJobReq,
			CreateJob: rpcpb.CreateJobReq{Job: metapb.Job{Type: metapb.JobType(1), Content: []byte("job1")}}},
		&rpcpb.Response{Type: rpcpb.TypeCreateJobRsp}))
	assert.NoError(t, p.handleRemoveJob(rc,
		&rpcpb.Request{Type: rpcpb.TypeCreateJobReq,
			CreateJob: rpcpb.CreateJobReq{Job: metapb.Job{Type: metapb.JobType(2), Content: []byte("job2")}}},
		&rpcpb.Response{Type: rpcpb.TypeCreateJobRsp}))
	assert.NoError(t, p.handleRemoveJob(rc,
		&rpcpb.Request{Type: rpcpb.TypeCreateJobReq,
			CreateJob: rpcpb.CreateJobReq{Job: metapb.Job{Type: metapb.JobType(3), Content: []byte("job3")}}},
		&rpcpb.Response{Type: rpcpb.TypeCreateJobRsp}))
	assert.Equal(t, 0, len(jp.removes))
}
