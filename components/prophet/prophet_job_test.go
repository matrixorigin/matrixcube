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
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/stretchr/testify/assert"
)

type testJobProcessor struct {
	sync.RWMutex
	starts  map[metapb.JobType]metapb.Job
	stops   map[metapb.JobType]metapb.Job
	removes map[metapb.JobType]metapb.Job
}

func (p *testJobProcessor) startNum() int {
	p.RLock()
	defer p.RUnlock()
	return len(p.starts)
}

func (p *testJobProcessor) stopNum() int {
	p.RLock()
	defer p.RUnlock()
	return len(p.stops)
}

func (p *testJobProcessor) removeNum() int {
	p.RLock()
	defer p.RUnlock()
	return len(p.removes)
}

func newTestJobProcessor() *testJobProcessor {
	return &testJobProcessor{
		starts:  make(map[metapb.JobType]metapb.Job),
		stops:   make(map[metapb.JobType]metapb.Job),
		removes: make(map[metapb.JobType]metapb.Job),
	}
}

func (p *testJobProcessor) Start(job metapb.Job, s storage.JobStorage, aware config.ShardsAware) {
	p.Lock()
	defer p.Unlock()

	p.starts[job.Type] = job
	delete(p.stops, job.Type)
}

func (p *testJobProcessor) Stop(job metapb.Job, s storage.JobStorage, aware config.ShardsAware) {
	p.Lock()
	defer p.Unlock()

	p.stops[job.Type] = job
	delete(p.starts, job.Type)
}

func (p *testJobProcessor) Remove(job metapb.Job, s storage.JobStorage, aware config.ShardsAware) {
	p.Lock()
	defer p.Unlock()

	p.removes[job.Type] = job
}

func (p *testJobProcessor) Execute([]byte, storage.JobStorage, config.ShardsAware) ([]byte, error) {
	return nil, nil
}

func TestStartAndStopAndRemoveJobs(t *testing.T) {
	clusterSize := 3
	cluster := newTestClusterProphet(t, clusterSize, nil)
	defer func() {
		for _, p := range cluster {
			p.Stop()
		}
	}()

	var leader Prophet
	for i := 0; i < clusterSize; i++ {
		if cluster[i].GetMember().ID() == cluster[i].GetLeader().ID {
			leader = cluster[i]
			break
		}
	}
	assert.NotNil(t, leader)

	p := leader.(*defaultProphet)
	rc := p.GetRaftCluster()
	jp := newTestJobProcessor()
	p.cfg.Prophet.RegisterJobProcessor(metapb.JobType(1), jp)
	p.cfg.Prophet.RegisterJobProcessor(metapb.JobType(2), jp)
	p.cfg.Prophet.RegisterJobProcessor(metapb.JobType(3), jp)

	assert.NoError(t, p.handleCreateJob(rc,
		&rpcpb.ProphetRequest{Type: rpcpb.TypeCreateJobReq,
			CreateJob: rpcpb.CreateJobReq{Job: metapb.Job{Type: metapb.JobType(1), Content: []byte("job1")}}},
		&rpcpb.ProphetResponse{Type: rpcpb.TypeCreateJobRsp}))
	assert.NoError(t, p.handleCreateJob(rc,
		&rpcpb.ProphetRequest{Type: rpcpb.TypeCreateJobReq,
			CreateJob: rpcpb.CreateJobReq{Job: metapb.Job{Type: metapb.JobType(2), Content: []byte("job2")}}},
		&rpcpb.ProphetResponse{Type: rpcpb.TypeCreateJobRsp}))
	assert.NoError(t, p.handleCreateJob(rc,
		&rpcpb.ProphetRequest{Type: rpcpb.TypeCreateJobReq,
			CreateJob: rpcpb.CreateJobReq{Job: metapb.Job{Type: metapb.JobType(3), Content: []byte("job3")}}},
		&rpcpb.ProphetResponse{Type: rpcpb.TypeCreateJobRsp}))

	assert.Equal(t, 3, jp.startNum())
	assert.Equal(t, 0, jp.stopNum())

	p.stopJobs()
	time.Sleep(time.Second)
	assert.Equal(t, 3, jp.stopNum())
	assert.Equal(t, 0, jp.startNum())

	jp = newTestJobProcessor()
	p.cfg.Prophet.RegisterJobProcessor(metapb.JobType(1), jp)
	p.cfg.Prophet.RegisterJobProcessor(metapb.JobType(2), jp)
	p.cfg.Prophet.RegisterJobProcessor(metapb.JobType(3), jp)
	p.startJobs()
	time.Sleep(time.Second)
	assert.Equal(t, 3, jp.startNum())
	assert.Equal(t, 0, jp.stopNum())

	jp = newTestJobProcessor()
	p.cfg.Prophet.RegisterJobProcessor(metapb.JobType(1), jp)
	p.cfg.Prophet.RegisterJobProcessor(metapb.JobType(2), jp)
	p.cfg.Prophet.RegisterJobProcessor(metapb.JobType(3), jp)
	assert.NoError(t, p.handleRemoveJob(rc,
		&rpcpb.ProphetRequest{Type: rpcpb.TypeRemoveJobReq,
			RemoveJob: rpcpb.RemoveJobReq{Job: metapb.Job{Type: metapb.JobType(1), Content: []byte("job1")}}},
		&rpcpb.ProphetResponse{Type: rpcpb.TypeRemoveJobRsp}))
	assert.NoError(t, p.handleRemoveJob(rc,
		&rpcpb.ProphetRequest{Type: rpcpb.TypeRemoveJobReq,
			RemoveJob: rpcpb.RemoveJobReq{Job: metapb.Job{Type: metapb.JobType(2), Content: []byte("job2")}}},
		&rpcpb.ProphetResponse{Type: rpcpb.TypeRemoveJobRsp}))
	assert.NoError(t, p.handleRemoveJob(rc,
		&rpcpb.ProphetRequest{Type: rpcpb.TypeRemoveJobReq,
			RemoveJob: rpcpb.RemoveJobReq{Job: metapb.Job{Type: metapb.JobType(3), Content: []byte("job3")}}},
		&rpcpb.ProphetResponse{Type: rpcpb.TypeRemoveJobRsp}))
	assert.Equal(t, 3, jp.removeNum())
}
