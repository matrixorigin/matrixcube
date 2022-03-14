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
	"context"
	"fmt"

	"github.com/matrixorigin/matrixcube/components/prophet/cluster"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"go.uber.org/zap"
)

func (p *defaultProphet) startJobs() {
	p.stopper.RunTask(context.Background(), func(ctx context.Context) {
		p.logger.Info("start jobs")
		defer p.logger.Info("start jobs completed")

		p.jobMu.Lock()
		defer p.jobMu.Unlock()
		p.jobMu.jobs = make(map[metapb.JobType]metapb.Job)

		for {
			err := p.storage.LoadJobs(16, func(job metapb.Job) {
				p.jobMu.jobs[job.Type] = job
			})
			if err == nil {
				break
			}

			select {
			case <-ctx.Done():
				return
			default:
			}

			p.logger.Error("fail to load job, retry later",
				zap.Error(err))
		}

		p.logger.Info("load jobs", zap.Int("count", len(p.jobMu.jobs)))
		for _, job := range p.jobMu.jobs {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if job.State == metapb.JobState_Completed {
				err := p.GetStorage().RemoveJob(job.Type)
				if err != nil {
					p.logger.Error("fail to remove completed job",
						zap.String("type", job.Type.String()),
						zap.Error(err))
				}
				continue
			}

			processor := p.cfg.Prophet.GetJobProcessor(job.Type)
			if processor != nil {
				processor.Start(job, p.storage, p.basicCluster)
				p.updateJobStatus(job, metapb.JobState_Working)
				continue
			}

			p.logger.Error("missing job processor",
				zap.String("type", job.Type.String()))
		}
	})
}

func (p *defaultProphet) stopJobs() {
	p.stopper.RunTask(context.Background(), func(ctx context.Context) {
		p.logger.Info("stop jobs")
		defer p.logger.Info("stop jobs completed")

		p.jobMu.Lock()
		defer p.jobMu.Unlock()

		p.logger.Info("stop jobs", zap.Int("count", len(p.jobMu.jobs)))
		for _, job := range p.jobMu.jobs {
			processor := p.cfg.Prophet.GetJobProcessor(job.Type)
			if processor != nil {
				processor.Stop(job, p.storage, p.basicCluster)
				continue
			}

			p.logger.Error("missing job processor",
				zap.String("type", job.Type.String()))
		}

		p.jobMu.jobs = make(map[metapb.JobType]metapb.Job)
	})
}

func (p *defaultProphet) handleCreateJob(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	p.jobMu.Lock()
	defer p.jobMu.Unlock()

	job := req.CreateJob.Job
	processor := p.cfg.Prophet.GetJobProcessor(job.Type)
	if processor == nil {
		return fmt.Errorf("missing job processor for type %d", job.Type)
	}

	if _, ok := p.jobMu.jobs[job.Type]; ok {
		return nil
	}

	if err := p.updateJobStatus(job, metapb.JobState_Created); err != nil {
		return err
	}

	processor.Start(job, p.storage, p.basicCluster)
	p.jobMu.jobs[job.Type] = job
	p.updateJobStatus(job, metapb.JobState_Working)
	return nil
}

func (p *defaultProphet) handleRemoveJob(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	p.jobMu.Lock()
	defer p.jobMu.Unlock()

	job := req.RemoveJob.Job
	processor := p.cfg.Prophet.GetJobProcessor(job.Type)
	if processor == nil {
		return fmt.Errorf("missing job processor for type %d, %+v", job.Type, job)
	}

	if _, ok := p.jobMu.jobs[job.Type]; !ok {
		return nil
	}

	if err := p.updateJobStatus(job, metapb.JobState_Completed); err != nil {
		return err
	}

	processor.Remove(job, p.storage, p.basicCluster)
	delete(p.jobMu.jobs, job.Type)
	return nil
}

func (p *defaultProphet) handleExecuteJob(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	job := req.ExecuteJob.Job
	processor := p.cfg.Prophet.GetJobProcessor(job.Type)
	if processor == nil {
		return fmt.Errorf("missing job processor for type %d", job.Type)
	}

	if _, ok := p.jobMu.jobs[job.Type]; !ok {
		return fmt.Errorf("missing job for type %d, the job maybe not created or started", job.Type)
	}

	data, err := processor.Execute(req.ExecuteJob.Data, p.storage, rc.GetCacheCluster())
	if err != nil {
		return err
	}

	resp.ExecuteJob.Data = data
	return nil
}

func (p *defaultProphet) updateJobStatus(job metapb.Job, state metapb.JobState) error {
	job.State = state
	return p.GetStorage().PutJob(job)
}
