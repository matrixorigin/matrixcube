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
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/util"
)

func (p *defaultProphet) startJobTask() {
	if p.cfg.JobHandler == nil {
		return
	}

	if p.jobCancel != nil {
		p.jobCancel()
	}

	p.jobCtx, p.jobCancel = context.WithCancel(context.Background())
	go func(ctx context.Context) {
		util.GetLogger().Infof("execute job task started")
		if p.cfg.TestCtx != nil {
			p.cfg.TestCtx.Store("jobTask", "started")
		}

		ticker := time.NewTicker(p.cfg.JobCheckerDuration)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				util.GetLogger().Infof("execute job task stopped")
				if p.cfg.TestCtx != nil {
					p.cfg.TestCtx.Store("jobTask", "stopped")
				}
				return
			case <-ticker.C:
				err := p.storage.LoadJobs(16, p.cfg.JobHandler)
				if err != nil {
					util.GetLogger().Errorf("execute job failed with %+v", err)
				}
			}
		}
	}(p.jobCtx)
}

func (p *defaultProphet) stopJobTask() {
	if p.jobCancel != nil {
		p.jobCancel()
	}
}
