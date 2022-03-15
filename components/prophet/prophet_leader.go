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
	"go.uber.org/zap"
)

// startLeaderLoop start election loop
func (p *defaultProphet) startElectionLoop() {
	p.member.ElectionLoop()
	p.logger.Info("election loop started")
	<-p.completeC
}

func (p *defaultProphet) becomeLeader() error {
	p.logger.Info("********become leader now********")

	if err := p.startRaftCluster(); err != nil {
		p.logger.Error("fail to create raft cluster", zap.Error(err))
		return err
	}

	p.initClient()
	p.createEventNotifer()
	p.notifyElectionComplete()
	p.startJobs()
	p.startCustom()
	p.cfg.Prophet.Handler.ProphetBecomeLeader()
	return nil
}

func (p *defaultProphet) becomeFollower() error {
	p.logger.Info("********become follower now********")

	p.initClient()
	p.stopRaftCluster()
	p.stopEventNotifer()
	p.notifyElectionComplete()
	p.stopJobs()
	p.stopCustom()
	p.cfg.Prophet.Handler.ProphetBecomeFollower()
	return nil
}

func (p *defaultProphet) notifyElectionComplete() {
	p.notifyOnce.Do(func() {
		close(p.completeC)
	})
}

func (p *defaultProphet) startRaftCluster() error {
	if p.cluster.IsRunning() {
		return nil
	}

	return p.cluster.Start(p)
}

func (p *defaultProphet) stopRaftCluster() {
	p.cluster.Stop()
}

func (p *defaultProphet) createEventNotifer() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stopEventNotifer()
	p.mu.wn = newWatcherNotifier(p.cluster, p.logger)
	p.mu.wn.start()
}

func (p *defaultProphet) stopEventNotifer() {
	if p.mu.wn != nil {
		p.mu.wn.stop()
		p.mu.wn = nil
	}
}

func (p *defaultProphet) initClient() {
	p.clientOnce.Do(func() {
		p.client = NewClient(
			WithRPCTimeout(p.cfg.Prophet.RPCTimeout.Duration),
			WithLeaderGetter(p.GetLeader),
			WithLogger(p.logger))
	})
}
