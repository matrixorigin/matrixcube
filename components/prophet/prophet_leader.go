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
	"github.com/matrixorigin/matrixcube/components/prophet/util"
)

func (p *defaultProphet) startLeaderLoop() {
	go p.member.ElectionLoop(p.ctx)
	<-p.completeC
}

func (p *defaultProphet) enableLeader() error {
	util.GetLogger().Infof("********%s become to leader now********", p.cfg.Name)

	if err := p.createRaftCluster(); err != nil {
		util.GetLogger().Errorf("create raft cluster failed with %+v", err)
		return err
	}

	p.initClient()
	p.createEventNotifer()
	p.notifyElectionComplete()
	p.startJobTask()
	p.startCustom()
	p.cfg.Handler.ProphetBecomeLeader()
	return nil
}

func (p *defaultProphet) disableLeader() error {
	util.GetLogger().Infof("********%s become to follower now********", p.cfg.Name)

	p.initClient()
	p.stopRaftCluster()
	p.stopEventNotifer()
	p.notifyElectionComplete()
	p.stopJobTask()
	p.stopCustom()
	p.cfg.Handler.ProphetBecomeFollower()
	return nil
}

func (p *defaultProphet) notifyElectionComplete() {
	p.notifyOnce.Do(func() {
		close(p.completeC)
	})
}

func (p *defaultProphet) createRaftCluster() error {
	if p.cluster.IsRunning() {
		return nil
	}

	return p.cluster.Start(p)
}

func (p *defaultProphet) stopRaftCluster() {
	p.cluster.Stop()
}

func (p *defaultProphet) createEventNotifer() {
	p.wn = newWatcherNotifier(p.cluster)
	p.wn.start()
}

func (p *defaultProphet) stopEventNotifer() {
	if p.wn != nil {
		p.wn.stop()
	}
}

func (p *defaultProphet) initClient() {
	p.clientOnce.Do(func() {
		p.client = NewClient(p.cfg.Adapter,
			WithRPCTimeout(p.cfg.RPCTimeout.Duration),
			WithLeaderGetter(p.GetLeader))
	})
}
