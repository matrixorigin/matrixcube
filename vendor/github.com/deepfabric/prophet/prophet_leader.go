package prophet

import (
	"github.com/deepfabric/prophet/util"
)

func (p *defaultProphet) startLeaderLoop() {
	go p.member.ElectionLoop(p.ctx)
	<-p.completeC
	p.client = NewClient(p.cfg.Adapter,
		WithRPCTimeout(p.cfg.RPCTimeout.Duration),
		WithLeaderGetter(p.GetLeader))
}

func (p *defaultProphet) enableLeader() error {
	util.GetLogger().Infof("********%s become to leader now********", p.cfg.Name)

	if err := p.createRaftCluster(); err != nil {
		util.GetLogger().Errorf("create raft cluster failed with %+v", err)
		return err
	}

	p.createEventNotifer()
	p.notifyElectionComplete()
	p.cfg.Handler.ProphetBecomeLeader()
	return nil
}

func (p *defaultProphet) disableLeader() error {
	util.GetLogger().Infof("********%s become to follower now********", p.cfg.Name)

	p.stopRaftCluster()
	p.notifyElectionComplete()
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
