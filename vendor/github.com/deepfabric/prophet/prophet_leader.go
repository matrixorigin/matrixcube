package prophet

import (
	"context"
	"encoding/json"
	"math"
	"sync/atomic"
	"time"
)

var (
	loopInterval = 100 * time.Millisecond
)

// Node is prophet info
type Node struct {
	Name string `json:"name"`
	Addr string `json:"addr"`
}

func mustUnmarshal(data []byte) *Node {
	value := &Node{}
	err := json.Unmarshal(data, value)
	if err != nil {
		log.Fatalf("unmarshal leader node failed with %+v", err)
	}

	return value
}

func (n *Node) marshal() string {
	data, _ := json.Marshal(n)
	return string(data)
}

func (p *defaultProphet) startLeaderLoop() {
	leaderSignature := ""
	if p.opts.cfg.StorageNode {
		leaderSignature = p.signature
	}

	go p.elector.ElectionLoop(context.Background(),
		math.MaxUint64,
		leaderSignature,
		p.enableLeader,
		p.disableLeader)
	<-p.completeC
}

func (p *defaultProphet) enableLeader() {
	log.Infof("********become to leader now********")
	p.leader = p.node

	p.rt = newRuntime(p)
	p.rt.load()

	p.coordinator = newCoordinator(p.cfg, p.runner, p.rt)
	p.coordinator.start()

	p.wn = newWatcherNotifier(p.rt)
	go p.wn.start()

	// now, we are leader
	atomic.StoreInt64(&p.leaderFlag, 1)

	p.notifyElectionComplete()
	p.cfg.Handler.ProphetBecomeLeader()
}

func (p *defaultProphet) disableLeader() {
	atomic.StoreInt64(&p.leaderFlag, 0)
	log.Infof("********become to follower now********")

	value, err := p.elector.CurrentLeader(math.MaxUint64)
	if err != nil {
		log.Fatalf("get current leader failed with %+v", err)
	}
	p.leader = nil
	if len(value) > 0 {
		p.leader = mustUnmarshal([]byte(value))
	}

	// now, we are not leader
	if p.coordinator != nil {
		p.coordinator.stop()
		p.rt = nil
	}

	if p.wn != nil {
		p.wn.stop()
	}

	p.notifyElectionComplete()
	p.cfg.Handler.ProphetBecomeFollower()
}

func (p *defaultProphet) isLeader() bool {
	return 1 == atomic.LoadInt64(&p.leaderFlag)
}

func (p *defaultProphet) notifyElectionComplete() {
	p.notifyOnce.Do(func() {
		close(p.completeC)
	})
}

func (p *defaultProphet) isMatchLeader(leaderNode *Node) bool {
	return leaderNode != nil &&
		p.node.Name == leaderNode.Name
}
