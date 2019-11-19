package prophet

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/util/task"
)

var (
	// ErrNorLeader not leader
	ErrNorLeader = errors.New("Not Leader")
	// ErrDoTimeout timeout
	ErrDoTimeout = errors.New("Do Timeout")
)

const (
	cmdAddPeer        = 1
	cmdRemovePeer     = 2
	cmdScale          = 3
	cmdSend           = 4
	cmdHB             = 5
	cmdHBACK          = 6
	cmdRemove         = 7
	cmdBecomeLeader   = 8
	cmdBecomeFollower = 9
	cmdTransferLeader = 10
	cmdCustom         = 11
)

type cmd struct {
	cmdType int
	cb      func(interface{}, error)
	data    interface{}
}

func (c *cmd) complete(resp interface{}, err error) {
	if c.cb != nil {
		c.cb(resp, err)
	}
}

func (c *cmd) reset() {
	c.cmdType = 0
	c.data = nil
	c.cb = nil
}

var (
	cmdPool sync.Pool
)

func acquireCMD() *cmd {
	value := cmdPool.Get()
	if value == nil {
		return &cmd{}
	}

	return value.(*cmd)
}

func releaseCMD(value *cmd) {
	value.reset()
	cmdPool.Put(value)
}

// PeerReplicaHandler the interface to handle replica event message
type PeerReplicaHandler interface {
	AddPeer(Resource, Peer)
	RemovePeer(Resource, Peer) bool
	Scale(Resource, interface{}) (bool, []*PeerReplica)
	Heartbeat(Resource) bool
	Destory(Resource)

	ResourceBecomeLeader(Resource)
	ResourceBecomeFollower(Resource)
}

// PeerReplica is the Resource peer replicatation.
// Every Resource has N replicatation in N stores.
type PeerReplica struct {
	workerID   uint64
	tag        string
	leaderPeer uint64
	peer       Peer
	meta       Resource
	store      ResourceStore
	localStore LocalStore

	pendingPeers  []Peer
	tasks         []uint64
	heartbeatsMap *sync.Map // uint64 -> last hb time

	handler PeerReplicaHandler
	leader  bool
	elector Elector
	cancel  context.CancelFunc
	cmds    *task.RingBuffer
}

// CreatePeerReplica create a resource replica at current container
func CreatePeerReplica(store ResourceStore, meta Resource, handler PeerReplicaHandler, elector Elector) (*PeerReplica, error) {
	peer, ok := FindPeer(meta.Peers(), store.Meta().ID())
	if !ok {
		return nil, fmt.Errorf("find no peer for store %d in resource %v",
			store.Meta().ID(),
			meta)
	}

	return NewPeerReplica(store, meta, peer, handler, elector), nil
}

// NewPeerReplica create a resource replica at current container
func NewPeerReplica(store ResourceStore, meta Resource, peer Peer, handler PeerReplicaHandler, elector Elector) *PeerReplica {
	tag := fmt.Sprintf("[res-%d]:", meta.ID())
	if peer.ID == 0 {
		log.Fatalf("%s invalid peer id 0",
			tag)
	}

	pr := new(PeerReplica)
	pr.tag = tag
	pr.meta = meta
	pr.peer = peer
	pr.store = store
	pr.heartbeatsMap = &sync.Map{}
	pr.elector = elector
	pr.cmds = task.NewRingBuffer(1024) // TODO: configuration
	pr.handler = handler

	pr.startElector()

	log.Infof("%s created with %+v",
		pr.tag,
		pr.meta)
	return pr
}

// Tag returns the pr tag
func (pr *PeerReplica) Tag() string {
	return pr.tag
}

// Peer returns the current peer of this resource
func (pr *PeerReplica) Peer() *Peer {
	return &pr.peer
}

// Resource returns the meta
func (pr *PeerReplica) Resource() Resource {
	return pr.meta
}

// Destroy destory the resource replica from current container
func (pr *PeerReplica) Destroy() {
	pr.cmds.Dispose()
	pr.cancel()
	pr.elector.Stop(pr.meta.ID())
	pr.handler.Destory(pr.meta)
}

// IsLeader returns the peer replica is the leader
func (pr *PeerReplica) IsLeader() bool {
	return pr.leader
}

// AddPeer add a peer to this resource
func (pr *PeerReplica) AddPeer(peer Peer) {
	c := acquireCMD()
	c.cmdType = cmdAddPeer
	c.data = peer
	err := pr.cmds.Put(c)
	if err != nil {
		log.Fatalf("%s add peer event failed with %+v",
			pr.tag,
			err)
	}
}

// RemovePeer remove peer
func (pr *PeerReplica) RemovePeer(peer Peer) {
	c := acquireCMD()
	c.cmdType = cmdRemovePeer
	c.data = peer
	err := pr.cmds.Put(c)
	if err != nil {
		log.Fatalf("%s remove peer event failed with %+v",
			pr.tag,
			err)
	}
}

// Scale scale this pr
func (pr *PeerReplica) Scale(data interface{}) {
	c := acquireCMD()
	c.cmdType = cmdScale
	c.data = data
	err := pr.cmds.Put(c)
	if err != nil {
		log.Fatalf("%s scale event failed with %+v",
			pr.tag,
			err)
	}
}

// CollectPendingPeers returns the pending peers
func (pr *PeerReplica) CollectPendingPeers() []*Peer {
	var values []*Peer
	for _, peer := range pr.pendingPeers {
		p := peer
		values = append(values, &p)
	}
	return values
}

// CollectDownPeers returns the down peers
func (pr *PeerReplica) CollectDownPeers(maxDuration time.Duration) []*PeerStats {
	now := time.Now()
	var downPeers []*PeerStats
	for _, p := range pr.meta.Peers() {
		if p.ID == pr.peer.ID {
			continue
		}

		if last, ok := pr.heartbeatsMap.Load(p.ID); ok {
			missing := now.Sub(last.(time.Time))
			if missing >= maxDuration {
				state := &PeerStats{}
				state.Peer = p
				state.DownSeconds = uint64(missing.Seconds())
				downPeers = append(downPeers, state)
			}
		}
	}
	return downPeers
}

// Heartbeat send the hb to other peers
func (pr *PeerReplica) Heartbeat() {
	c := acquireCMD()
	c.cmdType = cmdSend
	err := pr.cmds.Put(c)
	if err != nil {
		log.Fatalf("%s send hb to peers event failed with %+v",
			pr.tag,
			err)
	}
}

// ChangeLeaderTo change leader to new leader
func (pr *PeerReplica) ChangeLeaderTo(leader uint64, cb func(interface{}, error)) {
	if leader == pr.peer.ID {
		if cb != nil {
			cb(nil, nil)
		}
		return
	}

	c := acquireCMD()
	c.cmdType = cmdTransferLeader
	c.cb = cb
	c.data = leader
	err := pr.cmds.Put(c)
	if err != nil {
		log.Fatalf("%s change leader to %d event failed with %+v",
			pr.tag,
			leader,
			err)
	}
}

// Do do something async
func (pr *PeerReplica) Do(doFunc func(error), timeout time.Duration) {
	completeC := make(chan struct{})

	c := acquireCMD()
	c.cmdType = cmdCustom
	c.data = doFunc
	c.cb = func(data interface{}, err error) {
		completeC <- struct{}{}
	}

	err := pr.cmds.Put(c)
	if err != nil {
		close(completeC)
		doFunc(err)
		releaseCMD(c)
		return
	}

	select {
	case <-completeC:
		close(completeC)
	case <-time.After(timeout):
		close(completeC)
		doFunc(ErrDoTimeout)
	}
}

func (pr *PeerReplica) startElector() {
	ctx, cancel := context.WithCancel(context.Background())
	pr.cancel = cancel

	go pr.elector.ElectionLoop(ctx, pr.meta.ID(), fmt.Sprintf("peer-%d", pr.peer.ID), pr.becomeLeader, pr.becomeFollower)
}

func (pr *PeerReplica) becomeLeader() {
	c := acquireCMD()
	c.cmdType = cmdBecomeLeader

	err := pr.cmds.Put(c)
	if err != nil {
		log.Fatalf("%s add become leader event failed with %+v",
			pr.tag,
			err)
	}
}

func (pr *PeerReplica) becomeFollower() {
	completeC := make(chan struct{})

	c := acquireCMD()
	c.cmdType = cmdBecomeFollower
	c.cb = func(data interface{}, err error) {
		completeC <- struct{}{}
	}

	err := pr.cmds.Put(c)
	if err != nil {
		log.Warningf("%s add become follower event failed with %+v",
			pr.tag,
			err)
		c.cb(nil, nil)
	}

	select {
	case <-completeC:
		close(completeC)
		return
	case <-time.After(time.Minute):
		if !pr.cmds.IsDisposed() {
			log.Fatalf("%s become follower event handle timeout",
				pr.tag)
		}
	}

	close(completeC)
}

func (pr *PeerReplica) onHB(msg *hbMsg) interface{} {
	completeC := make(chan interface{})

	c := acquireCMD()
	c.cmdType = cmdHB
	c.data = msg.res
	c.cb = func(data interface{}, err error) {
		completeC <- data
	}

	err := pr.cmds.Put(c)
	if err != nil {
		log.Fatalf("%s add heartbeat event failed with %+v",
			pr.tag,
			err)
	}

	select {
	case rsp := <-completeC:
		close(completeC)
		return rsp
	case <-time.After(time.Minute):
		if !pr.cmds.IsDisposed() {
			log.Fatalf("%s add heartbeat event handle timeout",
				pr.tag)
		}

	}

	close(completeC)
	return nil
}

func (pr *PeerReplica) onHBACK(msg *hbACKMsg) interface{} {
	completeC := make(chan interface{})

	c := acquireCMD()
	c.cmdType = cmdHBACK
	c.data = msg
	c.cb = func(data interface{}, err error) {
		completeC <- data
	}

	err := pr.cmds.Put(c)
	if err != nil {
		log.Fatalf("%s add heartbeat ack event failed with %+v",
			pr.tag,
			err)
	}

	select {
	case rsp := <-completeC:
		close(completeC)
		return rsp
	case <-time.After(time.Minute):
		if !pr.cmds.IsDisposed() {
			log.Fatalf("%s add heartbeat ack event handle timeout",
				pr.tag)
		}
	}

	close(completeC)
	return nil
}

func (pr *PeerReplica) onRemove(msg *removeMsg) interface{} {
	completeC := make(chan interface{})

	c := acquireCMD()
	c.cmdType = cmdRemove
	c.data = msg
	c.cb = func(data interface{}, err error) {
		completeC <- data
	}

	err := pr.cmds.Put(c)
	if err != nil {
		log.Fatalf("%s add remove event failed with %+v",
			pr.tag,
			err)
	}

	select {
	case rsp := <-completeC:
		close(completeC)
		return rsp
	case <-time.After(time.Minute):
		log.Fatalf("%s add remove ack event handle timeout",
			pr.tag)
	}

	return nil
}

func (pr *PeerReplica) handleEvent() bool {
	if pr.cmds.Len() == 0 && !pr.cmds.IsDisposed() {
		return false
	}

	data, err := pr.cmds.Get()
	if err != nil {
		return false
	}

	c := data.(*cmd)

	if c.cmdType == cmdSend {
		pr.handleSendHB(c)
	} else if c.cmdType == cmdHB {
		pr.handleHB(c)
	} else if c.cmdType == cmdHBACK {
		pr.handleHBACK(c)
	} else if c.cmdType == cmdRemove {
		pr.handleRemove(c)
	} else if c.cmdType == cmdAddPeer {
		pr.handleAddPeer(c)
	} else if c.cmdType == cmdScale {
		pr.handleScale(c)
	} else if c.cmdType == cmdRemovePeer {
		pr.handleRemovePeer(c)
	} else if c.cmdType == cmdBecomeLeader {
		pr.handleBecomeLeader(c)
	} else if c.cmdType == cmdBecomeFollower {
		pr.handleBecomeFollower(c)
	} else if c.cmdType == cmdTransferLeader {
		pr.handleTransferLeader(c)
	} else if c.cmdType == cmdCustom {
		c.data.(func(error))(nil)
		c.complete(nil, nil)
	} else {
		log.Fatalf("%s handle unknown cmd type %d, %+v",
			pr.tag,
			c.cmdType,
			c)
	}

	releaseCMD(c)
	return true
}

func (pr *PeerReplica) handleRemovePeer(c *cmd) {
	peer := c.data.(Peer)

	if pr.store.Meta().ID() == peer.ContainerID {
		pr.handleRemove(c)
		return
	}

	if pr.handler.RemovePeer(pr.meta, peer) {
		pr.heartbeatsMap.Delete(peer.ID)
	}

	pr.store.LocalStore().MustPutResource(pr.meta)

	pr.store.GetTransport().Send(peer.ContainerID, &removeMsg{
		id: pr.meta.ID(),
	})

	log.Infof("%s peer %+v removed, after removed value %+v",
		pr.tag,
		peer,
		pr.meta)
}

func (pr *PeerReplica) handleAddPeer(c *cmd) {
	if !pr.leader {
		return
	}

	peer := c.data.(Peer)
	_, ok := FindPeer(pr.meta.Peers(), peer.ContainerID)
	if ok {
		return
	}

	pr.handler.AddPeer(pr.meta, peer)
	pr.heartbeatsMap.Store(peer.ID, time.Now())
	pr.pendingPeers = append(pr.pendingPeers, peer)
	pr.store.LocalStore().MustPutResource(pr.meta)

	log.Infof("%s new peer %+v added, after added value %+v",
		pr.tag,
		peer,
		pr.meta)

}

func (pr *PeerReplica) handleScale(c *cmd) {
	if !pr.leader {
		return
	}

	log.Infof("%s handle scale by %+v",
		pr.tag,
		c.data)

	changed, newPRs := pr.handler.Scale(pr.meta, c.data)
	if len(newPRs) == 0 {
		if changed {
			pr.store.LocalStore().MustPutResource(pr.meta)
		}
		return
	}

	var saved []Resource
	saved = append(saved, pr.meta)
	for _, p := range newPRs {
		saved = append(saved, p.meta)
	}
	pr.store.LocalStore().MustPutResource(saved...)

	for _, p := range newPRs {
		pr.store.AddReplica(p)
	}

	log.Infof("%s after scale %+v, create %d new resources",
		pr.tag,
		pr.meta,
		len(newPRs))
}

func (pr *PeerReplica) handleSendHB(c *cmd) {
	if !pr.leader {
		return
	}

	for _, p := range pr.meta.Peers() {
		if p.ContainerID != pr.store.Meta().ID() {
			pr.store.GetTransport().Send(p.ContainerID, &hbMsg{
				res: pr.meta.Clone(),
			})
		}
	}
}

func (pr *PeerReplica) handleHB(c *cmd) {
	res := c.data.(Resource)

	// remote resource is stale, remove it
	if pr.meta.Stale(res) {
		log.Infof("%s remote res %+v is stale, current is %+v, remove it on handle hb",
			pr.tag,
			res,
			pr.meta)

		c.complete(&removeMsg{
			id: pr.meta.ID(),
		}, nil)
		return
	}

	if pr.handler.Heartbeat(res) {
		pr.store.LocalStore().MustPutResource(pr.meta)
		log.Infof("%s updated to %+v",
			pr.tag,
			pr.meta)
	}

	c.complete(&hbACKMsg{
		res:  pr.meta.Clone(),
		peer: pr.peer,
	}, nil)
}

func (pr *PeerReplica) handleHBACK(c *cmd) {
	msg := c.data.(*hbACKMsg)

	// stale current peer, remove
	if msg.res.Stale(pr.meta) {
		log.Infof("%s current res %+v is stale, remove current",
			pr.tag,
			pr.meta)

		pr.handleRemove(c)
		return
	}

	pr.removePendingPeer(msg.peer)
	pr.heartbeatsMap.Store(msg.peer.ID, time.Now())
	c.complete(nil, nil)
}

func (pr *PeerReplica) handleRemove(c *cmd) {
	pr.store.RemoveReplica(pr.meta.ID())
	pr.Destroy()
	c.complete(nil, nil)
	log.Infof("%s destroy complete by remove",
		pr.tag)
}

func (pr *PeerReplica) handleBecomeLeader(c *cmd) {
	if pr.leader {
		return
	}

	log.Infof("%s ********become leader now********",
		pr.tag)
	pr.leader = true
	pr.handler.ResourceBecomeLeader(pr.meta)
}

func (pr *PeerReplica) handleBecomeFollower(c *cmd) {
	if !pr.leader {
		c.complete(nil, nil)
		return
	}

	pr.leader = false
	pr.handler.ResourceBecomeFollower(pr.meta)
	c.complete(nil, nil)
	log.Infof("%s ********become follower now********",
		pr.tag)
}

func (pr *PeerReplica) handleTransferLeader(c *cmd) {
	leader := c.data.(uint64)

	log.Infof("%s my id is %d, transfer leader to peer %d",
		pr.tag,
		pr.peer.ID,
		leader)

	if !pr.leader {
		c.complete(nil, ErrNorLeader)
		return
	}

	if leader == pr.peer.ID {
		c.complete(nil, nil)
		return
	}

	err := pr.elector.ChangeLeaderTo(pr.meta.ID(),
		fmt.Sprintf("peer-%d", pr.peer.ID),
		fmt.Sprintf("peer-%d", leader))
	if err != nil {
		log.Errorf("%s transfer leader to peer %d failed with %+v",
			pr.tag,
			leader,
			err)
		c.complete(nil, err)
		return
	}

	c.cb = nil
	pr.leaderPeer = leader
	pr.handleBecomeFollower(c)
}

func (pr *PeerReplica) removePendingPeer(peer Peer) {
	var values []Peer
	for _, p := range pr.pendingPeers {
		if p.ID != peer.ID {
			values = append(values, p)
		}
	}

	pr.pendingPeers = values
}
