package prophet

import (
	"context"
	"sync"
	"time"

	"github.com/fagongzi/util/task"
)

// ResourceStore is a container of resources, which maintains a set of resources
type ResourceStore interface {
	Start()

	Meta() Container
	GetTransport() ReplicaTransport
	GetContainerAddr(uint64) (string, error)
	HandleReplicaMsg(interface{}) interface{}

	ForeachReplica(func(*PeerReplica) bool)

	// GetPeerReplica returns a peer replicatation from the store,
	// when `leader` is true, only return the leader replicatation
	GetPeerReplica(id uint64, leader bool) *PeerReplica
	// AddReplica add a replicatation
	AddReplica(*PeerReplica)
	RemoveReplica(id uint64)

	MustStartTask(func(context.Context)) uint64
	MustStopTask(uint64)

	LocalStore() LocalStore
}

type defaultResourceStore struct {
	sync.RWMutex

	meta      Container
	transport ReplicaTransport

	pd          Prophet
	localStore  LocalStore
	handler     PeerReplicaHandler
	replicas    sync.Map // uint64 -> peer_replica
	runner      *task.Runner
	hbInterval  time.Duration
	workerCount uint64

	elector Elector
}

// NewResourceStore creates a resource store
func NewResourceStore(meta Container,
	localStore LocalStore,
	pd Prophet,
	elector Elector,
	handler PeerReplicaHandler,
	factory func() Resource,
	hbInterval time.Duration,
	workerCount uint64) ResourceStore {
	s := &defaultResourceStore{
		meta:        meta,
		pd:          pd,
		hbInterval:  hbInterval,
		handler:     handler,
		workerCount: workerCount,
		localStore:  localStore,
		elector:     elector,
		runner:      task.NewRunner(),
	}
	s.transport = NewReplicaTransport(meta.ShardAddr(), s, factory)
	return s
}

func (s *defaultResourceStore) Start() {
	s.transport.Start()
	log.Infof("sharding transport start at %s", s.meta.ShardAddr())

	_, err := s.runner.RunCancelableTask(s.runHBTask)
	if err != nil {
		log.Fatalf("run hb task failed with %+v", err)
	}

	for i := uint64(0); i < s.workerCount; i++ {
		idx := uint64(i)
		_, err = s.runner.RunCancelableTask(func(ctx context.Context) {
			s.runPRTask(ctx, idx)
		})
		if err != nil {
			log.Fatalf("run pr event loop task failed with %+v", err)
		}
	}
}

func (s *defaultResourceStore) LocalStore() LocalStore {
	return s.localStore
}

func (s *defaultResourceStore) Meta() Container {
	return s.meta
}

func (s *defaultResourceStore) GetTransport() ReplicaTransport {
	return s.transport
}

func (s *defaultResourceStore) GetContainerAddr(id uint64) (string, error) {
	c, err := s.pd.GetStore().GetContainer(id)
	if err != nil {
		return "", err
	}

	if c == nil {
		return "", nil
	}

	return c.ShardAddr(), nil
}

func (s *defaultResourceStore) ForeachReplica(fn func(*PeerReplica) bool) {
	s.replicas.Range(func(key, value interface{}) bool {
		return fn(value.(*PeerReplica))
	})
}

func (s *defaultResourceStore) GetPeerReplica(id uint64, leader bool) *PeerReplica {
	if pr, ok := s.replicas.Load(id); ok {
		p := pr.(*PeerReplica)
		if !leader ||
			(leader && p.IsLeader()) {
			return p
		}

		return nil
	}

	return nil
}

func (s *defaultResourceStore) AddReplica(pr *PeerReplica) {
	pr.workerID = uint64(s.workerCount-1) & pr.meta.ID()
	s.replicas.Store(pr.meta.ID(), pr)

	log.Infof("%s added to local resource store", pr.tag)
}

func (s *defaultResourceStore) HandleReplicaMsg(data interface{}) interface{} {
	if msg, ok := data.(*hbMsg); ok {
		return s.handleHB(msg)
	} else if msg, ok := data.(*hbACKMsg); ok {
		return s.handleHBACK(msg)
	} else if msg, ok := data.(*removeMsg); ok {
		return s.handleRemovePR(msg)
	}

	log.Fatalf("not support msg %T: %+v",
		data,
		data)
	return nil
}

func (s *defaultResourceStore) MustStartTask(task func(context.Context)) uint64 {
	id, err := s.runner.RunCancelableTask(task)
	if err != nil {
		log.Fatalf("run task failed with %+v", err)
	}

	return id
}

func (s *defaultResourceStore) MustStopTask(task uint64) {
	err := s.runner.StopCancelableTask(task)
	if err != nil {
		log.Fatalf("stop task %d failed with %+v", task, err)
	}
}

func (s *defaultResourceStore) runPRTask(ctx context.Context, id uint64) {
	log.Infof("pr event worker %d start", id)

	hasEvent := true
	for {
		select {
		case <-ctx.Done():
			log.Infof("pr event worker %d exit", id)
			return
		default:
			if !hasEvent {
				time.Sleep(time.Millisecond * 10)
			}

			hasEvent = false
			s.ForeachReplica(func(pr *PeerReplica) bool {
				if pr.workerID == id && pr.handleEvent() {
					hasEvent = true
				}

				return true
			})
		}
	}
}

func (s *defaultResourceStore) runHBTask(ctx context.Context) {
	log.Infof("resource store hb task started")

	ticker := time.NewTicker(s.hbInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Infof("resource store hb task exit")
			return
		case <-ticker.C:
			s.doHB()
		}
	}
}

func (s *defaultResourceStore) doHB() {
	s.ForeachReplica(func(pr *PeerReplica) bool {
		if pr.IsLeader() {
			pr.Heartbeat()
		}

		return true
	})
}

func (s *defaultResourceStore) handleHB(msg *hbMsg) interface{} {
	pr := s.GetPeerReplica(msg.res.ID(), false)
	if pr == nil {
		pr, err := CreatePeerReplica(s, msg.res, s.handler, s.elector)
		if err != nil {
			log.Fatalf("create peer replica %+v failed with %+v",
				msg.res,
				err)
			return nil
		}

		s.AddReplica(pr)
		return nil
	}

	return pr.onHB(msg)
}

func (s *defaultResourceStore) handleHBACK(msg *hbACKMsg) interface{} {
	pr := s.GetPeerReplica(msg.res.ID(), true)
	if pr == nil {
		return nil
	}

	rsp := pr.onHBACK(msg)
	if rsp != nil {
		s.transport.Send(msg.peer.ContainerID, rsp)
	}

	return nil
}

func (s *defaultResourceStore) handleRemovePR(msg *removeMsg) interface{} {
	pr := s.GetPeerReplica(msg.id, false)
	if pr == nil {
		return nil
	}

	pr.onRemove(msg)
	return nil
}

func (s *defaultResourceStore) RemoveReplica(id uint64) {
	s.replicas.Delete(id)
}
