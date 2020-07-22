package raftstore

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/prophet"
	"github.com/fagongzi/util/task"
)

// Router route the request to the corresponding shard
type Router interface {
	// Start the router
	Start() error
	// SelectShard returns a shard and leader store that the key is in the range [shard.Start, shard.End).
	// If returns leader address is "", means the current shard has no leader
	SelectShard(group uint64, key []byte) (uint64, string)
	// Every do with all shards
	Every(uint64, bool, func(uint64, string))

	// LeaderAddress return leader peer store address
	LeaderAddress(uint64) string
	// RandomPeerAddress return random peer store address
	RandomPeerAddress(uint64) string
}

type op struct {
	value uint64
}

func (o *op) next() uint64 {
	return atomic.AddUint64(&o.value, 1)
}

type defaultRouter struct {
	pd          prophet.Prophet
	watcher     *prophet.Watcher
	runner      *task.Runner
	eventC      chan *prophet.EventNotify
	eventTaskID uint64

	keyConvertFunc            keyConvertFunc
	keyRanges                 sync.Map // group id -> *util.ShardTree
	leaders                   sync.Map // shard id -> leader peer store address string
	stores                    sync.Map // store id -> metapb.Store
	shards                    sync.Map // shard id -> metapb.Shard
	missingStoreLeaderChanged sync.Map //
	opts                      sync.Map // shard id -> *op
}

func newRouter(pd prophet.Prophet, runner *task.Runner, keyConvertFunc keyConvertFunc) Router {
	return &defaultRouter{
		pd:             pd,
		watcher:        prophet.NewWatcherWithProphet(pd),
		runner:         runner,
		keyConvertFunc: keyConvertFunc,
	}
}

func (r *defaultRouter) Start() error {
	r.eventC = r.watcher.Watch(prophet.EventFlagResource | prophet.EventFlagContainer | prophet.EventInit)
	id, err := r.runner.RunCancelableTask(r.eventLoop)
	if err != nil {
		return err
	}

	r.eventTaskID = id
	return nil
}

func (r *defaultRouter) SelectShard(group uint64, key []byte) (uint64, string) {
	shard := r.keyConvertFunc(group, key, r.searchShard)
	return shard.ID, r.LeaderAddress(shard.ID)
}

func (r *defaultRouter) Every(group uint64, mustLeader bool, doFunc func(uint64, string)) {
	r.shards.Range(func(key, value interface{}) bool {
		shard := value.(metapb.Shard)
		if shard.Group == group {
			if mustLeader {
				doFunc(shard.ID, r.LeaderAddress(shard.ID))
			} else {
				storeID := r.selectStore(&shard)
				doFunc(shard.ID, r.mustGetStore(storeID).RPCAddr)
			}
		}

		return true
	})
}

func (r *defaultRouter) LeaderAddress(id uint64) string {
	if value, ok := r.leaders.Load(id); ok {
		return value.(string)
	}

	return ""
}

func (r *defaultRouter) RandomPeerAddress(id uint64) string {
	if value, ok := r.shards.Load(id); ok {
		shard := value.(metapb.Shard)
		return r.mustGetStore(r.selectStore(&shard)).RPCAddr
	}

	return ""
}

func (r *defaultRouter) selectStore(shard *metapb.Shard) uint64 {
	var ops *op
	if v, ok := r.opts.Load(shard.ID); ok {
		ops = v.(*op)
	} else {
		ops = &op{}
		v, exists := r.opts.LoadOrStore(shard.ID, ops)
		if exists {
			ops = v.(*op)
		}
	}

	return shard.Peers[int(ops.next())%len(shard.Peers)].StoreID
}

func (r *defaultRouter) searchShard(group uint64, key []byte) metapb.Shard {
	if value, ok := r.keyRanges.Load(group); ok {
		return value.(*util.ShardTree).Search(key)
	}

	return metapb.Shard{}
}

func (r *defaultRouter) eventLoop(ctx context.Context) {
	logger.Infof("router event loop task started")

	refreshTimer := time.NewTicker(time.Minute)
	defer refreshTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Infof("router event loop task stopped")
			return
		case evt := <-r.eventC:
			r.handleEvent(evt)
		case <-refreshTimer.C:
			r.watcher.Reset()
		}
	}
}

func (r *defaultRouter) handleEvent(evt *prophet.EventNotify) {
	switch evt.Event {
	case prophet.EventInit:
		logger.Infof("start init event")
		r.keyRanges.Range(func(key, value interface{}) bool {
			r.keyRanges.Delete(key)
			return true
		})
		evt.ReadInitEventValues(r.updateShard, r.updateStore)
	case prophet.EventResourceCreated:
		r.updateShard(evt.Value, 0)
	case prophet.EventResourceChaned:
		r.updateShard(evt.Value, 0)
	case prophet.EventResourceLeaderChanged:
		shardID, leaderID := evt.ReadLeaderChangerValue()
		r.updateLeader(shardID, leaderID)
	case prophet.EventContainerCreated:
		r.updateStore(evt.Value)
	case prophet.EventContainerChanged:
		r.updateStore(evt.Value)
	}
}

func (r *defaultRouter) updateShard(data []byte, leader uint64) {
	res := &resourceAdapter{}
	err := res.Unmarshal(data)
	if err != nil {
		logger.Fatalf("unmarshal shard failed with %+v", err)
	}

	r.shards.Store(res.meta.ID, res.meta)
	r.updateShardKeyRange(res.meta)
	if leader > 0 {
		r.updateLeader(res.meta.ID, leader)
	}

	if v, ok := r.missingStoreLeaderChanged.Load(res.meta.ID); ok {
		r.updateLeader(res.meta.ID, v.(uint64))
	}
}

func (r *defaultRouter) updateStore(data []byte) {
	s := &containerAdapter{}
	err := s.Unmarshal(data)
	if err != nil {
		logger.Fatalf("unmarshal store failed with %+v", err)
	}

	r.stores.Store(s.meta.ID, s.meta)
}

func (r *defaultRouter) updateLeader(shardID, leader uint64) {
	shard := r.mustGetShard(shardID)

	for _, p := range shard.Peers {
		if p.ID == leader {
			r.missingStoreLeaderChanged.Delete(shardID)
			r.leaders.Store(shard.ID, r.mustGetStore(p.StoreID).RPCAddr)
			return
		}
	}

	// the shard updated will notify later
	r.missingStoreLeaderChanged.Store(shardID, leader)
}

func (r *defaultRouter) mustGetStore(id uint64) metapb.Store {
	value, ok := r.stores.Load(id)
	if !ok {
		logger.Fatalf("BUG: store %d must exist", id)
	}

	return value.(metapb.Store)
}

func (r *defaultRouter) mustGetShard(id uint64) metapb.Shard {
	value, ok := r.shards.Load(id)
	if !ok {
		logger.Fatalf("BUG: shard %d must exist", id)
	}

	return value.(metapb.Shard)
}

func (r *defaultRouter) updateShardKeyRange(shard metapb.Shard) {
	if value, ok := r.keyRanges.Load(shard.Group); ok {
		value.(*util.ShardTree).Update(shard)
		return
	}

	tree := util.NewShardTree()
	tree.Update(shard)

	value, loaded := r.keyRanges.LoadOrStore(shard.Group, tree)
	if loaded {
		value.(*util.ShardTree).Update(shard)
	}
}
