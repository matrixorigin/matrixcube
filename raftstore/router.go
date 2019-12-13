package raftstore

import (
	"context"
	"sync"

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
}

type defaultRouter struct {
	pd          prophet.Prophet
	watcher     *prophet.Watcher
	runner      *task.Runner
	eventC      chan *prophet.EventNotify
	eventTaskID uint64

	keyConvertFunc keyConvertFunc
	keyRanges      sync.Map // group id -> *util.ShardTree
	leaders        sync.Map // shard id -> leader peer store address string
	stores         sync.Map // store id -> metapb.Store
	shards         sync.Map // shard id -> metapb.Shard
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
	if value, ok := r.leaders.Load(shard.ID); ok {
		return shard.ID, value.(string)
	}
	return shard.ID, ""
}

func (r *defaultRouter) searchShard(group uint64, key []byte) metapb.Shard {
	if value, ok := r.keyRanges.Load(group); ok {
		return value.(*util.ShardTree).Search(key)
	}

	return metapb.Shard{}
}

func (r *defaultRouter) eventLoop(ctx context.Context) {
	logger.Infof("router event loop task started")

	for {
		select {
		case <-ctx.Done():
			logger.Infof("router event loop task stopped")
			return
		case evt := <-r.eventC:
			r.handleEvent(evt)
		}
	}
}

func (r *defaultRouter) handleEvent(evt *prophet.EventNotify) {
	switch evt.Event {
	case prophet.EventInit:
		logger.Infof("start init event")
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
			r.leaders.Store(shard.ID, r.mustGetStore(p.StoreID).RPCAddr)
			return
		}
	}

	logger.Fatalf("BUG: missing leader store")
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
