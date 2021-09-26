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

package raftstore

import (
	"sync"
	"sync/atomic"

	"github.com/lni/goutils/syncutil"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet"
	"github.com/matrixorigin/matrixcube/components/prophet/event"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/util"
	"go.uber.org/zap"
)

// Router route the request to the corresponding shard
type Router interface {
	// Start the router
	Start() error
	// SelectShard returns a shard and leader store that the key is in the range [shard.Start, shard.End).
	// If returns leader address is "", means the current shard has no leader
	SelectShard(group uint64, key []byte) (uint64, string)
	// Every do with all shards
	Every(group uint64, mustLeader bool, fn func(shard *Shard, store meta.Store))
	// ForeachShards foreach shards
	ForeachShards(group uint64, fn func(shard *Shard) bool)

	// LeaderStore return leader replica store
	LeaderReplicaStore(shardID uint64) meta.Store
	// RandomReplicaStore return random replica store
	RandomReplicaStore(shardID uint64) meta.Store

	// GetShardStats returns the runtime stats info of the shard
	GetShardStats(id uint64) *metapb.ResourceStats
	// GetStoreStats returns the runtime stats info of the store
	GetStoreStats(id uint64) *metapb.ContainerStats

	// GetWatcher returns the prophet event watcher
	GetWatcher() prophet.Watcher
}

type op struct {
	value uint64
}

func (o *op) next() uint64 {
	return atomic.AddUint64(&o.value, 1)
}

type defaultRouter struct {
	logger                    *zap.Logger
	watcher                   prophet.Watcher
	stopper                   *syncutil.Stopper
	eventC                    chan rpcpb.EventNotify
	keyRanges                 sync.Map // group id -> *util.ShardTree
	leaders                   sync.Map // shard id -> leader replica store
	stores                    sync.Map // store id -> metapb.Store metadata
	shards                    sync.Map // shard id -> metapb.Shard
	missingStoreLeaderChanged sync.Map // shard id -> leader replica id
	opts                      sync.Map // shard id -> *op
	shardStats                sync.Map // shard id -> ResourceStats
	storeStats                sync.Map // store id -> ContainerStats

	removedHandleFunc func(id uint64)
	createHandleFunc  func(shard Shard)
}

func newRouter(store *store, watcher prophet.Watcher, removedHandleFunc func(id uint64), createHandleFunc func(shard Shard)) (Router, error) {
	return &defaultRouter{
		logger:            store.logger.Named("router").With(store.storeField()),
		stopper:           store.stopper,
		watcher:           watcher,
		eventC:            watcher.GetNotify(),
		removedHandleFunc: removedHandleFunc,
		createHandleFunc:  createHandleFunc,
	}, nil
}

func (r *defaultRouter) GetWatcher() prophet.Watcher {
	return r.watcher
}

func (r *defaultRouter) Start() error {
	r.stopper.RunWorker(r.eventLoop)
	return nil
}

func (r *defaultRouter) SelectShard(group uint64, key []byte) (uint64, string) {
	shard := r.searchShard(group, key)
	return shard.ID, r.LeaderReplicaStore(shard.ID).ClientAddr
}

func (r *defaultRouter) Every(group uint64, mustLeader bool, doFunc func(*Shard, meta.Store)) {
	r.shards.Range(func(key, value interface{}) bool {
		shard := value.(Shard)
		if shard.Group == group {
			if mustLeader {
				doFunc(&shard, r.LeaderReplicaStore(shard.ID))
			} else {
				storeID := r.selectStore(&shard)
				doFunc(&shard, r.mustGetStore(storeID))
			}
		}

		return true
	})
}

func (r *defaultRouter) ForeachShards(group uint64, fn func(shard *Shard) bool) {
	r.shards.Range(func(key, value interface{}) bool {
		shard := value.(Shard)
		if shard.Group == group {
			return fn(&shard)
		}

		return true
	})
}

func (r *defaultRouter) LeaderReplicaStore(shardID uint64) meta.Store {
	if value, ok := r.leaders.Load(shardID); ok {
		return value.(meta.Store)
	}
	r.logger.Debug("missing leader",
		log.ShardIDField(shardID))
	return meta.Store{}
}

func (r *defaultRouter) RandomReplicaStore(shardID uint64) meta.Store {
	if value, ok := r.shards.Load(shardID); ok {
		shard := value.(Shard)
		return r.mustGetStore(r.selectStore(&shard))
	}

	return meta.Store{}
}

func (r *defaultRouter) GetShardStats(id uint64) *metapb.ResourceStats {
	if v, ok := r.shardStats.Load(id); ok {
		return v.(*metapb.ResourceStats)
	}

	return nil
}

func (r *defaultRouter) GetStoreStats(id uint64) *metapb.ContainerStats {
	if v, ok := r.storeStats.Load(id); ok {
		return v.(*metapb.ContainerStats)
	}

	return nil
}

func (r *defaultRouter) selectStore(shard *Shard) uint64 {
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

	return shard.Replicas[int(ops.next())%len(shard.Replicas)].ContainerID
}

func (r *defaultRouter) searchShard(group uint64, key []byte) Shard {
	if value, ok := r.keyRanges.Load(group); ok {
		return value.(*util.ShardTree).Search(key)
	}
	r.logger.Debug("fail to search shard",
		zap.Uint64("group", group),
		log.HexField("key", key))
	return Shard{}
}

func (r *defaultRouter) eventLoop() {
	r.logger.Info("router event loop task started")

	for {
		select {
		case <-r.stopper.ShouldStop():
			r.logger.Info("router event loop task stopped")
			return
		case evt := <-r.eventC:
			r.handleEvent(evt)
		}
	}
}

func (r *defaultRouter) handleEvent(evt rpcpb.EventNotify) {
	switch evt.Type {
	case event.EventInit:
		r.logger.Info("reset",
			zap.String("event", event.EventTypeName(evt.Type)),
			zap.Int("shard-count", len(evt.InitEvent.Resources)),
			zap.Int("store-count", len(evt.InitEvent.Containers)))
		r.keyRanges.Range(func(key, value interface{}) bool {
			r.keyRanges.Delete(key)
			return true
		})

		for _, data := range evt.InitEvent.Containers {
			r.updateStore(data)
		}

		for i, data := range evt.InitEvent.Resources {
			r.updateShard(data, evt.InitEvent.Leaders[i], false, false)
		}
	case event.EventResource:
		r.updateShard(evt.ResourceEvent.Data, evt.ResourceEvent.Leader,
			evt.ResourceEvent.Removed, evt.ResourceEvent.Create)
	case event.EventContainer:
		r.updateStore(evt.ContainerEvent.Data)
	case event.EventResourceStats:
		r.shardStats.Store(evt.ResourceStatsEvent.ResourceID, evt.ResourceStatsEvent)
	case event.EventContainerStats:
		r.storeStats.Store(evt.ContainerStatsEvent.ContainerID, evt.ContainerStatsEvent)
	}
}

func (r *defaultRouter) updateShard(data []byte, leader uint64, removed bool, create bool) {
	res := &resourceAdapter{}
	err := res.Unmarshal(data)
	if err != nil {
		r.logger.Fatal("fail to unmarshal shard",
			zap.Error(err),
			log.HexField("data", data))
	}

	if removed {
		r.logger.Info("need to delete shard",
			log.ShardField("shard", res.meta))

		r.removedHandleFunc(res.meta.ID)
		if value, ok := r.keyRanges.Load(res.meta.Group); ok {
			value.(*util.ShardTree).Remove(res.meta)
		}
		r.shards.Delete(res.meta.ID)
		r.missingStoreLeaderChanged.Delete(res.meta.ID)
		r.leaders.Delete(res.meta.ID)
		return
	}

	if create {
		r.logger.Info("need to create shard",
			log.ShardField("shard", res.meta))
		r.createHandleFunc(res.meta)
		return
	}

	r.shards.Store(res.meta.ID, res.meta)
	r.updateShardKeyRange(res.meta)

	r.logger.Info("shard metadata updated",
		log.ShardField("shard", res.meta))

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
		r.logger.Fatal("fail to unmarshal store",
			zap.Error(err),
			log.HexField("data", data))
	}

	r.stores.Store(s.meta.ID, s.meta)
}

func (r *defaultRouter) updateLeader(shardID, leader uint64) {
	shard := r.mustGetShard(shardID)

	for _, p := range shard.Replicas {
		if p.ID == leader {
			s := r.mustGetStore(p.ContainerID)
			r.missingStoreLeaderChanged.Delete(shardID)
			r.leaders.Store(shard.ID, s)
			r.logger.Info("shard leader updated",
				log.ShardIDField(shardID),
				log.ReplicaField("leader-replica", p),
				zap.String("address", s.ClientAddr))
			return
		}
	}

	// the shard updated will notify later
	r.missingStoreLeaderChanged.Store(shardID, leader)
	r.logger.Info("skip shard leader",
		log.ShardIDField(shardID),
		log.ReasonField("missing store"))
}

func (r *defaultRouter) mustGetStore(id uint64) meta.Store {
	value, ok := r.stores.Load(id)
	if !ok {
		r.logger.Fatal("store must exist",
			log.StoreIDField(id))
	}

	return value.(meta.Store)
}

func (r *defaultRouter) mustGetShard(id uint64) Shard {
	value, ok := r.shards.Load(id)
	if !ok {
		r.logger.Fatal("shard must exist",
			log.ShardIDField(id))
	}

	return value.(Shard)
}

func (r *defaultRouter) updateShardKeyRange(shard Shard) {
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
