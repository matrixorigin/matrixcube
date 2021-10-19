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
	SelectShard(group uint64, key []byte) (Shard, string)
	// Every do with all shards
	Every(group uint64, mustLeader bool, fn func(shard Shard, store meta.Store) bool)
	// ForeachShards foreach shards
	ForeachShards(group uint64, fn func(shard Shard) bool)
	// GetShard returns the shard by shard id
	GetShard(id uint64) Shard

	// LeaderStore return leader replica store
	LeaderReplicaStore(shardID uint64) meta.Store
	// RandomReplicaStore return random replica store
	RandomReplicaStore(shardID uint64) meta.Store

	// GetShardStats returns the runtime stats info of the shard
	GetShardStats(id uint64) metapb.ResourceStats
	// GetStoreStats returns the runtime stats info of the store
	GetStoreStats(id uint64) metapb.ContainerStats
}

type op struct {
	value uint64
}

func (o *op) next() uint64 {
	return atomic.AddUint64(&o.value, 1)
}

type routerOptions struct {
	logger             *zap.Logger
	fields             []zap.Field
	stopper            *syncutil.Stopper
	removeShardHandler func(id uint64)
	createShardHandler func(shard Shard)
}

func (opts *routerOptions) adjust() {
	opts.logger = log.Adjust(opts.logger)

	if opts.stopper == nil {
		opts.stopper = syncutil.NewStopper()
	}

	if opts.removeShardHandler == nil {
		opts.removeShardHandler = func(id uint64) {}
	}

	if opts.createShardHandler == nil {
		opts.createShardHandler = func(shard Shard) {}
	}
}

type routerBuilder struct {
	options *routerOptions
}

func newRouterBuilder() *routerBuilder {
	return &routerBuilder{
		options: &routerOptions{},
	}
}

func (rb *routerBuilder) withLogger(logger *zap.Logger, fields ...zap.Field) *routerBuilder {
	rb.options.logger = logger
	return rb
}

func (rb *routerBuilder) withStopper(stopper *syncutil.Stopper) *routerBuilder {
	rb.options.stopper = stopper
	return rb
}

func (rb *routerBuilder) withRemoveShardHandle(handle func(id uint64)) *routerBuilder {
	rb.options.removeShardHandler = handle
	return rb
}

func (rb *routerBuilder) withCreatShardHandle(handle func(shard Shard)) *routerBuilder {
	rb.options.createShardHandler = handle
	return rb
}

func (rb *routerBuilder) build(eventC chan rpcpb.EventNotify) (Router, error) {
	return newRouter(eventC, rb.options)
}

type defaultRouter struct {
	options *routerOptions
	logger  *zap.Logger
	eventC  chan rpcpb.EventNotify

	mu struct {
		sync.RWMutex

		keyRanges                map[uint64]*util.ShardTree       // shard.Group -> *util.ShardTree
		leaders                  map[uint64]meta.Store            // shard id -> leader replica store
		stores                   map[uint64]meta.Store            // store id -> metapb.Store metadata
		shards                   map[uint64]Shard                 // shard id -> metapb.Shard
		missingLeaderStoreShards map[uint64]Replica               // shard id -> Replica
		opts                     map[uint64]op                    // shard id -> op
		shardStats               map[uint64]metapb.ResourceStats  // shard id -> metapb.ResourceStats
		storeStats               map[uint64]metapb.ContainerStats // store id -> metapb.ContainerStats
	}
}

func newRouter(eventC chan rpcpb.EventNotify, options *routerOptions) (Router, error) {
	options.adjust()
	r := &defaultRouter{
		options: options,
		logger:  options.logger.Named("router").With(options.fields...),
		eventC:  eventC,
	}
	r.mu.keyRanges = make(map[uint64]*util.ShardTree)
	r.mu.leaders = make(map[uint64]meta.Store)
	r.mu.stores = make(map[uint64]meta.Store)
	r.mu.shards = make(map[uint64]meta.Shard)
	r.mu.missingLeaderStoreShards = make(map[uint64]Replica)
	r.mu.opts = make(map[uint64]op)
	r.mu.shardStats = make(map[uint64]metapb.ResourceStats)
	r.mu.storeStats = make(map[uint64]metapb.ContainerStats)
	return r, nil
}

func (r *defaultRouter) Start() error {
	r.options.stopper.RunWorker(r.eventLoop)
	return nil
}

func (r *defaultRouter) SelectShard(group uint64, key []byte) (Shard, string) {
	shard := r.searchShard(group, key)
	return shard, r.LeaderReplicaStore(shard.ID).ClientAddr
}

func (r *defaultRouter) GetShard(id uint64) Shard {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.mu.shards[id]
}

func (r *defaultRouter) Every(group uint64, mustLeader bool, doFunc func(Shard, meta.Store) bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for id, shard := range r.mu.shards {
		if shard.Group == group {
			next := false
			if mustLeader {
				next = doFunc(shard, r.getLeaderReplicaStoreLocked(id))
			} else {
				storeID := r.selectStoreLocked(shard)
				next = doFunc(shard, r.mustGetStoreLocked(storeID))
			}
			if !next {
				return
			}
		}
	}
}

func (r *defaultRouter) ForeachShards(group uint64, fn func(shard Shard) bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, shard := range r.mu.shards {
		if shard.Group == group {
			if !fn(shard) {
				return
			}
		}
	}
}

func (r *defaultRouter) LeaderReplicaStore(shardID uint64) meta.Store {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.getLeaderReplicaStoreLocked(shardID)
}

func (r *defaultRouter) RandomReplicaStore(shardID uint64) meta.Store {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if shard, ok := r.mu.shards[shardID]; ok {
		return r.mustGetStoreLocked(r.selectStoreLocked(shard))
	}

	return meta.Store{}
}

func (r *defaultRouter) GetShardStats(id uint64) metapb.ResourceStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.mu.shardStats[id]
}

func (r *defaultRouter) GetStoreStats(id uint64) metapb.ContainerStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.mu.storeStats[id]
}

func (r *defaultRouter) eventLoop() {
	r.logger.Info("router event loop task started")

	for {
		select {
		case <-r.options.stopper.ShouldStop():
			r.logger.Info("router event loop task stopped")
			return
		case evt := <-r.eventC:
			r.handleEvent(evt)
		}
	}
}

func (r *defaultRouter) handleEvent(evt rpcpb.EventNotify) {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch evt.Type {
	case event.EventInit:
		r.logger.Info("reset",
			zap.String("event", event.EventTypeName(evt.Type)),
			zap.Int("shard-count", len(evt.InitEvent.Resources)),
			zap.Int("store-count", len(evt.InitEvent.Containers)))
		for key := range r.mu.keyRanges {
			delete(r.mu.keyRanges, key)
		}

		for _, data := range evt.InitEvent.Containers {
			r.updateStoreLocked(data)
		}

		for i, data := range evt.InitEvent.Resources {
			r.updateShardLocked(data, evt.InitEvent.Leaders[i], false, false)
		}
	case event.EventResource:
		r.updateShardLocked(evt.ResourceEvent.Data, evt.ResourceEvent.Leader,
			evt.ResourceEvent.Removed, evt.ResourceEvent.Create)
	case event.EventContainer:
		r.updateStoreLocked(evt.ContainerEvent.Data)
	case event.EventResourceStats:
		r.mu.shardStats[evt.ResourceStatsEvent.ResourceID] = *evt.ResourceStatsEvent
	case event.EventContainerStats:
		r.mu.storeStats[evt.ContainerStatsEvent.ContainerID] = *evt.ContainerStatsEvent
	}
}

func (r *defaultRouter) updateShardLocked(data []byte, leaderReplicaID uint64, removed bool, create bool) {
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

		r.options.removeShardHandler(res.meta.ID)
		if tree, ok := r.mu.keyRanges[res.meta.Group]; ok {
			tree.Remove(res.meta)
		}
		delete(r.mu.shards, res.meta.ID)
		delete(r.mu.missingLeaderStoreShards, res.meta.ID)
		delete(r.mu.leaders, res.meta.ID)
		return
	}

	if create {
		r.logger.Info("need to create shard",
			log.ShardField("shard", res.meta))
		r.options.createShardHandler(res.meta)
		return
	}

	r.mu.shards[res.meta.ID] = res.meta
	r.updateShardKeyRangeLocked(res.meta)

	r.logger.Info("shard metadata updated",
		log.ShardField("shard", res.meta))

	if leaderReplicaID > 0 {
		r.updateLeaderLocked(res.meta.ID, leaderReplicaID)
	}
}

func (r *defaultRouter) updateStoreLocked(data []byte) {
	s := &containerAdapter{}
	err := s.Unmarshal(data)
	if err != nil {
		r.logger.Fatal("fail to unmarshal store",
			zap.Error(err),
			log.HexField("data", data))
	}

	r.mu.stores[s.meta.ID] = s.meta
	for k, v := range r.mu.missingLeaderStoreShards {
		if v.ContainerID == s.meta.ID {
			if _, ok := r.mu.shards[k]; ok {
				r.updateLeaderLocked(k, v.ID)
			}
		}
	}
}

func (r *defaultRouter) updateLeaderLocked(shardID, leaderReplicaID uint64) {
	shard := r.mustGetShardLocked(shardID)

	for _, p := range shard.Replicas {
		if p.ID == leaderReplicaID {
			if s, ok := r.mu.stores[p.ContainerID]; ok {
				delete(r.mu.missingLeaderStoreShards, shardID)
				r.mu.leaders[shard.ID] = s
				r.logger.Info("shard leader updated",
					log.ShardIDField(shardID),
					log.ReplicaField("leader-replica", p),
					zap.String("address", s.ClientAddr))
				return
			}

			// wait store event
			r.mu.missingLeaderStoreShards[shardID] = p
			break
		}
	}

	r.logger.Info("skip shard leader",
		log.ShardIDField(shardID),
		log.ReasonField("missing store"))
}

func (r *defaultRouter) mustGetShardLocked(id uint64) Shard {
	value, ok := r.mu.shards[id]
	if !ok {
		r.logger.Fatal("shard must exist",
			log.ShardIDField(id))
	}

	return value
}

func (r *defaultRouter) updateShardKeyRangeLocked(shard Shard) {
	if tree, ok := r.mu.keyRanges[shard.Group]; ok {
		tree.Update(shard)
		return
	}

	tree := util.NewShardTree()
	tree.Update(shard)

	r.mu.keyRanges[shard.Group] = tree
}

func (r *defaultRouter) mustGetStoreLocked(id uint64) meta.Store {
	value, ok := r.mu.stores[id]
	if !ok {
		r.logger.Fatal("store must exist",
			log.StoreIDField(id))
	}

	return value
}

func (r *defaultRouter) getLeaderReplicaStoreLocked(shardID uint64) meta.Store {
	if value, ok := r.mu.leaders[shardID]; ok {
		return value
	}
	r.logger.Debug("missing leader",
		log.ShardIDField(shardID))
	return meta.Store{}
}

func (r *defaultRouter) selectStoreLocked(shard Shard) uint64 {
	ops := r.mu.opts[shard.ID]
	storeID := shard.Replicas[int(ops.next())%len(shard.Replicas)].ContainerID
	r.mu.opts[shard.ID] = ops
	return storeID
}

func (r *defaultRouter) searchShard(group uint64, key []byte) Shard {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if tree, ok := r.mu.keyRanges[group]; ok {
		return tree.Search(key)
	}
	r.logger.Debug("fail to search shard",
		zap.Uint64("group", group),
		log.HexField("key", key))
	return Shard{}
}
