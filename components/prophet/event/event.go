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

package event

import (
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
)

var (
	// InitEvent event init
	InitEvent uint32 = 1 << 1
	// ShardEvent shard event
	ShardEvent uint32 = 1 << 2
	// StoreEvent store creation event
	StoreEvent uint32 = 1 << 3
	// ShardStatsEvent shard stats
	ShardStatsEvent uint32 = 1 << 4
	// StoreStatsEvent store stats
	StoreStatsEvent uint32 = 1 << 5
	// AllEvent all event
	AllEvent uint32 = 0xffffffff

	names = map[uint32]string{
		InitEvent:       "init",
		ShardEvent:      "shard",
		ShardStatsEvent: "shard-stats",
		StoreEvent:      "store",
		StoreStatsEvent: "store-stats",
		AllEvent:        "all",
	}
)

// TypeName returns event type name
func TypeName(value uint32) string {
	return names[value]
}

// Snapshot cache snapshot
type Snapshot struct {
	Shards  []metapb.Shard
	Stores  []metapb.Store
	Leaders map[uint64]uint64
}

// MatchEvent returns the flag has the target event
func MatchEvent(event, flag uint32) bool {
	return event == 0 || event&flag != 0
}

// NewInitEvent create init event
func NewInitEvent(snap Snapshot) (*rpcpb.InitEventData, error) {
	resp := &rpcpb.InitEventData{}

	for _, v := range snap.Stores {
		data, err := v.Marshal()
		if err != nil {
			return nil, err
		}

		resp.Stores = append(resp.Stores, data)
	}

	for _, v := range snap.Shards {
		data, err := v.Marshal()
		if err != nil {
			return nil, err
		}

		resp.Shards = append(resp.Shards, data)
		resp.Leaders = append(resp.Leaders, snap.Leaders[v.GetID()])
	}

	return resp, nil
}

// NewShardEvent create shard event
func NewShardEvent(target metapb.Shard, leaderID uint64, removed bool, create bool) rpcpb.EventNotify {
	value, err := target.Marshal()
	if err != nil {
		return rpcpb.EventNotify{}
	}

	return rpcpb.EventNotify{
		Type: ShardEvent,
		ShardEvent: &rpcpb.ShardEventData{
			Data:    value,
			Leader:  leaderID,
			Removed: removed,
			Create:  create,
		},
	}
}

// NewShardStatsEvent create shard stats event
func NewShardStatsEvent(stats *metapb.ShardStats) rpcpb.EventNotify {
	return rpcpb.EventNotify{
		Type:            ShardStatsEvent,
		ShardStatsEvent: stats,
	}
}

// NewStoreStatsEvent create store stats event
func NewStoreStatsEvent(stats *metapb.StoreStats) rpcpb.EventNotify {
	return rpcpb.EventNotify{
		Type:            StoreStatsEvent,
		StoreStatsEvent: stats,
	}
}

// NewStoreEvent create store event
func NewStoreEvent(target metapb.Store) rpcpb.EventNotify {
	value, err := target.Marshal()
	if err != nil {
		return rpcpb.EventNotify{}
	}

	return rpcpb.EventNotify{
		Type: StoreEvent,
		StoreEvent: &rpcpb.StoreEventData{
			Data: value,
		},
	}
}
