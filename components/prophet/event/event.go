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
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
)

var (
	// EventInit event init
	EventInit uint32 = 1 << 1
	// EventShard resource event
	EventShard uint32 = 1 << 2
	// EventStore container create event
	EventStore uint32 = 1 << 3
	// EventShardStats resource stats
	EventShardStats uint32 = 1 << 4
	// EventStoreStats container stats
	EventStoreStats uint32 = 1 << 5
	// EventFlagAll all event
	EventFlagAll = 0xffffffff

	names = map[uint32]string{
		EventInit:           "init",
		EventShard:       "resource",
		EventShardStats:  "resource-stats",
		EventStore:      "container",
		EventStoreStats: "container-stats",
	}
)

// EventTypeName returns event type name
func EventTypeName(value uint32) string {
	return names[value]
}

// Snapshot cache snapshot
type Snapshot struct {
	Shards  []*metadata.ShardWithRWLock
	Stores []*metadata.StoreWithRWLock
	Leaders    map[uint64]uint64
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
		resp.Leaders = append(resp.Leaders, snap.Leaders[v.ID()])
	}

	return resp, nil
}

// NewShardEvent create resource event
func NewShardEvent(target *metadata.ShardWithRWLock, leaderID uint64, removed bool, create bool) rpcpb.EventNotify {
	value, err := target.Marshal()
	if err != nil {
		return rpcpb.EventNotify{}
	}

	return rpcpb.EventNotify{
		Type: EventShard,
		ShardEvent: &rpcpb.ShardEventData{
			Data:    value,
			Leader:  leaderID,
			Removed: removed,
			Create:  create,
		},
	}
}

// NewShardStatsEvent create resource stats event
func NewShardStatsEvent(stats *metapb.ShardStats) rpcpb.EventNotify {
	return rpcpb.EventNotify{
		Type:               EventShardStats,
		ShardStatsEvent: stats,
	}
}

// NewStoreStatsEvent create container stats event
func NewStoreStatsEvent(stats *metapb.StoreStats) rpcpb.EventNotify {
	return rpcpb.EventNotify{
		Type:                EventStoreStats,
		StoreStatsEvent: stats,
	}
}

// NewStoreEvent create container event
func NewStoreEvent(target *metadata.StoreWithRWLock) rpcpb.EventNotify {
	value, err := target.Marshal()
	if err != nil {
		return rpcpb.EventNotify{}
	}

	return rpcpb.EventNotify{
		Type: EventStore,
		StoreEvent: &rpcpb.StoreEventData{
			Data: value,
		},
	}
}
