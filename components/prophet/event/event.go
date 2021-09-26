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
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
)

var (
	// EventInit event init
	EventInit uint32 = 1 << 1
	// EventResource resource event
	EventResource uint32 = 1 << 2
	// EventContainer container create event
	EventContainer uint32 = 1 << 3
	// EventResourceStats resource stats
	EventResourceStats uint32 = 1 << 4
	// EventContainerStats container stats
	EventContainerStats uint32 = 1 << 5
	// EventFlagAll all event
	EventFlagAll = 0xffffffff

	names = map[uint32]string{
		EventInit:           "init",
		EventResource:       "resource",
		EventResourceStats:  "resource-stats",
		EventContainer:      "container",
		EventContainerStats: "container-stats",
	}
)

// EventTypeName returns event type name
func EventTypeName(value uint32) string {
	return names[value]
}

// Snapshot cache snapshot
type Snapshot struct {
	Resources  []metadata.Resource
	Containers []metadata.Container
	Leaders    map[uint64]uint64
}

// MatchEvent returns the flag has the target event
func MatchEvent(event, flag uint32) bool {
	return event == 0 || event&flag != 0
}

// NewInitEvent create init event
func NewInitEvent(snap Snapshot) (*rpcpb.InitEventData, error) {
	resp := &rpcpb.InitEventData{}

	for _, v := range snap.Containers {
		data, err := v.Marshal()
		if err != nil {
			return nil, err
		}

		resp.Containers = append(resp.Containers, data)
	}

	for _, v := range snap.Resources {
		data, err := v.Marshal()
		if err != nil {
			return nil, err
		}

		resp.Resources = append(resp.Resources, data)
		resp.Leaders = append(resp.Leaders, snap.Leaders[v.ID()])
	}

	return resp, nil
}

// NewResourceEvent create resource event
func NewResourceEvent(target metadata.Resource, leaderID uint64, removed bool, create bool) rpcpb.EventNotify {
	value, err := target.Marshal()
	if err != nil {
		return rpcpb.EventNotify{}
	}

	return rpcpb.EventNotify{
		Type: EventResource,
		ResourceEvent: &rpcpb.ResourceEventData{
			Data:    value,
			Leader:  leaderID,
			Removed: removed,
			Create:  create,
		},
	}
}

// NewResourceStatsEvent create resource stats event
func NewResourceStatsEvent(stats *metapb.ResourceStats) rpcpb.EventNotify {
	return rpcpb.EventNotify{
		Type:               EventResourceStats,
		ResourceStatsEvent: stats,
	}
}

// NewContainerStatsEvent create container stats event
func NewContainerStatsEvent(stats *metapb.ContainerStats) rpcpb.EventNotify {
	return rpcpb.EventNotify{
		Type:                EventContainerStats,
		ContainerStatsEvent: stats,
	}
}

// NewContainerEvent create container event
func NewContainerEvent(target metadata.Container) rpcpb.EventNotify {
	value, err := target.Marshal()
	if err != nil {
		return rpcpb.EventNotify{}
	}

	return rpcpb.EventNotify{
		Type: EventContainer,
		ContainerEvent: &rpcpb.ContainerEventData{
			Data: value,
		},
	}
}
