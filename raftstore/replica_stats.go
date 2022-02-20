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
	"time"

	"github.com/matrixorigin/matrixcube/pb/metapb"
)

type replicaStats struct {
	prophetHeartbeatTime uint64
	writtenKeys          uint64
	writtenBytes         uint64
	readKeys             uint64
	readBytes            uint64
	raftLogSizeHint      uint64
	deleteKeysHint       uint64
	approximateSize      uint64
	approximateKeys      uint64
}

func newReplicaStats() *replicaStats {
	return &replicaStats{}
}

func (rs *replicaStats) heartbeatState() metapb.ResourceStats {
	now := uint64(time.Now().Unix())
	stats := metapb.ResourceStats{
		WrittenBytes:    rs.writtenBytes,
		WrittenKeys:     rs.writtenKeys,
		ReadBytes:       rs.readBytes,
		ReadKeys:        rs.readKeys,
		ApproximateKeys: rs.approximateKeys,
		ApproximateSize: rs.approximateSize,
		Interval: &metapb.TimeInterval{
			Start: rs.prophetHeartbeatTime,
			End:   uint64(time.Now().Unix()),
		},
	}
	rs.prophetHeartbeatTime = now
	return stats
}
