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

package snapshot

import (
	"github.com/fagongzi/goetty"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
)

var (
	// Creating creating step
	Creating = 1
	// Sending snapshot sending step
	Sending = 2
)

// SnapshotManager manager snapshot
type SnapshotManager interface {
	Register(msg *bhraftpb.SnapshotMessage, step int) bool
	Deregister(msg *bhraftpb.SnapshotMessage, step int)
	Create(msg *bhraftpb.SnapshotMessage) error
	Exists(msg *bhraftpb.SnapshotMessage) bool
	WriteTo(msg *bhraftpb.SnapshotMessage, conn goetty.IOSession) (uint64, error)
	CleanSnap(msg *bhraftpb.SnapshotMessage) error
	ReceiveSnapData(msg *bhraftpb.SnapshotMessage) error
	Apply(msg *bhraftpb.SnapshotMessage) error
	ReceiveSnapCount() uint64
}
