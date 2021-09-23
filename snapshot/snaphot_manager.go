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
	"github.com/matrixorigin/matrixcube/pb/meta"
)

var (
	// Creating creating step
	Creating = 1
	// Sending snapshot sending step
	Sending = 2
)

// SnapshotManager manager snapshot
type SnapshotManager interface {
	Close()
	Register(msg *meta.SnapshotMessage, step int) bool
	Deregister(msg *meta.SnapshotMessage, step int)
	Create(msg *meta.SnapshotMessage) error
	Exists(msg *meta.SnapshotMessage) bool
	WriteTo(msg *meta.SnapshotMessage, conn goetty.IOSession) (uint64, error)
	CleanSnap(msg *meta.SnapshotMessage) error
	ReceiveSnapData(msg *meta.SnapshotMessage) error
	Apply(msg *meta.SnapshotMessage) error
	ReceiveSnapCount() uint64
}
