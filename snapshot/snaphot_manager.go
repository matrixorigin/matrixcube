package snapshot

import (
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
)

var (
	logger = log.NewLoggerWithPrefix("[snapshot]")
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
