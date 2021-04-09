package transport

import (
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
)

// Transport raft transport
type Transport interface {
	Start()
	Stop()
	Send(*bhraftpb.RaftMessage)
	SendingSnapshotCount() uint64
}
