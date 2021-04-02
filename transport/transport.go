package transport

import (
	"github.com/deepfabric/beehive/pb/bhraftpb"
)

// Transport raft transport
type Transport interface {
	Start()
	Stop()
	Send(*bhraftpb.RaftMessage)
	SendingSnapshotCount() uint64
}
