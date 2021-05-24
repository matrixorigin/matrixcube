package mockhbstream

import (
	"errors"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
)

// HeartbeatStream is used to mock HeartbeatStream for test use.
type HeartbeatStream struct {
	ch      chan *rpcpb.ResourceHeartbeatRsp
	timeout time.Duration
}

// NewHeartbeatStream creates a new HeartbeatStream.
func NewHeartbeatStream() HeartbeatStream {
	return NewHeartbeatStreamWithTimeout(time.Millisecond * 10)
}

// NewHeartbeatStreamWithTimeout creates a new HeartbeatStream.
func NewHeartbeatStreamWithTimeout(timeout time.Duration) HeartbeatStream {
	return HeartbeatStream{
		ch:      make(chan *rpcpb.ResourceHeartbeatRsp),
		timeout: timeout,
	}
}

// Send mocks method.
func (s HeartbeatStream) Send(m *rpcpb.ResourceHeartbeatRsp) error {
	select {
	case <-time.After(time.Second):
		return errors.New("timeout")
	case s.ch <- m:
	}
	return nil
}

// SendMsg is used to send the message.
func (s HeartbeatStream) SendMsg(res *core.CachedResource, msg *rpcpb.ResourceHeartbeatRsp) {}

// BindStream mock method.
func (s HeartbeatStream) BindStream(containerID uint64, stream opt.HeartbeatStream) {}

// Recv mocks method.
func (s HeartbeatStream) Recv() *rpcpb.ResourceHeartbeatRsp {
	select {
	case <-time.After(s.timeout):
		return nil
	case res := <-s.ch:
		return res
	}
}
