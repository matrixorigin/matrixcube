// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
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

package mockhbstream

import (
	"errors"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
)

// HeartbeatStream is used to mock HeartbeatStream for test use.
type HeartbeatStream struct {
	ch      chan *rpcpb.ShardHeartbeatRsp
	timeout time.Duration
}

// NewHeartbeatStream creates a new HeartbeatStream.
func NewHeartbeatStream() HeartbeatStream {
	return NewHeartbeatStreamWithTimeout(time.Millisecond * 10)
}

// NewHeartbeatStreamWithTimeout creates a new HeartbeatStream.
func NewHeartbeatStreamWithTimeout(timeout time.Duration) HeartbeatStream {
	return HeartbeatStream{
		ch:      make(chan *rpcpb.ShardHeartbeatRsp),
		timeout: timeout,
	}
}

// Send mocks method.
func (s HeartbeatStream) Send(m *rpcpb.ShardHeartbeatRsp) error {
	select {
	case <-time.After(time.Second):
		return errors.New("timeout")
	case s.ch <- m:
	}
	return nil
}

// SendMsg is used to send the message.
func (s HeartbeatStream) SendMsg(res *core.CachedShard, msg *rpcpb.ShardHeartbeatRsp) {}

// BindStream mock method.
func (s HeartbeatStream) BindStream(containerID uint64, stream opt.HeartbeatStream) {}

// Recv mocks method.
func (s HeartbeatStream) Recv() *rpcpb.ShardHeartbeatRsp {
	select {
	case <-time.After(s.timeout):
		return nil
	case res := <-s.ch:
		return res
	}
}
