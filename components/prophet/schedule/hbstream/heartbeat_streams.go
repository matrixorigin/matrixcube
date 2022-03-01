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

package hbstream

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"go.uber.org/zap"
)

const (
	heartbeatStreamKeepAliveInterval = time.Minute
	heartbeatChanCapacity            = 1024
)

type streamUpdate struct {
	containerID uint64
	stream      opt.HeartbeatStream
}

// HeartbeatStreams is the bridge of communication with your storage application instance.
type HeartbeatStreams struct {
	wg                sync.WaitGroup
	hbStreamCtx       context.Context
	hbStreamCancel    context.CancelFunc
	clusterID         uint64
	streams           map[uint64]opt.HeartbeatStream
	msgCh             chan *rpcpb.ShardHeartbeatRsp
	streamCh          chan streamUpdate
	containerInformer core.StoreSetInformer
	logger            *zap.Logger
	needRun           bool // For test only.
}

// NewHeartbeatStreams creates a new HeartbeatStreams which enable background running by default.
func NewHeartbeatStreams(ctx context.Context, clusterID uint64, containerInformer core.StoreSetInformer, logger *zap.Logger) *HeartbeatStreams {
	return newHbStreams(ctx, clusterID, containerInformer, true, logger)
}

// NewTestHeartbeatStreams creates a new HeartbeatStreams for test purpose only.
// Please use NewHeartbeatStreams for other usage.
func NewTestHeartbeatStreams(ctx context.Context, clusterID uint64, containerInformer core.StoreSetInformer, needRun bool, logger *zap.Logger) *HeartbeatStreams {
	return newHbStreams(ctx, clusterID, containerInformer, needRun, logger)
}

func newHbStreams(ctx context.Context, clusterID uint64, containerInformer core.StoreSetInformer, needRun bool, logger *zap.Logger) *HeartbeatStreams {
	hbStreamCtx, hbStreamCancel := context.WithCancel(ctx)
	hs := &HeartbeatStreams{
		hbStreamCtx:       hbStreamCtx,
		hbStreamCancel:    hbStreamCancel,
		clusterID:         clusterID,
		streams:           make(map[uint64]opt.HeartbeatStream),
		msgCh:             make(chan *rpcpb.ShardHeartbeatRsp, heartbeatChanCapacity),
		streamCh:          make(chan streamUpdate, 1),
		containerInformer: containerInformer,
		needRun:           needRun,
		logger:            log.Adjust(logger),
	}
	if needRun {
		hs.wg.Add(1)
		go hs.run()
	}
	return hs
}

func (s *HeartbeatStreams) run() {
	defer func() {
		if err := recover(); err != nil {
			panic(fmt.Sprintf("hb streams runner failed with %+v", err))
		}
	}()

	defer s.wg.Done()

	for {
		select {
		case update := <-s.streamCh:
			s.streams[update.containerID] = update.stream
		case msg := <-s.msgCh:
			containerID := msg.GetTargetReplica().StoreID
			containerLabel := strconv.FormatUint(containerID, 10)
			container := s.containerInformer.GetStore(containerID)
			if container == nil {
				s.logger.Error("fail to get container, not found",
					log.ResourceField(msg.ShardID),
					zap.Uint64("container", containerID))
				delete(s.streams, containerID)
				continue
			}
			containerAddress := container.Meta.GetClientAddr()
			if stream, ok := s.streams[containerID]; ok {
				if err := stream.Send(msg); err != nil {
					s.logger.Error("fail to send heartbeat message",
						log.ResourceField(msg.ShardID),
						zap.Error(err))
					delete(s.streams, containerID)
					heartbeatStreamCounter.WithLabelValues(containerAddress, containerLabel, "push", "err").Inc()
				} else {
					heartbeatStreamCounter.WithLabelValues(containerAddress, containerLabel, "push", "ok").Inc()
				}
			} else {
				s.logger.Debug("heartbeat stream not found, skip send message",
					log.ResourceField(msg.ShardID),
					zap.Uint64("container", containerID))
				heartbeatStreamCounter.WithLabelValues(containerAddress, containerLabel, "push", "skip").Inc()
			}
		case <-s.hbStreamCtx.Done():
			return
		}
	}
}

// Close closes background running.
func (s *HeartbeatStreams) Close() {
	s.hbStreamCancel()
	s.wg.Wait()
}

// BindStream binds a stream with a specified container.
func (s *HeartbeatStreams) BindStream(containerID uint64, stream opt.HeartbeatStream) {
	update := streamUpdate{
		containerID: containerID,
		stream:      stream,
	}
	select {
	case s.streamCh <- update:
	case <-s.hbStreamCtx.Done():
	}
}

// SendMsg sends a message to related container.
func (s *HeartbeatStreams) SendMsg(res *core.CachedShard, msg *rpcpb.ShardHeartbeatRsp) {
	if res.GetLeader() == nil {
		return
	}

	msg.ShardID = res.Meta.GetID()
	msg.ShardEpoch = res.Meta.GetEpoch()
	msg.TargetReplica = res.GetLeader()

	select {
	case s.msgCh <- msg:
	case <-s.hbStreamCtx.Done():
	}
}

// MsgLength gets the length of msgCh.
// For test only.
func (s *HeartbeatStreams) MsgLength() int {
	return len(s.msgCh)
}

// Drain consumes message from msgCh when disable background running.
// For test only.
func (s *HeartbeatStreams) Drain(count int) error {
	if s.needRun {
		return errors.New("hbstream running enabled")
	}
	for i := 0; i < count; i++ {
		<-s.msgCh
	}
	return nil
}
