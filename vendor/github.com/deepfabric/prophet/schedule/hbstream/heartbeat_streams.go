package hbstream

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/deepfabric/prophet/core"
	"github.com/deepfabric/prophet/pb/rpcpb"
	"github.com/deepfabric/prophet/schedule/opt"
	"github.com/deepfabric/prophet/util"
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
	msgCh             chan *rpcpb.ResourceHeartbeatRsp
	streamCh          chan streamUpdate
	containerInformer core.ContainerSetInformer
	needRun           bool // For test only.
}

// NewHeartbeatStreams creates a new HeartbeatStreams which enable background running by default.
func NewHeartbeatStreams(ctx context.Context, clusterID uint64, containerInformer core.ContainerSetInformer) *HeartbeatStreams {
	return newHbStreams(ctx, clusterID, containerInformer, true)
}

// NewTestHeartbeatStreams creates a new HeartbeatStreams for test purpose only.
// Please use NewHeartbeatStreams for other usage.
func NewTestHeartbeatStreams(ctx context.Context, clusterID uint64, containerInformer core.ContainerSetInformer, needRun bool) *HeartbeatStreams {
	return newHbStreams(ctx, clusterID, containerInformer, needRun)
}

func newHbStreams(ctx context.Context, clusterID uint64, containerInformer core.ContainerSetInformer, needRun bool) *HeartbeatStreams {
	hbStreamCtx, hbStreamCancel := context.WithCancel(ctx)
	hs := &HeartbeatStreams{
		hbStreamCtx:       hbStreamCtx,
		hbStreamCancel:    hbStreamCancel,
		clusterID:         clusterID,
		streams:           make(map[uint64]opt.HeartbeatStream),
		msgCh:             make(chan *rpcpb.ResourceHeartbeatRsp, heartbeatChanCapacity),
		streamCh:          make(chan streamUpdate, 1),
		containerInformer: containerInformer,
		needRun:           needRun,
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
			util.GetLogger().Fatalf("hb streams runner failed with %+v", err)
		}
	}()

	defer s.wg.Done()

	for {
		select {
		case update := <-s.streamCh:
			s.streams[update.containerID] = update.stream
		case msg := <-s.msgCh:
			containerID := msg.GetTargetPeer().ContainerID
			containerLabel := strconv.FormatUint(containerID, 10)
			container := s.containerInformer.GetContainer(containerID)
			if container == nil {
				util.GetLogger().Errorf("resource %d get container %d failed with not found",
					msg.ResourceID,
					containerID)
				delete(s.streams, containerID)
				continue
			}
			containerAddress := container.Meta.Addr()
			if stream, ok := s.streams[containerID]; ok {
				if err := stream.Send(msg); err != nil {
					util.GetLogger().Errorf("resource %d send heartbeat message failed with %+v",
						msg.ResourceID,
						err)
					delete(s.streams, containerID)
					heartbeatStreamCounter.WithLabelValues(containerAddress, containerLabel, "push", "err").Inc()
				} else {
					heartbeatStreamCounter.WithLabelValues(containerAddress, containerLabel, "push", "ok").Inc()
				}
			} else {
				util.GetLogger().Debug("resource %d heartbeat stream %d not found, skip send message",
					msg.ResourceID,
					containerID)
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
func (s *HeartbeatStreams) SendMsg(res *core.CachedResource, msg *rpcpb.ResourceHeartbeatRsp) {
	if res.GetLeader() == nil {
		return
	}

	msg.ResourceID = res.Meta.ID()
	msg.ResourceEpoch = res.Meta.Epoch()
	msg.TargetPeer = res.GetLeader()

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
