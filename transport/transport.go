// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Copyright 2021 MatrixOrigin.
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
//
// this file is adopted from github.com/lni/dragonboat

package transport

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lni/goutils/netutil"
	circuit "github.com/lni/goutils/netutil/rubyist/circuitbreaker"
	"github.com/lni/goutils/syncutil"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/vfs"
)

const (
	idleTimeout              = 20 * time.Second
	maxMsgBatchSize   uint64 = 1024 * 1024 * 8
	sendQueueLen      uint64 = 512
	concurrencyFactor uint64 = 2
	dialTimeoutSecond uint64 = 10
)

type Trans interface {
	Send(metapb.RaftMessage) bool
	SendSnapshot(metapb.RaftMessage) bool
	SetFilter(func(metapb.RaftMessage) bool)
	SendingSnapshotCount() uint64
	Start() error
	Close() error
}

type StoreResolver func(storeID uint64) (string, error)

type MessageHandler func(metapb.RaftMessageBatch)

type SnapshotChunkHandler func(metapb.SnapshotChunk) bool

type UnreachableHandler func(uint64, uint64)

type SnapshotStatusHandler func(uint64, uint64, raftpb.Snapshot, bool)

type nodeInfo struct {
	ShardID   uint64
	ReplicaID uint64
}

type nodeMap map[nodeInfo]struct{}

// Connection is the interface used by the transport module for sending Raft
// messages. Each Connection works for a specified target store instance,
// it is possible for a target to have multiple concurrent Connection
// instances in use.
type Connection interface {
	// Close closes the Connection instance.
	Close()
	// SendMessageBatch sends the specified message batch to the target. It is
	// recommended to deliver the message batch to the target in order to enjoy
	// best possible performance, but out of order delivery is allowed at the
	// cost of reduced performance.
	SendMessageBatch(batch metapb.RaftMessageBatch) error
}

// SnapshotConnection is the interface used by the transport module for sending
// snapshot chunks. Each SnapshotConnection works for a specified target
// store instance.
type SnapshotConnection interface {
	// Close closes the SnapshotConnection instance.
	Close()
	// SendChunk sends the snapshot chunk to the target. It is
	// recommended to have the snapshot chunk delivered in order for the best
	// performance, but out of order delivery is allowed at the cost of reduced
	// performance.
	SendChunk(chunk metapb.SnapshotChunk) error
}

// TransImpl is the interface to be implemented by a customized transport
// module. A transport module is responsible for exchanging Raft messages,
// snapshots and other metadata between store instances.
type TransImpl interface {
	// Name returns the type name of the TransImpl instance.
	Name() string
	// Start launches the transport module and make it ready to start sending and
	// receiving Raft messages. If necessary, TransImpl may take this opportunity
	// to start listening for incoming data.
	Start() error
	// Close closes the transport module.
	Close() error
	// GetConnection returns an Connection instance used for sending messages
	// to the specified target store instance.
	GetConnection(ctx context.Context, target string) (Connection, error)
	// GetSnapshotConnection returns an Connection instance used for transporting
	// snapshots to the specified store instance.
	GetSnapshotConnection(ctx context.Context,
		target string) (SnapshotConnection, error)
}

type targetInfo struct {
	addr string
	key  string
}

// Transport is the transport layer for delivering raft messages and snapshots.
type Transport struct {
	mu struct {
		sync.Mutex
		queues   map[string]chan metapb.RaftMessage
		breakers map[string]*circuit.Breaker
	}
	logger         *zap.Logger
	storeID        uint64
	jobs           uint64
	ctx            context.Context
	cancel         context.CancelFunc
	handler        MessageHandler
	filter         atomic.Value
	unreachable    UnreachableHandler
	snapshotStatus SnapshotStatusHandler
	resolver       StoreResolver
	trans          TransImpl
	dir            snapshot.SnapshotDirFunc
	chunks         *Chunk
	stopper        *syncutil.Stopper
	addrs          sync.Map // storeID -> targetInfo
	addrsRevert    sync.Map // addr -> storeID
	fs             vfs.FS
}

func NewTransport(logger *zap.Logger, addr string,
	storeID uint64, handler MessageHandler,
	unreachable UnreachableHandler, snapshotStatus SnapshotStatusHandler,
	dir snapshot.SnapshotDirFunc,
	resolver StoreResolver, fs vfs.FS) *Transport {
	t := &Transport{
		logger:         log.Adjust(logger),
		storeID:        storeID,
		handler:        handler,
		unreachable:    unreachable,
		snapshotStatus: snapshotStatus,
		dir:            dir,
		resolver:       resolver,
		stopper:        syncutil.NewStopper(),
		fs:             fs,
	}
	t.chunks = NewChunk(t.logger, t.handler, t.dir, fs)
	t.trans = NewTCPTransport(logger, addr, handler, t.chunks.Add)
	t.mu.queues = make(map[string]chan metapb.RaftMessage)
	t.mu.breakers = make(map[string]*circuit.Breaker)
	t.ctx, t.cancel = context.WithCancel(context.Background())

	t.stopper.RunWorker(func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				t.chunks.Tick()
			case <-t.stopper.ShouldStop():
				return
			}
		}
	})

	return t
}

func (t *Transport) Start() error {
	return t.trans.Start()
}

// Close closes the Transport object.
func (t *Transport) Close() error {
	t.cancel()
	t.stopper.Stop()
	return t.trans.Close()
}

// Name returns the type name of the transport module
func (t *Transport) Name() string {
	return t.trans.Name()
}

func (t *Transport) SetFilter(f func(metapb.RaftMessage) bool) {
	if f == nil {
		panic("nil filter")
	}
	t.filter.Store(f)
}

func (t *Transport) SendingSnapshotCount() uint64 {
	return 0
}

func (t *Transport) Send(m metapb.RaftMessage) bool {
	if m.Message.Type == raftpb.MsgSnap {
		panic("sending snapshot message as regular message")
	}

	storeID := m.To.StoreID
	if filter := t.filter.Load(); filter != nil {
		ff, ok := filter.(func(metapb.RaftMessage) bool)
		if !ok {
			panic(fmt.Errorf("invalid transport filter %T", ff))
		}
		if ff(m) {
			return false
		}
	}

	targetInfo, resolved := t.resolve(storeID, m.ShardID)
	if !resolved {
		return false
	}

	// fail fast
	if !t.getCircuitBreaker(targetInfo.addr).Ready() {
		return false
	}

	t.mu.Lock()
	ch, ok := t.mu.queues[targetInfo.key]
	if !ok {
		ch = make(chan metapb.RaftMessage, sendQueueLen)
		t.mu.queues[targetInfo.key] = ch
	}
	t.mu.Unlock()

	if !ok {
		shutdownQueue := func() {
			t.mu.Lock()
			delete(t.mu.queues, targetInfo.key)
			t.mu.Unlock()
		}
		t.stopper.RunWorker(func() {
			affected := make(nodeMap)
			if !t.connectAndProcess(targetInfo.addr, ch, affected) {
				t.notifyUnreachable(targetInfo.addr, affected)
			}
			shutdownQueue()
		})
	}

	select {
	case ch <- m:
		return true
	default:
		// queue is full
		return false
	}
}

func (t *Transport) connectAndProcess(addr string,
	ch chan metapb.RaftMessage, affected nodeMap) bool {
	breaker := t.getCircuitBreaker(addr)
	successes := breaker.Successes()
	consecFailures := breaker.ConsecFailures()
	if err := func() error {
		t.logger.Debug("trying to connect to remote host",
			zap.String("addr", addr))
		conn, err := t.trans.GetConnection(t.ctx, addr)
		if err != nil {
			t.logger.Error("failed to connect",
				zap.String("addr", addr),
				zap.Error(err))
			return err
		}
		defer conn.Close()
		breaker.Success()
		if successes == 0 || consecFailures > 0 {
			t.logger.Debug("connection established",
				zap.String("addr", addr))
		}
		return t.processMessages(addr, ch, conn, affected)
	}(); err != nil {
		t.logger.Warn("circuit breaker failed",
			zap.String("addr", addr),
			zap.Error(err))
		breaker.Fail()
		return false
	}
	return true
}

func (t *Transport) notifyUnreachable(addr string, affected nodeMap) {
	t.logger.Warn("remote became unreachable",
		zap.String("addr", addr))
	for n := range affected {
		t.unreachable(n.ShardID, n.ReplicaID)
	}
}

func (t *Transport) processMessages(addr string,
	ch chan metapb.RaftMessage, conn Connection, affected nodeMap) error {
	idleTimer := time.NewTimer(idleTimeout)
	defer idleTimer.Stop()
	sz := uint64(0)
	batch := metapb.RaftMessageBatch{}
	requests := make([]metapb.RaftMessage, 0)
	for {
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(idleTimeout)
		select {
		case <-t.stopper.ShouldStop():
			return nil
		case <-idleTimer.C:
			return nil
		case req := <-ch:
			n := nodeInfo{
				ShardID:   req.ShardID,
				ReplicaID: req.From.ID,
			}
			affected[n] = struct{}{}
			// TODO: this is slow
			sz += uint64(req.Size())
			requests = append(requests, req)
			for done := false; !done && sz < maxMsgBatchSize; {
				select {
				case req = <-ch:
					sz += uint64(req.Size())
					requests = append(requests, req)
				case <-t.stopper.ShouldStop():
					return nil
				default:
					done = true
				}
			}
			twoBatch := false
			if sz < maxMsgBatchSize || len(requests) == 1 {
				batch.Messages = requests
			} else {
				twoBatch = true
				batch.Messages = requests[:len(requests)-1]
			}
			if err := t.sendMessageBatch(conn, batch); err != nil {
				t.logger.Error("send batch failed",
					zap.String("target", addr),
					zap.Error(err))
				return err
			}
			if twoBatch {
				batch.Messages = []metapb.RaftMessage{requests[len(requests)-1]}
				if err := t.sendMessageBatch(conn, batch); err != nil {
					t.logger.Error("send batch failed",
						zap.String("target", addr),
						zap.Error(err))
					return err
				}
			}
			sz = 0
			requests, batch = lazyFree(requests, batch)
			requests = requests[:0]
		}
	}
}

func lazyFree(reqs []metapb.RaftMessage,
	mb metapb.RaftMessageBatch) ([]metapb.RaftMessage, metapb.RaftMessageBatch) {
	for i := 0; i < len(reqs); i++ {
		reqs[i].Message.Entries = nil
	}
	mb.Messages = []metapb.RaftMessage{}
	return reqs, mb
}

func (t *Transport) sendMessageBatch(conn Connection,
	batch metapb.RaftMessageBatch) error {
	// TODO: add pre-send hook here
	return conn.SendMessageBatch(batch)
}

// getCircuitBreaker returns the circuit breaker used for the specified
// target node.
func (t *Transport) getCircuitBreaker(key string) *circuit.Breaker {
	t.mu.Lock()
	breaker, ok := t.mu.breakers[key]
	if !ok {
		breaker = netutil.NewBreaker()
		t.mu.breakers[key] = breaker
	}
	t.mu.Unlock()

	return breaker
}

func (t *Transport) resolve(storeID uint64, shardID uint64) (targetInfo, bool) {
	info, ok := t.addrs.Load(storeID)
	if ok {
		return info.(targetInfo), true
	}

	addr, err := t.resolver(storeID)
	if err != nil || addr == "" {
		t.logger.Error("failed to resolve store addr",
			zap.Error(err))
		return targetInfo{}, false
	}
	rec := targetInfo{
		addr: addr,
		key:  fmt.Sprintf("%s-%d", addr, shardID%concurrencyFactor),
	}
	t.addrs.Store(storeID, rec)
	t.addrsRevert.Store(addr, storeID)
	return rec, true
}
