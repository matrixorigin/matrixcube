package transport

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/codec"
	"github.com/fagongzi/goetty/codec/length"
	"github.com/fagongzi/goetty/pool"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/util/task"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

var (
	errConnect = errors.New("not connected")
)

// Transport raft transport
type Transport interface {
	// Start start the transport, receiving and sending messages
	Start()
	// Stop stop the transport
	Stop()
	// Send send the raft message to other node
	Send(meta.RaftMessage)
	// SendingSnapshotCount returns the count of sending snapshots
	SendingSnapshotCount() uint64
}

// ContainerResolver container resolver func
type ContainerResolver func(id uint64) (metadata.Container, error)

// MessageHandler message handler
type MessageHandler func(msg interface{})

type defaultTransport struct {
	opts        *options
	logger      *zap.Logger
	storeID     uint64
	snapMgr     snapshot.SnapshotManager
	decoder     codec.Decoder
	encoder     codec.Encoder
	server      goetty.NetApplication
	conns       sync.Map // store id -> pool.IOSessionPool
	resolver    ContainerResolver
	handler     MessageHandler
	addrs       sync.Map // store id -> addr
	addrsRevert sync.Map // addr -> store id
	raftMsgs    []*task.Queue
	raftMask    uint64
	snapMsgs    []*task.Queue
	snapMask    uint64
}

// NewDefaultTransport create  default transport
func NewDefaultTransport(
	storeID uint64,
	addr string,
	snapMgr snapshot.SnapshotManager,
	handler MessageHandler,
	resolver ContainerResolver,
	opts ...Option) Transport {
	t := &defaultTransport{
		opts:     &options{},
		storeID:  storeID,
		snapMgr:  snapMgr,
		resolver: resolver,
		handler:  handler,
	}

	for _, opt := range opts {
		opt(t.opts)
	}
	t.opts.adjust()
	t.logger = t.opts.logger.Named("transport").With(log.ListenAddressField(addr))

	baseEncoder := newRaftEncoder()
	baseDecoder := newRaftDecoder()
	t.encoder, t.decoder = length.NewWithSize(baseEncoder, baseDecoder, 0, 0, 0, t.opts.maxBodySize)
	app, err := goetty.NewTCPApplication(addr, t.onMessage,
		goetty.WithAppSessionOptions(goetty.WithCodec(t.encoder, t.decoder),
			goetty.WithTimeout(t.opts.readTimeout, t.opts.writeTimeout),
			goetty.WithLogger(zap.L().Named("cube-trans")),
			goetty.WithEnableAsyncWrite(t.opts.sendBatch)))
	if err != nil {
		t.logger.Fatal("fail to create transport",
			zap.Error(err))
	}

	t.server = app

	for i := uint64(0); i < t.opts.raftWorkerCount; i++ {
		t.raftMsgs = append(t.raftMsgs, task.New(32))
	}
	t.raftMask = t.opts.raftWorkerCount - 1

	for i := uint64(0); i < t.opts.snapWorkerCount; i++ {
		t.snapMsgs = append(t.snapMsgs, task.New(32))
	}
	t.snapMask = t.opts.snapWorkerCount - 1

	return t
}

func (t *defaultTransport) Start() {
	for _, q := range t.raftMsgs {
		go t.readyToSendRaft(q)
	}
	for _, q := range t.snapMsgs {
		go t.readyToSendSnapshots(q)
	}

	err := t.server.Start()
	if err != nil {
		t.logger.Fatal("fail to start transport",
			zap.Error(err))
	}
}

func (t *defaultTransport) Stop() {
	for _, q := range t.snapMsgs {
		q.Dispose()
	}
	for _, q := range t.raftMsgs {
		q.Dispose()
	}

	t.server.Stop()
	t.logger.Info("transfer stopped")
}

func (t *defaultTransport) SendingSnapshotCount() uint64 {
	c := int64(0)
	for _, q := range t.snapMsgs {
		c += q.Len()
	}

	return uint64(c)
}

func (t *defaultTransport) Send(msg meta.RaftMessage) {
	storeID := msg.To.ContainerID
	if storeID == t.storeID {
		t.handler(msg)
		return
	}

	if msg.Message.Type == raftpb.MsgSnap {
		snapMsg := &meta.SnapshotMessage{}
		protoc.MustUnmarshal(snapMsg, msg.Message.Snapshot.Data)
		snapMsg.Header.From = msg.From
		snapMsg.Header.To = msg.To

		q := t.snapMsgs[t.snapMask&storeID]
		q.Put(snapMsg)
		metric.SetRaftSnapQueueMetric(q.Len())
	}

	q := t.raftMsgs[t.raftMask&storeID]
	q.Put(msg)
	metric.SetRaftMsgQueueMetric(q.Len())
}

func (t *defaultTransport) onMessage(rs goetty.IOSession, msg interface{}, seq uint64) error {
	t.handler(msg)
	return nil
}

func (t *defaultTransport) readyToSendRaft(q *task.Queue) {
	items := make([]interface{}, t.opts.sendBatch)
	buffers := make(map[uint64][]meta.RaftMessage)

	for {
		n, err := q.Get(t.opts.sendBatch, items)
		if err != nil {
			t.logger.Info("send raft worker stopped")
			return
		}

		for i := int64(0); i < n; i++ {
			msg := items[i].(meta.RaftMessage)
			var values []meta.RaftMessage
			if v, ok := buffers[msg.To.ContainerID]; ok {
				values = v
			}

			values = append(values, msg)
			buffers[msg.To.ContainerID] = values
		}

		for k, msgs := range buffers {
			if len(msgs) > 0 {
				err := t.doSend(msgs, k)
				for _, msg := range msgs {
					t.postSend(msg, err)
				}
			}
		}

		for k, msgs := range buffers {
			buffers[k] = msgs[:0]
		}

		metric.SetRaftMsgQueueMetric(q.Len())
	}
}

func (t *defaultTransport) readyToSendSnapshots(q *task.Queue) {
	items := make([]interface{}, t.opts.sendBatch)

	for {
		n, err := q.Get(t.opts.sendBatch, items)
		if err != nil {
			t.logger.Info("send snapshot worker stopped")
			return
		}

		for i := int64(0); i < n; i++ {
			msg := items[i].(*meta.SnapshotMessage)
			id := msg.Header.To.ContainerID

			conn, err := t.getConn(id)
			if err != nil {
				t.logger.Error("fail to create connection to store, retry later",
					log.StoreIDField(id),
					zap.Error(err))
				q.Put(msg)
				continue
			}

			err = t.doSendSnapshotMessage(msg, conn)
			t.putConn(id, conn)

			if err != nil {
				t.logger.Error("fail to send snapshot message, retry later",
					log.ShardIDField(msg.Header.Shard.ID),
					zap.Error(err))
				q.Put(msg)
			}
		}

		metric.SetRaftSnapQueueMetric(q.Len())
	}
}

func (t *defaultTransport) doSendSnapshotMessage(msg *meta.SnapshotMessage, conn goetty.IOSession) error {
	if t.snapMgr.Register(msg, snapshot.Sending) {
		defer t.snapMgr.Deregister(msg, snapshot.Sending)

		t.logger.Info("start sending pending snapshot",
			log.ShardIDField(msg.Header.Shard.ID),
			log.EpochField("epoch", msg.Header.Shard.Epoch),
			zap.Uint64("term", msg.Header.Term),
			zap.Uint64("index", msg.Header.Index))

		start := time.Now()
		if !t.snapMgr.Exists(msg) {
			return fmt.Errorf("transport: missing snapshot file, header=<%+v>",
				msg.Header)
		}

		size, err := t.snapMgr.WriteTo(msg, conn)
		if err != nil {
			conn.Close()
			return err
		}

		t.logger.Info("pending snapshot sent succeed",
			log.ShardIDField(msg.Header.Shard.ID),
			log.EpochField("epoch", msg.Header.Shard.Epoch),
			zap.Uint64("term", msg.Header.Term),
			zap.Uint64("index", msg.Header.Index),
			zap.Uint64("size", size))

		metric.ObserveSnapshotSendingDuration(start)
	}

	return nil
}

func (t *defaultTransport) postSend(msg meta.RaftMessage, err error) {
	if err != nil {
		t.logger.Error("fail to send raft message ",
			log.ShardIDField(msg.ShardID),
			log.ReplicaField("from", msg.From),
			log.ReplicaField("to", msg.To),
			log.RaftMessageField("raft-msg", msg),
			zap.Error(err))
		if t.opts.errorHandlerFunc != nil {
			t.opts.errorHandlerFunc(msg, err)
		}
	}
}

func (t *defaultTransport) doSend(msgs []meta.RaftMessage, to uint64) error {
	conn, err := t.getConn(to)
	if err != nil {
		return err
	}

	err = t.doBatchWrite(msgs, conn)
	t.putConn(to, conn)
	return err
}

func (t *defaultTransport) doBatchWrite(msgs []meta.RaftMessage, conn goetty.IOSession) error {
	for _, m := range msgs {
		err := conn.Write(m)
		if err != nil {
			conn.Close()
			return err
		}
	}

	err := conn.Flush()
	if err != nil {
		conn.Close()
		return err
	}

	return nil
}

func (t *defaultTransport) putConn(id uint64, conn goetty.IOSession) {
	if p, ok := t.conns.Load(id); ok {
		p.(pool.IOSessionPool).Put(conn)
	} else {
		conn.Close()
	}
}

func (t *defaultTransport) getConn(id uint64) (goetty.IOSession, error) {
	conn, err := t.getConnLocked(id)
	if err != nil {
		return nil, err
	}

	if t.checkConnect(id, conn) {
		return conn, nil
	}

	t.putConn(id, conn)
	return nil, errConnect
}

func (t *defaultTransport) getConnLocked(id uint64) (goetty.IOSession, error) {
	if p, ok := t.conns.Load(id); ok {
		return p.(pool.IOSessionPool).Get()
	}

	p, err := pool.NewIOSessionPool(nil, 1, 2, func(remote interface{}) (goetty.IOSession, error) {
		return t.createConn()
	})
	if err != nil {
		return nil, err
	}

	if old, loaded := t.conns.LoadOrStore(id, p); loaded {
		return old.(pool.IOSessionPool).Get()
	}
	return p.Get()
}

func (t *defaultTransport) checkConnect(id uint64, conn goetty.IOSession) bool {
	if nil == conn {
		return false
	}

	if conn.Connected() {
		return true
	}

	addr, err := t.resolverStoreAddr(id)
	if err != nil {
		return false
	}

	ok, err := conn.Connect(addr, time.Second*10)
	if err != nil {
		t.logger.Error("fail to connect to store",
			log.StoreIDField(id),
			zap.Error(err))
		return false
	}

	t.logger.Info("connected to store",
		log.StoreIDField(id))
	return ok
}

func (t *defaultTransport) createConn() (goetty.IOSession, error) {
	return goetty.NewIOSession(goetty.WithCodec(t.encoder, t.decoder),
		goetty.WithTimeout(t.opts.readTimeout, t.opts.writeTimeout)), nil
}

func (t *defaultTransport) resolverStoreAddr(storeID uint64) (string, error) {
	addr, ok := t.addrs.Load(storeID)

	if !ok {
		addr, ok = t.addrs.Load(storeID)
		if ok {
			return addr.(string), nil
		}

		container, err := t.resolver(storeID)
		if err != nil {
			return "", err
		}

		if container == nil {
			return "", fmt.Errorf("store %d not registered", storeID)
		}

		addr = container.ShardAddr()
		t.addrs.Store(storeID, addr)
		t.addrsRevert.Store(addr, storeID)
	}

	return addr.(string), nil
}
