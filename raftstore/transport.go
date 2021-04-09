package raftstore

import (
	"fmt"
	"sync"
	"time"

	"github.com/deepfabric/beehive/metric"
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/bhraftpb"
	sn "github.com/deepfabric/beehive/snapshot"
	"github.com/deepfabric/beehive/transport"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/codec"
	"github.com/fagongzi/goetty/codec/length"
	"github.com/fagongzi/goetty/pool"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
	"go.etcd.io/etcd/raft/raftpb"
)

type defaultTransport struct {
	store                  *store
	decoder                codec.Decoder
	encoder                codec.Encoder
	server                 goetty.NetApplication
	readTimeout, writeTime time.Duration
	conns                  sync.Map // store id -> pool.IOSessionPool
	resolverFunc           func(id uint64) (string, error)
	addrs                  sync.Map // store id -> addr
	addrsRevert            sync.Map // addr -> store id
	raftMsgs               [][]*task.Queue
	raftMask               []uint64
	snapMsgs               [][]*task.Queue
	snapMask               []uint64
}

func newTransport(store *store) transport.Transport {
	t := &defaultTransport{
		store:       store,
		readTimeout: 2 * time.Duration(store.cfg.Raft.HeartbeatTicks) * store.cfg.Raft.TickInterval.Duration,
		writeTime:   time.Minute,
	}

	baseEncoder := newRaftEncoder()
	baseDecoder := newRaftDecoder()
	t.encoder, t.decoder = length.NewWithSize(baseEncoder, baseDecoder, 0, 0, 0, int(store.cfg.Raft.MaxProposalBytes)*2)
	app, err := goetty.NewTCPApplication(store.cfg.RaftAddr, t.onMessage,
		goetty.WithAppSessionOptions(goetty.WithCodec(t.encoder, t.decoder),
			goetty.WithTimeout(t.readTimeout, t.writeTime),
			goetty.WithEnableAsyncWrite(int64(store.cfg.Raft.SendRaftBatchSize))))
	if err != nil {
		logger.Fatalf("create transport failed with %+v", err)
	}

	t.server = app
	t.resolverFunc = t.resolverStoreAddr

	for i := uint64(0); i < store.cfg.Groups; i++ {
		t.raftMsgs = append(t.raftMsgs, make([]*task.Queue, store.cfg.Worker.SendRaftMsgWorkerCount))
		t.raftMask = append(t.raftMask, store.cfg.Worker.SendRaftMsgWorkerCount-1)
		for j := uint64(0); j < t.store.cfg.Worker.SendRaftMsgWorkerCount; j++ {
			t.raftMsgs[i][j] = &task.Queue{}
		}

		t.snapMsgs = append(t.snapMsgs, make([]*task.Queue, store.cfg.Snapshot.MaxConcurrencySnapChunks))
		t.snapMask = append(t.snapMask, store.cfg.Snapshot.MaxConcurrencySnapChunks-1)
		for j := uint64(0); j < t.store.cfg.Snapshot.MaxConcurrencySnapChunks; j++ {
			t.snapMsgs[i][j] = &task.Queue{}
		}
	}

	return t
}

func (t *defaultTransport) Start() {
	for _, qs := range t.raftMsgs {
		for _, q := range qs {
			go t.readyToSendRaft(q)
		}
	}

	for _, qs := range t.snapMsgs {
		for _, q := range qs {
			go t.readyToSendSnapshots(q)
		}
	}

	err := t.server.Start()
	if err != nil {
		logger.Fatalf("transport start at %s failed with %+v",
			t.store.cfg.RaftAddr,
			err)
	}
}

func (t *defaultTransport) Stop() {
	for _, qs := range t.snapMsgs {
		for _, q := range qs {
			q.Dispose()
		}
	}

	for _, qs := range t.raftMsgs {
		for _, q := range qs {
			q.Dispose()
		}
	}

	t.server.Stop()
	logger.Infof("transfer stopped")
}

func (t *defaultTransport) SendingSnapshotCount() uint64 {
	c := int64(0)
	for _, qs := range t.snapMsgs {
		for _, q := range qs {
			c += q.Len()
		}
	}

	return uint64(c)
}

func (t *defaultTransport) Send(msg *bhraftpb.RaftMessage) {
	storeID := msg.To.ContainerID
	if storeID == t.store.meta.meta.ID {
		t.store.handle(msg)
		return
	}

	if msg.Message.Type == raftpb.MsgSnap {
		snapMsg := &bhraftpb.SnapshotMessage{}
		protoc.MustUnmarshal(snapMsg, msg.Message.Snapshot.Data)
		snapMsg.Header.From = msg.From
		snapMsg.Header.To = msg.To

		q := t.snapMsgs[msg.Group][t.snapMask[msg.Group]&storeID]
		q.Put(snapMsg)
		metric.SetRaftSnapQueueMetric(q.Len())
	}

	q := t.raftMsgs[msg.Group][t.raftMask[msg.Group]&storeID]
	q.Put(msg)
	metric.SetRaftMsgQueueMetric(q.Len())
}

func (t *defaultTransport) onMessage(rs goetty.IOSession, msg interface{}, seq uint64) error {
	t.store.handle(msg)
	return nil
}

func (t *defaultTransport) readyToSendRaft(q *task.Queue) {
	items := make([]interface{}, t.store.cfg.Raft.SendRaftBatchSize)
	buffers := make(map[uint64][]*bhraftpb.RaftMessage)

	for {
		n, err := q.Get(int64(t.store.cfg.Raft.SendRaftBatchSize), items)
		if err != nil {
			logger.Infof("send raft worker stopped")
			return
		}

		for i := int64(0); i < n; i++ {
			msg := items[i].(*bhraftpb.RaftMessage)
			var values []*bhraftpb.RaftMessage
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
	items := make([]interface{}, t.store.cfg.Raft.SendRaftBatchSize)

	for {
		n, err := q.Get(int64(t.store.cfg.Raft.SendRaftBatchSize), items)
		if err != nil {
			logger.Infof("send snapshot worker stopped")
			return
		}

		for i := int64(0); i < n; i++ {
			msg := items[i].(*bhraftpb.SnapshotMessage)
			id := msg.Header.To.ContainerID

			conn, err := t.getConn(id)
			if err != nil {
				logger.Errorf("create conn to %d failed with %+v, retry later",
					id,
					err)
				q.Put(msg)
				continue
			}

			err = t.doSendSnapshotMessage(msg, conn)
			t.putConn(id, conn)

			if err != nil {
				logger.Errorf("send snap %s failed with %+v, retry later",
					msg.String(),
					err)
				q.Put(msg)
			}
		}

		metric.SetRaftSnapQueueMetric(q.Len())
	}
}

func (t *defaultTransport) doSendSnapshotMessage(msg *bhraftpb.SnapshotMessage, conn goetty.IOSession) error {
	if t.store.snapshotManager.Register(msg, sn.Sending) {
		defer t.store.snapshotManager.Deregister(msg, sn.Sending)

		logger.Infof("shard %d start send pending snap, epoch=<%s> term=<%d> index=<%d>",
			msg.Header.Shard.ID,
			msg.Header.Shard.Epoch.String(),
			msg.Header.Term,
			msg.Header.Index)

		start := time.Now()
		if !t.store.snapshotManager.Exists(msg) {
			return fmt.Errorf("transport: missing snapshot file, header=<%+v>",
				msg.Header)
		}

		size, err := t.store.snapshotManager.WriteTo(msg, conn)
		if err != nil {
			conn.Close()
			return err
		}

		logger.Infof("shard %d pending snap sent succ, size=<%d>, epoch=<%s> term=<%d> index=<%d>",
			msg.Header.Shard.ID,
			size,
			msg.Header.Shard.Epoch.String(),
			msg.Header.Term,
			msg.Header.Index)

		metric.ObserveSnapshotSendingDuration(start)
	}

	return nil
}

func (t *defaultTransport) postSend(msg *bhraftpb.RaftMessage, err error) {
	if err != nil {
		logger.Errorf("shard %d send msg from %d to %d failed with %+v",
			msg.ShardID,
			msg.From.ID,
			msg.To.ID,
			err)

		if pr := t.store.getPR(msg.ShardID, true); pr != nil {
			pr.addReport(msg.Message)
		}
	}

	pb.ReleaseRaftMessage(msg)
}

func (t *defaultTransport) doSend(msgs []*bhraftpb.RaftMessage, to uint64) error {
	conn, err := t.getConn(to)
	if err != nil {
		return err
	}

	err = t.doBatchWrite(msgs, conn)
	t.putConn(to, conn)
	return err
}

func (t *defaultTransport) doBatchWrite(msgs []*bhraftpb.RaftMessage, conn goetty.IOSession) error {
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

	p, err := pool.NewIOSessionPool(nil, 1, int(2*t.store.cfg.Groups), func(remote interface{}) (goetty.IOSession, error) {
		return t.createConn()
	})
	if err != nil {
		return nil, err
	}

	if old, loaded := t.conns.LoadOrStore(id, p); loaded {
		return old.(pool.IOSessionPool).Get()
	}
	return p.(pool.IOSessionPool).Get()
}

func (t *defaultTransport) checkConnect(id uint64, conn goetty.IOSession) bool {
	if nil == conn {
		return false
	}

	if conn.Connected() {
		return true
	}

	addr, err := t.resolverFunc(id)
	if err != nil {
		return false
	}

	ok, err := conn.Connect(addr, time.Second*10)
	if err != nil {
		logger.Errorf("connect to store %d failed with %+v",
			id,
			err)
		return false
	}

	logger.Infof("connected to store %d", id)
	return ok
}

func (t *defaultTransport) createConn() (goetty.IOSession, error) {
	return goetty.NewIOSession(goetty.WithCodec(t.encoder, t.decoder),
		goetty.WithTimeout(t.readTimeout, t.writeTime)), nil
}

func (t *defaultTransport) resolverStoreAddr(storeID uint64) (string, error) {
	addr, ok := t.addrs.Load(storeID)

	if !ok {
		addr, ok = t.addrs.Load(storeID)
		if ok {
			return addr.(string), nil
		}

		container, err := t.store.pd.GetStorage().GetContainer(storeID)
		if err != nil {
			return "", err
		}

		if container == nil {
			return "", fmt.Errorf("store %d not registered", storeID)
		}

		addr = container.(*containerAdapter).meta.RaftAddr
		t.addrs.Store(storeID, addr)
		t.addrsRevert.Store(addr, storeID)
	}

	return addr.(string), nil
}
