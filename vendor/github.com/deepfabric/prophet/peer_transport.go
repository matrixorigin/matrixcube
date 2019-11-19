package prophet

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/task"
)

var (
	errConnect = errors.New("not connected")
)

const (
	hb     byte = 0
	hbACK  byte = 1
	remove byte = 2
)

type hbMsg struct {
	res Resource
}

type hbACKMsg struct {
	res  Resource
	peer Peer
}

type removeMsg struct {
	id uint64
}

type shardCodec struct {
	factory func() Resource
}

func (c *shardCodec) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	t := readByte(in)
	switch t {
	case hb:
		msg := &hbMsg{
			res: c.factory(),
		}

		data := readBytes(readInt(in), in)
		err := msg.res.Unmarshal(data)
		if err != nil {
			return false, nil, err
		}

		return true, msg, nil
	case hbACK:
		msg := &hbACKMsg{
			res: c.factory(),
		}

		msg.peer.ID = readUInt64(in)
		msg.peer.ContainerID = readUInt64(in)

		data := readBytes(readInt(in), in)
		err := msg.res.Unmarshal(data)
		if err != nil {
			return false, nil, err
		}

		return true, msg, nil
	case remove:
		msg := &removeMsg{}
		msg.id = readUInt64(in)

		return true, msg, nil
	}

	return false, nil, fmt.Errorf("%d not support", t)
}

func (c *shardCodec) Encode(data interface{}, out *goetty.ByteBuf) error {
	if msg, ok := data.(*hbMsg); ok {
		data, err := msg.res.Marshal()
		if err != nil {
			return err
		}

		out.WriteByte(hb)
		out.WriteInt(len(data))
		out.Write(data)
	} else if msg, ok := data.(*hbACKMsg); ok {
		data, err := msg.res.Marshal()
		if err != nil {
			return err
		}

		out.WriteByte(hbACK)
		out.WriteUInt64(msg.peer.ID)
		out.WriteUInt64(msg.peer.ContainerID)
		out.WriteInt(len(data))
		out.Write(data)
	} else if msg, ok := data.(*removeMsg); ok {
		out.WriteByte(remove)
		out.WriteUInt64(msg.id)
	} else {
		log.Fatalf("not support msg %T %+v",
			data,
			data)
	}

	return nil
}

func readUInt64(buf *goetty.ByteBuf) uint64 {
	value, _ := buf.ReadUInt64()
	return value
}

func readInt(buf *goetty.ByteBuf) int {
	value, _ := buf.ReadInt()
	return value
}

func readByte(buf *goetty.ByteBuf) byte {
	value, _ := buf.ReadByte()
	return value
}

func readBytes(n int, buf *goetty.ByteBuf) []byte {
	_, value, _ := buf.ReadBytes(n)
	return value
}

type sendMsg struct {
	to   uint64
	data interface{}
}

// ReplicaTransport transport from peers
type ReplicaTransport interface {
	Start()
	Stop()

	Send(uint64, interface{})
}

// NewReplicaTransport returns a replica transport to send and received message
func NewReplicaTransport(addr string, store ResourceStore, factory func() Resource) ReplicaTransport {
	biz := &shardCodec{factory: factory}
	encoder := goetty.NewIntLengthFieldBasedEncoder(biz)
	decoder := goetty.NewIntLengthFieldBasedDecoder(biz)

	t := &defaultReplicaTransport{
		addr:    addr,
		decoder: decoder,
		encoder: encoder,
		server: goetty.NewServer(addr,
			goetty.WithServerDecoder(decoder),
			goetty.WithServerEncoder(encoder)),
		conns:       make(map[uint64]goetty.IOSessionPool),
		addrs:       &sync.Map{},
		addrsRevert: &sync.Map{},
		store:       store,
		msgs:        task.New(1024),
	}

	return t
}

type defaultReplicaTransport struct {
	sync.RWMutex

	addr        string
	decoder     goetty.Decoder
	encoder     goetty.Encoder
	store       ResourceStore
	server      *goetty.Server
	addrs       *sync.Map
	addrsRevert *sync.Map
	conns       map[uint64]goetty.IOSessionPool
	msgs        *task.Queue
}

func (t *defaultReplicaTransport) Start() {
	go t.readyToSend()
	go func() {
		err := t.server.Start(t.doConnection)
		if err != nil {
			log.Fatalf("replica transport start failed with %+v",
				err)
		}
	}()
	<-t.server.Started()
}

func (t *defaultReplicaTransport) Stop() {
	t.msgs.Dispose()
	t.server.Stop()
	log.Infof("replica transport stopped")
}

func (t *defaultReplicaTransport) doConnection(session goetty.IOSession) error {
	remoteIP := session.RemoteIP()

	log.Infof("%s connected", remoteIP)
	for {
		msg, err := session.Read()
		if err != nil {
			if err == io.EOF {
				log.Infof("closed by %s", remoteIP)
			} else {
				log.Warningf("read error from conn-%s, errors:\n%+v",
					remoteIP,
					err)
			}

			return err
		}

		ack := t.store.HandleReplicaMsg(msg)
		if ack != nil {
			session.WriteAndFlush(ack)
		}
	}
}

func (t *defaultReplicaTransport) Send(to uint64, msg interface{}) {
	if to == t.store.Meta().ID() {
		t.store.HandleReplicaMsg(msg)
		return
	}

	t.msgs.Put(&sendMsg{
		to:   to,
		data: msg,
	})
}

func (t *defaultReplicaTransport) readyToSend() {
	items := make([]interface{}, batch, batch)

	for {
		n, err := t.msgs.Get(batch, items)
		if err != nil {
			log.Infof("transfer sent worker stopped")
			return
		}

		for i := int64(0); i < n; i++ {
			msg := items[i].(*sendMsg)
			t.doSend(msg)
		}
	}
}

func (t *defaultReplicaTransport) doSend(msg *sendMsg) error {
	conn, err := t.getConn(msg.to)
	if err != nil {
		return err
	}

	err = t.doWrite(msg.data, conn)
	t.putConn(msg.to, conn)
	return err
}

func (t *defaultReplicaTransport) doWrite(msg interface{}, conn goetty.IOSession) error {
	err := conn.WriteAndFlush(msg)
	if err != nil {
		conn.Close()
		return err
	}

	return nil
}

func (t *defaultReplicaTransport) getConn(containerID uint64) (goetty.IOSession, error) {
	conn, err := t.getConnLocked(containerID)
	if err != nil {
		return nil, err
	}

	if t.checkConnect(containerID, conn) {
		return conn, nil
	}

	t.putConn(containerID, conn)
	return nil, errConnect
}

func (t *defaultReplicaTransport) putConn(id uint64, conn goetty.IOSession) {
	t.RLock()
	pool := t.conns[id]
	t.RUnlock()

	if pool != nil {
		pool.Put(conn)
	} else {
		conn.Close()
	}
}

func (t *defaultReplicaTransport) getConnLocked(id uint64) (goetty.IOSession, error) {
	var err error

	t.RLock()
	pool := t.conns[id]
	t.RUnlock()

	if pool == nil {
		t.Lock()
		pool = t.conns[id]
		if pool == nil {
			pool, err = goetty.NewIOSessionPool(1, 1, func() (goetty.IOSession, error) {
				return t.createConn(id)
			})

			if err != nil {
				return nil, err
			}

			t.conns[id] = pool
		}
		t.Unlock()
	}

	return pool.Get()
}

func (t *defaultReplicaTransport) checkConnect(id uint64, conn goetty.IOSession) bool {
	if nil == conn {
		return false
	}

	if conn.IsConnected() {
		return true
	}

	ok, err := conn.Connect()
	if err != nil {
		log.Errorf("connect to container %d failure, errors:\n %+v",
			id,
			err)
		return false
	}

	// read loop
	go func() {
		for {
			msg, err := conn.Read()
			if err != nil {
				return
			}

			ack := t.store.HandleReplicaMsg(msg)
			if ack != nil {
				log.Fatalf("%+v %T unexpect ack %T", msg, data, ack)
			}
		}
	}()

	log.Infof("connected to container %d", id)
	return ok
}

func (t *defaultReplicaTransport) createConn(id uint64) (goetty.IOSession, error) {
	addr, err := t.getContainerAddr(id)
	if err != nil {
		return nil, err
	}

	return goetty.NewConnector(addr,
		goetty.WithClientDecoder(t.decoder),
		goetty.WithClientEncoder(t.encoder)), nil
}

func (t *defaultReplicaTransport) getContainerAddr(containerID uint64) (string, error) {
	var err error
	addr, ok := t.addrs.Load(containerID)
	if !ok {
		addr, err = t.store.GetContainerAddr(containerID)
		if err != nil {
			return "", err
		}

		t.addrs.Store(containerID, addr)
		t.addrsRevert.Store(addr, containerID)
	}

	return addr.(string), nil
}
