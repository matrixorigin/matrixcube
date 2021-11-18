// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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
// Copyright 2020 MatrixOrigin.
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
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"net"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/fagongzi/util/protoc"
	"github.com/lni/goutils/netutil"
	"github.com/lni/goutils/syncutil"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/pb/meta"
)

const (
	SnapshotChunkSize = 1024 * 1024 * 4
)

var (
	// ErrBadMessage is the error returned to indicate the incoming message is
	// corrupted.
	ErrBadMessage              = errors.New("invalid message")
	errPoisonReceived          = errors.New("poison received")
	magicNumber                = [2]byte{0xAE, 0x7D}
	poisonNumber               = [2]byte{0x0, 0x0}
	payloadBufferSize          = SnapshotChunkSize + 1024*128
	tlsHandshackTimeout        = 10 * time.Second
	magicNumberDuration        = 1 * time.Second
	headerDuration             = 2 * time.Second
	readDuration               = 5 * time.Second
	writeDuration              = 5 * time.Second
	keepAlivePeriod            = 10 * time.Second
	perConnBufSize      uint64 = 1024 * 1024 * 4
	recvBufSize         uint64 = 1024 * 1024 * 4
)

const (
	// TCPTransportName is the name of the tcp transport module.
	TCPTransportName         = "go-tcp-transport"
	requestHeaderSize        = 18
	raftType          uint16 = 100
	snapshotType      uint16 = 200
)

type requestHeader struct {
	size   uint64
	crc    uint32
	method uint16
}

// TODO:
// TCP is never reliable [1]. dragonboat uses application layer crc32 checksum
// to help protecting raft state and log from some faulty network switches or
// buggy kernels. However, this is not necessary when TLS encryption is used.
// Update tcp.go to stop crc32 checking messages when TLS is used.
//
// [1] twitter's 2015 data corruption accident -
// https://www.evanjones.ca/checksum-failure-is-a-kernel-bug.html
// https://www.evanjones.ca/tcp-and-ethernet-checksums-fail.html
func (h *requestHeader) encode(buf []byte) []byte {
	if len(buf) < requestHeaderSize {
		panic("input buf too small")
	}
	binary.BigEndian.PutUint16(buf, h.method)
	binary.BigEndian.PutUint64(buf[2:], h.size)
	binary.BigEndian.PutUint32(buf[10:], 0)
	binary.BigEndian.PutUint32(buf[14:], h.crc)
	v := crc32.ChecksumIEEE(buf[:requestHeaderSize])
	binary.BigEndian.PutUint32(buf[10:], v)
	return buf[:requestHeaderSize]
}

func (h *requestHeader) decode(buf []byte) bool {
	if len(buf) < requestHeaderSize {
		return false
	}
	incoming := binary.BigEndian.Uint32(buf[10:])
	binary.BigEndian.PutUint32(buf[10:], 0)
	expected := crc32.ChecksumIEEE(buf[:requestHeaderSize])
	if incoming != expected {
		return false
	}
	binary.BigEndian.PutUint32(buf[10:], incoming)
	method := binary.BigEndian.Uint16(buf)
	if method != raftType && method != snapshotType {
		return false
	}
	h.method = method
	h.size = binary.BigEndian.Uint64(buf[2:])
	h.crc = binary.BigEndian.Uint32(buf[14:])
	return true
}

func sendPoison(conn net.Conn, poison []byte) error {
	tt := time.Now().Add(magicNumberDuration).Add(magicNumberDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return err
	}
	if _, err := conn.Write(poison); err != nil {
		return err
	}
	return nil
}

func sendPoisonAck(conn net.Conn, poisonAck []byte) error {
	return sendPoison(conn, poisonAck)
}

func waitPoisonAck(conn net.Conn) {
	ack := make([]byte, len(poisonNumber))
	tt := time.Now().Add(keepAlivePeriod)
	if err := conn.SetReadDeadline(tt); err != nil {
		return
	}
	if _, err := io.ReadFull(conn, ack); err != nil {
		return
	}
}

func writeMessage(conn net.Conn,
	header requestHeader, buf []byte, headerBuf []byte, encrypted bool) error {
	header.size = uint64(len(buf))
	if !encrypted {
		header.crc = crc32.ChecksumIEEE(buf)
	}
	headerBuf = header.encode(headerBuf)
	tt := time.Now().Add(magicNumberDuration).Add(headerDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return err
	}
	if _, err := conn.Write(magicNumber[:]); err != nil {
		return err
	}
	if _, err := conn.Write(headerBuf); err != nil {
		return err
	}
	sent := 0
	bufSize := int(recvBufSize)
	for sent < len(buf) {
		if sent+bufSize > len(buf) {
			bufSize = len(buf) - sent
		}
		tt = time.Now().Add(writeDuration)
		if err := conn.SetWriteDeadline(tt); err != nil {
			return err
		}
		if _, err := conn.Write(buf[sent : sent+bufSize]); err != nil {
			return err
		}
		sent += bufSize
	}
	if sent != len(buf) {
		panic("sent != len(buf)")
	}
	return nil
}

func readMessage(logger *zap.Logger, conn net.Conn,
	header []byte, rbuf []byte, encrypted bool) (requestHeader, []byte, error) {
	tt := time.Now().Add(headerDuration)
	if err := conn.SetReadDeadline(tt); err != nil {
		return requestHeader{}, nil, err
	}
	if _, err := io.ReadFull(conn, header); err != nil {
		logger.Error("failed to get the header")
		return requestHeader{}, nil, err
	}
	rheader := requestHeader{}
	if !rheader.decode(header) {
		logger.Error("invalid header")
		return requestHeader{}, nil, ErrBadMessage
	}
	if rheader.size == 0 {
		logger.Error("invalid payload length")
		return requestHeader{}, nil, ErrBadMessage
	}
	var buf []byte
	if rheader.size > uint64(len(rbuf)) {
		buf = make([]byte, rheader.size)
	} else {
		buf = rbuf[:rheader.size]
	}
	received := uint64(0)
	var recvBuf []byte
	if rheader.size < recvBufSize {
		recvBuf = buf[:rheader.size]
	} else {
		recvBuf = buf[:recvBufSize]
	}
	toRead := rheader.size
	for toRead > 0 {
		tt = time.Now().Add(readDuration)
		if err := conn.SetReadDeadline(tt); err != nil {
			return requestHeader{}, nil, err
		}
		if _, err := io.ReadFull(conn, recvBuf); err != nil {
			return requestHeader{}, nil, err
		}
		toRead -= uint64(len(recvBuf))
		received += uint64(len(recvBuf))
		if toRead < recvBufSize {
			recvBuf = buf[received : received+toRead]
		} else {
			recvBuf = buf[received : received+recvBufSize]
		}
	}
	if received != rheader.size {
		panic("unexpected size")
	}
	if !encrypted && crc32.ChecksumIEEE(buf) != rheader.crc {
		logger.Error("invalid payload checksum")
		return requestHeader{}, nil, ErrBadMessage
	}
	return rheader, buf, nil
}

func readMagicNumber(conn net.Conn, magicNum []byte) error {
	tt := time.Now().Add(magicNumberDuration)
	if err := conn.SetReadDeadline(tt); err != nil {
		return err
	}
	if _, err := io.ReadFull(conn, magicNum); err != nil {
		return err
	}
	if bytes.Equal(magicNum, poisonNumber[:]) {
		return errPoisonReceived
	}
	if !bytes.Equal(magicNum, magicNumber[:]) {
		return ErrBadMessage
	}
	return nil
}

type connection struct {
	conn net.Conn
}

func newConnection(conn net.Conn) net.Conn {
	return &connection{conn: conn}
}

func (c *connection) Close() error {
	return c.conn.Close()
}

func (c *connection) Read(b []byte) (int, error) {
	return c.conn.Read(b)
}

func (c *connection) Write(b []byte) (int, error) {
	return c.conn.Write(b)
}

func (c *connection) LocalAddr() net.Addr {
	panic("not implemented")
}

func (c *connection) RemoteAddr() net.Addr {
	panic("not implemented")
}

func (c *connection) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *connection) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *connection) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// TCPConnection is the connection used for sending raft messages to remote
// nodes.
type TCPConnection struct {
	logger    *zap.Logger
	conn      net.Conn
	header    []byte
	payload   []byte
	encrypted bool
}

var _ Connection = (*TCPConnection)(nil)

// NewTCPConnection creates and returns a new TCPConnection instance.
func NewTCPConnection(logger *zap.Logger,
	conn net.Conn, encrypted bool) *TCPConnection {
	return &TCPConnection{
		logger:    logger,
		conn:      newConnection(conn),
		header:    make([]byte, requestHeaderSize),
		payload:   make([]byte, perConnBufSize),
		encrypted: encrypted,
	}
}

// Close closes the TCPConnection instance.
func (c *TCPConnection) Close() {
	if err := c.conn.Close(); err != nil {
		c.logger.Error("failed to close the connection",
			zap.Error(err))
	}
}

// SendMessageBatch sends a raft message batch to remote node.
func (c *TCPConnection) SendMessageBatch(batch meta.RaftMessageBatch) error {
	header := requestHeader{method: raftType}
	buf := protoc.MustMarshal(&batch)
	return writeMessage(c.conn, header, buf, c.header, c.encrypted)
}

// TCPSnapshotConnection is the connection for sending raft snapshot chunks to
// remote nodes.
type TCPSnapshotConnection struct {
	logger    *zap.Logger
	conn      net.Conn
	header    []byte
	encrypted bool
}

var _ SnapshotConnection = (*TCPSnapshotConnection)(nil)

// NewTCPSnapshotConnection creates and returns a new snapshot connection.
func NewTCPSnapshotConnection(logger *zap.Logger,
	conn net.Conn, encrypted bool) *TCPSnapshotConnection {
	return &TCPSnapshotConnection{
		logger:    logger,
		conn:      newConnection(conn),
		header:    make([]byte, requestHeaderSize),
		encrypted: encrypted,
	}
}

// Close closes the snapshot connection.
func (c *TCPSnapshotConnection) Close() {
	defer func() {
		if err := c.conn.Close(); err != nil {
			c.logger.Error("failed to close the connection",
				zap.Error(err))
		}
	}()
	if err := sendPoison(c.conn, poisonNumber[:]); err != nil {
		return
	}
	waitPoisonAck(c.conn)
}

// SendChunk sends the specified snapshot chunk to remote node.
func (c *TCPSnapshotConnection) SendChunk(chunk meta.SnapshotChunk) error {
	header := requestHeader{method: snapshotType}
	buf := protoc.MustMarshal(&chunk)
	return writeMessage(c.conn, header, buf, c.header, c.encrypted)
}

// TCP is a TCP based transport module for exchanging raft messages and
// snapshots between NodeHost instances.
type TCP struct {
	logger         *zap.Logger
	addr           string
	stopper        *syncutil.Stopper
	connStopper    *syncutil.Stopper
	requestHandler MessageHandler
	chunkHandler   SnapshotChunkHandler
	//nhConfig       config.NodeHostConfig
	encrypted bool
}

var _ TransImpl = (*TCP)(nil)

// NewTCPTransport creates and returns a new TCP transport module.
func NewTCPTransport(logger *zap.Logger, addr string,
	requestHandler MessageHandler, chunkHandler SnapshotChunkHandler) TransImpl {
	return &TCP{
		addr:           addr,
		logger:         logger,
		stopper:        syncutil.NewStopper(),
		connStopper:    syncutil.NewStopper(),
		requestHandler: requestHandler,
		chunkHandler:   chunkHandler,
	}
}

// Start starts the TCP transport module.
func (t *TCP) Start() error {
	listener, err := netutil.NewStoppableListener(t.addr,
		nil, t.stopper.ShouldStop())
	if err != nil {
		return err
	}
	t.connStopper.RunWorker(func() {
		// sync.WaitGroup's doc mentions that
		// "Note that calls with a positive delta that occur when the counter is
		//  zero must happen before a Wait."
		// It is unclear that whether the stdlib is going complain in future
		// releases when Wait() is called when the counter is zero and Add() with
		// positive delta has never been called.
		<-t.connStopper.ShouldStop()
	})
	t.stopper.RunWorker(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if err == netutil.ErrListenerStopped {
					return
				}
				panic(err)
			}
			var once sync.Once
			closeFn := func() {
				once.Do(func() {
					if err := conn.Close(); err != nil {
						t.logger.Error("failed to close the connection",
							zap.Error(err))
					}
				})
			}
			t.connStopper.RunWorker(func() {
				<-t.stopper.ShouldStop()
				closeFn()
			})
			t.connStopper.RunWorker(func() {
				t.serveConn(conn)
				closeFn()
			})
		}
	})
	return nil
}

// Close closes the TCP transport module.
func (t *TCP) Close() error {
	t.stopper.Stop()
	t.connStopper.Stop()
	return nil
}

// GetConnection returns a new raftio.IConnection for sending raft messages.
func (t *TCP) GetConnection(ctx context.Context, target string) (Connection, error) {
	conn, err := t.getConnection(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewTCPConnection(t.logger, conn, t.encrypted), nil
}

// GetSnapshotConnection returns a new raftio.IConnection for sending raft
// snapshots.
func (t *TCP) GetSnapshotConnection(ctx context.Context,
	target string) (SnapshotConnection, error) {
	conn, err := t.getConnection(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewTCPSnapshotConnection(t.logger, conn, t.encrypted), nil
}

// Name returns a human readable name of the TCP transport module.
func (t *TCP) Name() string {
	return TCPTransportName
}

func (t *TCP) serveConn(conn net.Conn) {
	magicNum := make([]byte, len(magicNumber))
	header := make([]byte, requestHeaderSize)
	tbuf := make([]byte, payloadBufferSize)
	for {
		err := readMagicNumber(conn, magicNum)
		if err != nil {
			if errors.Is(err, errPoisonReceived) {
				if err := sendPoisonAck(conn, poisonNumber[:]); err != nil {
					t.logger.Error("failed to send poison ack",
						zap.Error(err))
				}
				return
			}
			if errors.Is(err, ErrBadMessage) {
				return
			}
			operr, ok := err.(net.Error)
			if ok && operr.Timeout() {
				continue
			} else {
				return
			}
		}
		rheader, buf, err := readMessage(t.logger, conn, header, tbuf, t.encrypted)
		if err != nil {
			return
		}
		if rheader.method == raftType {
			batch := meta.RaftMessageBatch{}
			if err := batch.Unmarshal(buf); err != nil {
				return
			}
			t.requestHandler(batch)
		} else {
			chunk := meta.SnapshotChunk{}
			if err := chunk.Unmarshal(buf); err != nil {
				return
			}
			if !t.chunkHandler(chunk) {
				t.logger.Error("snapshot chunk rejected",
					zap.String("key", chunkKey(chunk)))
				return
			}
		}
	}
}

func setTCPConn(conn *net.TCPConn) error {
	if err := conn.SetLinger(0); err != nil {
		return err
	}
	if err := conn.SetKeepAlive(true); err != nil {
		return err
	}
	return conn.SetKeepAlivePeriod(keepAlivePeriod)
}

// FIXME:
// context.Context is ignored
func (t *TCP) getConnection(ctx context.Context,
	target string) (net.Conn, error) {
	timeout := time.Duration(dialTimeoutSecond) * time.Second
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		return nil, err
	}
	tcpconn, ok := conn.(*net.TCPConn)
	if ok {
		if err := setTCPConn(tcpconn); err != nil {
			return nil, err
		}
	}
	return conn, nil
}
