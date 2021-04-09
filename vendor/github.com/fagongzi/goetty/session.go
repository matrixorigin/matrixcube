package goetty

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/goetty/queue"
)

var (
	// ErrIllegalState illegal state error
	ErrIllegalState = errors.New("illegal state")
	// ErrDisableConnect disable to connect
	ErrDisableConnect = errors.New("io session is disable to connect")

	stateReadyToConnect int32 = 0
	stateConnectting    int32 = 1
	stateConnected      int32 = 2
	stateClosing        int32 = 3

	stopFlag = &struct{}{}
)

// IOSession session
type IOSession interface {
	ID() uint64
	Connect(addr string, timeout time.Duration) (bool, error)
	Close() error
	Connected() bool
	Read() (interface{}, error)
	Write(msg interface{}) error
	WriteAndFlush(msg interface{}) error
	Flush() error
	InBuf() *buf.ByteBuf
	OutBuf() *buf.ByteBuf
	SetAttr(key string, value interface{})
	GetAttr(key string) interface{}
	RemoteAddr() string
	RemoteIP() string
}

type baseIO struct {
	id                   uint64
	opts                 *options
	state                int32
	conn                 net.Conn
	remoteIP, remoteAddr string
	in                   *buf.ByteBuf
	out                  *buf.ByteBuf
	attrs                sync.Map
	disableConnect       bool
	asyncQueue           queue.Queue
}

// NewIOSession create a new io session
func NewIOSession(opts ...Option) IOSession {
	return newBaseIO(0, nil, opts...)
}

func newBaseIO(id uint64, conn net.Conn, opts ...Option) IOSession {
	bopts := &options{}

	for _, opt := range opts {
		opt(bopts)
	}

	bopts.adjust()
	return newBaseIOWithOptions(id, conn, bopts)
}

func newBaseIOWithOptions(id uint64, conn net.Conn, opts *options) IOSession {
	bio := &baseIO{
		id:   id,
		opts: opts,
		in:   buf.NewByteBuf(opts.readBufSize),
		out:  buf.NewByteBuf(opts.writeBufSize),
	}

	if conn != nil {
		bio.initConn(conn)
		bio.disableConnect = true
	}

	return bio
}

func (bio *baseIO) ID() uint64 {
	return bio.id
}

func (bio *baseIO) Connect(addr string, timeout time.Duration) (bool, error) {
	if bio.disableConnect {
		return false, ErrDisableConnect
	}

	old := bio.getState()
	switch old {
	case stateReadyToConnect:
		break
	case stateClosing:
		return false, fmt.Errorf("the session is closing in other goroutine")
	case stateConnectting:
		return false, fmt.Errorf("the session is connecting in other goroutine")
	case stateConnected:
		return true, nil
	}

	// only stateReadyToConnect state can connect
	if !atomic.CompareAndSwapInt32(&bio.state, stateReadyToConnect, stateConnectting) {
		current := bio.getState()
		if current == stateConnected {
			return true, nil
		}

		return false, fmt.Errorf("the session is closing or connecting is other goroutine")
	}

	bio.reset()
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if nil != err {
		atomic.StoreInt32(&bio.state, stateReadyToConnect)
		return false, err
	}

	bio.initConn(conn)
	return true, nil
}

func (bio *baseIO) Connected() bool {
	return bio.getState() == stateConnected
}

func (bio *baseIO) Close() error {
	old := bio.getState()
	switch old {
	case stateReadyToConnect:
		return nil
	case stateClosing:
		return fmt.Errorf("the session is closing in other goroutine")
	case stateConnectting:
		return fmt.Errorf("the session is connecting in other goroutine")
	case stateConnected:
		break
	}

	// only stateConnected state close
	if !atomic.CompareAndSwapInt32(&bio.state, stateConnected, stateClosing) {
		current := bio.getState()
		if current == stateReadyToConnect {
			return nil
		}

		return fmt.Errorf("the session is closing or connecting is other goroutine")
	}

	bio.reset()
	atomic.StoreInt32(&bio.state, stateReadyToConnect)

	if bio.disableConnect {
		bio.in.Release()
		bio.out.Release()
	}

	if bio.opts.asyncWrite {
		bio.asyncQueue.Put(stopFlag)
	}
	return nil
}

func (bio *baseIO) Read() (interface{}, error) {
	for {
		if !bio.Connected() {
			return nil, ErrIllegalState
		}

		var msg interface{}
		var err error
		var complete bool
		for {
			if bio.in.Readable() > 0 {
				complete, msg, err = bio.opts.decoder.Decode(bio.in)

				if !complete && err == nil {
					complete, msg, err = bio.readFromConn(bio.opts.readTimeout)
				}
			} else {
				bio.in.Clear()
				complete, msg, err = bio.readFromConn(bio.opts.readTimeout)
			}

			if nil != err {
				bio.in.Clear()
				return nil, err
			}

			if complete {
				break
			}
		}

		if bio.in.Readable() == 0 {
			bio.in.Clear()
		}

		return msg, nil
	}
}

func (bio *baseIO) Write(msg interface{}) error {
	if bio.opts.asyncWrite {
		bio.asyncQueue.Put(msg)
		return nil
	}
	return bio.write(msg, false)
}

// WriteAndFlush write a msg to server
func (bio *baseIO) WriteAndFlush(msg interface{}) error {
	if bio.opts.asyncWrite {
		return bio.asyncQueue.Put(msg)
	}
	return bio.write(msg, true)
}

// Flush writes bytes that in the internal bytebuf
func (bio *baseIO) Flush() error {
	if !bio.Connected() {
		return ErrIllegalState
	}

	buf := bio.out
	defer buf.Clear()

	total := buf.Readable()
	written := 0
	for {
		if written == total {
			break
		}

		if 0 != bio.opts.writeTimeout {
			bio.conn.SetWriteDeadline(time.Now().Add(bio.opts.writeTimeout))
		} else {
			bio.conn.SetWriteDeadline(time.Time{})
		}
		n, err := bio.conn.Write(buf.RawBuf()[buf.GetReaderIndex()+written : buf.GetWriteIndex()])
		if err != nil {

			return err
		}

		written += n
	}

	return nil
}

func (bio *baseIO) RemoteAddr() string {
	return bio.remoteAddr
}

func (bio *baseIO) RemoteIP() string {
	return bio.remoteIP
}

func (bio *baseIO) InBuf() *buf.ByteBuf {
	return bio.in
}

func (bio *baseIO) OutBuf() *buf.ByteBuf {
	return bio.out
}

func (bio *baseIO) SetAttr(key string, value interface{}) {
	bio.attrs.Store(key, value)
}

func (bio *baseIO) GetAttr(key string) interface{} {
	if v, ok := bio.attrs.Load(key); ok {
		return v
	}

	return nil
}

func (bio *baseIO) write(msg interface{}, flush bool) error {
	if !bio.Connected() {
		return ErrIllegalState
	}

	err := bio.opts.encoder.Encode(msg, bio.out)
	bio.opts.releaseMsgFunc(msg)
	if err != nil {
		return err
	}

	if flush {
		err = bio.Flush()
		if err != nil {
			return err
		}
	}

	return nil
}

func (bio *baseIO) writeLoop(q queue.Queue) {
	defer q.Dispose()

	items := make([]interface{}, bio.opts.asyncFlushBatch, bio.opts.asyncFlushBatch)
	for {
		n, err := q.Get(bio.opts.asyncFlushBatch, items)
		if nil != err {
			bio.opts.logger.Fatalf("BUG: can not failed")
		}

		for i := int64(0); i < n; i++ {
			if items[i] == stopFlag {
				return
			}

			bio.write(items[i], false)
		}

		err = bio.Flush()
		if err != nil {
			bio.opts.logger.Errorf("flush messages failed with %+v, closed this session", err)
			return
		}
	}
}

func (bio *baseIO) readFromConn(timeout time.Duration) (bool, interface{}, error) {
	if 0 != timeout {
		bio.conn.SetReadDeadline(time.Now().Add(timeout))
	} else {
		bio.conn.SetReadDeadline(time.Time{})
	}

	n, err := io.Copy(bio.in, bio.conn)
	if err != nil {
		return false, nil, err
	}

	if n == 0 {
		return false, nil, io.EOF
	}

	return bio.opts.decoder.Decode(bio.in)
}

func (bio *baseIO) reset() {
	if bio.conn != nil {
		bio.conn.Close()
	}
	bio.in.Clear()
	bio.out.Clear()
	bio.remoteAddr = ""
	bio.remoteIP = ""
}

func (bio *baseIO) getState() int32 {
	return atomic.LoadInt32(&bio.state)
}

func (bio *baseIO) initConn(conn net.Conn) {
	bio.conn = conn
	bio.remoteAddr = conn.RemoteAddr().String()
	if bio.remoteAddr != "" {
		bio.remoteIP = strings.Split(bio.remoteAddr, ":")[0]
	}

	bio.opts.connOptionFunc(bio.conn)
	if bio.opts.asyncWrite {
		bio.asyncQueue = queue.New(64)
		go bio.writeLoop(bio.asyncQueue)
	}
	atomic.StoreInt32(&bio.state, stateConnected)
}
