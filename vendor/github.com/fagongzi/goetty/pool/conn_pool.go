package pool

import (
	"errors"
	"fmt"
	"sync"

	"github.com/fagongzi/goetty"
)

var (
	// ErrClosed is the error resulting if the pool is closed via pool.Close().
	ErrClosed = errors.New("pool is closed")
)

// IOSessionPool interface describes a pool implementation. A pool should have maximum
// capacity. An ideal pool is threadsafe and easy to use.
type IOSessionPool interface {
	// Get returns a new connection from the pool. Closing the connections puts
	// it back to the Pool. Closing it when the pool is destroyed or full will
	// be counted as an error.
	Get() (goetty.IOSession, error)

	// Put puts the connection back to the pool. If the pool is full or closed,
	// conn is simply closed. A nil conn will be rejected.
	Put(goetty.IOSession) error

	// Close closes the pool and all its connections. After Close() the pool is
	// no longer usable.
	Close()

	// Len returns the current number of connections of the pool.
	Len() int
}

// Factory is a function to create new connections.
type Factory func(interface{}) (goetty.IOSession, error)

// NewIOSessionPool returns a new pool based on buffered channels with an initial
// capacity and maximum capacity. Factory is used when initial capacity is
// greater than zero to fill the pool. A zero initialCap doesn't fill the Pool
// until a new Get() is called. During a Get(), If there is no new connection
// available in the pool, a new connection will be created via the Factory()
// method.
func NewIOSessionPool(remote interface{}, initialCap, maxCap int, factory Factory) (IOSessionPool, error) {
	if initialCap < 0 || maxCap <= 0 || initialCap > maxCap {
		return nil, errors.New("invalid capacity settings")
	}

	c := &channelPool{
		remote:  remote,
		conns:   make(chan goetty.IOSession, maxCap),
		factory: factory,
	}

	// create initial connections, if something goes wrong,
	// just close the pool error out.
	for i := 0; i < initialCap; i++ {
		conn, err := factory(remote)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		c.conns <- conn
	}

	return c, nil
}

type channelPool struct {
	sync.Mutex

	remote  interface{}
	conns   chan goetty.IOSession
	factory Factory
}

func (c *channelPool) getConns() chan goetty.IOSession {
	c.Lock()
	conns := c.conns
	c.Unlock()
	return conns
}

// Get implements the Pool interfaces Get() method. If there is no new
// connection available in the pool, a new connection will be created via the
// Factory() method.
func (c *channelPool) Get() (goetty.IOSession, error) {
	conns := c.getConns()
	if conns == nil {
		return nil, ErrClosed
	}

	// wrap our connections with out custom net.Conn implementation (wrapConn
	// method) that puts the connection back to the pool if it's closed.
	select {
	case conn := <-conns:
		if conn == nil {
			return nil, ErrClosed
		}

		return conn, nil
	default:
		conn, err := c.factory(c.remote)
		if err != nil {
			return nil, err
		}

		return conn, nil
	}
}

// Put implements the Pool interfaces Put() method. If the pool is full or closed,
// conn is simply closed. A nil conn will be rejected.
func (c *channelPool) Put(conn goetty.IOSession) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.Lock()

	if c.conns == nil {
		c.Unlock()
		// pool is closed, close passed connection
		return conn.Close()
	}

	// put the resource back into the pool. If the pool is full, this will
	// block and the default case will be executed.
	select {
	case c.conns <- conn:
		c.Unlock()
		return nil
	default:
		// pool is full, close passed connection
		c.Unlock()
		return conn.Close()
	}
}

func (c *channelPool) Close() {
	c.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.Close()
	}
}

func (c *channelPool) Len() int {
	return len(c.getConns())
}
