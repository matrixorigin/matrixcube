goetty
-------
Goetty is a framework to help you build socket application.

Example
--------
server
```go
package example

import (
	"log"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/codec/simple"
)

// EchoServer echo server
type EchoServer struct {
	addr string
	app  goetty.NetApplication
}

// NewEchoServer create new server
func NewEchoServer(addr string) *EchoServer {
	svr := &EchoServer{}
	encoder, decoder := simple.NewStringCodec()
	app, err := goetty.NewTCPApplication(addr, svr.handle,
		goetty.WithAppSessionOptions(goetty.WithCodec(encoder, decoder)))
	if err != nil {
		log.Panicf("start server failed with %+v", err)
	}

	return &EchoServer{
		addr: addr,
		app:  app,
	}
}

// Start start
func (s *EchoServer) Start() error {
	return s.Start()
}

// Stop stop
func (s *EchoServer) Stop() error {
	return s.Stop()
}

func (s *EchoServer) handle(session goetty.IOSession, msg interface{}, received uint64) error {
	log.Printf("received %+v from %s, already received %d msgs",
		msg,
		session.RemoteAddr(),
		received)
	return session.WriteAndFlush(msg)
}

```

client
```go
package example

import (
	"log"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/codec/simple"
)

// EchoClient echo client
type EchoClient struct {
	serverAddr string
	conn       goetty.IOSession
}

// NewEchoClient new client
func NewEchoClient(serverAddr string) (*EchoClient, error) {
	c := &EchoClient{
		serverAddr: serverAddr,
	}

	encoder, decoder := simple.NewStringCodec()
	c.conn = goetty.NewIOSession(goetty.WithCodec(encoder, decoder))
	_, err := c.conn.Connect(serverAddr, time.Second*3)
	return c, err
}

// SendMsg send msg to server
func (c *EchoClient) SendMsg(msg string) error {
	return c.conn.WriteAndFlush(msg)
}

// ReadLoop read loop
func (c *EchoClient) ReadLoop() error {
	// start loop to read msg from server
	for {
		msg, err := c.conn.Read() // if you want set a read deadline, you can use 'WithTimeout option'
		if err != nil {
			log.Printf("read failed with %+v", err)
			return err
		}

		log.Printf("received %+v", msg)
	}
}

```

