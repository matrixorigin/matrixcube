package util

import (
	"sync"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/task"
)

var (
	stopFlag = &struct{}{}
)

// Session session
type Session struct {
	ID          interface{}
	Addr        string
	resps       *task.Queue
	conn        goetty.IOSession
	releaseFunc func(interface{})
	stopOnce    sync.Once
	stopped     chan struct{}
}

// NewSession create a client session
func NewSession(conn goetty.IOSession, releaseFunc func(interface{})) *Session {
	s := &Session{
		ID:          conn.ID(),
		Addr:        conn.RemoteAddr(),
		resps:       task.New(32),
		conn:        conn,
		releaseFunc: releaseFunc,
		stopped:     make(chan struct{}),
	}

	go s.writeLoop()
	return s
}

// Close close the client session
func (s *Session) Close() {
	s.resps.Put(stopFlag)
	<-s.stopped
}

// OnResp receive a response
func (s *Session) OnResp(resp interface{}) {
	if s != nil {
		s.resps.Put(resp)
	} else {
		s.releaseResp(resp)
	}
}

func (s *Session) doClose() {
	s.stopOnce.Do(func() {
		s.resps.Disposed()
		s.stopped <- struct{}{}
	})
}

func (s *Session) releaseResp(resp interface{}) {
	if s.releaseFunc != nil && resp != nil {
		s.releaseFunc(resp)
	}
}

func (s *Session) writeLoop() {
	items := make([]interface{}, 16, 16)
	for {
		n, err := s.resps.Get(16, items)
		if nil != err {
			log.Fatalf("BUG: can not failed")
		}

		for i := int64(0); i < n; i++ {
			if items[i] == stopFlag {
				log.Infof("session %d[%s] closed",
					s.ID,
					s.Addr)
				s.doClose()
				return
			}

			s.conn.Write(items[i])
			s.releaseResp(items[i])
		}

		s.conn.Flush()
	}
}
