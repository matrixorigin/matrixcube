package raftstore

import (
	"io"
	"sync"

	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/task"
)

// RPC requests RPC
type RPC interface {
	// Start start the RPC
	Start() error
	// Stop stop the RPC
	Stop()
}

type defaultRPC struct {
	store    *store
	server   *goetty.Server
	sessions sync.Map // int64 -> *session
}

func newRPC(store *store) RPC {
	return &defaultRPC{
		store: store,
		server: goetty.NewServer(store.cfg.RPCAddr,
			goetty.WithServerDecoder(rpcDecoder),
			goetty.WithServerEncoder(rpcEncoder)),
	}
}

func (rpc *defaultRPC) Start() error {
	c := make(chan error)
	go func() {
		c <- rpc.server.Start(rpc.doConnection)
	}()

	select {
	case <-rpc.server.Started():
		return nil
	case err := <-c:
		return err
	}
}

func (rpc *defaultRPC) Stop() {
	rpc.server.Stop()
}

func (rpc *defaultRPC) doConnection(conn goetty.IOSession) error {
	rs := newSession(conn)
	go rs.writeLoop()
	rpc.sessions.Store(rs.id, rs)
	logger.Infof("session %d[%s] connected",
		rs.id,
		rs.addr)

	defer func() {
		rpc.sessions.Delete(rs.id)
		rs.close()
	}()

	for {
		value, err := conn.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			logger.Errorf("session %d[%s] read failed with %+v",
				rs.id,
				rs.addr,
				err)
			return err
		}

		req := value.(*raftcmdpb.Request)
		err = rpc.store.OnRequest(req, rpc.onResp)
		if err != nil {
			rsp := pb.AcquireResponse()
			rsp.ID = req.ID
			rsp.Error.Message = err.Error()
			rs.onResp(rsp)
			pb.ReleaseRequest(req)
		}
	}
}

func (rpc *defaultRPC) onResp(resp *raftcmdpb.RaftCMDResponse) {
	hasError := resp.Header != nil
	for _, rsp := range resp.Responses {
		if value, ok := rpc.sessions.Load(rsp.SessionID); ok {
			rs := value.(*session)
			if hasError {
				if resp.Header.Error.RaftEntryTooLarge == nil {
					rsp.Type = raftcmdpb.RaftError
				} else {
					rsp.Type = raftcmdpb.Invalid
				}

				rsp.Error = resp.Header.Error
			}

			rs.onResp(rsp)
		} else {
			pb.ReleaseResponse(rsp)
		}
	}

	pb.ReleaseRaftCMDResponse(resp)
}

var (
	stopFlag = &struct{}{}
)

type session struct {
	id    int64
	resps *task.Queue
	conn  goetty.IOSession
	addr  string
}

func newSession(conn goetty.IOSession) *session {
	return &session{
		id:    conn.ID().(int64),
		resps: task.New(32),
		conn:  conn,
		addr:  conn.RemoteAddr(),
	}
}

func (s *session) close() {
	s.resps.Put(stopFlag)
}

func (s *session) doClose() {
	s.resps.Disposed()
}

func (s *session) onResp(resp *raftcmdpb.Response) {
	if s != nil {
		s.resps.Put(resp)
	} else {
		pb.ReleaseResponse(resp)
	}
}

func (s *session) writeLoop() {
	items := make([]interface{}, 16, 16)
	for {
		n, err := s.resps.Get(16, items)
		if nil != err {
			logger.Fatalf("BUG: can not failed")
		}

		for i := int64(0); i < n; i++ {
			if items[i] == stopFlag {
				logger.Infof("session %d[%s] closed",
					s.id,
					s.addr)
				s.doClose()
				return
			}

			rsp := items[i].(*raftcmdpb.Response)
			s.conn.Write(rsp)
			pb.ReleaseResponse(rsp)
		}

		s.conn.Flush()
	}
}
