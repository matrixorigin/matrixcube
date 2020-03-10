package raftstore

import (
	"encoding/hex"
	"io"
	"sync"

	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/util"
	"github.com/fagongzi/goetty"
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
	sessions sync.Map // interface{} -> *util.Session
}

func newRPC(store *store) RPC {
	rpc := &defaultRPC{
		store: store,
		server: goetty.NewServer(store.cfg.RPCAddr,
			goetty.WithServerDecoder(rpcDecoder),
			goetty.WithServerEncoder(rpcEncoder)),
	}

	store.RegisterRPCRequestCB(rpc.onResp)
	return rpc
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

func releaseResponse(resp interface{}) {
	pb.ReleaseResponse(resp.(*raftcmdpb.Response))
}

func (rpc *defaultRPC) doConnection(conn goetty.IOSession) error {
	rs := util.NewSession(conn, releaseResponse)
	rpc.sessions.Store(rs.ID, rs)
	logger.Infof("session %d[%s] connected",
		rs.ID,
		rs.Addr)

	defer func() {
		rpc.sessions.Delete(rs.ID)
		rs.Close()
	}()

	for {
		value, err := conn.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			logger.Errorf("session %d[%s] read failed with %+v",
				rs.ID,
				rs.Addr,
				err)
			return err
		}

		req := value.(*raftcmdpb.Request)
		req.PID = rs.ID.(int64)
		err = rpc.store.OnRequest(req)
		if err != nil {
			rsp := pb.AcquireResponse()
			rsp.ID = req.ID
			rsp.Error.Message = err.Error()
			rs.OnResp(rsp)
			pb.ReleaseRequest(req)
		}
	}
}

func (rpc *defaultRPC) onResp(header *raftcmdpb.RaftResponseHeader, rsp *raftcmdpb.Response) {
	if value, ok := rpc.sessions.Load(rsp.PID); ok {
		rs := value.(*util.Session)
		if header != nil {
			if header.Error.RaftEntryTooLarge == nil {
				rsp.Type = raftcmdpb.RaftError
			} else {
				rsp.Type = raftcmdpb.Invalid
			}

			rsp.Error = header.Error
		}

		if logger.DebugEnabled() {
			logger.Debugf("%s rpc received response", hex.EncodeToString(rsp.ID))
		}
		rs.OnResp(rsp)
	} else {
		if logger.DebugEnabled() {
			logger.Debugf("%s rpc received response, missing session", hex.EncodeToString(rsp.ID))
		}

		pb.ReleaseResponse(rsp)
	}
}
