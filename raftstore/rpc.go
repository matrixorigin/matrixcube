package raftstore

import (
	"encoding/hex"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/codec/length"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
)

type defaultRPC struct {
	store *store
	app   goetty.NetApplication
}

func newRPC(store *store) *defaultRPC {
	rpc := &defaultRPC{
		store: store,
	}

	encoder, decoder := length.NewWithSize(rc, rc, 0, 0, 0, int(store.cfg.Raft.MaxEntryBytes)*2)
	app, err := goetty.NewTCPApplication(store.cfg.ClientAddr, rpc.onMessage,
		goetty.WithAppSessionOptions(goetty.WithCodec(encoder, decoder),
			goetty.WithEnableAsyncWrite(16),
			goetty.WithLogger(logger),
			goetty.WithReleaseMsgFunc(releaseResponse)))
	if err != nil {
		logger.Fatalf("create rpc failed with %+v", err)
	}

	store.RegisterRPCRequestCB(rpc.onResp)
	rpc.app = app
	return rpc
}

func (rpc *defaultRPC) Start() error {
	return rpc.app.Start()
}

func (rpc *defaultRPC) Stop() {
	rpc.app.Stop()
}

func releaseResponse(resp interface{}) {
	pb.ReleaseResponse(resp.(*raftcmdpb.Response))
}

func (rpc *defaultRPC) onMessage(rs goetty.IOSession, value interface{}, seq uint64) error {
	req := value.(*raftcmdpb.Request)
	req.PID = int64(rs.ID())
	err := rpc.store.OnRequest(req)
	if err != nil {
		rsp := pb.AcquireResponse()
		rsp.ID = req.ID
		rsp.Error.Message = err.Error()
		rs.WriteAndFlush(rsp)
		pb.ReleaseRequest(req)
	}
	return nil
}

func (rpc *defaultRPC) onResp(header *raftcmdpb.RaftResponseHeader, rsp *raftcmdpb.Response) {
	if rs, _ := rpc.app.GetSession(uint64(rsp.PID)); rs != nil {
		if header != nil {
			if header.Error.RaftEntryTooLarge == nil {
				rsp.Type = raftcmdpb.CMDType_RaftError
			} else {
				rsp.Type = raftcmdpb.CMDType_Invalid
			}

			rsp.Error = header.Error
		}

		if logger.DebugEnabled() {
			logger.Debugf("%s rpc received response", hex.EncodeToString(rsp.ID))
		}
		rs.WriteAndFlush(rsp)
	} else {
		if logger.DebugEnabled() {
			logger.Debugf("%s rpc received response, missing session", hex.EncodeToString(rsp.ID))
		}
		pb.ReleaseResponse(rsp)
	}
}
