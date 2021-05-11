package prophet

import (
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/buf"
	"github.com/matrixorigin/matrixcube/components/prophet/codec"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
)

func (p *defaultProphet) startListen() {
	encoder, decoder := codec.NewServerCodec(10 * buf.MB)
	app, err := goetty.NewTCPApplication(p.cfg.RPCAddr,
		p.handleRPCRequest,
		goetty.WithAppSessionOptions(goetty.WithCodec(encoder, decoder),
			goetty.WithEnableAsyncWrite(16),
			goetty.WithLogger(util.GetLogger())))
	if err != nil {
		util.GetLogger().Fatalf("start transport failed with %+v", err)
	}
	p.trans = app
	err = p.trans.Start()
	if err != nil {
		util.GetLogger().Fatalf("start transport failed with %+v", err)
	}
}
