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

package prophet

import (
	"github.com/fagongzi/goetty"
	"github.com/matrixorigin/matrixcube/components/prophet/codec"
	"github.com/matrixorigin/matrixcube/util/buf"
	"go.uber.org/zap"
)

func (p *defaultProphet) startListen() {
	encoder, decoder := codec.NewServerCodec(10 * buf.MB)
	app, err := goetty.NewTCPApplication(p.cfg.Prophet.RPCAddr,
		p.handleRPCRequest,
		goetty.WithAppSessionOptions(goetty.WithCodec(encoder, decoder),
			goetty.WithEnableAsyncWrite(16),
			goetty.WithLogger(p.logger.Named("rpc"))))
	if err != nil {
		p.logger.Fatal("fail to start transport", zap.Error(err))
	}
	p.trans = app
	err = p.trans.Start()
	if err != nil {
		p.logger.Fatal("fail to start transport", zap.Error(err))
	}
}
