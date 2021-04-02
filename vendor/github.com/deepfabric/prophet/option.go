package prophet

import (
	"time"

	"github.com/deepfabric/prophet/codec"
	"github.com/deepfabric/prophet/pb/metapb"
	"github.com/deepfabric/prophet/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/buf"
)

// Option client option
type Option func(*options)

type options struct {
	leaderGetter func() *metapb.Member
	rpcTimeout   time.Duration
}

func (opts *options) adjust() {
	if opts.rpcTimeout == 0 {
		opts.rpcTimeout = time.Second * 10
	}
}

// WithLeaderGetter set a func to get a leader
func WithLeaderGetter(value func() *metapb.Member) Option {
	return func(opts *options) {
		opts.leaderGetter = value
	}
}

// WithRPCTimeout set rpc timeout
func WithRPCTimeout(value time.Duration) Option {
	return func(opts *options) {
		opts.rpcTimeout = value
	}
}

func createConn() goetty.IOSession {
	encoder, decoder := codec.NewClientCodec(10 * buf.MB)
	return goetty.NewIOSession(goetty.WithCodec(encoder, decoder),
		goetty.WithLogger(util.GetLogger()),
		goetty.WithEnableAsyncWrite(16))

}
