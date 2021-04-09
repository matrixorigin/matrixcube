package goetty

import (
	"net"
	"time"

	"github.com/fagongzi/goetty/codec"
)

const (
	// DefaultSessionBucketSize default bucket size of session map
	DefaultSessionBucketSize = uint64(64)
	// DefaultReadBuf read buf size
	DefaultReadBuf = 256
	// DefaultWriteBuf write buf size
	DefaultWriteBuf = 256
)

// AppOption application option
type AppOption func(*appOptions)

type appOptions struct {
	sessionOpts       *options
	sessionBucketSize uint64
	errorMsgFactory   func(IOSession, interface{}, error) interface{}
}

// WithAppSessionOptions set the number of maps to store session
func WithAppSessionOptions(value ...Option) AppOption {
	return func(opts *appOptions) {
		for _, opt := range value {
			opt(opts.sessionOpts)
		}
	}
}

// WithAppSessionBucketSize set the number of maps to store session
func WithAppSessionBucketSize(value uint64) AppOption {
	return func(opts *appOptions) {
		opts.sessionBucketSize = value
	}
}

// WithAppErrorMsgFactory set function to process error, closed the client session if this field not set
func WithAppErrorMsgFactory(value func(IOSession, interface{}, error) interface{}) AppOption {
	return func(opts *appOptions) {
		opts.errorMsgFactory = value
	}
}

func (opts *appOptions) adjust() {
	opts.sessionOpts.adjust()

	if opts.sessionBucketSize == 0 {
		opts.sessionBucketSize = DefaultSessionBucketSize
	}
}

// Option transport option
type Option func(*options)

type options struct {
	logger                    Logger
	decoder                   codec.Decoder
	encoder                   codec.Encoder
	readBufSize, writeBufSize int
	writeTimeout, readTimeout time.Duration
	connOptionFunc            func(net.Conn)
	asyncWrite                bool
	asyncFlushBatch           int64
	releaseMsgFunc            func(interface{})
}

func (opts *options) adjust() {
	if opts.readBufSize == 0 {
		opts.readBufSize = DefaultReadBuf
	}

	if opts.writeBufSize == 0 {
		opts.writeBufSize = DefaultWriteBuf
	}

	if opts.releaseMsgFunc == nil {
		opts.releaseMsgFunc = func(interface{}) {}
	}

	if opts.connOptionFunc == nil {
		opts.connOptionFunc = func(net.Conn) {}
	}

	if opts.logger == nil {
		opts.logger = newStdLog()
	}
}

// WithLogger set logger
func WithLogger(value Logger) Option {
	return func(opts *options) {
		opts.logger = value
	}
}

// WithConnOptionFunc set conn options func
func WithConnOptionFunc(connOptionFunc func(net.Conn)) Option {
	return func(opts *options) {
		opts.connOptionFunc = connOptionFunc
	}
}

// WithCodec set codec
func WithCodec(encoder codec.Encoder, decoder codec.Decoder) Option {
	return func(opts *options) {
		opts.encoder = encoder
		opts.decoder = decoder
	}
}

// WithBufSize set read/write buf size
func WithBufSize(read, write int) Option {
	return func(opts *options) {
		opts.readBufSize = read
		opts.writeBufSize = write
	}
}

// WithTimeout set read/write timeout
func WithTimeout(read, write time.Duration) Option {
	return func(opts *options) {
		opts.writeTimeout = write
		opts.readTimeout = read
	}
}

// WithEnableAsyncWrite enable async write
func WithEnableAsyncWrite(asyncFlushBatch int64) Option {
	return func(opts *options) {
		opts.asyncWrite = true
		opts.asyncFlushBatch = asyncFlushBatch
	}
}

// WithReleaseMsgFunc set the number of maps to store session
func WithReleaseMsgFunc(value func(interface{})) Option {
	return func(opts *options) {
		opts.releaseMsgFunc = value
	}
}
