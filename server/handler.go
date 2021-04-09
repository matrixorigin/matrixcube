package server

import (
	"github.com/deepfabric/beehive/command"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/fagongzi/goetty/codec"
)

// Handler is the request handler
type Handler interface {
	// BuildRequest build the request, fill the key, cmd, type,
	// and the custom type
	BuildRequest(*raftcmdpb.Request, interface{}) error
	// Codec returns the decoder and encoder to transfer request and response
	Codec() (codec.Encoder, codec.Decoder)
	// AddReadFunc add read handler func
	AddReadFunc(cmdType uint64, cb command.ReadCommandFunc)
	// AddWriteFunc add write handler func
	AddWriteFunc(cmdType uint64, cb command.WriteCommandFunc)
}
