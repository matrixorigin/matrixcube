package server

import (
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/fagongzi/goetty"
)

// Handler is the request handler
type Handler interface {
	// BuildRequest build the request, fill the key, cmd, type,
	// and the custom type
	BuildRequest(*raftcmdpb.Request, interface{}) error
	// Codec returns the decoder and encoder to transfer request and response
	Codec() (goetty.Decoder, goetty.Encoder)
	// AddReadFunc add read handler func
	AddReadFunc(cmd string, cmdType uint64, cb raftstore.ReadCommandFunc)
	// AddWriteFunc add write handler func
	AddWriteFunc(cmd string, cmdType uint64, cb raftstore.WriteCommandFunc)
}
