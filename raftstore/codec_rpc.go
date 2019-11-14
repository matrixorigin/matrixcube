package raftstore

import (
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
)

var (
	rc         = &rpcCodec{}
	rpcDecoder = goetty.NewIntLengthFieldBasedDecoder(rc)
	rpcEncoder = goetty.NewIntLengthFieldBasedEncoder(rc)
)

type rpcCodec struct {
}

func (c *rpcCodec) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	req := pb.AcquireRequest()
	protoc.MustUnmarshal(req, in.GetMarkedRemindData())
	in.MarkedBytesReaded()
	return true, req, nil
}

func (c *rpcCodec) Encode(data interface{}, out *goetty.ByteBuf) error {
	rsp := data.(*raftcmdpb.Response)
	size := rsp.Size()
	index := out.GetWriteIndex()
	out.Expansion(size)
	protoc.MustMarshalTo(rsp, out.RawBuf()[index:index+size])
	out.SetWriterIndex(index + size)
	return nil
}
