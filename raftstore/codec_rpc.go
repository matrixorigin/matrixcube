package raftstore

import (
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
)

var (
	rc = &rpcCodec{}
)

type rpcCodec struct {
	clientSide bool
}

func (c *rpcCodec) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	var value protoc.PB
	if c.clientSide {
		value = pb.AcquireResponse()
	} else {
		value = pb.AcquireRequest()
	}

	err := value.Unmarshal(in.GetMarkedRemindData())
	if err != nil {
		return false, nil, err
	}

	in.MarkedBytesReaded()
	return true, value, nil
}

func (c *rpcCodec) Encode(data interface{}, out *goetty.ByteBuf) error {
	var rsp protoc.PB
	if c.clientSide {
		rsp = data.(*raftcmdpb.Request)
	} else {
		rsp = data.(*raftcmdpb.Response)
	}

	size := rsp.Size()
	index := out.GetWriteIndex()
	out.Expansion(size)
	protoc.MustMarshalTo(rsp, out.RawBuf()[index:index+size])
	out.SetWriterIndex(index + size)
	return nil
}
