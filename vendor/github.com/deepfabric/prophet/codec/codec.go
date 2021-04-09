package codec

import (
	"fmt"

	"github.com/deepfabric/prophet/pb/rpcpb"
	"github.com/fagongzi/goetty/buf"
	gcodec "github.com/fagongzi/goetty/codec"
	"github.com/fagongzi/goetty/codec/length"
	"github.com/fagongzi/util/protoc"
)

var (
	c = &serverCodec{}
)

// NewServerCodec create server codec
func NewServerCodec(maxBodySize int) (gcodec.Encoder, gcodec.Decoder) {
	return length.NewWithSize(c, c, 0, 0, 0, maxBodySize)
}

type serverCodec struct {
}

func (c *serverCodec) Decode(in *buf.ByteBuf) (bool, interface{}, error) {
	data := in.GetMarkedRemindData()
	req := &rpcpb.Request{}
	err := req.Unmarshal(data)
	if err != nil {
		return false, nil, err
	}

	in.MarkedBytesReaded()
	return true, req, nil
}

func (c *serverCodec) Encode(data interface{}, out *buf.ByteBuf) error {
	if resp, ok := data.(*rpcpb.Response); ok {
		index := out.GetWriteIndex()
		size := resp.Size()
		out.Expansion(size)
		protoc.MustMarshalTo(resp, out.RawBuf()[index:index+size])
		out.SetWriterIndex(index + size)
		return nil
	}

	return fmt.Errorf("not support %T %+v", data, data)
}
