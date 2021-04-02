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
	cc = &clientCodec{}
)

// NewClientCodec create client side codec
func NewClientCodec(maxBodySize int) (gcodec.Encoder, gcodec.Decoder) {
	return length.NewWithSize(cc, cc, 0, 0, 0, maxBodySize)
}

type clientCodec struct {
}

func (c *clientCodec) Decode(in *buf.ByteBuf) (bool, interface{}, error) {
	data := in.GetMarkedRemindData()
	resp := &rpcpb.Response{}
	err := resp.Unmarshal(data)
	if err != nil {
		return false, nil, err
	}

	in.MarkedBytesReaded()
	return true, resp, nil
}

func (c *clientCodec) Encode(data interface{}, out *buf.ByteBuf) error {
	if req, ok := data.(*rpcpb.Request); ok {
		index := out.GetWriteIndex()
		size := req.Size()
		out.Expansion(size)
		protoc.MustMarshalTo(req, out.RawBuf()[index:index+size])
		out.SetWriterIndex(index + size)
		return nil
	}

	return fmt.Errorf("not support %T %+v", data, data)
}
