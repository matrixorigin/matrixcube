package length

import (
	"fmt"

	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/goetty/codec"
)

const (
	// FieldLength field length bytes
	FieldLength = 4

	// DefaultMaxBodySize max default body size, 10M
	DefaultMaxBodySize = 1024 * 1024 * 10
)

type lengthCodec struct {
	baseDecoder         codec.Decoder
	baseEncoder         codec.Encoder
	lengthFieldOffset   int
	lengthAdjustment    int
	initialBytesToStrip int
	maxBodySize         int
}

// New returns a default IntLengthFieldBased codec
func New(baseEncoder codec.Encoder, baseDecoder codec.Decoder) (codec.Encoder, codec.Decoder) {
	return NewWithSize(baseEncoder, baseDecoder, 0, 0, 0, DefaultMaxBodySize)
}

// NewWithSize  create IntLengthFieldBased codec
// initialBytesToStrip + lengthFieldOffset + 4(length)
// lengthAdjustment, some case as below:
// 1. 0 :                                             base decoder received: body
// 2. -4:                                             base decoder received: 4(length) + body
// 3. -(4 + lengthFieldOffset):                       base decoder received: lengthFieldOffset + 4(length) + body
// 4. -(4 + lengthFieldOffset + initialBytesToStrip): base decoder received: initialBytesToStrip + lengthFieldOffset + 4(length)
func NewWithSize(baseEncoder codec.Encoder, baseDecoder codec.Decoder, lengthFieldOffset, lengthAdjustment, initialBytesToStrip, maxBodySize int) (codec.Encoder, codec.Decoder) {
	c := &lengthCodec{
		baseEncoder:         baseEncoder,
		baseDecoder:         baseDecoder,
		lengthFieldOffset:   lengthFieldOffset,
		lengthAdjustment:    lengthAdjustment,
		initialBytesToStrip: initialBytesToStrip,
		maxBodySize:         maxBodySize,
	}

	return c, c
}

func (c *lengthCodec) Decode(in *buf.ByteBuf) (bool, interface{}, error) {
	readable := in.Readable()

	minFrameLength := c.initialBytesToStrip + c.lengthFieldOffset + FieldLength
	if readable < minFrameLength {
		return false, nil, nil
	}

	length, err := in.PeekInt(c.initialBytesToStrip + c.lengthFieldOffset)
	if err != nil {
		return true, nil, err
	}

	if length > c.maxBodySize {
		return false, nil, fmt.Errorf("too big body size %d, max is %d", length, c.maxBodySize)
	}

	skip := minFrameLength + c.lengthAdjustment
	minFrameLength += length
	if readable < minFrameLength {
		return false, nil, nil
	}

	in.Skip(skip)
	in.MarkN(length)
	return c.baseDecoder.Decode(in)
}

func (c *lengthCodec) Encode(data interface{}, out *buf.ByteBuf) error {
	idx := out.GetWriteIndex()
	out.Expansion(4)
	out.SetWriterIndex(idx + 4)
	err := c.baseEncoder.Encode(data, out)
	if err != nil {
		return err
	}

	buf.Int2BytesTo(out.GetWriteIndex()-idx-4, out.RawBuf()[idx:])
	return nil
}
