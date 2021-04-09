package codec

import (
	"github.com/fagongzi/goetty/buf"
)

// Encoder encode interface
type Encoder interface {
	Encode(data interface{}, out *buf.ByteBuf) error
}

// Decoder decoder interface
type Decoder interface {
	Decode(in *buf.ByteBuf) (complete bool, msg interface{}, err error)
}

type emptyDecoder struct{}

func (e *emptyDecoder) Decode(in *buf.ByteBuf) (complete bool, msg interface{}, err error) {
	return true, in, nil
}

type emptyEncoder struct{}

func (e *emptyEncoder) Encode(data interface{}, out *buf.ByteBuf) error {
	return nil
}

// NewEmptyEncoder returns a empty encoder
func NewEmptyEncoder() Encoder {
	return &emptyEncoder{}
}

// NewEmptyDecoder returns a empty decoder
func NewEmptyDecoder() Decoder {
	return &emptyDecoder{}
}
