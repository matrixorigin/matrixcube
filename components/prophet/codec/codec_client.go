// Copyright 2021 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package codec

import (
	"fmt"

	"github.com/fagongzi/goetty/buf"
	gcodec "github.com/fagongzi/goetty/codec"
	"github.com/fagongzi/goetty/codec/length"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
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
