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
