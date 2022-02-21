// Copyright 2020 MatrixOrigin.
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

package raftstore

import (
	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
)

var (
	rc = &rpcCodec{}
)

type rpcCodec struct {
	clientSide bool
}

func (c *rpcCodec) Decode(in *buf.ByteBuf) (bool, interface{}, error) {
	if c.clientSide {
		value := rpcpb.Response{}
		err := value.Unmarshal(in.GetMarkedRemindData())
		if err != nil {
			return false, nil, err
		}

		in.MarkedBytesReaded()
		return true, value, nil
	}

	value := rpcpb.Request{}
	err := value.Unmarshal(in.GetMarkedRemindData())
	if err != nil {
		return false, nil, err
	}

	in.MarkedBytesReaded()
	return true, value, nil
}

func (c *rpcCodec) Encode(data interface{}, out *buf.ByteBuf) error {
	var rsp protoc.PB
	if c.clientSide {
		v := data.(rpcpb.Request)
		rsp = &v
	} else {
		v := data.(rpcpb.Response)
		rsp = &v
	}

	size := rsp.Size()
	index := out.GetWriteIndex()
	out.Expansion(size)
	protoc.MustMarshalTo(rsp, out.RawBuf()[index:index+size])
	out.SetWriterIndex(index + size)
	return nil
}
