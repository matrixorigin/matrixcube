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

package transport

import (
	"fmt"

	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/meta"
)

const (
	typeRaft = 1
	typeSnap = 2
	typeAck  = 3
)

type raftDecoder struct {
}

type raftEncoder struct {
}

func newRaftDecoder() *raftDecoder {
	return &raftDecoder{}
}

func newRaftEncoder() *raftEncoder {
	return &raftEncoder{}
}

func (decoder raftDecoder) Decode(in *buf.ByteBuf) (bool, interface{}, error) {
	t, err := in.ReadByte()
	if err != nil {
		return true, nil, err
	}

	data := in.GetMarkedRemindData()

	switch t {
	case typeSnap:
		msg := meta.SnapshotMessage{}
		protoc.MustUnmarshal(&msg, data)
		in.MarkedBytesReaded()
		return true, msg, nil
	case typeRaft:
		msg := meta.RaftMessage{}
		protoc.MustUnmarshal(&msg, data)
		in.MarkedBytesReaded()
		return true, msg, nil
	}

	return false, nil, fmt.Errorf("[matrixcube]: bug, not support msg type %d", t)
}

func (e raftEncoder) Encode(data interface{}, out *buf.ByteBuf) error {
	t := typeRaft
	var m protoc.PB

	if v, ok := data.(meta.RaftMessage); ok {
		t = typeRaft
		m = &v
	} else if v, ok := data.(meta.SnapshotMessage); ok {
		t = typeSnap
		m = &v
	} else {
		log.Fatalf("[matrixcube]: bug, not support msg type %T", data)
	}

	size := m.Size()
	out.WriteByte(byte(t))
	if size > 0 {
		index := out.GetWriteIndex()
		out.Expansion(size)
		protoc.MustMarshalTo(m, out.RawBuf()[index:index+size])
		out.SetWriterIndex(index + size)
	}

	return nil
}
