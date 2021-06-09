package transport

import (
	"fmt"

	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
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
		msg := &bhraftpb.SnapshotMessage{}
		protoc.MustUnmarshal(msg, data)
		in.MarkedBytesReaded()
		return true, msg, nil
	case typeRaft:
		msg := pb.AcquireRaftMessage()
		protoc.MustUnmarshal(msg, data)
		in.MarkedBytesReaded()
		return true, msg, nil
	}

	return false, nil, fmt.Errorf("[matrixcube]: bug, not support msg type %d", t)
}

func (e raftEncoder) Encode(data interface{}, out *buf.ByteBuf) error {
	t := typeRaft
	var m protoc.PB

	if v, ok := data.(*bhraftpb.RaftMessage); ok {
		t = typeRaft
		m = v
	} else if v, ok := data.(*bhraftpb.SnapshotMessage); ok {
		t = typeSnap
		m = v
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
