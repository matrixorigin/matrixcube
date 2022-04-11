package executor

import (
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/storage"
)

// NewWriteRequest return write request
func NewWriteRequest(k, v []byte) storage.Request {
	return storage.Request{
		CmdType: uint64(rpcpb.CmdKVSet),
		Key:     k,
		Cmd: protoc.MustMarshal(&rpcpb.KVSetRequest{
			Key:   k,
			Value: v,
		}),
	}
}

// NewReadRequest return read request
func NewReadRequest(k []byte) storage.Request {
	return storage.Request{
		CmdType: uint64(rpcpb.CmdKVGet),
		Key:     k,
		Cmd: protoc.MustMarshal(&rpcpb.KVGetRequest{
			Key: k,
		}),
	}
}
