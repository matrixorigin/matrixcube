package redis

import (
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/pb/redispb"
	"github.com/fagongzi/util/protoc"
)

func (h *handler) set(shard uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	resp := &raftcmdpb.Response{}
	value := redispb.RedisResponse{}
	if len(args.Args) != 1 {
		value.ErrorResult = invalidCommand
		resp.Value = protoc.MustMarshal(&value)
		return resp
	}

	err := h.getRedisKV(shard).Set(req.Key, args.Args[0])
	if err != nil {
		value.ErrorResult = []byte(err.Error())
		resp.Value = protoc.MustMarshal(&value)
		return resp
	}

	value.StatusResult = []byte("OK")
	return &raftcmdpb.Response{
		Value: protoc.MustMarshal(&value),
	}
}
