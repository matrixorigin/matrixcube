package redis

import (
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/pb/redispb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
)

// ============================= write methods

func (h *handler) sadd(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) < 1 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	value, err := h.getRedisSet(shard.ID).SAdd(req.Key, args.Args...)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	writtenBytes := uint64(0)
	if value > 0 {
		for _, arg := range args.Args {
			writtenBytes += uint64(len(arg))
		}
	}
	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:          redispb.IntegerResp,
		IntegerResult: value,
	})
	return writtenBytes, int64(writtenBytes), resp
}

func (h *handler) srem(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) < 1 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	value, err := h.getRedisSet(shard.ID).SRem(req.Key, args.Args...)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	writtenBytes := uint64(0)
	for _, arg := range args.Args {
		writtenBytes += uint64(len(arg))
	}
	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:          redispb.IntegerResp,
		IntegerResult: value,
	})
	return writtenBytes, -int64(writtenBytes), resp
}

func (h *handler) scard(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	value, err := h.getRedisSet(shard.ID).SCard(req.Key)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	writtenBytes := uint64(len(req.Key))
	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:          redispb.IntegerResp,
		IntegerResult: value,
	})
	return writtenBytes, -int64(writtenBytes), resp
}

func (h *handler) spop(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	value, err := h.getRedisSet(shard.ID).SPop(req.Key)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	writtenBytes := uint64(len(req.Key))
	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:       redispb.BulkResp,
		BulkResult: value,
	})
	return writtenBytes, -int64(writtenBytes + uint64(len(value))), resp
}

// ============================= read methods

func (h *handler) smembers(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()

	value, err := h.getRedisSet(shard.ID).SMembers(req.Key)
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:             redispb.SliceArrayResp,
		SliceArrayResult: value,
	})
	return resp
}

func (h *handler) sismember(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 1 {
		resp.Value = invalidCommandResp
		return resp
	}

	value, err := h.getRedisSet(shard.ID).SIsMember(req.Key, args.Args[0])
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:          redispb.IntegerResp,
		IntegerResult: value,
	})
	return resp
}
