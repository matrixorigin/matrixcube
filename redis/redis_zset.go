package redis

import (
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/pb/redispb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/protoc"
)

// ============================= write methods

func (h *handler) zadd(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	score, err := format.ParseStrFloat64(hack.SliceToString(args.Args[0]))
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	value, err := h.getRedisZSet(shard).ZAdd(req.Key, score, args.Args[1])
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	writtenBytes := uint64(0)
	if value > 0 {
		writtenBytes += uint64(len(args.Args[0]) + len(args.Args[1]))
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:          redispb.IntegerResp,
		IntegerResult: value,
	})
	return writtenBytes, int64(writtenBytes), resp
}

func (h *handler) zcard(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	value, err := h.getRedisZSet(shard).ZCard(req.Key)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:          redispb.IntegerResp,
		IntegerResult: value,
	})
	return 0, 0, resp
}

func (h *handler) zincrby(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	by, err := format.ParseStrFloat64(hack.SliceToString(args.Args[1]))
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	value, err := h.getRedisZSet(shard).ZIncrBy(req.Key, args.Args[0], by)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:       redispb.BulkResp,
		BulkResult: value,
	})
	return 0, 0, resp
}

func (h *handler) zrem(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) < 1 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	value, err := h.getRedisZSet(shard).ZRem(req.Key, args.Args...)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	diffBytes := int64(0)
	if value > 0 {
		for _, arg := range args.Args {
			diffBytes += int64(len(arg))
		}
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:          redispb.IntegerResp,
		IntegerResult: value,
	})
	return 0, -diffBytes, resp
}

func (h *handler) zremrangebylex(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	value, err := h.getRedisZSet(shard).ZRemRangeByLex(req.Key, args.Args[0], args.Args[1])
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:          redispb.IntegerResp,
		IntegerResult: value,
	})
	return 0, 0, resp
}

func (h *handler) zremrangebyrank(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	start, err := format.ParseStrInt64(hack.SliceToString(args.Args[0]))
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	stop, err := format.ParseStrInt64(hack.SliceToString(args.Args[1]))
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	value, err := h.getRedisZSet(shard).ZRemRangeByRank(req.Key, start, stop)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:          redispb.IntegerResp,
		IntegerResult: value,
	})
	return 0, 0, resp
}

func (h *handler) zremrangebyscore(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	value, err := h.getRedisZSet(shard).ZRemRangeByScore(req.Key, args.Args[0], args.Args[1])
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:          redispb.IntegerResp,
		IntegerResult: value,
	})
	return 0, 0, resp
}

func (h *handler) zscore(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 1 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	value, err := h.getRedisZSet(shard).ZScore(req.Key, args.Args[0])
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:       redispb.BulkResp,
		BulkResult: value,
	})
	return 0, 0, resp
}

// ============================= read methods

func (h *handler) zcount(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return resp
	}

	value, err := h.getRedisZSet(shard).ZCount(req.Key, args.Args[0], args.Args[1])
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

func (h *handler) zlexcount(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return resp
	}

	value, err := h.getRedisZSet(shard).ZLexCount(req.Key, args.Args[0], args.Args[1])
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

func (h *handler) zrange(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) < 2 {
		resp.Value = invalidCommandResp
		return resp
	}

	start, err := format.ParseStrInt64(hack.SliceToString(args.Args[0]))
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	stop, err := format.ParseStrInt64(hack.SliceToString(args.Args[1]))
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	value, err := h.getRedisZSet(shard).ZRange(req.Key, start, stop)
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:                 redispb.ScorePairArrayResp,
		ScorePairArrayResult: value,
		Withscores:           len(args.Args) == 3,
	})
	return resp
}

func (h *handler) zrangebylex(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return resp
	}

	value, err := h.getRedisZSet(shard).ZRangeByLex(req.Key, args.Args[0], args.Args[1])
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:                 redispb.SliceArrayResp,
		ScorePairArrayResult: value,
	})
	return resp
}

func (h *handler) zrangebyscore(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) < 2 {
		resp.Value = invalidCommandResp
		return resp
	}

	value, err := h.getRedisZSet(shard).ZRangeByScore(req.Key, args.Args[0], args.Args[1])
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:                 redispb.ScorePairArrayResp,
		ScorePairArrayResult: value,
		Withscores:           len(args.Args) == 3,
	})
	return resp
}

func (h *handler) zrank(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 1 {
		resp.Value = invalidCommandResp
		return resp
	}

	value, err := h.getRedisZSet(shard).ZRank(req.Key, args.Args[0])
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	if value < 0 {
		resp.Value = emptyBulkResp
	} else {
		resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
			Type:          redispb.IntegerResp,
			IntegerResult: value,
		})
	}

	return resp
}
