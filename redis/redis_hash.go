package redis

import (
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/pb/redispb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/protoc"
)

// ============================= write methods

func (h *handler) hset(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	value, err := h.getRedisHash(shard.ID).HSet(req.Key, args.Args[0], args.Args[1])
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

func (h *handler) hdel(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) < 1 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	value, err := h.getRedisHash(shard.ID).HDel(req.Key, args.Args...)
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

func (h *handler) hmset(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) < 2 || len(args.Args)%2 != 0 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	writtenBytes := uint64(0)
	l := len(args.Args) / 2
	fields := make([][]byte, l)
	values := make([][]byte, l)

	for i := 0; i < l; i++ {
		fields[i] = args.Args[2*i]
		values[i] = args.Args[2*i+1]

		writtenBytes += uint64(len(fields[i]))
		writtenBytes += uint64(len(values[i]))
	}

	err := h.getRedisHash(shard.ID).HMSet(req.Key, fields, values)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	resp.Value = statusResp
	return writtenBytes, int64(writtenBytes), resp
}

func (h *handler) hsetnx(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	value, err := h.getRedisHash(shard.ID).HSetNX(req.Key, args.Args[0], args.Args[1])
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

func (h *handler) hincrby(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	incrment, err := format.ParseStrInt64(hack.SliceToString(args.Args[1]))
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	value, err := h.getRedisHash(shard.ID).HIncrBy(req.Key, args.Args[0], incrment)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:          redispb.IntegerResp,
		IntegerResult: format.MustParseStrInt64(hack.SliceToString(value)),
	})
	return 0, 0, resp
}

// ============================= read methods

func (h *handler) hget(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 1 {
		resp.Value = invalidCommandResp
		return resp
	}

	value, err := h.getRedisHash(shard.ID).HGet(req.Key, args.Args[0])
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:       redispb.BulkResp,
		BulkResult: value,
	})
	return resp
}

func (h *handler) hexists(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 1 {
		resp.Value = invalidCommandResp
		return resp
	}

	exists, err := h.getRedisHash(shard.ID).HExists(req.Key, args.Args[0])
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	var value int64
	if exists {
		value = 1
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:          redispb.IntegerResp,
		IntegerResult: value,
	})
	return resp
}

func (h *handler) hkeys(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()

	value, err := h.getRedisHash(shard.ID).HKeys(req.Key)
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

func (h *handler) hvals(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()

	value, err := h.getRedisHash(shard.ID).HVals(req.Key)
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

func (h *handler) hgetall(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()

	value, err := h.getRedisHash(shard.ID).HGetAll(req.Key)
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:              redispb.KVPairArrayResp,
		KVPairArrayResult: value,
	})
	return resp
}

func (h *handler) hscanget(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return resp
	}

	count, err := format.ParseStrInt64(hack.SliceToString(args.Args[1]))
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	value, err := h.getRedisHash(shard.ID).HScanGet(req.Key, args.Args[0], int(count))
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:              redispb.KVPairArrayResp,
		KVPairArrayResult: value,
	})
	return resp
}

func (h *handler) hlen(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()

	value, err := h.getRedisHash(shard.ID).HLen(req.Key)
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

func (h *handler) hmget(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) < 1 {
		resp.Value = invalidCommandResp
		return resp
	}

	value, errs := h.getRedisHash(shard.ID).HMGet(req.Key, args.Args...)
	if len(errs) > 0 {
		errors := make([][]byte, len(errs))
		for idx, err := range errs {
			errors[idx] = hack.StringToSlice(err.Error())
		}

		resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
			Type:         redispb.ErrorsResp,
			ErrorResults: errors,
		})
		return resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:             redispb.SliceArrayResp,
		SliceArrayResult: value,
	})
	return resp
}

func (h *handler) hstrlen(shard metapb.Shard, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 1 {
		resp.Value = invalidCommandResp
		return resp
	}

	value, err := h.getRedisHash(shard.ID).HStrLen(req.Key, args.Args[0])
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
