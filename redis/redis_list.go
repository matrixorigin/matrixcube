package redis

import (
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/pb/redispb"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/protoc"
)

// ============================= write methods

func (h *handler) linsert(shard metapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 3 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	pos, err := format.ParseStrInt64(hack.SliceToString(args.Args[0]))
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	value, err := h.getRedisList(shard.ID).LInsert(req.Key, int(pos), args.Args[1], args.Args[2])
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	writtenBytes := uint64(0)
	if value > 0 {
		writtenBytes += uint64(len(args.Args[2]))
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:          redispb.IntegerResp,
		IntegerResult: value,
	})
	return writtenBytes, int64(writtenBytes), resp
}

func (h *handler) lpop(shard metapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	value, err := h.getRedisList(shard.ID).LPop(req.Key)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:       redispb.BulkResp,
		BulkResult: value,
	})
	return 0, -int64(len(value)), resp
}

func (h *handler) lpush(shard metapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) < 1 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	value, err := h.getRedisList(shard.ID).LPush(req.Key, args.Args...)
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

func (h *handler) lpushx(shard metapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 1 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	value, err := h.getRedisList(shard.ID).LPushX(req.Key, args.Args[0])
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

func (h *handler) lrem(shard metapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	count, err := format.ParseStrInt64(hack.SliceToString(args.Args[0]))
	if err != nil {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	value, err := h.getRedisList(shard.ID).LRem(req.Key, count, args.Args[1])
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	diffBytes := int64(len(args.Args[1])) * value
	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:          redispb.IntegerResp,
		IntegerResult: value,
	})
	return 0, -diffBytes, resp
}

func (h *handler) lset(shard metapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	index, err := format.ParseStrInt64(hack.SliceToString(args.Args[0]))
	if err != nil {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	err = h.getRedisList(shard.ID).LSet(req.Key, index, args.Args[1])
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	writtenBytes := uint64(len(args.Args[1]))
	resp.Value = statusResp
	return writtenBytes, 0, resp
}

func (h *handler) ltrim(shard metapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	begin, err := format.ParseStrInt64(hack.SliceToString(args.Args[0]))
	if err != nil {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	end, err := format.ParseStrInt64(hack.SliceToString(args.Args[1]))
	if err != nil {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	err = h.getRedisList(shard.ID).LTrim(req.Key, begin, end)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	resp.Value = statusResp
	return 0, 0, resp
}

func (h *handler) rpop(shard metapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	value, err := h.getRedisList(shard.ID).RPop(req.Key)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	resp.Value = protoc.MustMarshal(&redispb.RedisResponse{
		Type:       redispb.BulkResp,
		BulkResult: value,
	})
	return 0, -int64(len(value)), resp
}

func (h *handler) rpush(shard metapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) < 1 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	value, err := h.getRedisList(shard.ID).RPush(req.Key, args.Args...)
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

func (h *handler) rpushx(shard metapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 1 {
		resp.Value = invalidCommandResp
		return 0, 0, resp
	}

	value, err := h.getRedisList(shard.ID).RPushX(req.Key, args.Args[0])
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

// ============================= read methods

func (h *handler) lindex(shard metapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 1 {
		resp.Value = invalidCommandResp
		return resp
	}

	index, err := format.ParseStrInt64(hack.SliceToString(args.Args[0]))
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	value, err := h.getRedisList(shard.ID).LIndex(req.Key, index)
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

func (h *handler) llen(shard metapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) *raftcmdpb.Response {
	resp := pb.AcquireResponse()

	value, err := h.getRedisList(shard.ID).LLen(req.Key)
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

func (h *handler) lrange(shard metapb.Shard, req *raftcmdpb.Request, attrs map[string]interface{}) *raftcmdpb.Response {
	resp := pb.AcquireResponse()
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	if len(args.Args) != 2 {
		resp.Value = invalidCommandResp
		return resp
	}

	start, err := format.ParseStrInt64(hack.SliceToString(args.Args[0]))
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	end, err := format.ParseStrInt64(hack.SliceToString(args.Args[1]))
	if err != nil {
		resp.Value = errorResp(err)
		return resp
	}

	value, err := h.getRedisList(shard.ID).LRange(req.Key, start, end)
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
