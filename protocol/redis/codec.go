package redis

import (
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/pb/redispb"
	"github.com/fagongzi/goetty"
	redisProtoc "github.com/fagongzi/goetty/protocol/redis"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/protoc"
)

var (
	decoder = redisProtoc.NewRedisDecoder()
	encoder = &codec{}
)

type codec struct {
}

func (s *codec) Encode(data interface{}, out *goetty.ByteBuf) error {
	value := data.(*raftcmdpb.Response)
	if value.Error.Message != "" {
		redisProtoc.WriteError(hack.StringToSlice(value.Error.Message), out)
		return nil
	}

	resp := redispb.RedisResponse{}
	protoc.MustUnmarshal(&resp, value.Value)

	if len(resp.BulkResult) > 0 || resp.HasEmptyBulkResult {
		redisProtoc.WriteBulk(resp.BulkResult, out)
		return nil
	}

	if len(resp.FvPairArrayResult) > 0 || resp.HasEmptyFVPairArrayResult {
		redisProtoc.WriteFVPairArray(resp.FvPairArrayResult, out)
		return nil
	}

	if resp.IntegerResult > 0 {
		redisProtoc.WriteInteger(resp.IntegerResult, out)
		return nil
	}

	if len(resp.ScorePairArrayResult) > 0 || resp.HasEmptyScorePairArrayResult {
		redisProtoc.WriteScorePairArray(resp.ScorePairArrayResult, resp.Withscores, out)
		return nil
	}

	if len(resp.SliceArrayResult) > 0 || resp.HasEmptySliceArrayResult {
		redisProtoc.WriteSliceArray(resp.SliceArrayResult, out)
		return nil
	}

	if len(resp.StatusResult) > 0 {
		redisProtoc.WriteStatus(resp.StatusResult, out)
		return nil
	}

	return nil
}
