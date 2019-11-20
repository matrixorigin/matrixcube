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
	resp := redispb.RedisResponse{}
	protoc.MustUnmarshal(&resp, value.Value)

	switch resp.Type {
	case redispb.StatusResp:
		redisProtoc.WriteStatus(resp.StatusResult, out)
	case redispb.BulkResp:
		redisProtoc.WriteBulk(resp.BulkResult, out)
	case redispb.KVPairArrayResp:
		redisProtoc.WriteFVPairArray(resp.KVPairArrayResult, out)
	case redispb.IntegerResp:
		redisProtoc.WriteInteger(resp.IntegerResult, out)
	case redispb.ScorePairArrayResp:
		redisProtoc.WriteScorePairArray(resp.ScorePairArrayResult, resp.Withscores, out)
	case redispb.SliceArrayResp:
		redisProtoc.WriteSliceArray(resp.SliceArrayResult, out)
	case redispb.ErrorResp:
		redisProtoc.WriteError(hack.StringToSlice(value.Error.Message), out)
	case redispb.ErrorsResp:
		for _, err := range resp.ErrorResults {
			redisProtoc.WriteError(err, out)
		}
	}

	return nil
}
