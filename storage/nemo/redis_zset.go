package nemo

import (
	"bytes"
	"math"

	gonemo "github.com/deepfabric/go-nemo"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
)

type redisZSet struct {
	db *gonemo.NEMO
}

func (s *redisZSet) ZAdd(key []byte, score float64, member []byte) (int64, error) {
	return s.db.ZAdd(key, score, member)
}

func (s *redisZSet) ZCard(key []byte) (int64, error) {
	return s.db.ZCard(key)
}

func (s *redisZSet) ZCount(key []byte, min []byte, max []byte) (int64, error) {
	minF, includeMin, err := parseInclude(min)
	if err != nil {
		return 0, err
	}

	maxF, includeMax, err := parseInclude(max)
	if err != nil {
		return 0, err
	}

	return s.db.ZCount(key, minF, maxF, includeMin, includeMax)
}

func (s *redisZSet) ZIncrBy(key []byte, member []byte, by float64) ([]byte, error) {

	value, err := s.db.ZIncrby(key, member, by)

	return value, err

}

func (s *redisZSet) ZLexCount(key []byte, min []byte, max []byte) (int64, error) {
	min, includeMin := isInclude(min)
	max, includeMax := isInclude(max)

	return s.db.ZLexcount(key, min, max, includeMin, includeMax)
}

func (s *redisZSet) ZRange(key []byte, start int64, stop int64) ([][]byte, error) {
	scores, values, err := s.db.ZRange(key, start, stop)
	if err != nil {
		return nil, err
	}

	result := make([][]byte, 0, len(values)*2)
	for idx, value := range values {
		result = append(result, value, format.Float64ToString(scores[idx]))
	}
	return result, nil
}

func (s *redisZSet) ZRangeByLex(key []byte, min []byte, max []byte) ([][]byte, error) {
	min, includeMin := isInclude(min)
	max, includeMax := isInclude(max)

	if len(max) == 1 && max[0] == '+' {
		max[0] = byte('z' + 1)
	}

	return s.db.ZRangebylex(key, min, max, includeMin, includeMax)
}

func (s *redisZSet) ZRangeByScore(key []byte, min []byte, max []byte) ([][]byte, error) {
	minF, includeMin, err := parseInclude(min)
	if err != nil {
		return nil, err
	}

	maxF, includeMax, err := parseInclude(max)
	if err != nil {
		return nil, err
	}

	scores, values, err := s.db.ZRangebyScore(key, minF, maxF, includeMin, includeMax)
	if err != nil {
		return nil, err
	}

	result := make([][]byte, 0, len(values)*2)
	for idx, value := range values {
		result = append(result, value, format.Float64ToString(scores[idx]))
	}
	return result, nil
}

func (s *redisZSet) ZRank(key []byte, member []byte) (int64, error) {
	return s.db.ZRank(key, member)
}

func (s *redisZSet) ZRem(key []byte, members ...[]byte) (int64, error) {
	return s.db.ZRem(key, members...)
}

func (s *redisZSet) ZRemRangeByLex(key []byte, min []byte, max []byte) (int64, error) {
	min, includeMin := isInclude(min)
	max, includeMax := isInclude(max)

	return s.db.ZRemrangebylex(key, min, max, includeMin, includeMax)
}

func (s *redisZSet) ZRemRangeByRank(key []byte, start int64, stop int64) (int64, error) {
	return s.db.ZRemrangebyrank(key, start, stop)
}

func (s *redisZSet) ZRemRangeByScore(key []byte, min []byte, max []byte) (int64, error) {
	minF, includeMin, err := parseInclude(min)
	if err != nil {
		return 0, err
	}

	maxF, includeMax, err := parseInclude(max)
	if err != nil {
		return 0, err
	}

	return s.db.ZRemrangebyscore(key, minF, maxF, includeMin, includeMax)
}

func (s *redisZSet) ZScore(key []byte, member []byte) ([]byte, error) {
	exists, value, err := s.db.ZScore(key, member)
	if !exists {
		return nil, err
	}

	return format.Float64ToString(value), err
}

func isInclude(value []byte) ([]byte, bool) {
	include := value[0] != '('
	if value[0] == '(' || value[0] == '[' {
		value = value[1:]
	}

	return value, !include
}

var (
	max = []byte("+inf")
	min = []byte("-inf")
)

func parseInclude(value []byte) (float64, bool, error) {
	value, include := isInclude(value)
	if bytes.Compare(value, max) == 0 {
		return math.MaxFloat64, include, nil
	} else if bytes.Compare(value, min) == 0 {
		return math.SmallestNonzeroFloat64, include, nil
	}

	valueF, err := format.ParseStrFloat64(hack.SliceToString(value))
	return valueF, include, err
}
