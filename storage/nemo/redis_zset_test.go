package nemo

import (
	"os"
	"testing"

	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
	"github.com/stretchr/testify/assert"
)

var (
	tmpRedisZSetPath = "/tmp/nemo-redis-zset"
)

func TestZAdd(t *testing.T) {
	os.RemoveAll(tmpRedisZSetPath)
	s, err := NewStorage(tmpRedisZSetPath)
	assert.NoError(t, err, "TestZAdd failed")
	defer s.Close()

	key := []byte("TestZAdd")
	score := 10.0
	m1 := []byte("m1")

	n, err := s.RedisZSet().ZAdd(key, score, m1)
	assert.NoError(t, err, "")
	assert.Equal(t, int64(1), n, "TestZAdd failed")
}

func TestZCard(t *testing.T) {
	os.RemoveAll(tmpRedisZSetPath)
	s, err := NewStorage(tmpRedisZSetPath)
	assert.NoError(t, err, "TestZCard failed")
	defer s.Close()

	key := []byte("TestZCard")
	score := 10.0
	m1 := []byte("m1")

	n, err := s.RedisZSet().ZCard(key)
	assert.NoError(t, err, "TestZCard failed")
	assert.Equal(t, int64(0), n, "TestZAdd failed")

	n, err = s.RedisZSet().ZAdd(key, score, m1)
	assert.NoError(t, err, "TestZCard failed")
	assert.Equal(t, int64(1), n, "TestZAdd failed")

	n, err = s.RedisZSet().ZCard(key)
	assert.NoError(t, err, "TestZCard failed")
	assert.Equal(t, int64(1), n, "TestZAdd failed")
}

func TestZCount(t *testing.T) {
	os.RemoveAll(tmpRedisZSetPath)
	s, err := NewStorage(tmpRedisZSetPath)
	assert.NoError(t, err, "TestZCount failed")
	defer s.Close()

	key := []byte("TestZCount")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	min := []byte("1")
	max := []byte("3")
	n, err := s.RedisZSet().ZCount(key, min, max)
	assert.NoError(t, err, "TestZCount failed")
	assert.Equal(t, int64(0), n, "TestZCount failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)

	min = []byte("1")
	max = []byte("3")
	n, err = s.RedisZSet().ZCount(key, min, max)
	assert.NoError(t, err, "TestZCount failed")
	assert.Equal(t, int64(3), n, "TestZCount failed")

	min = []byte("(1")
	max = []byte("3")
	n, err = s.RedisZSet().ZCount(key, min, max)
	assert.NoError(t, err, "TestZCount failed")
	assert.Equal(t, int64(2), n, "TestZCount failed")

	min = []byte("(1")
	max = []byte("(3")
	n, err = s.RedisZSet().ZCount(key, min, max)
	assert.NoError(t, err, "TestZCount failed")
	assert.Equal(t, int64(1), n, "TestZCount failed")
}

func TestZIncrBy(t *testing.T) {
	os.RemoveAll(tmpRedisZSetPath)
	s, err := NewStorage(tmpRedisZSetPath)
	assert.NoError(t, err, "TestZIncrBy failed")
	defer s.Close()

	key := []byte("TestZIncrBy")
	score := 1.0
	m1 := []byte("m1")

	v, err := s.RedisZSet().ZIncrBy(key, m1, 1.0)
	assert.NoError(t, err, "TestZIncrBy failed")
	vf, _ := format.ParseStrFloat64(hack.SliceToString(v))
	assert.Equal(t, 1, int(vf), "TestZIncrBy failed")

	s.RedisZSet().ZAdd(key, score, m1)
	v, err = s.RedisZSet().ZIncrBy(key, m1, 1.0)
	assert.NoError(t, err, "TestZIncrBy failed")
	vf, _ = format.ParseStrFloat64(hack.SliceToString(v))
	assert.Equal(t, 2, int(vf), "TestZIncrBy failed")
}

func TestZLexCount(t *testing.T) {
	os.RemoveAll(tmpRedisZSetPath)
	s, err := NewStorage(tmpRedisZSetPath)
	assert.NoError(t, err, "TestZLexCount failed")
	defer s.Close()

	key := []byte("TestZLexCount")
	score := 1.0
	m1 := []byte("a")
	m2 := []byte("b")
	m3 := []byte("c")

	min := []byte("[a")
	max := []byte("[c")
	n, err := s.RedisZSet().ZLexCount(key, min, max)
	assert.NoError(t, err, "TestZLexCount failed")
	assert.Equal(t, int64(0), n, "TestZLexCount failed")

	s.RedisZSet().ZAdd(key, score, m1)
	s.RedisZSet().ZAdd(key, score, m2)
	s.RedisZSet().ZAdd(key, score, m3)

	min = []byte("[a")
	max = []byte("[c")
	n, err = s.RedisZSet().ZLexCount(key, min, max)
	assert.NoError(t, err, "TestZLexCount failed")
	assert.Equal(t, int64(3), n, "TestZLexCount failed")

	min = []byte("(a")
	max = []byte("(c")
	n, err = s.RedisZSet().ZLexCount(key, min, max)
	assert.NoError(t, err, "TestZLexCount failed")
	assert.Equal(t, int64(1), n, "TestZLexCount failed")

	min = []byte("(a")
	max = []byte("[c")
	n, err = s.RedisZSet().ZLexCount(key, min, max)
	assert.NoError(t, err, "TestZLexCount failed")
	assert.Equal(t, int64(2), n, "TestZLexCount failed")

	min = []byte("[a")
	max = []byte("(c")
	n, err = s.RedisZSet().ZLexCount(key, min, max)
	assert.NoError(t, err, "TestZLexCount failed")
	assert.Equal(t, int64(2), n, "TestZLexCount failed")
}

func TestZRange(t *testing.T) {
	os.RemoveAll(tmpRedisZSetPath)
	s, err := NewStorage(tmpRedisZSetPath)
	assert.NoError(t, err, "TestZRange failed")
	defer s.Close()

	key := []byte("TestZRange")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	values, err := s.RedisZSet().ZRange(key, 0, -1)
	assert.NoError(t, err, "TestZRange failed")
	assert.Equal(t, 0, len(values), "TestZRange failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)

	values, err = s.RedisZSet().ZRange(key, 0, -1)
	assert.NoError(t, err, "TestZRange failed")
	assert.Equal(t, 6, len(values), "TestZRange failed")

	values, err = s.RedisZSet().ZRange(key, 1, -1)
	assert.NoError(t, err, "TestZRange failed")
	assert.Equal(t, 4, len(values), "TestZRange failed")

	values, err = s.RedisZSet().ZRange(key, 2, -1)
	assert.NoError(t, err, "TestZRange failed")
	assert.Equal(t, 2, len(values), "TestZRange failed")

	values, err = s.RedisZSet().ZRange(key, 3, -1)
	assert.NoError(t, err, "TestZRange failed")
	assert.Equal(t, 0, len(values), "TestZRange failed")
}

func TestZRangeByLex(t *testing.T) {
	os.RemoveAll(tmpRedisZSetPath)
	s, err := NewStorage(tmpRedisZSetPath)
	assert.NoError(t, err, "TestZRangeByLex failed")
	defer s.Close()

	key := []byte("TestZRangeByLex")
	score := 0.0

	m1 := []byte("a")
	m2 := []byte("b")
	m3 := []byte("c")

	min := []byte("[a")
	max := []byte("[c")
	values, err := s.RedisZSet().ZRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRangeByLex failed")
	assert.Equal(t, 0, len(values), "TestZRangeByLex failed")

	s.RedisZSet().ZAdd(key, score, m1)
	s.RedisZSet().ZAdd(key, score, m2)
	s.RedisZSet().ZAdd(key, score, m3)

	min = []byte("[a")
	max = []byte("[c")
	values, err = s.RedisZSet().ZRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRangeByLex failed")
	assert.Equal(t, 3, len(values), "TestZRangeByLex failed")

	min = []byte("[a")
	max = []byte("(c")
	values, err = s.RedisZSet().ZRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRangeByLex failed")
	assert.Equal(t, 2, len(values), "TestZRangeByLex failed")

	min = []byte("(a")
	max = []byte("(c")
	values, err = s.RedisZSet().ZRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRangeByLex failed")
	assert.Equal(t, 1, len(values), "TestZRangeByLex failed")

	min = []byte("-")
	max = []byte("(c")
	values, err = s.RedisZSet().ZRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRangeByLex failed")
	assert.Equal(t, 2, len(values), "TestZRangeByLex failed")

	min = []byte("-")
	max = []byte("[c")
	values, err = s.RedisZSet().ZRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRangeByLex failed")
	assert.Equal(t, 3, len(values), "TestZRangeByLex failed")

	min = []byte("(a")
	max = []byte("+")
	values, err = s.RedisZSet().ZRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRangeByLex failed")
	assert.Equal(t, 2, len(values), "TestZRangeByLex failed")

	min = []byte("[a")
	max = []byte("+")
	values, err = s.RedisZSet().ZRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRangeByLex failed")
	assert.Equal(t, 3, len(values), "TestZRangeByLex failed")
}

func TestZRangeByScore(t *testing.T) {
	os.RemoveAll(tmpRedisZSetPath)
	s, err := NewStorage(tmpRedisZSetPath)
	assert.NoError(t, err, "TestZRangeByScore failed")
	defer s.Close()

	key := []byte("TestZRangeByScore")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	min := []byte("1")
	max := []byte("3")
	values, err := s.RedisZSet().ZRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRangeByScore failed")
	assert.Equal(t, 0, len(values), "TestZRangeByScore failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)

	min = []byte("1")
	max = []byte("3")
	values, err = s.RedisZSet().ZRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRangeByScore failed")
	assert.Equal(t, 6, len(values), "TestZRangeByScore failed")

	min = []byte("(1")
	max = []byte("3")
	values, err = s.RedisZSet().ZRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRangeByScore failed")
	assert.Equal(t, 4, len(values), "TestZRangeByScore failed")

	min = []byte("(1")
	max = []byte("(3")
	values, err = s.RedisZSet().ZRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRangeByScore failed")
	assert.Equal(t, 2, len(values), "TestZRangeByScore failed")

	min = []byte("-inf")
	max = []byte("+inf")
	values, err = s.RedisZSet().ZRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRangeByScore failed")
	assert.Equal(t, 6, len(values), "TestZRangeByScore failed")

	min = []byte("-inf")
	max = []byte("3")
	values, err = s.RedisZSet().ZRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRangeByScore failed")
	assert.Equal(t, 6, len(values), "TestZRangeByScore failed")

	min = []byte("1")
	max = []byte("+inf")
	values, err = s.RedisZSet().ZRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRangeByScore failed")
	assert.Equal(t, 6, len(values), "TestZRangeByScore failed")
}

func TestZRank(t *testing.T) {
	os.RemoveAll(tmpRedisZSetPath)
	s, err := NewStorage(tmpRedisZSetPath)
	assert.NoError(t, err, "TestZRank failed")
	defer s.Close()

	key := []byte("TestZRank")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	m4 := []byte("m4")

	index, err := s.RedisZSet().ZRank(key, m1)
	assert.NoError(t, err, "TestZRank failed")
	assert.True(t, index < 0, "TestZRank failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)

	index, err = s.RedisZSet().ZRank(key, m1)
	assert.NoError(t, err, "TestZRank failed")
	assert.Equal(t, int64(0), index, "TestZRank failed")

	index, err = s.RedisZSet().ZRank(key, m2)
	assert.NoError(t, err, "TestZRank failed")
	assert.Equal(t, int64(1), index, "TestZRank failed")

	index, err = s.RedisZSet().ZRank(key, m3)
	assert.NoError(t, err, "TestZRank failed")
	assert.Equal(t, int64(2), index, "TestZRank failed")

	index, err = s.RedisZSet().ZRank(key, m4)
	assert.NoError(t, err, "TestZRank failed")
	assert.True(t, index < 0, "TestZRank failed")
}

func TestZRem(t *testing.T) {
	os.RemoveAll(tmpRedisZSetPath)
	s, err := NewStorage(tmpRedisZSetPath)
	assert.NoError(t, err, "TestZRem failed")
	defer s.Close()

	key := []byte("TestZRem")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	count, err := s.RedisZSet().ZRem(key, m1)
	assert.NoError(t, err, "TestZRem failed")
	assert.Equal(t, int64(0), count, "TestZRem failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)

	count, err = s.RedisZSet().ZRem(key, m1)
	assert.NoError(t, err, "TestZRem failed")
	assert.Equal(t, int64(1), count, "TestZRem failed")

	count, err = s.RedisZSet().ZRem(key, m2, m3)
	assert.NoError(t, err, "TestZRem failed")
	assert.Equal(t, int64(2), count, "TestZRem failed")

	count, err = s.RedisZSet().ZRem(key, m1, m2, m3)
	assert.NoError(t, err, "TestZRem failed")
	assert.Equal(t, int64(0), count, "TestZRem failed")
}

func TestZRemRangeByLex(t *testing.T) {
	os.RemoveAll(tmpRedisZSetPath)
	s, err := NewStorage(tmpRedisZSetPath)
	assert.NoError(t, err, "TestZRemRangeByLex failed")
	defer s.Close()

	key := []byte("TestZRemRangeByLex")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	min := []byte("m1")
	max := []byte("m3")
	count, err := s.RedisZSet().ZRemRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByLex failed")
	assert.Equal(t, int64(0), count, "TestZRemRangeByLex failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)
	min = []byte("[m1")
	max = []byte("[m3")
	count, err = s.RedisZSet().ZRemRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByLex failed")
	assert.Equal(t, int64(3), count, "TestZRemRangeByLex failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)
	min = []byte("(m1")
	max = []byte("[m3")
	count, err = s.RedisZSet().ZRemRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByLex failed")
	assert.Equal(t, int64(2), count, "TestZRemRangeByLex failed")

	min = []byte("[m2")
	max = []byte("(m3")
	count, err = s.RedisZSet().ZRemRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByLex failed")
	assert.Equal(t, int64(0), count, "TestZRemRangeByLex failed")

	min = []byte("[m1")
	max = []byte("(m3")
	count, err = s.RedisZSet().ZRemRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByLex failed")
	assert.Equal(t, int64(1), count, "TestZRemRangeByLex failed")

	min = []byte("(m3")
	max = []byte("+")
	count, err = s.RedisZSet().ZRemRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByLex failed")
	assert.Equal(t, int64(0), count, "TestZRemRangeByLex failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)
	min = []byte("-")
	max = []byte("(m1")
	count, err = s.RedisZSet().ZRemRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByLex failed")
	assert.Equal(t, int64(0), count, "TestZRemRangeByLex failed")

	min = []byte("-")
	max = []byte("[m3")
	count, err = s.RedisZSet().ZRemRangeByLex(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByLex failed")
	assert.Equal(t, int64(3), count, "TestZRemRangeByLex failed")
}

func TestZRemRangeByRank(t *testing.T) {
	os.RemoveAll(tmpRedisZSetPath)
	s, err := NewStorage(tmpRedisZSetPath)
	assert.NoError(t, err, "TestZRemRangeByRank failed")
	defer s.Close()

	key := []byte("TestZRemRangeByRank")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	count, err := s.RedisZSet().ZRemRangeByRank(key, 0, -1)
	assert.NoError(t, err, "TestZRemRangeByRank failed")
	assert.Equal(t, int64(0), count, "TestZRemRangeByRank failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)

	count, err = s.RedisZSet().ZRemRangeByRank(key, 0, -1)
	assert.NoError(t, err, "TestZRemRangeByRank failed")
	assert.Equal(t, int64(3), count, "TestZRemRangeByRank failed")

	count, err = s.RedisZSet().ZRemRangeByRank(key, 0, -1)
	assert.NoError(t, err, "TestZRemRangeByRank failed")
	assert.Equal(t, int64(0), count, "TestZRemRangeByRank failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)

	count, err = s.RedisZSet().ZRemRangeByRank(key, 0, 1)
	assert.NoError(t, err, "TestZRemRangeByRank failed")
	assert.Equal(t, int64(2), count, "TestZRemRangeByRank failed")

	count, err = s.RedisZSet().ZRemRangeByRank(key, 0, 0)
	assert.NoError(t, err, "TestZRemRangeByRank failed")
	assert.Equal(t, int64(1), count, "TestZRemRangeByRank failed")
}

func TestZRemRangeByScore(t *testing.T) {
	os.RemoveAll(tmpRedisZSetPath)
	s, err := NewStorage(tmpRedisZSetPath)
	assert.NoError(t, err, "TestZRemRangeByScore failed")
	defer s.Close()

	key := []byte("TestZRemRangeByScore")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	min := []byte("[1")
	max := []byte("[3")
	count, err := s.RedisZSet().ZRemRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByScore failed")
	assert.Equal(t, int64(0), count, "TestZRemRangeByScore failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)

	min = []byte("[1")
	max = []byte("[3")
	count, err = s.RedisZSet().ZRemRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByScore failed")
	assert.Equal(t, int64(3), count, "TestZRemRangeByScore failed")

	count, err = s.RedisZSet().ZRemRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByScore failed")
	assert.Equal(t, int64(0), count, "TestZRemRangeByScore failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)
	min = []byte("(1")
	max = []byte("[3")
	count, err = s.RedisZSet().ZRemRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByScore failed")
	assert.Equal(t, int64(2), count, "TestZRemRangeByScore failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)
	min = []byte("(1")
	max = []byte("(3")
	count, err = s.RedisZSet().ZRemRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByScore failed")
	assert.Equal(t, int64(1), count, "TestZRemRangeByScore failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)
	min = []byte("[1")
	max = []byte("(3")
	count, err = s.RedisZSet().ZRemRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByScore failed")
	assert.Equal(t, int64(2), count, "TestZRemRangeByScore failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)
	min = []byte("-inf")
	max = []byte("[3")
	count, err = s.RedisZSet().ZRemRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByScore failed")
	assert.Equal(t, int64(3), count, "TestZRemRangeByScore failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)
	min = []byte("-inf")
	max = []byte("(3")
	count, err = s.RedisZSet().ZRemRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByScore failed")
	assert.Equal(t, int64(2), count, "TestZRemRangeByScore failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)
	min = []byte("[1")
	max = []byte("+inf")
	count, err = s.RedisZSet().ZRemRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByScore failed")
	assert.Equal(t, int64(3), count, "TestZRemRangeByScore failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)
	min = []byte("(1")
	max = []byte("+inf")
	count, err = s.RedisZSet().ZRemRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByScore failed")
	assert.Equal(t, int64(2), count, "TestZRemRangeByScore failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)
	min = []byte("-inf")
	max = []byte("+inf")
	count, err = s.RedisZSet().ZRemRangeByScore(key, min, max)
	assert.NoError(t, err, "TestZRemRangeByScore failed")
	assert.Equal(t, int64(3), count, "TestZRemRangeByScore failed")
}

func TestZScore(t *testing.T) {
	os.RemoveAll(tmpRedisZSetPath)
	s, err := NewStorage(tmpRedisZSetPath)
	assert.NoError(t, err, "TestZScore failed")
	defer s.Close()

	key := []byte("TestZScore")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	tmp, err := s.RedisZSet().ZScore(key, m1)
	assert.NoError(t, err, "TestZScore failed")
	assert.Empty(t, tmp, "TestZScore failed")

	s.RedisZSet().ZAdd(key, score1, m1)
	s.RedisZSet().ZAdd(key, score2, m2)
	s.RedisZSet().ZAdd(key, score3, m3)

	tmp, err = s.RedisZSet().ZScore(key, m1)
	score, _ := format.ParseStrFloat64(hack.SliceToString(tmp))
	assert.NoError(t, err, "TestZScore failed")
	assert.Equal(t, 1, int(score), "TestZScore failed")

	tmp, err = s.RedisZSet().ZScore(key, m2)
	score, _ = format.ParseStrFloat64(hack.SliceToString(tmp))
	assert.NoError(t, err, "TestZScore failed")
	assert.Equal(t, 2, int(score), "TestZScore failed")

	tmp, err = s.RedisZSet().ZScore(key, m3)
	score, _ = format.ParseStrFloat64(hack.SliceToString(tmp))
	assert.NoError(t, err, "TestZScore failed")
	assert.Equal(t, 3, int(score), "TestZScore failed")
}
