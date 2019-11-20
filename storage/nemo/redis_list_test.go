package nemo

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var (
	tmpRedisListPath = "/tmp/nemo-redis-list"
)

func TestLIndex(t *testing.T) {
	os.RemoveAll(tmpRedisListPath)
	s, err := NewStorage(tmpRedisListPath)
	assert.NoError(t, err, "TestLIndex failed")
	defer s.Close()

	key := []byte("TestLIndex")
	member := []byte("member1")

	v, err := s.RedisList().LIndex(key, 0)
	assert.NoError(t, err, "TestLIndex failed")
	assert.Empty(t, v, "TestLIndex failed")

	s.RedisList().LPush(key, member)

	v, err = s.RedisList().LIndex(key, 0)
	assert.NoError(t, err, "TestLIndex failed")
	assert.Equal(t, string(member), string(v), "TestLIndex failed")

	v, err = s.RedisList().LIndex(key, 1)
	assert.NoError(t, err, "TestLIndex failed")
	assert.Empty(t, v, "TestLIndex failed")
}

func TestLInsert(t *testing.T) {
	os.RemoveAll(tmpRedisListPath)
	s, err := NewStorage(tmpRedisListPath)
	assert.NoError(t, err, "TestLInsert failed")
	defer s.Close()

	before := 0
	after := 1

	key := []byte("TestLInsert")
	member := []byte("member1")
	pivot := []byte("member3")
	insert := []byte("member2")
	insert2 := []byte("member4")

	n, err := s.RedisList().LInsert(key, before, pivot, insert)
	assert.NoError(t, err, "TestLInsert failed")
	assert.Equal(t, int64(0), n, "TestLInsert failed")

	s.RedisList().LInsert(key, after, pivot, insert)
	assert.NoError(t, err, "")
	assert.Equal(t, int64(0), n, "TestLInsert failed")

	s.RedisList().LPush(key, pivot)
	s.RedisList().LPush(key, member)

	n, err = s.RedisList().LInsert(key, before, pivot, insert)
	assert.NoError(t, err, "TestLInsert failed")
	assert.Equal(t, int64(3), n, "TestLInsert failed")

	n, err = s.RedisList().LInsert(key, after, pivot, insert2)
	assert.NoError(t, err, "")
	assert.Equal(t, int64(4), n, "TestLInsert failed")

	v, _ := s.RedisList().LIndex(key, 1)
	assert.Equal(t, string(insert), string(v), "TestLInsert failed")

	v, _ = s.RedisList().LIndex(key, -1)
	assert.Equal(t, string(insert2), string(v), "TestLInsert failed")
}

func TestLLen(t *testing.T) {
	os.RemoveAll(tmpRedisListPath)
	s, err := NewStorage(tmpRedisListPath)
	assert.NoError(t, err, "TestLLen failed")
	defer s.Close()

	key := []byte("TestLLen")
	member1 := []byte("member1")
	member2 := []byte("member2")

	n, err := s.RedisList().LLen(key)
	assert.NoError(t, err, "TestLLen failed")
	assert.Equal(t, int64(0), n, "TestLLen failed")

	s.RedisList().LPush(key, member2)
	s.RedisList().LPush(key, member1)

	n, err = s.RedisList().LLen(key)
	assert.NoError(t, err, "TestLLen failed")
	assert.Equal(t, int64(2), n, "TestLLen failed")
}

func TestLPop(t *testing.T) {
	os.RemoveAll(tmpRedisListPath)
	s, err := NewStorage(tmpRedisListPath)
	assert.NoError(t, err, "TestLPop failed")
	defer s.Close()

	key := []byte("TestLPop")
	member1 := []byte("member1")
	member2 := []byte("member2")

	v, err := s.RedisList().LPop(key)
	assert.NoError(t, err, "TestLPop failed")
	assert.Empty(t, v, "TestLPop failed")

	s.RedisList().LPush(key, member2)
	s.RedisList().LPush(key, member1)

	v, err = s.RedisList().LPop(key)
	assert.NoError(t, err, "TestLPop failed")
	assert.Equal(t, string(member1), string(v), "TestLPop failed")

	v, err = s.RedisList().LPop(key)
	assert.NoError(t, err, "TestLPop failed")
	assert.Equal(t, string(member2), string(v), "TestLPop failed")

	v, err = s.RedisList().LPop(key)
	assert.NoError(t, err, "TestLPop failed")
	assert.Empty(t, v, "TestLPop failed")
}

func TestLPush(t *testing.T) {
	os.RemoveAll(tmpRedisListPath)
	s, err := NewStorage(tmpRedisListPath)
	assert.NoError(t, err, "TestLPush failed")
	defer s.Close()

	key := []byte("TestLPush")
	member1 := []byte("member1")
	member2 := []byte("member2")

	n, err := s.RedisList().LPush(key, member2)
	assert.NoError(t, err, "TestLPush failed")
	assert.Equal(t, int64(1), n, "TestLPush failed")

	n, err = s.RedisList().LPush(key, member1)
	assert.NoError(t, err, "TestLPush failed")
	assert.Equal(t, int64(2), n, "TestLPush failed")

	v, err := s.RedisList().LPop(key)
	assert.NoError(t, err, "TestLPush failed")
	assert.Equal(t, string(member1), string(v), "TestLPush failed")

	v, err = s.RedisList().LPop(key)
	assert.NoError(t, err, "TestLPush failed")
	assert.Equal(t, string(member2), string(v), "TestLPush failed")

	v, err = s.RedisList().LPop(key)
	assert.NoError(t, err, "TestLPush failed")
	assert.Empty(t, v, "TestLPush failed")
}

func TestLPushX(t *testing.T) {
	os.RemoveAll(tmpRedisListPath)
	s, err := NewStorage(tmpRedisListPath)
	assert.NoError(t, err, "TestLPushX failed")
	defer s.Close()

	key := []byte("TestLPushX")
	member1 := []byte("member1")
	member2 := []byte("member2")
	member3 := []byte("member3")

	n, err := s.RedisList().LPushX(key, member2)
	assert.NoError(t, err, "TestLPushX failed")
	assert.Equal(t, int64(0), n, "TestLPushX failed")

	s.RedisList().LPush(key, member3)

	n, err = s.RedisList().LPushX(key, member2)
	assert.NoError(t, err, "TestLPushX failed")
	assert.Equal(t, int64(2), n, "TestLPushX failed")

	n, err = s.RedisList().LPushX(key, member1)
	assert.NoError(t, err, "TestLPushX failed")
	assert.Equal(t, int64(3), n, "TestLPushX failed")

	v, err := s.RedisList().LPop(key)
	assert.NoError(t, err, "TestLPushX failed")
	assert.Equal(t, string(member1), string(v), "TestLPushX failed")

	v, err = s.RedisList().LPop(key)
	assert.NoError(t, err, "TestLPushX failed")
	assert.Equal(t, string(member2), string(v), "TestLPushX failed")
}

func TestLRange(t *testing.T) {
	os.RemoveAll(tmpRedisListPath)
	s, err := NewStorage(tmpRedisListPath)
	assert.NoError(t, err, "TestLRange failed")
	defer s.Close()

	key := []byte("TestLRange")
	member1 := []byte("member1")
	member2 := []byte("member2")

	values, err := s.RedisList().LRange(key, 0, 1)
	assert.NoError(t, err, "TestLRange failed")
	assert.Empty(t, values, "TestLRange failed")

	s.RedisList().LPush(key, member2)
	s.RedisList().LPush(key, member1)

	values, err = s.RedisList().LRange(key, 0, 0)
	assert.NoError(t, err, "TestLRange failed")
	assert.Equal(t, 1, len(values), "TestLRange failed")

	values, err = s.RedisList().LRange(key, 0, 1)
	assert.NoError(t, err, "TestLRange failed")
	assert.Equal(t, 2, len(values), "TestLRange failed")
}

func TestLRem(t *testing.T) {
	os.RemoveAll(tmpRedisListPath)
	s, err := NewStorage(tmpRedisListPath)
	assert.NoError(t, err, "TestLRem failed")
	defer s.Close()

	key := []byte("TestLRem")
	member1 := []byte("member1")
	member2 := []byte("member2")
	member3 := []byte("member1")

	n, err := s.RedisList().LRem(key, 1, member2)
	assert.NoError(t, err, "TestLRem failed")
	assert.Equal(t, int64(0), n, "TestLRem failed")

	s.RedisList().LPush(key, member3)
	s.RedisList().LPush(key, member2)
	s.RedisList().LPush(key, member1)

	n, err = s.RedisList().LRem(key, 1, member2)
	assert.NoError(t, err, "TestLRem failed")
	assert.Equal(t, int64(1), n, "TestLRem failed")

	n, err = s.RedisList().LRem(key, 2, member1)
	assert.NoError(t, err, "TestLRem failed")
	assert.Equal(t, int64(2), n, "TestLRem failed")
}

func TestLSet(t *testing.T) {
	os.RemoveAll(tmpRedisListPath)
	s, err := NewStorage(tmpRedisListPath)
	assert.NoError(t, err, "TestLSet failed")
	defer s.Close()

	key := []byte("TestLSet")
	member1 := []byte("member1")
	member2 := []byte("member2")

	err = s.RedisList().LSet(key, 0, member2)
	assert.Error(t, err, "TestLSet failed")

	s.RedisList().LPush(key, member2)

	err = s.RedisList().LSet(key, 0, member1)
	assert.NoError(t, err, "TestLSet failed")
}

func TestLTrim(t *testing.T) {
	os.RemoveAll(tmpRedisListPath)
	s, err := NewStorage(tmpRedisListPath)
	assert.NoError(t, err, "TestLTrim failed")
	defer s.Close()

	key := []byte("TestLTrim")
	member1 := []byte("member1")
	member2 := []byte("member2")

	err = s.RedisList().LTrim(key, 1, -1)
	assert.NoError(t, err, "TestLTrim failed")

	s.RedisList().LPush(key, member2)
	s.RedisList().LPush(key, member1)

	err = s.RedisList().LTrim(key, 1, -1)
	assert.NoError(t, err, "TestLTrim failed")

	n, _ := s.RedisList().LLen(key)
	assert.Equal(t, int64(1), n, "TestLTrim failed")
}

func TestRPop(t *testing.T) {
	os.RemoveAll(tmpRedisListPath)
	s, err := NewStorage(tmpRedisListPath)
	assert.NoError(t, err, "TestRPop failed")
	defer s.Close()

	key := []byte("TestRPop")
	member1 := []byte("member1")
	member2 := []byte("member2")

	v, err := s.RedisList().RPop(key)
	assert.NoError(t, err, "TestRPop failed")
	assert.Empty(t, v, "TestRPop failed")

	s.RedisList().RPush(key, member2)
	s.RedisList().RPush(key, member1)

	v, err = s.RedisList().RPop(key)
	assert.NoError(t, err, "TestRPop failed")
	assert.Equal(t, string(member1), string(v), "TestRPop failed")

	v, err = s.RedisList().RPop(key)
	assert.NoError(t, err, "TestRPop failed")
	assert.Equal(t, string(member2), string(v), "TestRPop failed")

	v, err = s.RedisList().RPop(key)
	assert.NoError(t, err, "TestRPop failed")
	assert.Empty(t, v, "TestRPop failed")
}

func TestRPush(t *testing.T) {
	os.RemoveAll(tmpRedisListPath)
	s, err := NewStorage(tmpRedisListPath)
	assert.NoError(t, err, "TestRPush failed")
	defer s.Close()

	key := []byte("TestRPush")
	member1 := []byte("member1")
	member2 := []byte("member2")

	s.RedisList().RPush(key, member2)
	s.RedisList().RPush(key, member1)

	v, err := s.RedisList().RPop(key)
	assert.NoError(t, err, "TestRPush failed")
	assert.Equal(t, string(member1), string(v), "TestRPush failed")

	v, err = s.RedisList().RPop(key)
	assert.NoError(t, err, "TestRPush failed")
	assert.Equal(t, string(member2), string(v), "TestRPush failed")

	v, err = s.RedisList().RPop(key)
	assert.NoError(t, err, "TestRPush failed")
	assert.Empty(t, v, "TestRPush failed")
}

func TestRPushX(t *testing.T) {
	os.RemoveAll(tmpRedisListPath)
	s, err := NewStorage(tmpRedisListPath)
	assert.NoError(t, err, "TestRPushX failed")
	defer s.Close()

	key := []byte("TestRPush")
	member1 := []byte("member1")
	member2 := []byte("member2")

	n, err := s.RedisList().RPushX(key, member1)
	assert.NoError(t, err, "TestRPushX failed")
	assert.Equal(t, int64(0), n, "TestRPushX failed")

	s.RedisList().RPush(key, member2)

	n, err = s.RedisList().RPushX(key, member1)
	assert.NoError(t, err, "TestRPushX failed")
	assert.Equal(t, int64(2), n, "TestRPushX failed")
}
