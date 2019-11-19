package nemo

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var (
	tmpRedisSetPath = "/tmp/nemo-redis-set"
)

func TestSAdd(t *testing.T) {
	os.RemoveAll(tmpRedisSetPath)
	s, err := NewStorage(tmpRedisSetPath)
	assert.NoError(t, err, "TestSAdd failed")
	defer s.Close()

	key := []byte("TestSAdd")
	m1 := []byte("m1")
	m2 := []byte("m2")

	n, err := s.RedisSet().SAdd(key, m1, m2)
	assert.NoError(t, err, "TestSRem failed")
	assert.Equal(t, int64(2), n, "TestSRem failed")
}

func TestSRem(t *testing.T) {
	os.RemoveAll(tmpRedisSetPath)
	s, err := NewStorage(tmpRedisSetPath)
	assert.NoError(t, err, "TestSRem failed")
	defer s.Close()

	key := []byte("TestSRem")
	m1 := []byte("m1")
	m2 := []byte("m2")

	s.RedisSet().SAdd(key, m1, m2)

	n, err := s.RedisSet().SRem(key, m1, m2)
	assert.NoError(t, err, "TestSRem failed")
	assert.Equal(t, int64(2), n, "TestSRem failed")
}

func TestSCard(t *testing.T) {
	os.RemoveAll(tmpRedisSetPath)
	s, err := NewStorage(tmpRedisSetPath)
	assert.NoError(t, err, "TestSCard failed")
	defer s.Close()

	key := []byte("TestSCard")
	m1 := []byte("m1")
	m2 := []byte("m2")

	n, err := s.RedisSet().SCard(key)
	assert.NoError(t, err, "TestSCard failed")
	assert.Equal(t, int64(0), n, "TestSCard failed")

	s.RedisSet().SAdd(key, m1, m2)

	n, err = s.RedisSet().SCard(key)
	assert.NoError(t, err, "TestSCard failed")
	assert.Equal(t, int64(2), n, "TestSCard failed")
}

func TestSMembers(t *testing.T) {
	os.RemoveAll(tmpRedisSetPath)
	s, err := NewStorage(tmpRedisSetPath)
	assert.NoError(t, err, "TestSMembers failed")
	defer s.Close()

	key := []byte("TestSMembers")
	m1 := []byte("m1")
	m2 := []byte("m2")

	values, err := s.RedisSet().SMembers(key)
	assert.NoError(t, err, "TestSMembers failed")
	assert.Equal(t, 0, len(values), "TestSMembers failed")

	s.RedisSet().SAdd(key, m1, m2)

	values, err = s.RedisSet().SMembers(key)
	assert.NoError(t, err, "TestSMembers failed")
	assert.Equal(t, 2, len(values), "TestSMembers failed")
}

func TestSIsMember(t *testing.T) {
	os.RemoveAll(tmpRedisSetPath)
	s, err := NewStorage(tmpRedisSetPath)
	assert.NoError(t, err, "TestSIsMember failed")
	defer s.Close()

	key := []byte("TestSIsMember")
	m1 := []byte("m1")
	m2 := []byte("m2")

	n, err := s.RedisSet().SIsMember(key, m1)
	assert.NoError(t, err, "TestSIsMember failed")
	assert.Equal(t, int64(0), n, "TestSIsMember failed")

	s.RedisSet().SAdd(key, m1, m2)

	n, err = s.RedisSet().SIsMember(key, m1)
	assert.NoError(t, err, "TestSIsMember failed")
	assert.Equal(t, int64(1), n, "TestSIsMember failed")

	n, err = s.RedisSet().SIsMember(key, m2)
	assert.NoError(t, err, "TestSIsMember failed")
	assert.Equal(t, int64(1), n, "TestSIsMember failed")
}

func TestSPop(t *testing.T) {
	os.RemoveAll(tmpRedisSetPath)
	s, err := NewStorage(tmpRedisSetPath)
	assert.NoError(t, err, "TestSPop failed")
	defer s.Close()

	key := []byte("TestSPop")
	m1 := []byte("m1")
	m2 := []byte("m2")

	v, err := s.RedisSet().SPop(key)
	assert.NoError(t, err, "TestSPop failed")
	assert.Empty(t, v, "TestSPop failed")

	s.RedisSet().SAdd(key, m1, m2)

	v, err = s.RedisSet().SPop(key)
	assert.NoError(t, err, "TestSPop failed")
	assert.True(t, len(v) > 0, "TestSPop failed")

	v, err = s.RedisSet().SPop(key)
	assert.NoError(t, err, "TestSPop failed")
	assert.True(t, len(v) > 0, "TestSPop failed")

	v, err = s.RedisSet().SPop(key)
	assert.NoError(t, err, "TestSPop failed")
	assert.Empty(t, v, "TestSPop failed")
}
