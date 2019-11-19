package nemo

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var (
	tmpRedisKVPath = "/tmp/nemo-redis-kv"
)

func TestSet(t *testing.T) {
	os.RemoveAll(tmpRedisKVPath)
	s, err := NewStorage(tmpRedisKVPath)
	assert.NoError(t, err, "TestSet failed")
	defer s.Close()

	key := []byte("key1")
	value := []byte("value1")

	err = s.RedisKV().Set(key, value)
	assert.NoError(t, err, "TestSet failed")
}

func TestGet(t *testing.T) {
	os.RemoveAll(tmpRedisKVPath)
	s, err := NewStorage(tmpRedisKVPath)
	assert.NoError(t, err, "TestSet failed")
	defer s.Close()

	key := []byte("TestGet")
	value := []byte("value2")

	v, err := s.RedisKV().Get(key)
	assert.NoError(t, err, "TestGet failed")
	assert.Empty(t, v, "TestGet failed")

	err = s.RedisKV().Set(key, value)
	assert.NoError(t, err, "TestGet failed")

	v, err = s.RedisKV().Get(key)
	assert.NoError(t, err, "TestGet failed")
	assert.Equal(t, string(value), string(v), "TestGet failed")
}

func TestIncrBy(t *testing.T) {
	os.RemoveAll(tmpRedisKVPath)
	s, err := NewStorage(tmpRedisKVPath)
	assert.NoError(t, err, "TestIncrBy failed")
	defer s.Close()

	key := []byte("TestIncrBy")
	err = s.RedisKV().Set(key, []byte("1"))
	assert.NoError(t, err, "TestIncrBy failed")

	n, err := s.RedisKV().IncrBy(key, 1)
	assert.NoError(t, err, "TestIncrBy failed")
	assert.Equal(t, int64(2), n, "TestIncrBy failed")
}

func TestDecrBy(t *testing.T) {
	os.RemoveAll(tmpRedisKVPath)
	s, err := NewStorage(tmpRedisKVPath)
	assert.NoError(t, err, "TestDecrBy failed")
	defer s.Close()

	key := []byte("TestDecrBy")
	err = s.RedisKV().Set(key, []byte("2"))
	assert.NoError(t, err, "TestDecrBy failed")

	n, err := s.RedisKV().DecrBy(key, 1)
	assert.NoError(t, err, "TestDecrBy failed")
	assert.Equal(t, int64(1), n, "TestIncrBy failed")
}

func TestGetSet(t *testing.T) {
	os.RemoveAll(tmpRedisKVPath)
	s, err := NewStorage(tmpRedisKVPath)
	assert.NoError(t, err, "TestGetSet failed")
	defer s.Close()

	key := []byte("TestGetSet1")
	value := []byte("old-value")
	newValue := []byte("new-value")

	old, err := s.RedisKV().GetSet(key, value)
	assert.NoError(t, err, "TestGetSet failed")
	assert.Empty(t, old, "TestGetSet failed")

	v, err := s.RedisKV().Get(key)
	assert.NoError(t, err, "TestGetSet failed")
	assert.Equal(t, string(value), string(v), "TestGetSet failed")

	old, err = s.RedisKV().GetSet(key, newValue)
	assert.NoError(t, err, "TestGetSet failed")
	assert.Equal(t, string(value), string(old), "TestGetSet failed")

	v, err = s.RedisKV().Get(key)
	assert.NoError(t, err, "TestGetSet failed")
	assert.Equal(t, string(newValue), string(v), "TestGetSet failed")
}

func TestAppend(t *testing.T) {
	os.RemoveAll(tmpRedisKVPath)
	s, err := NewStorage(tmpRedisKVPath)
	assert.NoError(t, err, "TestAppend failed")
	defer s.Close()

	key := []byte("TestAppend")
	value := []byte("value")

	n, err := s.RedisKV().Append(key, value)
	assert.NoError(t, err, "TestAppend failed")
	assert.Equal(t, int64(len(value)), n, "TestAppend failed")

	n, err = s.RedisKV().Append(key, value)
	assert.NoError(t, err, "TestAppend failed")
	assert.Equal(t, int64(2*len(value)), n, "TestAppend failed")
}

func TestSetNX(t *testing.T) {
	os.RemoveAll(tmpRedisKVPath)
	s, err := NewStorage(tmpRedisKVPath)
	assert.NoError(t, err, "TestSetNX failed")
	defer s.Close()

	key := []byte("TestSetNX")
	value := []byte("value")

	n, err := s.RedisKV().SetNX(key, value)
	assert.NoError(t, err, "TestSetNX failed")
	assert.Equal(t, int64(1), n, "TestSetNX failed")

	n, err = s.RedisKV().SetNX(key, value)
	assert.NoError(t, err, "TestSetNX failed")
	assert.Equal(t, int64(0), n, "TestSetNX failed")
}

func TestStrLen(t *testing.T) {
	os.RemoveAll(tmpRedisKVPath)
	s, err := NewStorage(tmpRedisKVPath)
	assert.NoError(t, err, "TestStrLen failed")
	defer s.Close()

	key := []byte("TestStrLen")
	value := []byte("value")

	n, err := s.RedisKV().StrLen(key)
	assert.NoError(t, err, "TestStrLen failed")
	assert.Equal(t, int64(0), n, "TestStrLen failed")

	err = s.RedisKV().Set(key, value)
	assert.NoError(t, err, "TestStrLen failed")

	n, err = s.RedisKV().StrLen(key)
	assert.NoError(t, err, "TestStrLen failed")
	assert.Equal(t, int64(len(value)), n, "TestStrLen failed")
}
