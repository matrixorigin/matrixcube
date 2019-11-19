package nemo

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var (
	tmpRedisHashPath = "/tmp/nemo-redis-hash"
)


func TestHSet(t *testing.T) {
	os.RemoveAll(tmpRedisHashPath)
	s, err := NewStorage(tmpRedisHashPath)
	assert.NoError(t, err, "TestHSet failed")
	defer s.Close()

	key := []byte("TestHSet")
	field := []byte("f1")
	value := []byte("v1")

	n, err := s.RedisHash().HSet(key, field, value)
	assert.NoError(t, err, "TestHSet failed")
	assert.Equal(t, int64(1), n, "TestHSet failed")
}

func TestHGet(t *testing.T) {
	os.RemoveAll(tmpRedisHashPath)
	s, err := NewStorage(tmpRedisHashPath)
	assert.NoError(t, err, "TestHGet failed")
	defer s.Close()

	key := []byte("TestHGet")
	field := []byte("f1")
	value := []byte("v1")

	field2 := []byte("f2")

	_, err = s.RedisHash().HSet(key, field, value)
	assert.NoError(t, err, "TestHGet failed")

	v, err := s.RedisHash().HGet(key, field)
	assert.NoError(t, err, "TestHGet failed")
	assert.Equal(t, string(value), string(v), "TestHSet failed")

	v, err = s.RedisHash().HGet(key, field2)
	assert.NoError(t, err, "TestHGet failed")
	assert.Empty(t, v, "TestHGet failed")
}

func TestHDel(t *testing.T) {
	os.RemoveAll(tmpRedisHashPath)
	s, err := NewStorage(tmpRedisHashPath)
	assert.NoError(t, err, "TestHDel failed")
	defer s.Close()

	key := []byte("TestHDel")
	field1 := []byte("f1")
	value1 := []byte("v1")

	field2 := []byte("f2")
	value2 := []byte("v2")

	n, err := s.RedisHash().HDel(key, field1, field2)
	assert.NoError(t, err, "TestHDel failed")

	s.RedisHash().HSet(key, field1, value1)
	s.RedisHash().HSet(key, field2, value2)

	n, err = s.RedisHash().HDel(key, field1, field2)
	assert.NoError(t, err, "TestHDel failed")
	assert.Equal(t, int64(2), n, "TestHDel failed")
}

func TestHExists(t *testing.T) {
	os.RemoveAll(tmpRedisHashPath)
	s, err := NewStorage(tmpRedisHashPath)
	assert.NoError(t, err, "TestHExists failed")
	defer s.Close()

	key := []byte("TestHExists")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")

	s.RedisHash().HSet(key, field1, value1)

	yes, err := s.RedisHash().HExists(key, field1)
	assert.NoError(t, err, "TestHExists failed")
	assert.True(t, yes, "TestHExists failed")

	yes, err = s.RedisHash().HExists(key, field2)
	assert.NoError(t, err, "TestHExists failed")
	assert.False(t, yes, "TestHExists failed")
}

func TestHKeys(t *testing.T) {
	os.RemoveAll(tmpRedisHashPath)
	s, err := NewStorage(tmpRedisHashPath)
	assert.NoError(t, err, "TestHKeys failed")
	defer s.Close()

	key := []byte("TestHKeys")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")
	value2 := []byte("v2")

	keys, err := s.RedisHash().HKeys(key)
	assert.NoError(t, err, "TestHKeys failed")
	assert.Empty(t, keys, "TestHKeys failed")

	s.RedisHash().HSet(key, field1, value1)
	s.RedisHash().HSet(key, field2, value2)

	keys, err = s.RedisHash().HKeys(key)
	assert.NoError(t, err, "TestHKeys failed")
	assert.Equal(t, 2, len(keys), "TestHKeys failed")

}

func TestHVals(t *testing.T) {
	os.RemoveAll(tmpRedisHashPath)
	s, err := NewStorage(tmpRedisHashPath)
	assert.NoError(t, err, "TestHVals failed")
	defer s.Close()

	key := []byte("TestHVals")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")
	value2 := []byte("v2")

	values, err := s.RedisHash().HVals(key)
	assert.NoError(t, err, "TestHVals failed")
	assert.Empty(t, values, "TestHVals failed")

	s.RedisHash().HSet(key, field1, value1)
	s.RedisHash().HSet(key, field2, value2)

	values, err = s.RedisHash().HVals(key)
	assert.NoError(t, err, "TestHVals failed")
	assert.Equal(t, 2, len(values), "TestHVals failed")
}

func TestHGetAll(t *testing.T) {
	os.RemoveAll(tmpRedisHashPath)
	s, err := NewStorage(tmpRedisHashPath)
	assert.NoError(t, err, "TestHGetAll failed")
	defer s.Close()

	key := []byte("TestHGetAll")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")
	value2 := []byte("v2")

	values, err := s.RedisHash().HGetAll(key)
	assert.NoError(t, err, "TestHGetAll failed")
	assert.Empty(t, values, "TestHGetAll failed")

	s.RedisHash().HSet(key, field1, value1)
	s.RedisHash().HSet(key, field2, value2)

	values, err = s.RedisHash().HGetAll(key)
	assert.NoError(t, err, "TestHGetAll failed")
	assert.Equal(t, 4, len(values), "TestHGetAll failed")
}

func TestHLen(t *testing.T) {
	os.RemoveAll(tmpRedisHashPath)
	s, err := NewStorage(tmpRedisHashPath)
	assert.NoError(t, err, "TestHLen failed")
	defer s.Close()

	key := []byte("TestHLen")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")
	value2 := []byte("v2")

	n, err := s.RedisHash().HLen(key)
	assert.NoError(t, err, "TestHLen failed")
	assert.Equal(t, int64(0), n, "TestHLen failed")

	s.RedisHash().HSet(key, field1, value1)
	s.RedisHash().HSet(key, field2, value2)

	n, err = s.RedisHash().HLen(key)
	assert.NoError(t, err, "TestHLen failed")
	assert.Equal(t, int64(2), n, "TestHLen failed")
}

func TestHMGet(t *testing.T) {
	os.RemoveAll(tmpRedisHashPath)
	s, err := NewStorage(tmpRedisHashPath)
	assert.NoError(t, err, "TestHMGet failed")
	defer s.Close()

	key := []byte("TestHMGet")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")
	value2 := []byte("v2")

	values, errs := s.RedisHash().HMGet(key, field1, field2)
	assert.Empty(t, errs, "TestHMGet failed")
	assert.Equal(t, 2, len(values), "TestHMGet failed")
	for i := 0; i < 2; i++ {
		assert.Empty(t, values[i], "TestHMGet failed")
	}

	s.RedisHash().HSet(key, field1, value1)
	s.RedisHash().HSet(key, field2, value2)

	values, errs = s.RedisHash().HMGet(key, field1, field2)
	assert.Empty(t, errs, "TestHMGet failed")
	assert.Equal(t, 2, len(values), "TestHMGet failed")
}

func TestHMSet(t *testing.T) {
	os.RemoveAll(tmpRedisHashPath)
	s, err := NewStorage(tmpRedisHashPath)
	assert.NoError(t, err, "TestHMSet failed")
	defer s.Close()

	key := []byte("TestHMSet")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")
	value2 := []byte("v2")

	err = s.RedisHash().HMSet(key, [][]byte{field1, field2}, [][]byte{value1, value2})
	assert.NoError(t, err, "TestHMSet failed")

	v, err := s.RedisHash().HGet(key, field1)
	assert.NoError(t, err, "TestHMSet failed")
	assert.Equal(t, string(value1), string(v), "TestHMSet failed")

	v, err = s.RedisHash().HGet(key, field2)
	assert.NoError(t, err, "TestHMSet failed")
	assert.Equal(t, string(value2), string(v), "TestHMSet failed")
}

func TestHSetNX(t *testing.T) {
	os.RemoveAll(tmpRedisHashPath)
	s, err := NewStorage(tmpRedisHashPath)
	assert.NoError(t, err, "TestHSetNX failed")
	defer s.Close()

	key := []byte("TestHSetNX")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")
	value2 := []byte("v2")

	s.RedisHash().HSet(key, field1, value1)

	n, err := s.RedisHash().HSetNX(key, field1, value1)
	assert.NoError(t, err, "TestHSetNX failed")
	assert.Equal(t, int64(0), n, "TestHSetNX failed")

	n, err = s.RedisHash().HSetNX(key, field2, value2)
	assert.NoError(t, err, "TestHSetNX failed")
	assert.Equal(t, int64(1), n, "TestHSetNX failed")
}

func TestHStrLen(t *testing.T) {
	os.RemoveAll(tmpRedisHashPath)
	s, err := NewStorage(tmpRedisHashPath)
	assert.NoError(t, err, "TestHStrLen failed")
	defer s.Close()

	key := []byte("TestHStrLen")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")

	s.RedisHash().HSet(key, field1, value1)

	n, err := s.RedisHash().HStrLen(key, field1)
	assert.NoError(t, err, "TestHStrLen failed")
	assert.Equal(t, int64(len(value1)), n, "TestHStrLen failed")

	n, err = s.RedisHash().HStrLen(key, field2)
	assert.NoError(t, err, "TestHStrLen failed")
	assert.Equal(t, int64(0), n, "TestHStrLen failed")
}

func TestHIncrBy(t *testing.T) {
	os.RemoveAll(tmpRedisHashPath)
	s, err := NewStorage(tmpRedisHashPath)
	assert.NoError(t, err, "TestHIncrBy failed")
	defer s.Close()

	key := []byte("TestHIncrBy")
	field1 := []byte("f1")

	n, err := s.RedisHash().HIncrBy(key, field1, 1)
	assert.NoError(t, err, "")
	assert.Equal(t, "1", string(n), "TestHIncrBy failed")

	n, err = s.RedisHash().HIncrBy(key, field1, 1)
	assert.NoError(t, err, "")
	assert.Equal(t, "2", string(n), "TestHIncrBy failed")
}
