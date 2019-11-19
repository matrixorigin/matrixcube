package nemo

import (
	gonemo "github.com/deepfabric/go-nemo"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
)

type redisKV struct {
	db *gonemo.NEMO
}

func (kv *redisKV) Set(key, value []byte) error {
	return kv.db.Set(key, value, 0)
}

func (kv *redisKV) MSet(keys [][]byte, values [][]byte) error {
	return kv.db.MSet(keys, values)
}

func (kv *redisKV) Get(key []byte) ([]byte, error) {
	return kv.db.Get(key)
}

func (kv *redisKV) IncrBy(key []byte, incrment int64) (int64, error) {
	v, err := kv.db.Incrby(key, incrment)
	if err != nil {
		return 0, err
	}

	return format.ParseStrInt64(hack.SliceToString(v))
}

func (kv *redisKV) DecrBy(key []byte, incrment int64) (int64, error) {
	v, err := kv.db.Decrby(key, incrment)
	if err != nil {
		return 0, err
	}

	return format.ParseStrInt64(hack.SliceToString(v))
}

func (kv *redisKV) GetSet(key, value []byte) ([]byte, error) {
	return kv.db.GetSet(key, value, 0)
}

func (kv *redisKV) Append(key, value []byte) (int64, error) {
	return kv.db.Append(key, value)
}

func (kv *redisKV) SetNX(key, value []byte) (int64, error) {
	return kv.db.Setnx(key, value, 0)
}

func (kv *redisKV) StrLen(key []byte) (int64, error) {
	return kv.db.StrLen(key)
}
