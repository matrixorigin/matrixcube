package nemo

import (
	gonemo "github.com/deepfabric/go-nemo"
)

type redisList struct {
	db *gonemo.NEMO
}

func (s *redisList) LIndex(key []byte, index int64) ([]byte, error) {
	return s.db.LIndex(key, index)
}

func (s *redisList) LInsert(key []byte, pos int, pivot []byte, value []byte) (int64, error) {
	return s.db.LInsert(key, pos, pivot, value)
}

func (s *redisList) LLen(key []byte) (int64, error) {
	return s.db.LLen(key)
}

func (s *redisList) LPop(key []byte) ([]byte, error) {
	return s.db.LPop(key)
}

func (s *redisList) LPush(key []byte, values ...[]byte) (int64, error) {
	return s.db.LPush(key, values[0])
}

func (s *redisList) LPushX(key []byte, value []byte) (int64, error) {
	return s.db.LPushx(key, value)
}

func (s *redisList) LRange(key []byte, begin int64, end int64) ([][]byte, error) {
	_, values, err := s.db.LRange(key, begin, end)
	return values, err
}

func (s *redisList) LRem(key []byte, count int64, value []byte) (int64, error) {
	return s.db.LRem(key, count, value)
}

func (s *redisList) LSet(key []byte, index int64, value []byte) error {
	return s.db.LSet(key, index, value)
}

func (s *redisList) LTrim(key []byte, begin int64, end int64) error {
	return s.db.LTrim(key, begin, end)
}

func (s *redisList) RPop(key []byte) ([]byte, error) {
	return s.db.RPop(key)
}

func (s *redisList) RPush(key []byte, values ...[]byte) (int64, error) {
	return s.db.RPush(key, values[0])
}

func (s *redisList) RPushX(key []byte, value []byte) (int64, error) {
	return s.db.RPushx(key, value)
}
