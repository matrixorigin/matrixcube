package nemo

import (
	"bytes"

	gonemo "github.com/deepfabric/go-nemo"
)

var (
	endScan = []byte("")
)

type redisHash struct {
	db *gonemo.NEMO
}

func (s *redisHash) HSet(key, field, value []byte) (int64, error) {
	n, err := s.db.HSet(key, field, value)
	return int64(n), err
}

func (s *redisHash) HGet(key, field []byte) ([]byte, error) {
	return s.db.HGet(key, field)
}

func (s *redisHash) HDel(key []byte, fields ...[]byte) (int64, error) {
	return s.db.HDel(key, fields...)
}

func (s *redisHash) HExists(key, field []byte) (bool, error) {
	return s.db.HExists(key, field)
}

func (s *redisHash) HKeys(key []byte) ([][]byte, error) {
	return s.db.HKeys(key)
}

func (s *redisHash) HVals(key []byte) ([][]byte, error) {
	return s.db.HVals(key)
}

func (s *redisHash) HScanGet(key, start []byte, count int) ([][]byte, error) {
	iter := s.db.HScan(key, start, endScan, true)
	var result [][]byte
	for {
		if len(result) == count {
			break
		}

		if !iter.Valid() {
			break
		}

		field := iter.Field()
		if !bytes.Equal(start, field) {
			result = append(result, field, iter.Value())
		}

		iter.Next()
	}

	iter.Free()
	return result, nil
}

func (s *redisHash) HGetAll(key []byte) ([][]byte, error) {
	fields, values, err := s.db.HGetall(key)
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 || len(values) == 0 {
		return nil, nil
	}

	result := make([][]byte, 0, len(fields)*2)
	for idx, field := range fields {
		result = append(result, field, values[idx])
	}

	return result, nil
}

func (s *redisHash) HLen(key []byte) (int64, error) {
	return s.db.HLen(key)
}

func (s *redisHash) HMGet(key []byte, fields ...[]byte) ([][]byte, []error) {
	values, errors := s.db.HMGet(key, fields)
	var errs []error
	if len(errors) > 0 {
		for _, err := range errors {
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	return values, errs
}

func (s *redisHash) HMSet(key []byte, fields, values [][]byte) error {
	_, err := s.db.HMSet(key, fields, values)
	return err
}

func (s *redisHash) HSetNX(key, field, value []byte) (int64, error) {
	return s.db.HSetnx(key, field, value)
}

func (s *redisHash) HStrLen(key, field []byte) (int64, error) {
	return s.db.HStrlen(key, field)
}

func (s *redisHash) HIncrBy(key, field []byte, incrment int64) ([]byte, error) {
	return s.db.HIncrby(key, field, incrment)
}
