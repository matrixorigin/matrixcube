package nemo

import (
	gonemo "github.com/deepfabric/go-nemo"
)

type redisSet struct {
	db *gonemo.NEMO
}

func (s *redisSet) SAdd(key []byte, members ...[]byte) (int64, error) {
	return s.db.SAdd(key, members...)
}

func (s *redisSet) SRem(key []byte, members ...[]byte) (int64, error) {
	return s.db.SRem(key, members...)
}

func (s *redisSet) SCard(key []byte) (int64, error) {
	return s.db.SCard(key)
}

func (s *redisSet) SMembers(key []byte) ([][]byte, error) {
	return s.db.SMembers(key)
}

func (s *redisSet) SIsMember(key []byte, member []byte) (int64, error) {
	yes, err := s.db.SIsMember(key, member)
	if err != nil {
		return 0, err
	}

	if yes {
		return 1, nil
	}

	return 0, nil
}

func (s *redisSet) SPop(key []byte) ([]byte, error) {
	exists, value, err := s.db.SPop(key)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, nil
	}
	return value, nil
}
