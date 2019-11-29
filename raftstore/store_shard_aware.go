package raftstore

import (
	"github.com/deepfabric/beehive/pb/metapb"
)

func (s *store) Created(metapb.Shard) {

}

func (s *store) Splited(metapb.Shard) {

}

func (s *store) Destory(metapb.Shard) {

}

func (s *store) BecomeLeader(metapb.Shard) {

}

func (s *store) BecomeFollower(metapb.Shard) {

}
