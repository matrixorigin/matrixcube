package aware

import (
	"github.com/deepfabric/beehive/pb/bhmetapb"
)

// ShardStateAware shard state aware
type ShardStateAware interface {
	// Created the shard was created on the current store
	Created(bhmetapb.Shard)
	// Splited the shard was splited on the current store
	Splited(bhmetapb.Shard)
	// Destory the shard was destoryed on the current store
	Destory(bhmetapb.Shard)
	// BecomeLeader the shard was become leader on the current store
	BecomeLeader(bhmetapb.Shard)
	// BecomeLeader the shard was become follower on the current store
	BecomeFollower(bhmetapb.Shard)
	// SnapshotApplied snapshot applied
	SnapshotApplied(bhmetapb.Shard)
}
