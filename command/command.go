package command

import (
	"github.com/deepfabric/beehive/pb/bhmetapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
)

// CommandWriteBatch command write batch
type CommandWriteBatch interface {
	// Add add a request to this batch, returns true if it can be executed in this batch,
	// otherwrise false
	Add(uint64, *raftcmdpb.Request, map[string]interface{}) (bool, *raftcmdpb.Response, error)
	// Execute excute the batch, and return the write bytes, and diff bytes that used to
	// modify the size of the current shard
	Execute(bhmetapb.Shard) (uint64, int64, error)
	// Reset reset the current batch for reuse
	Reset()
}

// CommandReadBatch command read batch
type CommandReadBatch interface {
	// Add add a request to this batch, returns true if it can be executed in this batch,
	// otherwrise false
	Add(uint64, *raftcmdpb.Request, map[string]interface{}) (bool, error)
	// Execute excute the batch, and return the responses
	Execute(bhmetapb.Shard) ([]*raftcmdpb.Response, uint64, error)
	// Reset reset the current batch for reuse
	Reset()
}

// ReadCommandFunc the read command handler func
type ReadCommandFunc func(bhmetapb.Shard, *raftcmdpb.Request, map[string]interface{}) (*raftcmdpb.Response, uint64)

// WriteCommandFunc the write command handler func, returns write bytes and the diff bytes
// that used to modify the size of the current shard
type WriteCommandFunc func(bhmetapb.Shard, *raftcmdpb.Request, map[string]interface{}) (uint64, int64, *raftcmdpb.Response)

// LocalCommandFunc directly exec on local func
type LocalCommandFunc func(bhmetapb.Shard, *raftcmdpb.Request) (*raftcmdpb.Response, error)
