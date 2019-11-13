package raftstore

import (
	"bytes"
	"errors"

	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/errorpb"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
)

var (
	errConnect = errors.New("not connected")
)

var (
	errStaleCMD           = errors.New("Stale command")
	errStaleEpoch         = errors.New("Stale epoch")
	errNotLeader          = errors.New("NotLeader")
	errShardNotFound      = errors.New("Shard not found")
	errMissingUUIDCMD     = errors.New("Missing request id")
	errLargeRaftEntrySize = errors.New("Raft entry is too large")
	errKeyNotInShard      = errors.New("Key not in shard")
	errStoreNotMatch      = errors.New("Store not match")

	infoStaleCMD  = new(errorpb.StaleCommand)
	storeNotMatch = new(errorpb.StoreNotMatch)
)

func buildTerm(term uint64, resp *raftcmdpb.RaftCMDResponse) {
	if resp.Header == nil {
		return
	}

	resp.Header.CurrentTerm = term
}

func buildUUID(uuid []byte, resp *raftcmdpb.RaftCMDResponse) {
	if resp.Header == nil {
		return
	}

	if len(resp.Header.ID) != 0 {
		resp.Header.ID = uuid
	}
}

func errorOtherCMDResp(err error) *raftcmdpb.RaftCMDResponse {
	resp := errorBaseResp(nil, 0)
	resp.Header.Error.Message = err.Error()
	return resp
}

func errorPbResp(err *errorpb.Error, uuid []byte, currentTerm uint64) *raftcmdpb.RaftCMDResponse {
	resp := errorBaseResp(uuid, currentTerm)
	resp.Header.Error = *err

	return resp
}

func errorStaleCMDResp(uuid []byte, currentTerm uint64) *raftcmdpb.RaftCMDResponse {
	resp := errorBaseResp(uuid, currentTerm)
	resp.Header.Error.Message = errStaleCMD.Error()
	resp.Header.Error.StaleCommand = infoStaleCMD

	return resp
}

func errorStaleEpochResp(uuid []byte, currentTerm uint64, newShards ...metapb.Shard) *raftcmdpb.RaftCMDResponse {
	resp := errorBaseResp(uuid, currentTerm)

	resp.Header.Error.Message = errStaleCMD.Error()
	resp.Header.Error.StaleEpoch = &errorpb.StaleEpoch{
		NewShards: newShards,
	}

	return resp
}

func errorBaseResp(uuid []byte, currentTerm uint64) *raftcmdpb.RaftCMDResponse {
	resp := pb.AcquireRaftCMDResponse()
	resp.Header = pb.AcquireRaftResponseHeader()
	buildTerm(currentTerm, resp)
	buildUUID(uuid, resp)

	return resp
}

func checkKeyInShard(key []byte, shard *metapb.Shard) *errorpb.Error {
	if bytes.Compare(key, shard.Start) >= 0 && (len(shard.End) == 0 || bytes.Compare(key, shard.End) < 0) {
		return nil
	}

	e := &errorpb.KeyNotInShard{
		Key:     key,
		ShardID: shard.ID,
		Start:   shard.Start,
		End:     shard.End,
	}

	return &errorpb.Error{
		Message:       errKeyNotInShard.Error(),
		KeyNotInShard: e,
	}
}
