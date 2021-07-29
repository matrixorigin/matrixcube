// Copyright 2020 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package raftstore

import (
	"bytes"
	"errors"

	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
)

var (
	errStaleCMD           = errors.New("stale command")
	errStaleEpoch         = errors.New("stale epoch")
	errNotLeader          = errors.New("notLeader")
	errShardNotFound      = errors.New("shard not found")
	errMissingUUIDCMD     = errors.New("missing request id")
	errLargeRaftEntrySize = errors.New("raft entry is too large")
	errKeyNotInShard      = errors.New("key not in shard")
	errStoreNotMatch      = errors.New("store not match")

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

func errorStaleEpochResp(uuid []byte, currentTerm uint64, newShards ...bhmetapb.Shard) *raftcmdpb.RaftCMDResponse {
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

func checkKeyInShard(key []byte, shard *bhmetapb.Shard) *errorpb.Error {
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
