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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
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

// ShardUnavailableErr is an error indicates the shard is unavailable
type ShardUnavailableErr struct {
	err string
}

// NewShardUnavailableErr returns a wrapped error that the shard is unavailable
func NewShardUnavailableErr(id uint64) error {
	return ShardUnavailableErr{err: fmt.Sprintf("shard %d is unavailable", id)}
}

// String implements error interface
func (err ShardUnavailableErr) Error() string {
	return err.err
}

// IsShardUnavailableErr checks if an error is ShardUnavailableErr
func IsShardUnavailableErr(err error) bool {
	_, ok := err.(ShardUnavailableErr)
	return ok
}

func buildID(id []byte, resp *rpcpb.ResponseBatch) {
	if resp.Header.IsEmpty() {
		return
	}

	if len(resp.Header.ID) == 0 {
		resp.Header.ID = id
	}
}

func errorOtherCMDResp(err error) rpcpb.ResponseBatch {
	resp := errorBaseResp(nil)
	resp.Header.Error.Message = err.Error()
	return resp
}

func errorPbResp(id []byte, err errorpb.Error) rpcpb.ResponseBatch {
	resp := errorBaseResp(id)
	resp.Header.Error = err
	return resp
}

func errorStaleCMDResp(id []byte) rpcpb.ResponseBatch {
	resp := errorBaseResp(id)
	resp.Header.Error.Message = errStaleCMD.Error()
	resp.Header.Error.StaleCommand = infoStaleCMD
	return resp
}

func errorStaleEpochResp(id []byte,
	newShards ...Shard) rpcpb.ResponseBatch {
	resp := errorBaseResp(id)
	resp.Header.Error.Message = errStaleCMD.Error()
	resp.Header.Error.StaleEpoch = &errorpb.StaleEpoch{
		NewShards: newShards,
	}
	return resp
}

func errorBaseResp(id []byte) rpcpb.ResponseBatch {
	resp := rpcpb.ResponseBatch{}
	resp.Header.ID = id
	return resp
}

func checkKeyInShard(key []byte, shard Shard) *errorpb.Error {
	if bytes.Compare(key, shard.Start) >= 0 &&
		(len(shard.End) == 0 || bytes.Compare(key, shard.End) < 0) {
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

// ErrTryAgain indicates that an operation should retry later
type ErrTryAgain struct {
	// caller should wait for this period before retry
	Wait time.Duration
}

func (e *ErrTryAgain) Error() string {
	return fmt.Sprintf("should try again after %v", e.Wait)
}
