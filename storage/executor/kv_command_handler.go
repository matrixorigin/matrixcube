// Copyright 2022 MatrixOrigin.
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

package executor

import (
	"bytes"
	"math"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/buf"
	keysutil "github.com/matrixorigin/matrixcube/util/keys"
)

var (
	setResponse             = protoc.MustMarshal(&rpcpb.KVSetResponse{})
	batchSetResponse        = protoc.MustMarshal(&rpcpb.KVBatchSetResponse{})
	deleteResponse          = protoc.MustMarshal(&rpcpb.KVDeleteResponse{})
	batchDeleteResponse     = protoc.MustMarshal(&rpcpb.KVBatchDeleteResponse{})
	rangeDeleteResponse     = protoc.MustMarshal(&rpcpb.KVRangeDeleteResponse{})
	batchMixedWriteResponse = protoc.MustMarshal(&rpcpb.KVMixedWriteResponse{})

	emptyGetResponse = protoc.MustMarshal(&rpcpb.KVGetRequest{})
)

func handleSet(shard metapb.Shard, cmd []byte, wb util.WriteBatch, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVWriteCommandResult, error) {
	defer buffer.ResetWrite()

	var req rpcpb.KVSetRequest
	protoc.MustUnmarshal(&req, cmd)

	return doHandleSet(shard, req, wb, buffer, kvStore)
}

func doHandleSet(shard metapb.Shard, req rpcpb.KVSetRequest, wb util.WriteBatch, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVWriteCommandResult, error) {
	k := keysutil.EncodeDataKey(req.Key, buffer)
	wb.Set(k, req.Value)
	changed := len(k) + len(req.Value)
	return KVWriteCommandResult{
		DiffBytes:    int64(changed),
		WrittenBytes: uint64(changed),
		Response:     setResponse,
	}, nil
}

func handleBatchSet(shard metapb.Shard, cmd []byte, wb util.WriteBatch, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVWriteCommandResult, error) {
	var req rpcpb.KVBatchSetRequest
	protoc.MustUnmarshal(&req, cmd)

	changed := 0
	for i := range req.Requests {
		k := keysutil.EncodeDataKey(req.Requests[i].Key, buffer)
		v := req.Requests[i].Value
		wb.Set(k, v)
		buffer.ResetWrite()
		changed += len(k) + len(v)
	}

	return KVWriteCommandResult{
		DiffBytes:    int64(changed),
		WrittenBytes: uint64(changed),
		Response:     batchSetResponse,
	}, nil
}

func handleDelete(shard metapb.Shard, cmd []byte, wb util.WriteBatch, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVWriteCommandResult, error) {
	defer buffer.ResetWrite()

	var req rpcpb.KVDeleteRequest
	protoc.MustUnmarshal(&req, cmd)

	return doHandleDelete(shard, req, wb, buffer, kvStore)
}

func doHandleDelete(shard metapb.Shard, req rpcpb.KVDeleteRequest, wb util.WriteBatch, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVWriteCommandResult, error) {
	k := keysutil.EncodeDataKey(req.Key, buffer)
	wb.Delete(k)
	changed := len(k)
	return KVWriteCommandResult{
		DiffBytes:    -int64(changed),
		WrittenBytes: uint64(changed),
		Response:     deleteResponse,
	}, nil
}

func handleBatchDelete(shard metapb.Shard, cmd []byte, wb util.WriteBatch, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVWriteCommandResult, error) {
	var req rpcpb.KVBatchDeleteRequest
	protoc.MustUnmarshal(&req, cmd)

	changed := 0
	for i := range req.Keys {
		k := keysutil.EncodeDataKey(req.Keys[i], buffer)
		wb.Delete(k)
		buffer.ResetWrite()

		changed += len(k)
	}

	return KVWriteCommandResult{
		DiffBytes:    -int64(changed),
		WrittenBytes: uint64(changed),
		Response:     batchDeleteResponse,
	}, nil
}

func handleRangeDelete(shard metapb.Shard, cmd []byte, wb util.WriteBatch, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVWriteCommandResult, error) {
	defer buffer.ResetWrite()

	var req rpcpb.KVRangeDeleteRequest
	protoc.MustUnmarshal(&req, cmd)

	return doHandleRangeDelete(shard, req, wb, buffer, kvStore)
}

func doHandleRangeDelete(shard metapb.Shard, req rpcpb.KVRangeDeleteRequest, wb util.WriteBatch, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVWriteCommandResult, error) {
	from := keysutil.EncodeShardStart(req.Start, buffer)
	to := keysutil.EncodeShardEnd(req.End, buffer)
	wb.DeleteRange(from, to)
	changed := len(from) + len(to)
	return KVWriteCommandResult{
		DiffBytes:    -int64(changed),
		WrittenBytes: uint64(changed),
		Response:     rangeDeleteResponse,
	}, nil
}

func handleGet(shard metapb.Shard, cmd []byte, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVReadCommandResult, error) {
	defer buffer.ResetWrite()

	var req rpcpb.KVGetRequest
	protoc.MustUnmarshal(&req, cmd)

	v, err := kvStore.Get(keysutil.EncodeDataKey(req.Key, buffer))
	if err != nil {
		return KVReadCommandResult{}, err
	}

	if len(v) == 0 {
		return KVReadCommandResult{
			Response: emptyGetResponse,
		}, nil
	}

	var resp rpcpb.KVGetResponse
	resp.Value = v
	return KVReadCommandResult{
		ReadBytes: uint64(len(v)),
		Response:  protoc.MustMarshal(&resp),
	}, nil
}

func handleBatchGet(shard metapb.Shard, cmd []byte, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVReadCommandResult, error) {
	var req rpcpb.KVBatchGetRequest
	protoc.MustUnmarshal(&req, cmd)

	var resp rpcpb.KVBatchGetResponse
	resp.Values = make([][]byte, 0, len(req.Keys))

	readed := 0
	for _, key := range req.Keys {
		v, err := kvStore.Get(keysutil.EncodeDataKey(key, buffer))
		buffer.ResetWrite()
		if err != nil {
			return KVReadCommandResult{}, err
		}
		readed += len(v)
		resp.Values = append(resp.Values, v)
	}

	return KVReadCommandResult{
		ReadBytes: uint64(readed),
		Response:  protoc.MustMarshal(&resp),
	}, nil
}

func handleScan(shard metapb.Shard, cmd []byte, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVReadCommandResult, error) {
	var req rpcpb.KVScanRequest
	protoc.MustUnmarshal(&req, cmd)

	// req.Start < shard.Start, only scan the data in current shard
	if len(req.Start) == 0 ||
		bytes.Compare(req.Start, shard.Start) < 0 {
		req.Start = shard.Start
	}
	// req.End > shard.End, only scan the data in current shard
	if len(req.End) == 0 ||
		(len(shard.End) > 0 && bytes.Compare(req.End, shard.End) > 0) {
		req.End = shard.End
	}
	if req.Limit == 0 {
		req.Limit = math.MaxUint64
	}
	if req.LimitBytes == 0 {
		req.LimitBytes = math.MaxUint64
	}

	var resp rpcpb.KVScanResponse
	view := kvStore.GetView()
	defer view.Close()

	start := keysutil.EncodeShardStart(req.Start, buffer)
	end := keysutil.EncodeShardEnd(req.End, buffer)
	n := uint64(0)
	bytes := uint64(0)
	skipByLimit := false
	var keys []buf.Slice
	var values []buf.Slice
	err := kvStore.ScanInView(view, start, end, func(key, value []byte) (bool, error) {
		n++
		if req.OnlyCount {
			return true, nil
		}

		originKey := keysutil.DecodeDataKey(key)

		buffer.MarkWrite()
		buf.MustWrite(buffer, originKey)
		keys = append(keys, buffer.WrittenDataAfterMark())

		bytes += uint64(len(originKey))
		if req.WithValue {
			buffer.MarkWrite()
			buf.MustWrite(buffer, value)
			values = append(values, buffer.WrittenDataAfterMark())
			bytes += uint64(len(value))
		}

		if n >= req.Limit ||
			bytes >= req.LimitBytes {
			skipByLimit = true
			return false, nil
		}
		return true, nil
	}, false)
	if err != nil {
		return KVReadCommandResult{}, nil
	}

	if n == 0 || !skipByLimit {
		resp.Completed = true
	}

	resp.Count = n
	if !req.OnlyCount {
		resp.Keys = make([][]byte, 0, len(keys))
		for idx := range keys {
			resp.Keys = append(resp.Keys, keys[idx].Data())
		}

		if req.WithValue {
			resp.Values = make([][]byte, 0, len(values))
			for idx := range values {
				resp.Values = append(resp.Values, values[idx].Data())
			}
		}
	}

	resp.ShardEnd = shard.End
	return KVReadCommandResult{
		ReadBytes: bytes,
		Response:  protoc.MustMarshal(&resp),
	}, nil
}

func handleBatchMixedWrite(shard metapb.Shard, cmd []byte, wb util.WriteBatch, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVWriteCommandResult, error) {
	defer buffer.ResetWrite()

	var req rpcpb.KVBatchMixedWriteRequest
	protoc.MustUnmarshal(&req, cmd)

	var mixedResult KVWriteCommandResult
	for idx := range req.Requests {
		var result KVWriteCommandResult
		var err error
		switch rpcpb.InternalCmd(req.Requests[idx].CmdType) {
		case rpcpb.CmdKVSet:
			result, err = doHandleSet(shard, req.Requests[idx].Set, wb, buffer, kvStore)
		case rpcpb.CmdKVDelete:
			result, err = doHandleDelete(shard, req.Requests[idx].Delete, wb, buffer, kvStore)
		case rpcpb.CmdKVRangeDelete:
			result, err = doHandleRangeDelete(shard, req.Requests[idx].RangeDelete, wb, buffer, kvStore)
		}

		if err != nil {
			return mixedResult, err
		}
		mixedResult.DiffBytes += result.DiffBytes
		mixedResult.WrittenBytes += mixedResult.WrittenBytes
	}

	mixedResult.Response = batchMixedWriteResponse
	return mixedResult, nil
}
