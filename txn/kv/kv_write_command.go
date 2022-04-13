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

package kv

import (
	"bytes"
	"math"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/util/buf"
	keysutil "github.com/matrixorigin/matrixcube/util/keys"
)

func handleSet(shard metapb.Shard, req txnpb.TxnRequest, writer KVReaderAndWriter) error {
	var kvReq rpcpb.KVSetRequest
	protoc.MustUnmarshal(&kvReq, req.Operation.Payload)

	if req.Options.CreateTxnRecord {
		writer.WriteTxnRecord(req)
	}
	writer.WriteUncommitted(kvReq.Key, kvReq.Value)
	return nil
}

func handleBatchSet(shard metapb.Shard, req txnpb.TxnRequest, writer KVReaderAndWriter) error {
	var kvReq rpcpb.KVBatchSetRequest
	protoc.MustUnmarshal(&kvReq, req.Operation.Payload)

	if req.Options.CreateTxnRecord {
		writer.WriteTxnRecord(req)
	}

	for idx := range kvReq.Requests {
		writer.WriteUncommitted(kvReq.Requests[idx].Key, kvReq.Requests[idx].Value)
	}
	return nil
}

func handleDelete(shard metapb.Shard, req txnpb.TxnRequest, writer KVReaderAndWriter) error {
	var kvReq rpcpb.KVDeleteRequest
	protoc.MustUnmarshal(&kvReq, req.Operation.Payload)

	if req.Options.CreateTxnRecord {
		writer.WriteTxnRecord(req)
	}

	writer.WriteDeleteUncommitted(kvReq.Key)
	return nil
}

func handleBatchDelete(shard metapb.Shard, req txnpb.TxnRequest, writer KVReaderAndWriter) error {
	var kvReq rpcpb.KVBatchDeleteRequest
	protoc.MustUnmarshal(&kvReq, req.Operation.Payload)

	if req.Options.CreateTxnRecord {
		writer.WriteTxnRecord(req)
	}

	for idx := range kvReq.Keys {
		writer.WriteDeleteUncommitted(kvReq.Keys[idx])
	}
	return nil
}

func handleRangeDelete(shard metapb.Shard, req txnpb.TxnRequest, writer KVReaderAndWriter) error {
	var kvReq rpcpb.KVRangeDeleteRequest
	protoc.MustUnmarshal(&kvReq, req.Operation.Payload)

	if req.Options.CreateTxnRecord {
		writer.WriteTxnRecord(req)
	}

	// kvReq.Start < shard.Start, only scan the data in current shard
	if len(kvReq.Start) == 0 ||
		bytes.Compare(kvReq.Start, shard.Start) < 0 {
		kvReq.Start = shard.Start
	}
	// kvReq.End > shard.End, only scan the data in current shard
	if len(kvReq.End) == 0 ||
		(len(shard.End) > 0 && bytes.Compare(kvReq.End, shard.End) > 0) {
		kvReq.End = shard.End
	}

	return writer.Scan(kvReq.Start, kvReq.End, func(key, value []byte) (bool, error) {
		writer.WriteDeleteUncommitted(keysutil.Clone(key))
		return true, nil
	})
}

func handleGet(shard metapb.Shard, req txnpb.TxnRequest, reader KVReader) ([]byte, error) {
	var kvReq rpcpb.KVGetRequest
	protoc.MustUnmarshal(&kvReq, req.Operation.Payload)
	v, err := reader.Get(kvReq.Key)
	if err != nil {
		return nil, err
	}
	return protoc.MustMarshal(&rpcpb.KVGetResponse{Value: v}), nil
}

func handleScan(shard metapb.Shard, req txnpb.TxnRequest, reader KVReader) ([]byte, error) {
	var kvReq rpcpb.KVScanRequest
	protoc.MustUnmarshal(&kvReq, req.Operation.Payload)

	// kvReq.Start < shard.Start, only scan the data in current shard
	if len(kvReq.Start) == 0 ||
		bytes.Compare(kvReq.Start, shard.Start) < 0 {
		kvReq.Start = shard.Start
	}
	// kvReq.End > shard.End, only scan the data in current shard
	if len(kvReq.End) == 0 ||
		(len(shard.End) > 0 && bytes.Compare(kvReq.End, shard.End) > 0) {
		kvReq.End = shard.End
	}
	if kvReq.Limit == 0 {
		kvReq.Limit = math.MaxUint64
	}
	if kvReq.LimitBytes == 0 {
		kvReq.LimitBytes = math.MaxUint64
	}

	var resp rpcpb.KVScanResponse
	buffer := reader.ByteBuf()
	n := uint64(0)
	bytes := uint64(0)
	skipByLimit := false
	var keys []buf.Slice
	var values []buf.Slice
	err := reader.Scan(kvReq.Start, kvReq.End, func(key, value []byte) (bool, error) {
		n++
		if kvReq.OnlyCount {
			return true, nil
		}

		buffer.MarkWrite()
		buf.MustWrite(buffer, key)
		keys = append(keys, buffer.WrittenDataAfterMark())

		bytes += uint64(len(key))
		if kvReq.WithValue {
			buffer.MarkWrite()
			buf.MustWrite(buffer, value)
			values = append(values, buffer.WrittenDataAfterMark())
			bytes += uint64(len(value))
		}

		if n >= kvReq.Limit ||
			bytes >= kvReq.LimitBytes {
			skipByLimit = true
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return nil, nil
	}

	if n == 0 || !skipByLimit {
		resp.Completed = true
	}

	resp.Count = n
	if !kvReq.OnlyCount {
		resp.Keys = make([][]byte, 0, len(keys))
		for idx := range keys {
			resp.Keys = append(resp.Keys, keys[idx].Data())
		}

		if kvReq.WithValue {
			resp.Values = make([][]byte, 0, len(values))
			for idx := range values {
				resp.Values = append(resp.Values, values[idx].Data())
			}
		}
	}

	resp.ShardEnd = shard.End
	return protoc.MustMarshal(&resp), nil
}
