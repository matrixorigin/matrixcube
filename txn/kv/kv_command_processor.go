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
	"fmt"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/txn"
	"github.com/matrixorigin/matrixcube/util/buf"
	keysutil "github.com/matrixorigin/matrixcube/util/keys"
)

var _ txn.TransactionCommandProcessor = (*kvBasedTxnCommandProcessor)(nil)

// TxnWriteOpHandleFunc used to handle transcation write operation
type TxnWriteOpHandleFunc func(metapb.Shard, txnpb.TxnRequest, KVReaderAndWriter) error

// TxnReadOpHandleFunc used to handle transcation read operation
type TxnReadOpHandleFunc func(metapb.Shard, txnpb.TxnRequest, KVReader) ([]byte, error)

var (
	prefixLen          = 1
	normalValuePrefix  = []byte{byte(1)}
	deletedValuePrefix = []byte{byte(0)}
)

// KVWriter kv write
type KVWriter interface {
	// WriteUncommitted write the uncommitted data
	WriteUncommitted(key, value []byte)
	// WriteDeleteUncommitted write the uncommitted data for delete the key
	WriteDeleteUncommitted(key []byte)
	// WriteTxnRecord write txn record
	WriteTxnRecord(req txnpb.TxnRequest)
}

// KVReader kv reader
type KVReader interface {
	// ByteBuf returns the byte buffer
	ByteBuf() *buf.ByteBuf
	// Get the key
	Get([]byte) ([]byte, error)
	// Scan scan all keys in the range [start, end)
	Scan(start, end []byte, handler func(key, value []byte) (bool, error)) error
}

// KVReaderAndWriter kv reader and writer
type KVReaderAndWriter interface {
	KVReader
	KVWriter
}

type kvReaderAndWriter struct {
	buffer      *buf.ByteBuf
	txn         txnpb.TxnOpMeta
	req         rpcpb.KVBatchMixedWriteRequest
	uncommitted txn.UncommittedDataTree
	reader      txn.TransactionDataReader
}

func (w *kvReaderAndWriter) ByteBuf() *buf.ByteBuf {
	return w.buffer
}

func (w *kvReaderAndWriter) Get(key []byte) ([]byte, error) {
	v, err := w.reader.Read(key, w.txn, w.uncommitted)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}
	return v[prefixLen:], nil
}

func (w *kvReaderAndWriter) Scan(start, end []byte, handler func(key, value []byte) (bool, error)) error {
	return w.reader.ReadRange(start, end, w.txn, func(originKey, data []byte) (bool, error) {
		if bytes.Equal(data[:prefixLen], normalValuePrefix) {
			return handler(originKey, data[prefixLen:])
		}
		return true, nil
	}, w.uncommitted)
}

func (w *kvReaderAndWriter) encodeValue(value []byte, prefix []byte) []byte {
	return keysutil.Join(prefix, value)
}

func (w *kvReaderAndWriter) WriteUncommitted(key, value []byte) {
	w.writeUncommitted(key, w.encodeValue(value, normalValuePrefix))
}

func (w *kvReaderAndWriter) writeUncommitted(key, value []byte) {
	// current txn has no uncommitted, direct write
	if w.uncommitted == nil {
		w.writeNewUncommitted(key, value)
		return
	}

	// override curernt uncommitted data
	oldUncommitted, ok := w.uncommitted.Get(key)
	if !ok {
		w.writeNewUncommitted(key, value)
		w.uncommitted.Add(key, txnpb.TxnUncommittedMVCCMetadata{
			TxnMeta:   w.txn.TxnMeta,
			Timestamp: w.txn.WriteTimestamp,
			Sequence:  w.txn.Sequence,
		})
		return
	}

	if oldUncommitted.Timestamp.Equal(w.txn.WriteTimestamp) {
		// only update the value
		w.write(keysutil.EncodeTxnMVCCKey(key, w.txn.WriteTimestamp, w.buffer, false), value)
	} else if oldUncommitted.Timestamp.Less(w.txn.WriteTimestamp) {
		// update old uncommitted mvccmetadata, remove old mvcc version, add new version
		w.delete(keysutil.EncodeTxnMVCCKey(key, oldUncommitted.Timestamp, w.buffer, false))
		w.writeNewUncommitted(key, value)

		oldUncommitted.TxnMeta = w.txn.TxnMeta
		oldUncommitted.Timestamp = w.txn.WriteTimestamp
		oldUncommitted.Sequence = w.txn.Sequence
		w.uncommitted.Add(key, oldUncommitted)
	} else {
		panic(fmt.Sprintf("old uncommitted timestamp %s > current timestamp %s, cannot perform handle write",
			oldUncommitted.Timestamp.String(),
			w.txn.WriteTimestamp.String()))
	}
}

func (w *kvReaderAndWriter) WriteDeleteUncommitted(key []byte) {
	w.writeUncommitted(key, deletedValuePrefix)
}

func (w *kvReaderAndWriter) WriteTxnRecord(req txnpb.TxnRequest) {
	w.write(keysutil.EncodeTxnRecordKey(w.txn.TxnRecordRouteKey, w.txn.ID, w.buffer, false),
		getTxnRecord(w.txn.TxnMeta, req))
}

func (w *kvReaderAndWriter) writeNewUncommitted(key, value []byte) {
	w.write(key, protoc.MustMarshal(&txnpb.TxnUncommittedMVCCMetadata{
		TxnMeta:   w.txn.TxnMeta,
		Timestamp: w.txn.WriteTimestamp,
		Sequence:  w.txn.Sequence,
	}))
	w.write(keysutil.EncodeTxnMVCCKey(key, w.txn.WriteTimestamp, w.buffer, false), value)
}

func (w *kvReaderAndWriter) write(key []byte, value []byte) {
	w.req.Requests = append(w.req.Requests, rpcpb.KVMixedWriteRequest{
		CmdType: uint64(rpcpb.CmdKVSet),
		Set: rpcpb.KVSetRequest{
			Key:   key,
			Value: value,
		},
	})
}

func (w *kvReaderAndWriter) delete(key []byte) {
	w.req.Requests = append(w.req.Requests, rpcpb.KVMixedWriteRequest{
		CmdType: uint64(rpcpb.CmdKVDelete),
		Delete: rpcpb.KVDeleteRequest{
			Key: key,
		},
	})
}

func getTxnRecord(txn txnpb.TxnMeta, req txnpb.TxnRequest) []byte {
	record := txnpb.TxnRecord{
		TxnMeta:       txn,
		Status:        txnpb.TxnStatus_Pending,
		LastHeartbeat: txn.WriteTimestamp,
	}
	record.CompletedWrites = make(map[uint64]txnpb.KeySet, 1)
	record.CompletedWrites[req.Operation.ShardGroup] = req.Operation.Impacted
	return protoc.MustMarshal(&record)
}

// RegisterTxnCommandProcessor extended `TransactionCommandProcessor` to provide registration
// of transaction read and write processing functions.
type RegisterTxnCommandProcessor interface {
	txn.TransactionCommandProcessor

	// RegisterWrite register transaction write operation handle func
	RegisterWrite(uint32, TxnWriteOpHandleFunc)
	// RegisterRead register transaction read operation handle func
	RegisterRead(uint32, TxnReadOpHandleFunc)
}

type kvBasedTxnCommandProcessor struct {
	writesProcessors map[uint32]TxnWriteOpHandleFunc
	readProcessors   map[uint32]TxnReadOpHandleFunc
}

// NewKVTxnCommandProcessor returns a kv based transactional command executor based on the
// underlying kv store, with the following supported operations:
// 1. Set/BatchSet
// 2. Delete/BatchDelete/RangeDelete
// 3. Scan
// 4. Get/BatchGet
func NewKVTxnCommandProcessor() RegisterTxnCommandProcessor {
	p := &kvBasedTxnCommandProcessor{
		writesProcessors: make(map[uint32]TxnWriteOpHandleFunc),
		readProcessors:   make(map[uint32]TxnReadOpHandleFunc),
	}
	p.RegisterWrite(uint32(rpcpb.CmdKVSet), handleSet)
	p.RegisterWrite(uint32(rpcpb.CmdKVBatchSet), handleBatchSet)
	p.RegisterWrite(uint32(rpcpb.CmdKVDelete), handleDelete)
	p.RegisterWrite(uint32(rpcpb.CmdKVBatchDelete), handleBatchDelete)
	p.RegisterWrite(uint32(rpcpb.CmdKVRangeDelete), handleRangeDelete)

	p.RegisterRead(uint32(rpcpb.CmdKVGet), handleGet)
	p.RegisterRead(uint32(rpcpb.CmdKVBatchGet), handleBatchGet)
	p.RegisterRead(uint32(rpcpb.CmdKVScan), handleScan)
	return p
}

func (kp *kvBasedTxnCommandProcessor) RegisterWrite(op uint32, p TxnWriteOpHandleFunc) {
	if _, ok := kp.writesProcessors[op]; ok {
		panic(fmt.Sprintf("already register write processor %d", op))
	}
	kp.writesProcessors[op] = p
}

func (kp *kvBasedTxnCommandProcessor) RegisterRead(op uint32, p TxnReadOpHandleFunc) {
	if _, ok := kp.readProcessors[op]; ok {
		panic(fmt.Sprintf("already register read processor %d", op))
	}
	kp.readProcessors[op] = p
}

func (kp *kvBasedTxnCommandProcessor) HandleWrite(shard metapb.Shard, txn txnpb.TxnOpMeta,
	requests []txnpb.TxnRequest,
	reader txn.TransactionDataReader,
	uncommitted txn.UncommittedDataTree) (txnpb.ConsensusData, error) {

	buffer := buf.NewByteBuf(256)
	defer buffer.Release()

	rw := &kvReaderAndWriter{
		buffer:      buffer,
		txn:         txn,
		uncommitted: uncommitted,
		reader:      reader,
	}
	for _, req := range requests {
		p, ok := kp.writesProcessors[req.Operation.Op]
		if !ok {
			panic(fmt.Sprintf("missing %d txn write processor", req.Operation.Op))
		}
		if err := p(shard, req, rw); err != nil {
			return txnpb.ConsensusData{}, err
		}
	}
	return txnpb.ConsensusData{
		RequestType: uint64(rpcpb.CmdKVBatchMixedWrite),
		Data:        protoc.MustMarshal(&rw.req),
	}, nil
}

func (kp *kvBasedTxnCommandProcessor) HandleRead(shard metapb.Shard, txn txnpb.TxnOpMeta, req txnpb.TxnRequest,
	reader txn.TransactionDataReader,
	uncommitted txn.UncommittedDataTree) ([]byte, error) {

	buffer := buf.NewByteBuf(256)
	defer buffer.Release()

	rw := &kvReaderAndWriter{
		buffer:      buffer,
		txn:         txn,
		uncommitted: uncommitted,
		reader:      reader,
	}

	p, ok := kp.readProcessors[req.Operation.Op]
	if !ok {
		panic(fmt.Sprintf("missing %d txn read processor", req.Operation.Op))
	}
	return p(shard, req, rw)
}
