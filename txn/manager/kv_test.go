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

package txnmanager

import (
	"testing"

	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	txnkv "github.com/matrixorigin/matrixcube/txn/kv"
	"github.com/matrixorigin/matrixcube/vfs"
)

func TestKVTxnManager(t *testing.T) {

	fs := vfs.GetTestFS()
	kvStorage := mem.NewStorage()
	baseStorage := kv.NewBaseStorage(kvStorage, fs)
	storage := kv.NewKVDataStorage(baseStorage, executor.NewKVExecutor(baseStorage)).(storage.TransactionalDataStorage)

	cmdProcessor := txnkv.NewKVTxnCommandProcessor()

	router := raftstore.NewMockRouter()
	handler := func(req rpcpb.Request) (resp rpcpb.ResponseBatch, err error) {
		return
	}
	shardsProxy, err := raftstore.NewMockShardsProxy(router, handler)
	if err != nil {
		t.Fatal(err)
	}

	manager := NewTxnManager(
		storage,
		cmdProcessor,
		shardsProxy,
	)
	_ = manager

	//FIXME test

}
