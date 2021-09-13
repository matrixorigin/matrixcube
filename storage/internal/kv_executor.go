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

package internal

import (
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/storage"
)

type simpleKVCommandExecutor struct {
	kv storage.KVStorage
}

// NewSimpleKVCommandExecutor returns a simple kv command executor to support set/get command
func NewSimpleKVCommandExecutor(kv storage.KVStorage) storage.CommandExecutor {
	return &simpleKVCommandExecutor{kv: kv}
}

func (ce *simpleKVCommandExecutor) ExecuteWrite(storage.Context) ([]raftcmdpb.Response, error) {
	return nil, nil
}

func (ce *simpleKVCommandExecutor) ExecuteRead(storage.Context) ([]raftcmdpb.Response, error) {
	return nil, nil
}
