// Copyright 2021 MatrixOrigin.
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

//go:build !matrixone_test
// +build !matrixone_test

package raftstore

import (
	"github.com/matrixorigin/matrixcube/logdb"
)

// types and methods defined here are only used in tests.

// LogDBGetter is the interface used for accessing the logdb instance.
// Note that unexpected changes to the state of LogDB might corrupt the entire
// MatrixOne cluster. This interface should only be used in tests.
type LogDBGetter interface {
	// GetLogDB returns the LogDB instance.
	GetLogDB() logdb.LogDB
}

// this should only be accessed by tests
// unexpected changes to the state of LogDB might corrupt the entire MatrixOne
// cluster.
func (s *store) GetLogDB() logdb.LogDB {
	panic("should not call this function")
}
