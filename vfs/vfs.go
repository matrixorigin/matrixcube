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

package vfs

import (
	"github.com/cockroachdb/errors/oserror"
	pvfs "github.com/lni/vfs"
)

// File is a vfs file type
type File = pvfs.File

// FS is a vfs type
type FS = pvfs.FS

// Default is the default vfs suppose to be used in production. It is directly
// backed by the underlying operating system's file system.
var Default = pvfs.Default

// NewMemFS returns a new memory based FS implementation that can be used in
// tests. You are not suppose to use this in production as nothing is
// persistently stored.
//
// Ad a "strict" memory-backed FS implementation, in short changes after the
// last Sync() will be lost after ResetToSyncedState() is called. For more
// detailed descriptions on its behaviour, see
// https://github.com/cockroachdb/pebble/blob/master/vfs/vfs.go
func NewMemFS() FS {
	return pvfs.NewStrictMem()
}

// IsNotExist returns a boolean flag indicating whether the returned error is
// for file or directory not exist error.
func IsNotExist(err error) bool {
	return oserror.IsNotExist(err)
}
