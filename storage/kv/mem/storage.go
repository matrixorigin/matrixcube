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

package mem

import (
	cpebble "github.com/cockroachdb/pebble"

	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/vfs"
)

// NewStorage returns a pebble based storage backed by in-mem fs. All
// Key-Value paris will be stored in memory while snapshot related data will be
// stored in the filesystem specified in the input parameter fs.
func NewStorage(fs vfs.FS) *pebble.Storage {
	memfs := vfs.NewMemFS()
	opts := &cpebble.Options{
		FS: vfs.NewPebbleFS(memfs),
	}
	s, err := pebble.NewStorage("test-data", fs, opts)
	if err != nil {
		panic(err)
	}
	return s
}
