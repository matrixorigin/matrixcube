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

// NewStorage returns a pebble based storage backed by an in-memory fs.
func NewStorage() *pebble.Storage {
	opts := &cpebble.Options{
		FS: vfs.NewPebbleFS(vfs.NewMemFS()),
	}
	s, err := pebble.NewStorage("test-data", nil, opts)
	if err != nil {
		panic(err)
	}
	return s
}
