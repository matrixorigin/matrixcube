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

package util

// WriteBatch write batch
type WriteBatch interface {
	// Set set kv-value to the batch
	Set([]byte, []byte)
	// Delete add delete key to the batch
	Delete([]byte)
	// Reset reset the batch
	Reset()
	// Close close the batch
	Close()
}
