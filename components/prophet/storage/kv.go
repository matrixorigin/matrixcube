// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
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

package storage

// Batch batch opts
type Batch struct {
	// SaveKeys save opts
	SaveKeys []string
	// SaveValues save opts
	SaveValues []string
	// RemoveKeys remove keys
	RemoveKeys []string
}

// KV is an abstract interface for load/save prophet cluster data.
type KV interface {
	// Batch do batch
	Batch(batch *Batch) error
	// Save save key-value paire to storage
	Save(key, value string) error
	// Load load data of key
	Load(key string) (string, error)
	// Remove delete key from storage
	Remove(key string) error
	// LoadRange iterates all key-value pairs in the storage
	LoadRange(key, endKey string, limit int64) ([]string, []string, error)
	// SaveIfNotExists put the value at path
	// returns true, nil, nil if created
	// returns false, exists, nil if not created
	SaveIfNotExists(key string, value string, batch *Batch) (bool, string, error)
}
