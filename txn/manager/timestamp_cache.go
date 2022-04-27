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
	"bytes"

	"github.com/google/btree"
	"github.com/matrixorigin/matrixcube/util/hlc"
)

type TSCache struct {
	Key  []byte
	Time hlc.Timestamp
}

var _ btree.Item = new(TSCache)

func (t *TSCache) Less(than btree.Item) bool {
	return bytes.Compare(t.Key, than.(*TSCache).Key) < 0
}

func (t *TxnManager) SetTSCache(key []byte, ts hlc.Timestamp) {
	t.Lock()
	defer t.Unlock()
	cache := &TSCache{
		Key: key,
	}
	item := t.tsCache.Get(cache)
	if item != nil {
		cache = item.(*TSCache)
	}
	if ts.Greater(cache.Time) {
		cache.Time = ts
	}
	t.tsCache.ReplaceOrInsert(cache)
}

func (t *TxnManager) IsStaleWrite(key []byte, ts hlc.Timestamp) bool {
	t.Lock()
	defer t.Unlock()
	cache := &TSCache{
		Key: key,
	}
	item := t.tsCache.Get(cache)
	if item == nil {
		return false
	}
	cache = item.(*TSCache)
	return ts.LessEq(cache.Time)
}
