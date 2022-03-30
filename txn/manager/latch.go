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
	"sort"
	"sync"

	"github.com/google/btree"
)

type Latch struct {
	*sync.Cond
	Key     []byte
	Readers [][]byte // transaction ids
	Writers [][]byte // transaction ids
}

var _ btree.Item = new(Latch)

func (i *Latch) Less(than btree.Item) bool {
	return bytes.Compare(i.Key, than.(*Latch).Key) < 0
}

func (t *TxnManager) lockRead(transactionID []byte, key []byte) (unlock func()) {

	latch := t.getLatch(key)

	latch.L.Lock()
	// wait writers
	for len(latch.Writers) > 0 {
		latch.Wait()
	}

	// set tx id in latch readers
	i := sort.Search(len(latch.Readers), func(i int) bool {
		return bytes.Compare(key, latch.Readers[i]) < 0
	})
	if i < len(latch.Readers) {
		if bytes.Equal(key, latch.Readers[i]) {
			// already set
		} else {
			// insert
			latch.Readers = append(latch.Readers[:i],
				append([][]byte{key}, latch.Readers[i:]...)...)
		}
	} else {
		// append
		latch.Readers = append(latch.Readers, key)
	}

	latch.L.Unlock()
	latch.Broadcast()

	return func() {
		// unset tx id in latch readers
		latch.L.Lock()
		i := sort.Search(len(latch.Readers), func(i int) bool {
			return bytes.Compare(key, latch.Readers[i]) < 0
		})
		if i < len(latch.Readers) {
			if bytes.Equal(key, latch.Readers[i]) {
				// remove
				latch.Readers = append(latch.Readers[:i],
					latch.Readers[i+1:]...)
			}
		}
		latch.L.Unlock()
		latch.Broadcast()
	}
}

func (t *TxnManager) lockWrite(transactionID []byte, key []byte) (unlock func()) {

	latch := t.getLatch(key)

	latch.L.Lock()
	// wait writers
	for len(latch.Writers) > 0 {
		latch.Wait()
	}
	// wait readers
	for len(latch.Readers) > 0 {
		latch.Wait()
	}

	// set tx id in latch writers
	i := sort.Search(len(latch.Writers), func(i int) bool {
		return bytes.Compare(key, latch.Writers[i]) < 0
	})
	if i < len(latch.Writers) {
		if bytes.Equal(key, latch.Writers[i]) {
			// already set
		} else {
			// insert
			latch.Writers = append(latch.Writers[:i],
				append([][]byte{key}, latch.Writers[i:]...)...)
		}
	} else {
		// append
		latch.Writers = append(latch.Writers, key)
	}

	latch.L.Unlock()
	latch.Broadcast()

	return func() {
		// unset tx id in latch writers
		latch.L.Lock()
		i := sort.Search(len(latch.Writers), func(i int) bool {
			return bytes.Compare(key, latch.Writers[i]) < 0
		})
		if i < len(latch.Writers) {
			if bytes.Equal(key, latch.Writers[i]) {
				// remove
				latch.Writers = append(latch.Writers[:i],
					latch.Writers[i+1:]...)
			}
		}
		latch.L.Unlock()
		latch.Broadcast()
	}

}

func (t *TxnManager) getLatch(key []byte) *Latch {
	t.Lock()
	latch := &Latch{
		Key: key,
	}
	item := t.latches.Get(latch)
	if item != nil {
		latch = item.(*Latch)
	} else {
		latch.Cond = sync.NewCond(new(sync.Mutex))
		t.latches.ReplaceOrInsert(latch)
	}
	t.Unlock()
	return latch
}
