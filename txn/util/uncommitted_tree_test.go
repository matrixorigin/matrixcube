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

package util

import (
	"testing"

	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/stretchr/testify/assert"
)

func TestAddAnGet(t *testing.T) {
	k := []byte("k1")
	tree := NewUncommittedTree()
	tree.Add(k, txnpb.TxnUncommittedMVCCMetadata{TxnMeta: txnpb.TxnMeta{ID: k}})
	assert.Equal(t, 1, tree.Len())

	v, ok := tree.Get(k)
	assert.True(t, ok)
	assert.Equal(t, txnpb.TxnUncommittedMVCCMetadata{TxnMeta: txnpb.TxnMeta{ID: k}}, v)

	_, ok = tree.Get([]byte("k2"))
	assert.False(t, ok)
}

func TestLen(t *testing.T) {
	k := []byte("k1")
	tree := NewUncommittedTree()
	tree.Add(k, txnpb.TxnUncommittedMVCCMetadata{})
	assert.Equal(t, 1, tree.Len())
	tree.Add(k, txnpb.TxnUncommittedMVCCMetadata{})
	assert.Equal(t, 1, tree.Len())
	tree.Add([]byte("k2"), txnpb.TxnUncommittedMVCCMetadata{})
	assert.Equal(t, 2, tree.Len())
}

func TestAscendRange(t *testing.T) {
	k1 := []byte("k1")
	k2 := []byte("k2")
	k3 := []byte("k3")
	tree := NewUncommittedTree()
	tree.Add(k1, txnpb.TxnUncommittedMVCCMetadata{})
	tree.Add(k2, txnpb.TxnUncommittedMVCCMetadata{})
	tree.Add(k3, txnpb.TxnUncommittedMVCCMetadata{})

	n := 0
	assert.NoError(t, tree.AscendRange(k1, k3, func(key []byte, v txnpb.TxnUncommittedMVCCMetadata) (bool, error) {
		n++
		return true, nil
	}))
	assert.Equal(t, 2, n)

	n = 0
	assert.NoError(t, tree.AscendRange(k1, k3, func(key []byte, v txnpb.TxnUncommittedMVCCMetadata) (bool, error) {
		n++
		return false, nil
	}))
	assert.Equal(t, 1, n)
}
