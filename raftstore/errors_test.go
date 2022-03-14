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

package raftstore

import (
	"errors"

	"testing"

	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestBuildID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		batch      *rpcpb.ResponseBatch
		id, expect []byte
	}{
		{
			batch:  &rpcpb.ResponseBatch{},
			id:     []byte("id1"),
			expect: nil,
		},
		{
			batch:  &rpcpb.ResponseBatch{Header: rpcpb.ResponseBatchHeader{Error: errorpb.Error{Message: "error2"}}},
			id:     []byte("id2"),
			expect: []byte("id2"),
		},
		{
			batch:  &rpcpb.ResponseBatch{Header: rpcpb.ResponseBatchHeader{ID: []byte("id2"), Error: errorpb.Error{Message: "error3"}}},
			id:     []byte("id3"),
			expect: []byte("id2"),
		},
	}

	for i, c := range cases {
		buildID(c.id, c.batch)
		assert.Equal(t, c.expect, c.batch.Header.ID, "index %d", i)
	}
}

func TestErrorOtherCMDResp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		err   error
		batch rpcpb.ResponseBatch
	}{
		{
			err:   errors.New("error1"),
			batch: rpcpb.ResponseBatch{Header: rpcpb.ResponseBatchHeader{Error: errorpb.Error{Message: "error1"}}},
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.batch, errorOtherCMDResp(c.err), "index %d", i)
	}
}

func TestErrorPbResp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		id  []byte
		err errorpb.Error
	}{
		{
			id: []byte("id1"),
			err: errorpb.Error{
				Message: errNotLeader.Error(),
				NotLeader: &errorpb.NotLeader{
					ShardID: 1,
					Leader:  metapb.Replica{ID: 1, StoreID: 1},
				},
			},
		},
	}

	for i, c := range cases {
		b := errorPbResp(c.id, c.err)
		assert.Equal(t, c.id, b.Header.ID, "index %d", i)
		assert.Equal(t, c.err, b.Header.Error, "index %d", i)
	}
}

func TestErrorStaleCMDResp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		id  []byte
		err errorpb.Error
	}{
		{
			id: []byte("id1"),
			err: errorpb.Error{
				Message:      errStaleCMD.Error(),
				StaleCommand: infoStaleCMD,
			},
		},
	}

	for i, c := range cases {
		b := errorStaleCMDResp(c.id)
		assert.Equal(t, c.id, b.Header.ID, "index %d", i)
		assert.Equal(t, c.err, b.Header.Error, "index %d", i)
	}
}

func TestErrorStaleEpochResp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		id     []byte
		shards []metapb.Shard
		err    errorpb.Error
	}{
		{
			id:     []byte("id1"),
			shards: []metapb.Shard{{ID: 1}},
			err: errorpb.Error{
				Message: errStaleCMD.Error(),
				StaleEpoch: &errorpb.StaleEpoch{
					NewShards: []metapb.Shard{{ID: 1}},
				},
			},
		},
		{
			id:     []byte("id2"),
			shards: []metapb.Shard{{ID: 1}, {ID: 2}},
			err: errorpb.Error{
				Message: errStaleCMD.Error(),
				StaleEpoch: &errorpb.StaleEpoch{
					NewShards: []metapb.Shard{{ID: 1}, {ID: 2}},
				},
			},
		},
	}

	for i, c := range cases {
		b := errorStaleEpochResp(c.id, c.shards...)
		assert.Equal(t, c.id, b.Header.ID, "index %d", i)
		assert.Equal(t, c.err, b.Header.Error, "index %d", i)
	}
}

func TestErrorBaseResp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		id    []byte
		batch rpcpb.ResponseBatch
	}{
		{
			id:    []byte("id1"),
			batch: rpcpb.ResponseBatch{Header: rpcpb.ResponseBatchHeader{ID: []byte("id1")}},
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.batch, errorBaseResp(c.id), "index %d", i)
	}
}

func TestCheckKeyInShard(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		key     []byte
		shard   Shard
		checker func(t assert.TestingT, object interface{}, msgAndArgs ...interface{}) bool
	}{
		{
			key:     []byte("a"),
			shard:   Shard{ID: 1},
			checker: assert.Nil,
		},
		{
			key:     []byte("a"),
			shard:   Shard{ID: 1, Start: []byte("b")},
			checker: assert.NotNil,
		},
		{
			key:     []byte("b"),
			shard:   Shard{ID: 1, Start: []byte("a"), End: []byte("b")},
			checker: assert.NotNil,
		},
	}

	for i, c := range cases {
		c.checker(t, checkKeyInShard(c.key, c.shard), "index %d", i)
	}
}
