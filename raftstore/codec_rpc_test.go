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
	"testing"

	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestDecodeResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		resp rpcpb.Response
	}{
		{
			resp: rpcpb.Response{ID: []byte("1"), Value: []byte("v1")},
		},
		{
			resp: rpcpb.Response{ID: []byte("2"), Value: []byte("v2")},
		},
		{
			resp: rpcpb.Response{ID: []byte("3"), Value: []byte("v3")},
		},
	}

	rc := &rpcCodec{clientSide: true}
	for i, c := range cases {
		func() {
			buf := buf.NewByteBuf(32)
			defer buf.Release()

			buf.Write(protoc.MustMarshal(&c.resp))
			buf.MarkIndex(buf.GetWriteIndex())
			ok, v, err := rc.Decode(buf)
			assert.NoError(t, err, "index %d", i)
			assert.True(t, ok, "index %d", i)
			assert.Equal(t, c.resp, v, "index %d", i)
		}()
	}
}

func TestDecodeRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		resp rpcpb.Request
	}{
		{
			resp: rpcpb.Request{ID: []byte("1"), Cmd: []byte("v1")},
		},
		{
			resp: rpcpb.Request{ID: []byte("2"), Cmd: []byte("v2")},
		},
		{
			resp: rpcpb.Request{ID: []byte("3"), Cmd: []byte("v3")},
		},
	}

	rc := &rpcCodec{clientSide: false}
	for i, c := range cases {
		func() {
			buf := buf.NewByteBuf(32)
			defer buf.Release()

			buf.Write(protoc.MustMarshal(&c.resp))
			buf.MarkIndex(buf.GetWriteIndex())
			ok, v, err := rc.Decode(buf)
			assert.NoError(t, err, "index %d", i)
			assert.True(t, ok, "index %d", i)
			assert.Equal(t, c.resp, v, "index %d", i)
		}()
	}
}
