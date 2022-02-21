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
package codec

import (
	"testing"

	"github.com/fagongzi/goetty/buf"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/stretchr/testify/assert"
)

func TestCodec(t *testing.T) {
	se, sd := NewServerCodec(buf.MB)
	ce, cd := NewClientCodec(buf.MB)
	buf := buf.NewByteBuf(32)

	req := &rpcpb.ProphetRequest{ID: 1}
	resp := &rpcpb.ProphetResponse{ID: 1}

	assert.NoError(t, se.Encode(resp, buf), "TestCodec failed")
	completed, data, err := cd.Decode(buf)
	assert.NoError(t, err, "TestCodec failed")
	assert.True(t, completed, "TestCodec failed")
	assert.Equal(t, resp.ID, data.(*rpcpb.ProphetResponse).ID, "TestCodec failed")

	buf.Clear()
	assert.NoError(t, ce.Encode(req, buf), "TestCodec failed")
	completed, data, err = sd.Decode(buf)
	assert.NoError(t, err, "TestCodec failed")
	assert.True(t, completed, "TestCodec failed")
	assert.Equal(t, resp.ID, data.(*rpcpb.ProphetRequest).ID, "TestCodec failed")
}
