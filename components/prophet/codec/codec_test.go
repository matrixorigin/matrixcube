package codec

import (
	"testing"

	"github.com/fagongzi/goetty/buf"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/stretchr/testify/assert"
)

func TestCodec(t *testing.T) {
	se, sd := NewServerCodec(buf.MB)
	ce, cd := NewClientCodec(buf.MB)
	buf := buf.NewByteBuf(32)

	req := &rpcpb.Request{ID: 1}
	resp := &rpcpb.Response{ID: 1}

	assert.NoError(t, se.Encode(resp, buf), "TestCodec failed")
	completed, data, err := cd.Decode(buf)
	assert.NoError(t, err, "TestCodec failed")
	assert.True(t, completed, "TestCodec failed")
	assert.Equal(t, resp.ID, data.(*rpcpb.Response).ID, "TestCodec failed")

	buf.Clear()
	assert.NoError(t, ce.Encode(req, buf), "TestCodec failed")
	completed, data, err = sd.Decode(buf)
	assert.NoError(t, err, "TestCodec failed")
	assert.True(t, completed, "TestCodec failed")
	assert.Equal(t, resp.ID, data.(*rpcpb.Request).ID, "TestCodec failed")
}
