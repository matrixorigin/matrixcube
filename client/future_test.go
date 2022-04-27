package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReuse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	f := acquireFuture()
	f.cancel = func() {}
	f.ctx = ctx
	f.done([]byte("k1"), nil, nil)
	v, err := f.Get()
	assert.NoError(t, err)
	assert.Equal(t, "k1", string(v))
	f.Close()

	f = acquireFuture()
	f.cancel = func() {}
	f.ctx = ctx
	f.done([]byte("k2"), nil, nil)
	v, err = f.Get()
	assert.NoError(t, err)
	assert.Equal(t, "k2", string(v))
}
