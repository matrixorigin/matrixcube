package pebble

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

func TestSync(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)

	path := filepath.Join(util.GetTestDir(), "pebble", fmt.Sprintf("%d", time.Now().UnixNano()))
	fs.RemoveAll(path)
	fs.MkdirAll(path, 0755)
	opts := pebble.Options{}
	opts.DisableWAL = true
	opts.FS = vfs.NewPebbleFS(vfs.GetTestFS())

	s, err := NewStorage(path, &opts)
	assert.NoError(t, err)

	k := []byte("k")
	v := []byte("v")
	assert.NoError(t, s.Set(k, v))

	s.Close()
	s, err = NewStorage(path, &opts)
	assert.NoError(t, err)
	d, err := s.Get(k)
	assert.NoError(t, err)
	assert.Empty(t, d)
	assert.NoError(t, s.Set(k, v))
	assert.NoError(t, s.Sync())

	s.Close()
	s, err = NewStorage(path, &opts)
	assert.NoError(t, err)
	d, err = s.Get(k)
	assert.NoError(t, err)
	assert.Equal(t, v, d)
}
