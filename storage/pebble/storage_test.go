package pebble

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

var (
	tmpDir = "/tmp/cube/storage/pebble"
)

func TestSync(t *testing.T) {
	recreateTestTempDir(tmpDir)
	opts := pebble.Options{FS: vfs.NewPebbleFS(vfs.Default)}
	opts.DisableWAL = true

	path := filepath.Join(util.GetTestDir(), "storage/pebble")
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

func recreateTestTempDir(tmpDir string) {
	os.RemoveAll(tmpDir)
	os.MkdirAll(tmpDir, 0755)
}
