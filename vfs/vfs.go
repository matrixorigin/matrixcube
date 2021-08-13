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

package vfs

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors/oserror"
	pbvfs "github.com/cockroachdb/pebble/vfs"
	pvfs "github.com/lni/vfs"
)

// File is a vfs file type
type File = pvfs.File

// FS is a vfs type
type FS = pvfs.FS

// Default is the default vfs suppose to be used in production. It is directly
// backed by the underlying operating system's file system.
var Default = pvfs.Default

// NewMemFS returns a new memory based FS implementation that can be used in
// tests. You are not suppose to use this in production as nothing is
// persistently stored.
//
// Ad a "strict" memory-backed FS implementation, in short changes after the
// last Sync() will be lost after ResetToSyncedState() is called. For more
// detailed descriptions on its behaviour, see
// https://github.com/cockroachdb/pebble/blob/master/vfs/vfs.go
func NewMemFS() FS {
	return pvfs.NewStrictMem()
}

// GetTestFS creates and returns a FS instance to be used in tests. A FS backed
// by memory is returned when the MEMFS environmental variable is set, or it
// returns a regular Default FS backed by underlying operating system's file
// system.
func GetTestFS() FS {
	if _, ok := os.LookupEnv("MEMFS_TEST"); ok {
		return NewMemFS()
	}
	return Default
}

// PebbleFS is a wrapper struct that implements the pebble/vfs.FS interface.
type PebbleFS struct {
	fs FS
}

var _ pbvfs.FS = (*PebbleFS)(nil)

// NewPebbleFS creates a new pebble/vfs.FS instance.
func NewPebbleFS(fs FS) pbvfs.FS {
	return &PebbleFS{fs}
}

// GetFreeSpace ...
func (p *PebbleFS) GetFreeSpace(path string) (uint64, error) {
	return p.fs.GetFreeSpace(path)
}

// Create ...
func (p *PebbleFS) Create(name string) (pbvfs.File, error) {
	return p.fs.Create(name)
}

// Link ...
func (p *PebbleFS) Link(oldname, newname string) error {
	return p.fs.Link(oldname, newname)
}

// Open ...
func (p *PebbleFS) Open(name string, opts ...pbvfs.OpenOption) (pbvfs.File, error) {
	f, err := p.fs.Open(name)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt.Apply(f)
	}
	return f, nil
}

// OpenDir ...
func (p *PebbleFS) OpenDir(name string) (pbvfs.File, error) {
	return p.fs.OpenDir(name)
}

// Remove ...
func (p *PebbleFS) Remove(name string) error {
	return p.fs.Remove(name)
}

// RemoveAll ...
func (p *PebbleFS) RemoveAll(name string) error {
	return p.fs.RemoveAll(name)
}

// Rename ...
func (p *PebbleFS) Rename(oldname, newname string) error {
	return p.fs.Rename(oldname, newname)
}

// ReuseForWrite ...
func (p *PebbleFS) ReuseForWrite(oldname, newname string) (pbvfs.File, error) {
	return p.fs.ReuseForWrite(oldname, newname)
}

// MkdirAll ...
func (p *PebbleFS) MkdirAll(dir string, perm os.FileMode) error {
	return p.fs.MkdirAll(dir, perm)
}

// Lock ...
func (p *PebbleFS) Lock(name string) (io.Closer, error) {
	return p.fs.Lock(name)
}

// List ...
func (p *PebbleFS) List(dir string) ([]string, error) {
	return p.fs.List(dir)
}

// Stat ...
func (p *PebbleFS) Stat(name string) (os.FileInfo, error) {
	return p.fs.Stat(name)
}

// PathBase ...
func (p *PebbleFS) PathBase(path string) string {
	return p.fs.PathBase(path)
}

// PathJoin ...
func (p *PebbleFS) PathJoin(elem ...string) string {
	return p.fs.PathJoin(elem...)
}

// PathDir ...
func (p *PebbleFS) PathDir(path string) string {
	return p.fs.PathDir(path)
}

// IsNotExist returns a boolean value indicating whether the specified error is
// to indicate that a file or directory does not exist.
func IsNotExist(err error) bool {
	return oserror.IsNotExist(err)
}

// IsExist returns a boolean value indicating whether the specified error is to
// indicate that a file or directory already exists.
func IsExist(err error) bool {
	return oserror.IsExist(err)
}

// TempDir returns the directory use for storing temporary files.
func TempDir() string {
	return os.TempDir()
}

// Clean is a wrapper for filepath.Clean.
func Clean(dir string) string {
	return filepath.Clean(dir)
}

// ReportLeakedFD reports leaked file fds.
func ReportLeakedFD(fs FS, t *testing.T) {
	pvfs.ReportLeakedFD(fs, t)
}
