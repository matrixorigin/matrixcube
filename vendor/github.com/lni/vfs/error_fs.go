// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"errors"
	"io"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ErrInjected is an error artifically injected for testing fs error paths.
var ErrInjected = errors.New("injected error")

// Op is an enum describing the type of operation performed.
type Op int

const (
	// OpRead describes read operations.
	OpRead Op = iota
	// OpWrite describes write operations.
	OpWrite
	// OpSync describes the fsync operation.
	OpSync
)

// OnIndex constructs an injector that returns an error on
// the (n+1)-th invocation of its MaybeError function. It
// may be passed to Wrap to inject an error into an FS.
func OnIndex(index int32, op Op) *InjectIndex {
	return &InjectIndex{index: index, op: op}
}

// InjectIndex implements Injector, injecting an error at a specific index.
type InjectIndex struct {
	index int32
	op    Op
}

// Index returns the index at which the error will be injected.
func (ii *InjectIndex) Index() int32 { return atomic.LoadInt32(&ii.index) }

// SetIndex sets the index at which the error will be injected.
func (ii *InjectIndex) SetIndex(v int32) { atomic.StoreInt32(&ii.index, v) }

// MaybeError implements the Injector interface.
func (ii *InjectIndex) MaybeError(op Op) error {
	if ii.op != op {
		return nil
	}
	if atomic.AddInt32(&ii.index, -1) == -1 {
		return ErrInjected
	}
	return nil
}

// WithProbability returns a function that returns an error with the provided
// probability when passed op. It may be passed to Wrap to inject an error
// into an ErrFS with the provided probability. p should be within the range
// [0.0,1.0].
func WithProbability(op Op, p float64) Injector {
	mu := new(sync.Mutex)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	return injectorFunc(func(currOp Op) error {
		mu.Lock()
		defer mu.Unlock()
		if currOp == op && rnd.Float64() < p {
			return ErrInjected
		}
		return nil
	})
}

type injectorFunc func(Op) error

func (f injectorFunc) MaybeError(op Op) error { return f(op) }

// Injector injects errors into FS operations.
type Injector interface {
	MaybeError(Op) error
}

var _ FS = &ErrorFS{}

// FS implements vfs.FS, injecting errors into
// its operations.
type ErrorFS struct {
	fs  FS
	inj Injector
}

// Wrap wraps an existing vfs.FS implementation, returning a new
// vfs.FS implementation that shadows operations to the provided FS.
// It uses the provided Injector for deciding when to inject errors.
// If an error is injected, FS propagates the error instead of
// shadowing the operation.
func Wrap(fs FS, inj Injector) *ErrorFS {
	return &ErrorFS{
		fs:  fs,
		inj: inj,
	}
}

// WrapFile wraps an existing vfs.File, returning a new vfs.File that shadows
// operations to the provided vfs.File. It uses the provided Injector for
// deciding when to inject errors. If an error is injected, the file
// propagates the error instead of shadowing the operation.
func WrapFile(f File, inj Injector) File {
	return &errorFile{file: f, inj: inj}
}

// Unwrap returns the FS implementation underlying fs.
// See pebble/vfs.Root.
func (fs *ErrorFS) Unwrap() FS {
	return fs.fs
}

// Create implements FS.Create.
func (fs *ErrorFS) Create(name string) (File, error) {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return nil, err
	}
	f, err := fs.fs.Create(name)
	if err != nil {
		return nil, err
	}
	return &errorFile{f, fs.inj}, nil
}

// Link implements FS.Link.
func (fs *ErrorFS) Link(oldname, newname string) error {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return err
	}
	return fs.fs.Link(oldname, newname)
}

// Open implements FS.Open.
func (fs *ErrorFS) Open(name string, opts ...OpenOption) (File, error) {
	if err := fs.inj.MaybeError(OpRead); err != nil {
		return nil, err
	}
	f, err := fs.fs.Open(name)
	if err != nil {
		return nil, err
	}
	ef := &errorFile{f, fs.inj}
	for _, opt := range opts {
		opt.Apply(ef)
	}
	return ef, nil
}

// OpenDir implements FS.OpenDir.
func (fs *ErrorFS) OpenDir(name string) (File, error) {
	if err := fs.inj.MaybeError(OpRead); err != nil {
		return nil, err
	}
	f, err := fs.fs.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return &errorFile{f, fs.inj}, nil
}

// PathBase implements FS.PathBase.
func (fs *ErrorFS) PathBase(p string) string {
	return fs.fs.PathBase(p)
}

// PathDir implements FS.PathDir.
func (fs *ErrorFS) PathDir(p string) string {
	return fs.fs.PathDir(p)
}

// PathJoin implements FS.PathJoin.
func (fs *ErrorFS) PathJoin(elem ...string) string {
	return fs.fs.PathJoin(elem...)
}

// Remove implements FS.Remove.
func (fs *ErrorFS) Remove(name string) error {
	if _, err := fs.fs.Stat(name); os.IsNotExist(err) {
		return nil
	}

	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return err
	}
	return fs.fs.Remove(name)
}

// RemoveAll implements FS.RemoveAll.
func (fs *ErrorFS) RemoveAll(fullname string) error {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return err
	}
	return fs.fs.RemoveAll(fullname)
}

// Rename implements FS.Rename.
func (fs *ErrorFS) Rename(oldname, newname string) error {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return err
	}
	return fs.fs.Rename(oldname, newname)
}

// ReuseForWrite implements FS.ReuseForWrite.
func (fs *ErrorFS) ReuseForWrite(oldname, newname string) (File, error) {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return nil, err
	}
	return fs.fs.ReuseForWrite(oldname, newname)
}

// OpenForAppend implements FS.OpenForAppend.
func (fs *ErrorFS) OpenForAppend(name string) (File, error) {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return nil, err
	}
	return fs.fs.OpenForAppend(name)
}

// MkdirAll implements FS.MkdirAll.
func (fs *ErrorFS) MkdirAll(dir string, perm os.FileMode) error {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return err
	}
	return fs.fs.MkdirAll(dir, perm)
}

// Lock implements FS.Lock.
func (fs *ErrorFS) Lock(name string) (io.Closer, error) {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return nil, err
	}
	return fs.fs.Lock(name)
}

// List implements FS.List.
func (fs *ErrorFS) List(dir string) ([]string, error) {
	if err := fs.inj.MaybeError(OpRead); err != nil {
		return nil, err
	}
	return fs.fs.List(dir)
}

// Stat implements FS.Stat.
func (fs *ErrorFS) Stat(name string) (os.FileInfo, error) {
	if err := fs.inj.MaybeError(OpRead); err != nil {
		return nil, err
	}
	return fs.fs.Stat(name)
}

// GetFreeSpace implements FS.GetFreeSpace.
func (fs *ErrorFS) GetFreeSpace(path string) (uint64, error) {
	if err := fs.inj.MaybeError(OpRead); err != nil {
		return 0, err
	}
	return fs.fs.GetFreeSpace(path)
}

var _ File = &errorFile{}

// errorFile implements vfs.File. The interface is implemented on the pointer
// type to allow pointer equality comparisons.
type errorFile struct {
	file File
	inj  Injector
}

func (f *errorFile) Close() error {
	// We don't inject errors during close as those calls should never fail in
	// practice.
	return f.file.Close()
}

func (f *errorFile) Seek(offset int64, whence int) (int64, error) {
	return f.file.Seek(offset, whence)
}

func (f *errorFile) Read(p []byte) (int, error) {
	if err := f.inj.MaybeError(OpRead); err != nil {
		return 0, err
	}
	return f.file.Read(p)
}

func (f *errorFile) ReadAt(p []byte, off int64) (int, error) {
	if err := f.inj.MaybeError(OpRead); err != nil {
		return 0, err
	}
	return f.file.ReadAt(p, off)
}

func (f *errorFile) Write(p []byte) (int, error) {
	if err := f.inj.MaybeError(OpWrite); err != nil {
		return 0, err
	}
	return f.file.Write(p)
}

func (f *errorFile) WriteAt(p []byte, off int64) (int, error) {
	if err := f.inj.MaybeError(OpWrite); err != nil {
		return 0, err
	}
	return f.file.WriteAt(p, off)
}

func (f *errorFile) Stat() (os.FileInfo, error) {
	if err := f.inj.MaybeError(OpRead); err != nil {
		return nil, err
	}
	return f.file.Stat()
}

func (f *errorFile) Sync() error {
	if err := f.inj.MaybeError(OpSync); err != nil {
		return err
	}
	return f.file.Sync()
}
