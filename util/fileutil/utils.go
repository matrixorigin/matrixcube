// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
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

// this file is adopted from https://github.com/lni/dragonboat

package fileutil

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"runtime"

	"github.com/cockroachdb/errors"

	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/vfs"
)

const (
	// DefaultFileMode is the default file mode for files generated.
	DefaultFileMode    = 0640
	defaultDirFileMode = 0750
	deleteFilename     = "DELETED.matrixcube"
)

var firstError = util.FirstError

var ws = errors.WithStack

type Marshaler interface {
	Marshal() ([]byte, error)
}

type Unmarshaler interface {
	Unmarshal(data []byte) error
}

// MustWrite writes the specified data to the input writer. It will panic if
// there is any error.
func MustWrite(w io.Writer, data []byte) {
	if _, err := w.Write(data); err != nil {
		panic(err)
	}
}

// DirExist returns whether the specified filesystem entry exists.
func DirExist(name string, fs vfs.FS) (result bool, err error) {
	if name == "." || name == "/" {
		return true, nil
	}
	f, err := fs.OpenDir(name)
	if err != nil && vfs.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() {
		err = firstError(err, ws(f.Close()))
	}()
	s, err := f.Stat()
	if err != nil {
		return false, ws(err)
	}
	if !s.IsDir() {
		panic("not a dir")
	}
	return true, nil
}

// Exist returns whether the specified filesystem entry exists.
func Exist(name string, fs vfs.FS) (bool, error) {
	if name == "." || name == "/" {
		return true, nil
	}
	_, err := fs.Stat(name)
	if err != nil && vfs.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// MkdirAll creates the specified dir along with any necessary parents.
func MkdirAll(dir string, fs vfs.FS) error {
	exist, err := DirExist(dir, fs)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}
	parent := fs.PathDir(dir)
	exist, err = DirExist(parent, fs)
	if err != nil {
		return err
	}
	if !exist {
		if err := MkdirAll(parent, fs); err != nil {
			return err
		}
	}
	return Mkdir(dir, fs)
}

// Mkdir creates the specified dir.
func Mkdir(dir string, fs vfs.FS) error {
	parent := fs.PathDir(dir)
	exist, err := DirExist(parent, fs)
	if err != nil {
		return err
	}
	if !exist {
		panic(fmt.Sprintf("%s doesn't exist when creating %s", parent, dir))
	}
	if err := fs.MkdirAll(dir, defaultDirFileMode); err != nil {
		return err
	}
	return SyncDir(parent, fs)
}

// SyncDir calls fsync on the specified directory.
func SyncDir(dir string, fs vfs.FS) (err error) {
	if runtime.GOOS == "windows" {
		return nil
	}
	if dir == "." {
		return nil
	}
	f, err := fs.OpenDir(dir)
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, ws(f.Close()))
	}()
	fileInfo, err := f.Stat()
	if err != nil {
		return ws(err)
	}
	if !fileInfo.IsDir() {
		panic("not a dir")
	}
	df, err := fs.OpenDir(vfs.Clean(dir))
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, ws(df.Close()))
	}()
	return ws(df.Sync())
}

func getHash(data []byte) []byte {
	h := md5.New()
	MustWrite(h, data)
	s := h.Sum(nil)
	return s[8:]
}

// CreateFlagFile creates a flag file in the specific location. The flag file
// contains the marshaled data of the specified protobuf message.
//
// CreateFlagFile is not atomic meaning you can end up having a file at
// fs.PathJoin(dir, filename) with partial or corrupted content when the machine
// crashes in the middle of this function call. Special care must be taken to
// handle such situation, see how CreateFlagFile is used by snapshot images as
// an example.
func CreateFlagFile(dir string,
	filename string, msg Marshaler, fs vfs.FS) (err error) {
	fp := fs.PathJoin(dir, filename)
	f, err := fs.Create(fp)
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, ws(f.Close()))
		err = firstError(err, SyncDir(dir, fs))
	}()
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	h := getHash(data)
	n, err := f.Write(h)
	if err != nil {
		return ws(err)
	}
	if n != len(h) {
		return ws(io.ErrShortWrite)
	}
	n, err = f.Write(data)
	if err != nil {
		return ws(err)
	}
	if n != len(data) {
		return ws(io.ErrShortWrite)
	}
	return ws(f.Sync())
}

// GetFlagFileContent gets the content of the flag file found in the specified
// location. The data of the flag file will be unmarshaled into the specified
// protobuf message.
func GetFlagFileContent(dir string,
	filename string, obj Unmarshaler, fs vfs.FS) (err error) {
	fp := fs.PathJoin(dir, filename)
	f, err := fs.Open(vfs.Clean(fp))
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, ws(f.Close()))
	}()
	data, err := io.ReadAll(f)
	if err != nil {
		return ws(err)
	}
	if len(data) < 8 {
		panic("corrupted flag file")
	}
	h := data[:8]
	buf := data[8:]
	expectedHash := getHash(buf)
	if !bytes.Equal(h, expectedHash) {
		panic("corrupted flag file content")
	}
	if err := obj.Unmarshal(buf); err != nil {
		panic(err)
	}
	return nil
}

// HasFlagFile returns a boolean value indicating whether flag file can be
// found in the specified location.
func HasFlagFile(dir string, filename string, fs vfs.FS) bool {
	fp := fs.PathJoin(dir, filename)
	fi, err := fs.Stat(fp)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		return false
	}
	return true
}

// RemoveFlagFile removes the specified flag file.
func RemoveFlagFile(dir string, filename string, fs vfs.FS) error {
	return fs.Remove(fs.PathJoin(dir, filename))
}

// IsDirMarkedAsDeleted returns a boolean flag indicating whether the specified
// directory has been marked as deleted.
func IsDirMarkedAsDeleted(dir string, fs vfs.FS) (bool, error) {
	return Exist(fs.PathJoin(dir, deleteFilename), fs)
}

// MarkDirAsDeleted marks the specified directory as deleted.
func MarkDirAsDeleted(dir string, msg Marshaler, fs vfs.FS) error {
	return CreateFlagFile(dir, deleteFilename, msg, fs)
}
