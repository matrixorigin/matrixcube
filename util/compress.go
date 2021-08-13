// Copyright 2020 MatrixOrigin.
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

package util

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"strings"

	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/vfs"
)

var (
	logger = log.NewLoggerWithPrefix("[util]")
)

// GZIP compress a path to a gzip file
func GZIP(fs vfs.FS, path string) error {
	file, err := fs.Open(path)
	if err != nil {
		return err
	}

	dest := fmt.Sprintf("%s.gz", path)
	d, _ := fs.Create(dest)
	defer d.Close()
	gw := gzip.NewWriter(d)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	return compress(fs, file, fs.PathDir(path), tw)
}

// UnGZIP ungip file
func UnGZIP(fs vfs.FS, file string, dest string) error {
	return deCompress(fs, file, dest)
}

func compress(fs vfs.FS, file vfs.File, prefix string, tw *tar.Writer) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.IsDir() {
		prefix = prefix + "/" + info.Name()
		files, err := fs.List(prefix)
		if err != nil {
			return err
		}
		for _, fi := range files {
			f, err := fs.Open(fs.PathJoin(prefix, fi))
			if err != nil {
				return err
			}
			err = compress(fs, f, prefix, tw)
			if err != nil {
				return err
			}
		}
	} else {
		header, err := tar.FileInfoHeader(info, "")
		header.Name = prefix + "/" + header.Name
		if err != nil {
			return err
		}
		err = tw.WriteHeader(header)
		if err != nil {
			return err
		}
		_, err = io.Copy(tw, file)
		file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func deCompress(fs vfs.FS, tarFile, dest string) error {
	srcFile, err := fs.Open(tarFile)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	gr, err := gzip.NewReader(srcFile)
	if err != nil {
		return err
	}
	defer gr.Close()
	tr := tar.NewReader(gr)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}
		filename := dest + hdr.Name
		file, err := createFile(fs, filename)
		if err != nil {
			return err
		}
		io.Copy(file, tr)
	}
	return nil
}

func createFile(fs vfs.FS, name string) (vfs.File, error) {
	err := fs.MkdirAll(string([]rune(name)[0:strings.LastIndex(name, "/")]), 0755)
	if err != nil {
		return nil, err
	}
	return fs.Create(name)
}
