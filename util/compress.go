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

	"github.com/matrixorigin/matrixcube/vfs"
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

	return compress(fs, file, path, "", tw)
}

// UnGZIP ungip file
func UnGZIP(fs vfs.FS, file string, dest string) error {
	return deCompress(fs, file, dest)
}

// both file and filePath are provided here as we can no longer assume that the
// full path of the specified file is still accessible from the file object. we
// do need the full path info to list the directory when file is a directory.
func compress(fs vfs.FS, file vfs.File, filePath string, prefix string, tw *tar.Writer) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.IsDir() {
		prefix = prefix + "/" + info.Name()
		files, err := fs.List(filePath)
		if err != nil {
			return err
		}
		for _, fi := range files {
			path := fs.PathJoin(filePath, fi)
			f, err := fs.Open(path)
			if err != nil {
				return err
			}
			err = compress(fs, f, path, prefix, tw)
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
