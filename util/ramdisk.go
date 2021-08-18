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
	"os"
	"runtime"
)

const (
	// to create and mount the ramdisk
	// sudo diskutil erasevolume HFS+ "cube" `hdiutil attach -nomount ram://1048576`
	// to unmount the ramdisk
	// sudo diskutil unmountDisk /Volumes/cube
	ramDiskTestDir = "/Volumes/cube"
	defaultTestDir = "/tmp/cube"
)

// IsDarwin returns whether running on Darwin.
func IsDarwin() bool {
	return runtime.GOOS == "darwin"
}

// RAMDiskDirExist returns whether the specified directory is a mounted RAM
// disk,
func RAMDiskDirExist(dir string) bool {
	// TODO: add vfs support when vfs PR is merged
	// TODO: for now we just check whether the specified folder exist
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		panic(err)
	}
	return true
}

// GetTestDir returns the data directory to use in tests.
func GetTestDir() string {
	if IsDarwin() && RAMDiskDirExist(ramDiskTestDir) {
		return ramDiskTestDir
	}
	return defaultTestDir
}
