// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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

package snapshot

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/matrixorigin/matrixcube/util/fileutil"
	"github.com/matrixorigin/matrixcube/vfs"
)

var (
	// ErrSnapshotOutOfDate is the error to indicate that snapshot is out of date.
	ErrSnapshotOutOfDate = errors.New("snapshot out of date")
	// MetadataFilename is the filename of a snapshot's metadata file.
	MetadataFilename = "snapshot.metadata"
	// SnapshotFileSuffix is the filename suffix of a snapshot file.
	SnapshotFileSuffix = "gbsnap"
	// SnapshotDirNameRe is the regex of snapshot names.
	SnapshotDirNameRe = regexp.MustCompile(`^snapshot-[0-9A-F]+$`)
	// SnapshotDirNamePartsRe is used to find the index value from snapshot folder name.
	SnapshotDirNamePartsRe = regexp.MustCompile(`^snapshot-([0-9A-F]+)$`)
	// GenSnapshotDirNameRe is the regex of temp snapshot directory name used when
	// generating snapshots.
	GenSnapshotDirNameRe = regexp.MustCompile(`^snapshot-[0-9A-F]+\.generating$`)
	// RecvSnapshotDirNameRe is the regex of temp snapshot directory name used when
	// receiving snapshots from remote NodeHosts.
	RecvSnapshotDirNameRe = regexp.MustCompile(`^snapshot-[0-9A-F]+-[0-9A-F]+\.receiving$`)
)

var (
	genTmpDirSuffix  = "generating"
	recvTmpDirSuffix = "receiving"
)

var finalizeLock sync.Mutex

// SnapshotDirFunc is the function type that returns the snapshot dir
// for the specified replica.
type SnapshotDirFunc func(shardID uint64, replicaID uint64) string

// Mode is the snapshot env mode.
type Mode uint64

const (
	// CreatingMode is the mode used when taking snapshotting.
	CreatingMode Mode = iota
	// ReceivingMode is the mode used when receiving snapshots from remote nodes.
	ReceivingMode
	// ProcessingMode is the mode used when processing existing snapshots.
	ProcessingMode
)

// GetSnapshotDirName returns the snapshot dir name for the snapshot captured
// at the specified index.
func GetSnapshotDirName(index uint64) string {
	return getSnapshotDirName(index)
}

func getSuffix(mode Mode) string {
	if mode == CreatingMode {
		return genTmpDirSuffix
	} else if mode == ReceivingMode {
		return recvTmpDirSuffix
	}
	panic("unknown mode")
}

func mustBeChild(parent string, child string) {
	if v, err := filepath.Rel(parent, child); err != nil {
		panic(err)
	} else {
		if len(v) == 0 || strings.Contains(v, string(filepath.Separator)) ||
			strings.HasPrefix(v, ".") {
			panic(fmt.Sprintf("not a direct child, %s", v))
		}
	}
}

func getSnapshotDirName(index uint64) string {
	return fmt.Sprintf("snapshot-%016X", index)
}

func getTempReceivingDirName(rootDir string, index uint64, from uint64) string {
	dir := fmt.Sprintf("%s-%d.%s",
		getSnapshotDirName(index), from, getSuffix(ReceivingMode))
	return filepath.Join(rootDir, dir)
}

func getTempCreatingDirName(rootDir string) string {
	dir := fmt.Sprintf("snapshot.%s", getSuffix(CreatingMode))
	return filepath.Join(rootDir, dir)
}

func getFinalDirName(rootDir string, index uint64) string {
	return filepath.Join(rootDir, getSnapshotDirName(index))
}

// SSEnv is the struct used to manage involved directories for taking or
// receiving snapshots.
type SSEnv struct {
	fs vfs.FS
	// rootDir is the parent of all snapshot tmp/final dirs for a specified
	// raft node
	rootDir        string
	index          uint64
	from           uint64
	indexFinalized bool
	mode           Mode
}

// NewSSEnv creates and returns a new SSEnv instance.
func NewSSEnv(f SnapshotDirFunc,
	shardID uint64, replicaID uint64, index uint64,
	from uint64, mode Mode, fs vfs.FS) SSEnv {
	rootDir := f(shardID, replicaID)
	return SSEnv{
		index:   index,
		mode:    mode,
		rootDir: rootDir,
		fs:      fs,
	}
}

func (se *SSEnv) FinalizeIndex(index uint64) {
	if se.mode != CreatingMode {
		panic("not in creating mode")
	}
	se.index = index
	se.indexFinalized = true
}

// GetTempDir returns the temp snapshot directory.
func (se *SSEnv) GetTempDir() string {
	if se.mode == CreatingMode {
		return getTempCreatingDirName(se.rootDir)
	} else if se.mode == ReceivingMode {
		return getTempReceivingDirName(se.rootDir, se.index, se.from)
	}
	panic("unknown mode")
}

// GetFinalDir returns the final snapshot directory.
func (se *SSEnv) GetFinalDir() string {
	if se.mode == CreatingMode && !se.indexFinalized {
		panic("index not finlaized when creating snapshot")
	}
	return getFinalDirName(se.rootDir, se.index)
}

// GetRootDir returns the root directory. The temp and final snapshot
// directories are children of the root directory.
func (se *SSEnv) GetRootDir() string {
	return se.rootDir
}

// RemoveTempDir removes the temp snapshot directory.
func (se *SSEnv) RemoveTempDir() error {
	return se.removeDir(se.GetTempDir())
}

// MustRemoveTempDir removes the temp snapshot directory and panic if there
// is any error.
func (se *SSEnv) MustRemoveTempDir() {
	if err := se.removeDir(se.GetTempDir()); err != nil {
		exist, cerr := fileutil.DirExist(se.GetTempDir(), se.fs)
		if cerr != nil || exist {
			panic(err)
		}
	}
}

// FinalizeSnapshot finalizes the snapshot.
func (se *SSEnv) FinalizeSnapshot(msg fileutil.Marshaler) error {
	finalizeLock.Lock()
	defer finalizeLock.Unlock()
	if err := se.createFlagFile(msg); err != nil {
		return err
	}
	if se.finalDirExists() {
		return ErrSnapshotOutOfDate
	}
	return se.renameToFinalDir()
}

// CreateTempDir creates the temp snapshot directory.
func (se *SSEnv) CreateTempDir() error {
	return se.createDir(se.GetTempDir())
}

// RemoveFinalDir removes the final snapshot directory.
func (se *SSEnv) RemoveFinalDir() error {
	return se.removeDir(se.GetFinalDir())
}

// SaveSSMetadata saves the metadata of the snapshot file.
func (se *SSEnv) SaveSSMetadata(msg fileutil.Marshaler) error {
	return fileutil.CreateFlagFile(se.GetTempDir(), MetadataFilename, msg, se.fs)
}

// HasFlagFile returns a boolean flag indicating whether the flag file is
// available in the final directory.
func (se *SSEnv) HasFlagFile() bool {
	fp := se.fs.PathJoin(se.GetFinalDir(), fileutil.SnapshotFlagFilename)
	_, err := se.fs.Stat(fp)
	return !vfs.IsNotExist(err)
}

// RemoveFlagFile removes the flag file from the final directory.
func (se *SSEnv) RemoveFlagFile() error {
	return fileutil.RemoveFlagFile(se.GetFinalDir(),
		fileutil.SnapshotFlagFilename, se.fs)
}

func (se *SSEnv) createDir(dir string) error {
	mustBeChild(se.rootDir, dir)
	return fileutil.Mkdir(dir, se.fs)
}

func (se *SSEnv) removeDir(dir string) error {
	mustBeChild(se.rootDir, dir)
	if err := se.fs.RemoveAll(dir); err != nil {
		return err
	}
	return fileutil.SyncDir(se.rootDir, se.fs)
}

func (se *SSEnv) finalDirExists() bool {
	_, err := se.fs.Stat(se.GetFinalDir())
	return !vfs.IsNotExist(err)
}

func (se *SSEnv) renameToFinalDir() error {
	if err := se.fs.Rename(se.GetTempDir(), se.GetFinalDir()); err != nil {
		return err
	}
	return fileutil.SyncDir(se.rootDir, se.fs)
}

func (se *SSEnv) createFlagFile(msg fileutil.Marshaler) error {
	return fileutil.CreateFlagFile(se.GetTempDir(),
		fileutil.SnapshotFlagFilename, msg, se.fs)
}
