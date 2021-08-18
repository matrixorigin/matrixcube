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

package raftstore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/vfs"
	"golang.org/x/time/rate"
)

type defaultSnapshotManager struct {
	sync.RWMutex

	wg               sync.WaitGroup
	stopC            chan struct{}
	limiter          *rate.Limiter
	s                *store
	dir              string
	registry         map[string]struct{}
	receiveSnapCount uint64
}

func newDefaultSnapshotManager(s *store) snapshot.SnapshotManager {
	fs := s.cfg.FS
	dir := s.cfg.SnapshotDir()
	if !exist(fs, dir) {
		if err := fs.MkdirAll(dir, 0750); err != nil {
			logger.Fatalf("cannot create snapshot dir %s failed with %+v",
				dir,
				err)
		}
	}

	m := &defaultSnapshotManager{
		stopC: make(chan struct{}),
		limiter: rate.NewLimiter(rate.Every(time.Second/time.Duration(s.cfg.Snapshot.MaxConcurrencySnapChunks)),
			int(s.cfg.Snapshot.MaxConcurrencySnapChunks)),
		dir:      dir,
		s:        s,
		registry: make(map[string]struct{}),
	}

	m.wg.Add(1)
	snapshotDirName := path.Base(dir)
	go func() {
		interval := 2 * time.Hour
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			logger.Infof("start scan gc snap files")

			var paths []string
			files, err := fs.List(dir)
			if err != nil {
				panic(err)
			}
			for _, cur := range files {
				fi, err := fs.Stat(fs.PathJoin(dir, cur))
				if err != nil {
					panic(err)
				}

				if fi.IsDir() && fi.Name() == snapshotDirName {
					continue
				}

				now := time.Now()
				if now.Sub(fi.ModTime()) > interval {
					paths = append(paths, fs.PathJoin(dir, cur))
				}
			}

			for _, path := range paths {
				err := fs.RemoveAll(path)
				if err != nil {
					logger.Errorf("scan snap file %s failed with %+v",
						path,
						err)
				}
			}

			select {
			case <-ticker.C:
				continue
			case <-m.stopC:
				m.wg.Done()
				return
			}
		}
	}()

	return m
}

func formatKey(msg *bhraftpb.SnapshotMessage) string {
	return fmt.Sprintf("%d_%d_%d", msg.Header.Shard.ID, msg.Header.Term, msg.Header.Index)
}

func formatKeyStep(msg *bhraftpb.SnapshotMessage, step int) string {
	return fmt.Sprintf("%s_%d", formatKey(msg), step)
}

func (m *defaultSnapshotManager) Close() {
	close(m.stopC)
	m.wg.Wait()
}

func (m *defaultSnapshotManager) getPathOfSnapKey(msg *bhraftpb.SnapshotMessage) string {
	return fmt.Sprintf("%s/%s", m.dir, formatKey(msg))
}

func (m *defaultSnapshotManager) getPathOfSnapKeyGZ(msg *bhraftpb.SnapshotMessage) string {
	return fmt.Sprintf("%s.gz", m.getPathOfSnapKey(msg))
}

func (m *defaultSnapshotManager) getTmpPathOfSnapKeyGZ(msg *bhraftpb.SnapshotMessage) string {
	return fmt.Sprintf("%s.tmp", m.getPathOfSnapKey(msg))
}

func (m *defaultSnapshotManager) Register(msg *bhraftpb.SnapshotMessage, step int) bool {
	m.Lock()
	defer m.Unlock()

	fkey := formatKeyStep(msg, step)

	if _, ok := m.registry[fkey]; ok {
		return false
	}

	m.registry[fkey] = struct{}{}
	return true
}

func (m *defaultSnapshotManager) Deregister(msg *bhraftpb.SnapshotMessage, step int) {
	m.Lock()
	defer m.Unlock()

	fkey := formatKeyStep(msg, step)
	delete(m.registry, fkey)
}

func (m *defaultSnapshotManager) Create(msg *bhraftpb.SnapshotMessage) error {
	path := m.getPathOfSnapKey(msg)
	gzPath := m.getPathOfSnapKeyGZ(msg)
	start := encStartKey(&msg.Header.Shard)
	end := encEndKey(&msg.Header.Shard)
	db := m.s.DataStorageByGroup(msg.Header.Shard.Group, msg.Header.Shard.ID)
	fs := m.s.cfg.FS

	if !exist(fs, gzPath) {
		if !exist(fs, path) {
			err := db.CreateSnapshot(path, start, end)
			if err != nil {
				return err
			}

			if m.s.cfg.Customize.CustomSnapshotDataCreateFuncFactory != nil {
				if fn := m.s.cfg.Customize.CustomSnapshotDataCreateFuncFactory(msg.Header.Shard.Group); fn != nil {
					err := fn(path, msg.Header.Shard)
					if err != nil {
						return err
					}
				}
			}
		}
		err := util.GZIP(fs, path)
		if err != nil {
			return err
		}
	}

	info, err := fs.Stat(fmt.Sprintf("%s.gz", path))
	if err != nil {
		return err
	}

	metric.ObserveSnapshotBytes(info.Size())
	return nil
}

func (m *defaultSnapshotManager) Exists(msg *bhraftpb.SnapshotMessage) bool {
	file := m.getPathOfSnapKeyGZ(msg)
	fs := m.s.cfg.FS
	return exist(fs, file)
}

func (m *defaultSnapshotManager) WriteTo(msg *bhraftpb.SnapshotMessage, conn goetty.IOSession) (uint64, error) {
	file := m.getPathOfSnapKeyGZ(msg)

	if !m.Exists(msg) {
		return 0, fmt.Errorf("missing snapshot file: %s", file)
	}

	fs := m.s.cfg.FS
	info, err := fs.Stat(file)
	if err != nil {
		return 0, err
	}
	fileSize := info.Size()

	f, err := fs.Open(file)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var written int64
	buf := make([]byte, m.s.cfg.Snapshot.SnapChunkSize)
	ctx := context.TODO()

	logger.Infof("shard %d try to send snap, header=<%s>,size=<%d>",
		msg.Header.Shard.ID,
		msg.Header.String(),
		fileSize)

	for {
		nr, er := f.Read(buf)
		if nr > 0 {
			dst := &bhraftpb.SnapshotMessage{}
			dst.Header = msg.Header
			dst.Data = buf[0:nr]
			dst.FileSize = uint64(fileSize)
			dst.First = written == 0
			dst.Last = fileSize == written+int64(nr)

			written += int64(nr)
			err := m.limiter.Wait(ctx)
			if err != nil {
				return 0, err
			}

			err = conn.WriteAndFlush(dst)
			if err != nil {
				return 0, err
			}
		}
		if er != nil {
			if er != io.EOF {
				return 0, er
			}
			break
		}
	}

	logger.Infof("shard %d send snap complete",
		msg.Header.Shard.ID)
	return uint64(written), nil
}

func (m *defaultSnapshotManager) CleanSnap(msg *bhraftpb.SnapshotMessage) error {
	var err error

	fs := m.s.cfg.FS
	tmpFile := m.getTmpPathOfSnapKeyGZ(msg)
	if exist(fs, tmpFile) {
		logger.Infof("shard %d delete exists snap tmp file %s, header is %s",
			msg.Header.Shard.ID,
			tmpFile,
			msg.Header.String())
		err = fs.RemoveAll(tmpFile)
	}

	if err != nil {
		return err
	}

	file := m.getPathOfSnapKeyGZ(msg)
	if exist(fs, file) {
		logger.Infof("shard %d delete exists snap gz file %s, header is %s",
			msg.Header.Shard.ID,
			file,
			msg.Header.String())
		err = fs.RemoveAll(file)
	}

	if err != nil {
		return err
	}

	dir := m.getPathOfSnapKey(msg)
	if exist(fs, dir) {
		logger.Infof("shard %d delete exists snap dir, file=<%s>, header=<%s>",
			msg.Header.Shard.ID,
			dir,
			msg.Header.String())
		err = fs.RemoveAll(dir)
	}

	return err
}

func (m *defaultSnapshotManager) ReceiveSnapData(msg *bhraftpb.SnapshotMessage) error {
	var err error
	var f vfs.File

	if msg.First {
		m.Lock()
		m.receiveSnapCount++
		m.Unlock()
		err = m.cleanTmp(msg)
	}

	if err != nil {
		return err
	}

	fs := m.s.cfg.FS
	file := m.getTmpPathOfSnapKeyGZ(msg)
	if exist(fs, file) {
		f, err = fs.OpenForAppend(file)
		if err != nil {
			f.Close()
			return err
		}
	} else {
		f, err = fs.Create(file)
		if err != nil {
			f.Close()
			return err
		}
	}

	n, err := f.Write(msg.Data)
	if err != nil {
		f.Close()
		return err
	}

	if n != len(msg.Data) {
		f.Close()
		return fmt.Errorf("write snapshot file failed, expect=<%d> actual=<%d>",
			len(msg.Data),
			n)
	}

	f.Close()

	if msg.Last {
		m.Lock()
		if m.receiveSnapCount > 0 {
			m.receiveSnapCount--
		}
		m.Unlock()
		return m.check(msg)
	}

	return nil
}

func (m *defaultSnapshotManager) Apply(msg *bhraftpb.SnapshotMessage) error {
	file := m.getPathOfSnapKeyGZ(msg)
	if !m.Exists(msg) {
		return fmt.Errorf("missing snapshot file, path=%s", file)
	}

	defer m.CleanSnap(msg)

	err := util.UnGZIP(m.s.cfg.FS, file, m.dir)
	if err != nil {
		return err
	}
	dir := m.getPathOfSnapKey(msg)
	defer os.RemoveAll(dir)

	// apply snapshot of data
	err = m.s.DataStorageByGroup(msg.Header.Shard.Group, msg.Header.Shard.ID).ApplySnapshot(dir)
	if err != nil {
		return err
	}

	if m.s.cfg.Customize.CustomSnapshotDataApplyFuncFactory != nil {
		if fn := m.s.cfg.Customize.CustomSnapshotDataApplyFuncFactory(msg.Header.Shard.Group); fn != nil {
			err := fn(dir, msg.Header.Shard)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *defaultSnapshotManager) ReceiveSnapCount() uint64 {
	m.RLock()
	defer m.RUnlock()
	return m.receiveSnapCount
}

func (m *defaultSnapshotManager) cleanTmp(msg *bhraftpb.SnapshotMessage) error {
	var err error
	tmpFile := m.getTmpPathOfSnapKeyGZ(msg)
	fs := m.s.cfg.FS
	if exist(fs, tmpFile) {
		logger.Infof("shard %d delete exists snap tmp file, file=<%s>, header=<%s>",
			msg.Header.Shard.ID,
			tmpFile,
			msg.Header.String())
		err = fs.RemoveAll(tmpFile)
	}

	if err != nil {
		return err
	}

	return nil
}

func (m *defaultSnapshotManager) check(msg *bhraftpb.SnapshotMessage) error {
	file := m.getTmpPathOfSnapKeyGZ(msg)
	fs := m.s.cfg.FS
	if exist(fs, file) {
		info, err := fs.Stat(file)
		if err != nil {
			return err
		}

		if msg.FileSize != uint64(info.Size()) {
			return fmt.Errorf("snap file size not match, got=<%d> expect=<%d> path=<%s>",
				info.Size(),
				msg.FileSize,
				file)
		}

		return fs.Rename(file, m.getPathOfSnapKeyGZ(msg))
	}

	return fmt.Errorf("missing snapshot file, path=%s", file)
}

func exist(fs vfs.FS, name string) bool {
	_, err := fs.Stat(name)
	if err == nil {
		return true
	}
	if vfs.IsNotExist(err) {
		return false
	}
	panic(err)
}
