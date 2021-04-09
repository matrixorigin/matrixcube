package raftstore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/deepfabric/beehive/metric"
	"github.com/deepfabric/beehive/pb/bhraftpb"
	"github.com/deepfabric/beehive/snapshot"
	"github.com/deepfabric/beehive/util"
	"github.com/fagongzi/goetty"
	"golang.org/x/time/rate"
)

type defaultSnapshotManager struct {
	sync.RWMutex

	limiter          *rate.Limiter
	s                *store
	dir              string
	registry         map[string]struct{}
	receiveSnapCount uint64
}

func newDefaultSnapshotManager(s *store) snapshot.SnapshotManager {
	dir := s.cfg.SnapshotDir()
	if !exist(dir) {
		if err := os.MkdirAll(dir, 0750); err != nil {
			logger.Fatalf("cannot create snapshot dir %s failed with %+v",
				dir,
				err)
		}
	}

	snapshotDirName := path.Base(dir)
	go func() {
		interval := time.Hour * 2

		for {
			logger.Infof("start scan gc snap files")

			var paths []string
			err := filepath.Walk(dir, func(path string, f os.FileInfo, err error) error {
				if f == nil {
					return nil
				}

				if f.IsDir() && f.Name() == snapshotDirName {
					return nil
				}

				var skip error
				if f.IsDir() && f.Name() != snapshotDirName {
					skip = filepath.SkipDir
				}

				now := time.Now()
				if now.Sub(f.ModTime()) > interval {
					paths = append(paths, path)
				}

				return skip
			})

			if err != nil {
				logger.Errorf("scan snap file failed with %+v",
					err)
			}

			for _, path := range paths {
				err := os.RemoveAll(path)
				if err != nil {
					logger.Errorf("scan snap file %s failed with %+v",
						path,
						err)
				}
			}

			time.Sleep(interval)
		}
	}()

	return &defaultSnapshotManager{
		limiter: rate.NewLimiter(rate.Every(time.Second/time.Duration(s.cfg.Snapshot.MaxConcurrencySnapChunks)),
			int(s.cfg.Snapshot.MaxConcurrencySnapChunks)),
		dir:      dir,
		s:        s,
		registry: make(map[string]struct{}),
	}
}

func formatKey(msg *bhraftpb.SnapshotMessage) string {
	return fmt.Sprintf("%d_%d_%d", msg.Header.Shard.ID, msg.Header.Term, msg.Header.Index)
}

func formatKeyStep(msg *bhraftpb.SnapshotMessage, step int) string {
	return fmt.Sprintf("%s_%d", formatKey(msg), step)
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

	if !exist(gzPath) {
		if !exist(path) {
			err := db.CreateSnapshot(path, start, end)
			if err != nil {
				return err
			}

			if m.s.cfg.Customize.CustomSnapshotDataCreateFunc != nil {
				err := m.s.cfg.Customize.CustomSnapshotDataCreateFunc(path, msg.Header.Shard)
				if err != nil {
					return err
				}
			}
		}
		err := util.GZIP(path)
		if err != nil {
			return err
		}
	}

	info, err := os.Stat(fmt.Sprintf("%s.gz", path))
	if err != nil {
		return err
	}

	metric.ObserveSnapshotBytes(info.Size())
	return nil
}

func (m *defaultSnapshotManager) Exists(msg *bhraftpb.SnapshotMessage) bool {
	file := m.getPathOfSnapKeyGZ(msg)
	return exist(file)
}

func (m *defaultSnapshotManager) WriteTo(msg *bhraftpb.SnapshotMessage, conn goetty.IOSession) (uint64, error) {
	file := m.getPathOfSnapKeyGZ(msg)

	if !m.Exists(msg) {
		return 0, fmt.Errorf("missing snapshot file: %s", file)
	}

	info, err := os.Stat(file)
	if err != nil {
		return 0, err
	}
	fileSize := info.Size()

	f, err := os.Open(file)
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

	tmpFile := m.getTmpPathOfSnapKeyGZ(msg)
	if exist(tmpFile) {
		logger.Infof("shard %d delete exists snap tmp file %s, header is %s",
			msg.Header.Shard.ID,
			tmpFile,
			msg.Header.String())
		err = os.RemoveAll(tmpFile)
	}

	if err != nil {
		return err
	}

	file := m.getPathOfSnapKeyGZ(msg)
	if exist(file) {
		logger.Infof("shard %d delete exists snap gz file %s, header is %s",
			msg.Header.Shard.ID,
			file,
			msg.Header.String())
		err = os.RemoveAll(file)
	}

	if err != nil {
		return err
	}

	dir := m.getPathOfSnapKey(msg)
	if exist(dir) {
		logger.Infof("shard %d delete exists snap dir, file=<%s>, header=<%s>",
			msg.Header.Shard.ID,
			dir,
			msg.Header.String())
		err = os.RemoveAll(dir)
	}

	return err
}

func (m *defaultSnapshotManager) ReceiveSnapData(msg *bhraftpb.SnapshotMessage) error {
	var err error
	var f *os.File

	if msg.First {
		m.Lock()
		m.receiveSnapCount++
		m.Unlock()
		err = m.cleanTmp(msg)
	}

	if err != nil {
		return err
	}

	file := m.getTmpPathOfSnapKeyGZ(msg)
	if exist(file) {
		f, err = os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			f.Close()
			return err
		}
	} else {
		f, err = os.Create(file)
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

	err := util.UnGZIP(file, m.dir)
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

	if m.s.cfg.Customize.CustomSnapshotDataApplyFunc != nil {
		err := m.s.cfg.Customize.CustomSnapshotDataApplyFunc(dir, msg.Header.Shard)
		if err != nil {
			return err
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
	if exist(tmpFile) {
		logger.Infof("shard %d delete exists snap tmp file, file=<%s>, header=<%s>",
			msg.Header.Shard.ID,
			tmpFile,
			msg.Header.String())
		err = os.RemoveAll(tmpFile)
	}

	if err != nil {
		return err
	}

	return nil
}

func (m *defaultSnapshotManager) check(msg *bhraftpb.SnapshotMessage) error {
	file := m.getTmpPathOfSnapKeyGZ(msg)
	if exist(file) {
		info, err := os.Stat(file)
		if err != nil {
			return err
		}

		if msg.FileSize != uint64(info.Size()) {
			return fmt.Errorf("snap file size not match, got=<%d> expect=<%d> path=<%s>",
				info.Size(),
				msg.FileSize,
				file)
		}

		return os.Rename(file, m.getPathOfSnapKeyGZ(msg))
	}

	return fmt.Errorf("missing snapshot file, path=%s", file)
}

func exist(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}
