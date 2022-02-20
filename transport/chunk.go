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
//
// this file is adopted from github.com/lni/dragonboat

package transport

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/fagongzi/util/protoc"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/fileutil"
	"github.com/matrixorigin/matrixcube/vfs"
)

var (
	// ErrSnapshotOutOfDate is returned when the snapshot being received is
	// considered as out of date.
	ErrSnapshotOutOfDate            = errors.New("snapshot is out of date")
	gcIntervalTick           uint64 = 30
	snapshotChunkTimeoutTick uint64 = 900
	maxConcurrentSlot        uint64 = 128
)

var firstError = util.FirstError

func chunkKey(c meta.SnapshotChunk) string {
	return fmt.Sprintf("%d:%d:%d", c.ShardID, c.ReplicaID, c.Index)
}

type tracked struct {
	first meta.SnapshotChunk
	tick  uint64
	next  uint64
}

type ssLock struct {
	mu sync.Mutex
}

func (l *ssLock) lock() {
	l.mu.Lock()
}

func (l *ssLock) unlock() {
	l.mu.Unlock()
}

// Chunk managed on the receiving side
type Chunk struct {
	logger    *zap.Logger
	fs        vfs.FS
	dir       snapshot.SnapshotDirFunc
	onReceive func(meta.RaftMessageBatch)
	timeout   uint64
	tick      uint64
	gcTick    uint64

	mu struct {
		sync.Mutex
		tracked map[string]*tracked
		locks   map[string]*ssLock
	}
}

// NewChunk creates and returns a new snapshot chunks instance.
func NewChunk(logger *zap.Logger,
	onReceive func(meta.RaftMessageBatch),
	dir snapshot.SnapshotDirFunc, fs vfs.FS) *Chunk {
	c := &Chunk{
		logger:    logger,
		onReceive: onReceive,
		timeout:   snapshotChunkTimeoutTick,
		gcTick:    gcIntervalTick,
		dir:       dir,
		fs:        fs,
	}
	c.mu.tracked = make(map[string]*tracked)
	c.mu.locks = make(map[string]*ssLock)

	return c
}

// Add adds a received trunk to chunks.
func (c *Chunk) Add(chunk meta.SnapshotChunk) bool {
	key := chunkKey(chunk)
	lock := c.getSnapshotLock(key)
	lock.lock()
	defer lock.unlock()
	return c.addLocked(chunk)
}

// Tick moves the internal logical clock forward.
func (c *Chunk) Tick() {
	ct := atomic.AddUint64(&c.tick, 1)
	if ct%c.gcTick == 0 {
		c.gc()
	}
}

// Close closes the chunks instance.
func (c *Chunk) Close() {
	tracked := c.getTracked()
	for key, td := range tracked {
		func() {
			l := c.getSnapshotLock(key)
			l.lock()
			defer l.unlock()
			c.removeTempDir(td.first)
			c.reset(key)
		}()
	}
}

func (c *Chunk) gc() {
	tracked := c.getTracked()
	tick := c.getTick()
	for key, td := range tracked {
		func() {
			l := c.getSnapshotLock(key)
			l.lock()
			defer l.unlock()
			if tick-td.tick >= c.timeout {
				c.removeTempDir(td.first)
				c.reset(key)
			}
		}()
	}
}

func (c *Chunk) getTick() uint64 {
	return atomic.LoadUint64(&c.tick)
}

func (c *Chunk) reset(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetLocked(key)
}

func (c *Chunk) getTracked() map[string]*tracked {
	m := make(map[string]*tracked)
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range c.mu.tracked {
		m[k] = v
	}
	return m
}

func (c *Chunk) resetLocked(key string) {
	delete(c.mu.tracked, key)
}

func (c *Chunk) getSnapshotLock(key string) *ssLock {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.mu.locks[key]
	if !ok {
		l = &ssLock{}
		c.mu.locks[key] = l
	}
	return l
}

func (c *Chunk) isFull() bool {
	return uint64(len(c.mu.tracked)) >= maxConcurrentSlot
}

func (c *Chunk) record(chunk meta.SnapshotChunk) *tracked {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := chunkKey(chunk)
	td := c.mu.tracked[key]
	if chunk.ChunkID == 0 {
		c.logger.Debug("first snapshot chunk received",
			zap.String("key", chunkKey(chunk)))
		if td != nil {
			c.logger.Warn("removing unclaimed snapshot chunks",
				zap.String("key", key))
			c.removeTempDir(td.first)
		} else {
			if c.isFull() {
				c.logger.Error("max slot count reached, dropped a snapshot chunk",
					zap.String("key", key))
				return nil
			}
		}
		// add the first chunk to the tracked map
		td = &tracked{
			next:  1,
			first: chunk,
		}
		c.mu.tracked[key] = td
	} else {
		if td == nil {
			c.logger.Error("not tracked snapshot chunk ignored",
				zap.String("key", key))
			return nil
		}
		if td.next != chunk.ChunkID {
			c.logger.Error("out of order snapshot chunk",
				zap.String("key", key),
				zap.Uint64("want", td.next),
				zap.Uint64("got", chunk.ChunkID))
			return nil
		}
		from := chunk.From
		want := td.first.From
		if want != from {
			from := chunk.From
			want := td.first.From
			c.logger.Error("snapshot chunk from unexpected replica",
				zap.String("key", key),
				zap.Uint64("from", from),
				zap.Uint64("want", want))
			return nil
		}
		td.next = chunk.ChunkID + 1
	}
	td.tick = c.getTick()
	return td
}

func (c *Chunk) addLocked(chunk meta.SnapshotChunk) bool {
	key := chunkKey(chunk)
	td := c.record(chunk)
	if td == nil {
		c.logger.Warn("ignored a snapshot chunk",
			zap.String("key", key))
		return false
	}
	removed, err := c.nodeRemoved(chunk)
	if err != nil {
		c.logger.Fatal("failed to check whether node already removed",
			zap.Error(err))
	}
	if removed {
		c.removeTempDir(chunk)
		c.logger.Warn("ignored snapshot chunk for removed replica",
			zap.String("key", key))
		return false
	}
	if err := c.save(chunk); err != nil {
		c.removeTempDir(chunk)
		c.logger.Fatal("failed to save chunk",
			zap.String("key", key),
			zap.Error(err))
	}
	if chunk.IsLastChunk() {
		c.logger.Debug("last snapshot chunk received",
			zap.String("key", key))
		defer c.reset(key)
		if err := c.finalize(td); err != nil {
			c.removeTempDir(chunk)
			if !errors.Is(err, ErrSnapshotOutOfDate) {
				c.logger.Fatal("failed when finalizing snapshot dir",
					zap.String("key", key),
					zap.Error(err))
			}
			return false
		}
		snapshotMessage := c.toMessage(td.first)
		c.logger.Info("received a snapshot",
			zap.String("key", key))
		c.onReceive(snapshotMessage)
	}
	return true
}

func (c *Chunk) nodeRemoved(chunk meta.SnapshotChunk) (bool, error) {
	env := c.getEnv(chunk)
	dir := env.GetRootDir()
	return fileutil.IsDirMarkedAsDeleted(dir, c.fs)
}

func (c *Chunk) save(chunk meta.SnapshotChunk) (err error) {
	env := c.getEnv(chunk)
	if chunk.ChunkID == 0 {
		if err := env.CreateTempDir(); err != nil {
			return err
		}
	}
	fp := c.fs.PathJoin(env.GetTempDir(), chunk.FilePath)
	var f *chunkFile
	if chunk.FileChunkID == 0 {
		fb := c.fs.PathDir(fp)
		c.fs.MkdirAll(fb, 0755)
		f, err = createChunkFile(fp, c.fs)
	} else {
		f, err = openChunkFileForAppend(fp, c.fs)
	}
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, f.close())
	}()
	n, err := f.write(chunk.Data)
	if err != nil {
		return err
	}
	if len(chunk.Data) != n {
		return io.ErrShortWrite
	}
	if chunk.IsLastChunk() || chunk.IsLastFileChunk() {
		if err := f.sync(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Chunk) getEnv(chunk meta.SnapshotChunk) snapshot.SSEnv {
	return snapshot.NewSSEnv(c.dir, chunk.ShardID, chunk.ReplicaID,
		chunk.Index, chunk.From, snapshot.ReceivingMode, c.fs)
}

func (c *Chunk) finalize(td *tracked) error {
	env := c.getEnv(td.first)
	msg := c.toMessage(td.first)
	if len(msg.Messages) != 1 || msg.Messages[0].Message.Type != raftpb.MsgSnap {
		panic("invalid message")
	}
	err := env.FinalizeSnapshot()
	if err == snapshot.ErrSnapshotOutOfDate {
		return ErrSnapshotOutOfDate
	}
	return err
}

func (c *Chunk) removeTempDir(chunk meta.SnapshotChunk) {
	env := c.getEnv(chunk)
	env.MustRemoveTempDir()
}

func (c *Chunk) toMessage(chunk meta.SnapshotChunk) meta.RaftMessageBatch {
	if chunk.ChunkID != 0 {
		panic("not the first snapshot chunk")
	}
	si := &meta.SnapshotInfo{
		Extra: chunk.From,
	}
	s := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     chunk.Index,
			Term:      chunk.Term,
			ConfState: chunk.ConfState,
		},
		Data: protoc.MustMarshal(si),
	}
	m := raftpb.Message{
		Type:     raftpb.MsgSnap,
		From:     chunk.From,
		To:       chunk.ReplicaID,
		Snapshot: s,
	}
	return meta.RaftMessageBatch{
		Messages: []meta.RaftMessage{
			{
				ShardID: chunk.ShardID,
				To:      metapb.Replica{ID: chunk.ReplicaID, ContainerID: chunk.ContainerID},
				From:    metapb.Replica{ID: chunk.From},
				Message: m,
			},
		},
	}
}
