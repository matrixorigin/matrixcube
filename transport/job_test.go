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
	"context"
	"testing"

	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/vfs"
)

func TestSnapshotJobCanBeCreatedInSavedMode(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	defer vfs.ReportLeakedFD(fs, t)
	logger := log.GetDefaultZapLoggerWithLevel(zap.DebugLevel)
	transport := NewNOOPTransport()
	stopc := make(chan struct{})
	c := newJob(logger, context.Background(),
		1, 1, 32, transport, nil, stopc, defaultSnapshotChunkSize, fs)
	assert.Equal(t, 32, cap(c.ch))
}

func TestSendSavedSnapshotPutsAllChunksInCh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	logger := log.GetDefaultZapLoggerWithLevel(zap.DebugLevel)
	transport := NewNOOPTransport()
	stopc := make(chan struct{})
	c := newJob(logger, context.Background(),
		1, 1, 8, transport, nil, stopc, defaultSnapshotChunkSize, fs)

	assert.NoError(t, generateTestSnapshotDir(testSnapshotDir, fs))
	defer func() {
		require.NoError(t, fs.RemoveAll(testSnapshotDir))
	}()
	si := &metapb.SnapshotInfo{
		Extra: 12345,
	}
	shardID := uint64(100)
	from := uint64(1)
	to := uint64(2)
	index := uint64(300)
	term := uint64(200)

	m := metapb.RaftMessage{
		ShardID: shardID,
		From:    metapb.Replica{ID: from},
		To:      metapb.Replica{ID: to},
		Message: raftpb.Message{
			Type: raftpb.MsgSnap,
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: index,
					Term:  term,
				},
				Data: protoc.MustMarshal(si),
			},
		},
	}

	// 2 chunks for each file
	chunks, err := splitSnapshotMessage(m, testSnapshotDir, 1000, fs)
	assert.NoError(t, err)
	assert.Equal(t, 8, len(chunks))

	c.addSnapshot(chunks)
	assert.Equal(t, 8, len(c.ch))
}
