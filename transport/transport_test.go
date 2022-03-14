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
	"errors"
	"sync"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getNodeInfo(shardID uint64, replicaID uint64) nodeInfo {
	return nodeInfo{shardID, replicaID}
}

type testMessageHandler struct {
	mu                        sync.Mutex
	requestCount              map[nodeInfo]uint64
	unreachableCount          map[nodeInfo]uint64
	snapshotCount             map[nodeInfo]uint64
	snapshotFailedCount       map[nodeInfo]uint64
	snapshotSuccessCount      map[nodeInfo]uint64
	receivedSnapshotCount     map[nodeInfo]uint64
	receivedSnapshotFromCount map[nodeInfo]uint64
}

func newTestMessageHandler() *testMessageHandler {
	return &testMessageHandler{
		requestCount:              make(map[nodeInfo]uint64),
		unreachableCount:          make(map[nodeInfo]uint64),
		snapshotCount:             make(map[nodeInfo]uint64),
		snapshotFailedCount:       make(map[nodeInfo]uint64),
		snapshotSuccessCount:      make(map[nodeInfo]uint64),
		receivedSnapshotCount:     make(map[nodeInfo]uint64),
		receivedSnapshotFromCount: make(map[nodeInfo]uint64),
	}
}

func (h *testMessageHandler) HandleMessageBatch(reqs metapb.RaftMessageBatch) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, req := range reqs.Messages {
		epk := getNodeInfo(req.ShardID, req.To.ID)
		v, ok := h.requestCount[epk]
		if ok {
			h.requestCount[epk] = v + 1
		} else {
			h.requestCount[epk] = 1
		}
		if req.Message.Type == raftpb.MsgSnap {
			v, ok = h.snapshotCount[epk]
			if ok {
				h.snapshotCount[epk] = v + 1
			} else {
				h.snapshotCount[epk] = 1
			}
		}
	}
}

func (h *testMessageHandler) HandleSnapshotStatus(shardID uint64,
	replicaID uint64, failed bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	epk := getNodeInfo(shardID, replicaID)
	var p *map[nodeInfo]uint64
	if failed {
		p = &h.snapshotFailedCount
	} else {
		p = &h.snapshotSuccessCount
	}
	v, ok := (*p)[epk]
	if ok {
		(*p)[epk] = v + 1
	} else {
		(*p)[epk] = 1
	}
}

func (h *testMessageHandler) HandleUnreachable(shardID uint64,
	replicaID uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	epk := getNodeInfo(shardID, replicaID)
	v, ok := h.unreachableCount[epk]
	if ok {
		h.unreachableCount[epk] = v + 1
	} else {
		h.unreachableCount[epk] = 1
	}
}

func (h *testMessageHandler) HandleSnapshot(shardID uint64,
	replicaID uint64, from uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	epk := getNodeInfo(shardID, replicaID)
	v, ok := h.receivedSnapshotCount[epk]
	if ok {
		h.receivedSnapshotCount[epk] = v + 1
	} else {
		h.receivedSnapshotCount[epk] = 1
	}
	epk.ReplicaID = from
	v, ok = h.receivedSnapshotFromCount[epk]
	if ok {
		h.receivedSnapshotFromCount[epk] = v + 1
	} else {
		h.receivedSnapshotFromCount[epk] = 1
	}
}

func (h *testMessageHandler) getReceivedSnapshotCount(shardID uint64,
	replicaID uint64) uint64 {
	return h.getMessageCount(h.receivedSnapshotCount, shardID, replicaID)
}

func (h *testMessageHandler) getReceivedSnapshotFromCount(shardID uint64,
	replicaID uint64) uint64 {
	return h.getMessageCount(h.receivedSnapshotFromCount, shardID, replicaID)
}

func (h *testMessageHandler) getRequestCount(shardID uint64,
	replicaID uint64) uint64 {
	return h.getMessageCount(h.requestCount, shardID, replicaID)
}

func (h *testMessageHandler) getFailedSnapshotCount(shardID uint64,
	replicaID uint64) uint64 {
	return h.getMessageCount(h.snapshotFailedCount, shardID, replicaID)
}

func (h *testMessageHandler) getSnapshotSuccessCount(shardID uint64,
	replicaID uint64) uint64 {
	return h.getMessageCount(h.snapshotSuccessCount, shardID, replicaID)
}

func (h *testMessageHandler) getSnapshotCount(shardID uint64,
	replicaID uint64) uint64 {
	return h.getMessageCount(h.snapshotCount, shardID, replicaID)
}

func (h *testMessageHandler) getMessageCount(m map[nodeInfo]uint64,
	shardID uint64, replicaID uint64) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	epk := getNodeInfo(shardID, replicaID)
	v, ok := m[epk]
	if ok {
		return v
	}
	return 0
}

func TestStoreResolverReturnEmptyAddr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)

	trans := NewTransport(nil, testTransportAddr, 2,
		nil, nil, nil,
		getTestSnapshotDir, func(storeID uint64) (string, error) { return "", nil }, fs)
	require.NoError(t, trans.Start())
	defer trans.Close()

	assert.False(t, trans.Send(metapb.RaftMessage{}))
}

func TestStoreResolverReturnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)

	trans := NewTransport(nil, testTransportAddr, 2,
		nil, nil, nil,
		getTestSnapshotDir, func(storeID uint64) (string, error) { return "", errors.New("error") }, fs)
	require.NoError(t, trans.Start())
	defer trans.Close()

	assert.False(t, trans.Send(metapb.RaftMessage{}))
}

func TestSetNilFilter(t *testing.T) {
	hasPanic := false
	func() {
		defer func() {
			if err := recover(); err != nil {
				hasPanic = true
			}
		}()

		trans := &Transport{}
		trans.SetFilter(nil)
	}()
	assert.True(t, hasPanic)
}
