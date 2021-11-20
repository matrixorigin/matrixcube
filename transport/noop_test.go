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

	"github.com/matrixorigin/matrixcube/pb/meta"
)

// NOOPConnection is the connection used to exchange messages between node hosts.
type NOOPConnection struct {
}

var _ Connection = (*NOOPConnection)(nil)

// Close closes the NOOPConnection instance.
func (c *NOOPConnection) Close() {
}

// SendMessageBatch return ErrRequestedToFail when requested.
func (c *NOOPConnection) SendMessageBatch(batch meta.RaftMessageBatch) error {
	return nil
}

// NOOPSnapshotConnection is the connection used to send snapshots.
type NOOPSnapshotConnection struct {
	sendChunksCount uint64
	chunk           meta.SnapshotChunk
}

var _ SnapshotConnection = (*NOOPSnapshotConnection)(nil)

// Close closes the NOOPSnapshotConnection.
func (c *NOOPSnapshotConnection) Close() {
}

// SendChunk returns ErrRequestedToFail when requested.
func (c *NOOPSnapshotConnection) SendChunk(chunk meta.SnapshotChunk) error {
	c.sendChunksCount++
	c.chunk = chunk
	return nil
}

// NOOPTransport is a transport module for testing purposes. It does not
// actually has the ability to exchange messages or snapshots between
// nodehosts.
type NOOPTransport struct {
	snapConn *NOOPSnapshotConnection
}

// NewNOOPTransport creates a new NOOPTransport instance.
func NewNOOPTransport() TransImpl {
	return &NOOPTransport{}
}

// Start starts the NOOPTransport instance.
func (g *NOOPTransport) Start() error {
	return nil
}

// Close closes the NOOPTransport instance.
func (g *NOOPTransport) Close() error {
	return nil
}

// GetConnection returns a connection.
func (g *NOOPTransport) GetConnection(ctx context.Context,
	target string) (Connection, error) {
	return &NOOPConnection{}, nil
}

// GetSnapshotConnection returns a snapshot connection.
func (g *NOOPTransport) GetSnapshotConnection(ctx context.Context,
	target string) (SnapshotConnection, error) {
	g.snapConn = &NOOPSnapshotConnection{}
	return g.snapConn, nil
}

// Name returns the module name.
func (g *NOOPTransport) Name() string {
	return "noop-test"
}
