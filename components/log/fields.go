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

package log

import (
	"bytes"
	"encoding/hex"

	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

// ReasonField returns zap.StringField
func ReasonField(why string) zap.Field {
	return zap.String("reason", why)
}

// RequestIDField returns zap.StringField, use hex.EncodeToString as string value
func RequestIDField(data []byte) zap.Field {
	return zap.String("request-id", hex.EncodeToString(data))
}

// HexField returns zap.StringField, use hex.EncodeToString as string value
func HexField(key string, data []byte) zap.Field {
	return zap.String(key, hex.EncodeToString(data))
}

// ListenAddressField return address field
func ListenAddressField(address string) zap.Field {
	return zap.String("listen-address", address)
}

// WorkerTypeField return worker type field
func WorkerTypeField(name string) zap.Field {
	return zap.String("worker-type", name)
}

// WorkerIndexField return worker index field
func WorkerIndexField(index int) zap.Field {
	return zap.Int("worker-index", index)
}

// RaftMessageField return formated raft message zap string field
func RaftMessageField(key string, msg *bhraftpb.RaftMessage) zap.Field {
	var info bytes.Buffer

	appendRaftMessage(msg.Message, &info, true)
	appendPeer("from", msg.From, &info, false)
	appendPeer("to", msg.To, &info, false)
	appendShard(bhmetapb.Shard{ID: msg.ShardID, Group: msg.Group, Epoch: msg.ShardEpoch, Start: msg.Start, End: msg.End}, &info, false)

	info.WriteString(", tombstone: ")
	info.WriteString(format.BoolToString(msg.IsTombstone))

	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// ShardField return formated shard zap string field
func ShardField(key string, shard bhmetapb.Shard) zap.Field {
	var info bytes.Buffer
	appendShard(shard, &info, true)
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// RaftRequestField return formated raft request zap string field
func RaftRequestField(key string, req *raftcmdpb.Request) zap.Field {
	var info bytes.Buffer
	appendRaftRequest(req, &info, true)
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// RaftResponseField return formated raft response zap string field
func RaftResponseField(key string, resp *raftcmdpb.Response) zap.Field {
	var info bytes.Buffer
	appendRaftResponse(resp, &info, true)
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// PeersField return peers zap field
func PeersField(key string, peers []metapb.Peer) zap.Field {
	var info bytes.Buffer
	appendPeers("", peers, &info, true)
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// PeerField returns peer zap field
func PeerField(key string, peer metapb.Peer) zap.Field {
	var info bytes.Buffer
	appendPeer("", peer, &info, true)
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

func appendRaftResponse(resp *raftcmdpb.Response, info *bytes.Buffer, first bool) {
	if !first {
		info.WriteString(", ")
	}

	info.WriteString("id: ")
	info.WriteString(hex.EncodeToString(resp.ID))

	info.WriteString("sid: ")
	info.WriteString(format.Int64ToString(resp.SID))

	info.WriteString("pid: ")
	info.WriteString(format.Int64ToString(resp.PID))

	info.WriteString("type: ")
	info.WriteString(resp.Type.String())

	appendError(resp.Error, info)

	info.WriteString("value: ")
	info.WriteString(format.Uint64ToString(uint64(len(resp.Value))))
	info.WriteString(" bytes")

	info.WriteString("stale: ")
	info.WriteString(format.BoolToString(resp.Stale))
}

func appendRaftRequest(req *raftcmdpb.Request, info *bytes.Buffer, first bool) {
	if !first {
		info.WriteString(", ")
	}

	info.WriteString("id: ")
	info.WriteString(hex.EncodeToString(req.ID))

	info.WriteString("sid: ")
	info.WriteString(format.Int64ToString(req.SID))

	info.WriteString("pid: ")
	info.WriteString(format.Int64ToString(req.PID))

	info.WriteString("type: ")
	info.WriteString(req.Type.String())

	info.WriteString("custom-type: ")
	info.WriteString(format.Uint64ToString(req.CustemType))

	info.WriteString("cmd: ")
	info.WriteString(format.Uint64ToString(uint64(len(req.Cmd))))
	info.WriteString(" bytes")

	info.WriteString("to-shard: ")
	info.WriteString(format.Uint64ToString(req.ToShard))
}

func appendShard(shard bhmetapb.Shard, info *bytes.Buffer, first bool) {
	if !first {
		info.WriteString(", ")
	}

	info.WriteString("shard-id: ")
	info.WriteString(format.Uint64ToString(shard.ID))

	info.WriteString(", shard-group: ")
	info.WriteString(format.Uint64ToString(shard.Group))

	appendResourceEpoch("shard-epoch", shard.Epoch, info, false)

	info.WriteString(", shard-range: [")
	info.WriteString(hex.EncodeToString(shard.Start))
	info.WriteString(", ")
	info.WriteString(hex.EncodeToString(shard.End))
	info.WriteString(")")

	appendPeers("peers", shard.Peers, info, false)

	info.WriteString(", shard-state: ")
	info.WriteString(shard.State.String())

	if shard.Unique != "" {
		info.WriteString(", shard-unique: ")
		info.WriteString(shard.Unique)
	}

	l := len(shard.RuleGroups)
	if l > 0 {
		info.WriteString(", shard-rule-groups: [")
		for idx, rg := range shard.RuleGroups {
			info.WriteString(rg)
			if idx != (l - 1) {
				info.WriteString(" ")
			}
		}
		info.WriteString("]")
	}

	info.WriteString(", shard-disable-split: ")
	info.WriteString(format.BoolToString(shard.DisableSplit))
}

func appendRaftMessage(msg raftpb.Message, info *bytes.Buffer, first bool) {
	if !first {
		info.WriteString(", ")
	}

	info.WriteString("raft-msg-type: ")
	info.WriteString(msg.Type.String())

	info.WriteString(", raft-msg-index: ")
	info.WriteString(format.Uint64ToString(msg.Index))

	info.WriteString(", raft-msg-term: ")
	info.WriteString(format.Uint64ToString(msg.Term))

	info.WriteString(", raft-msg-logterm: ")
	info.WriteString(format.Uint64ToString(msg.LogTerm))

	info.WriteString(", raft-msg-commit: ")
	info.WriteString(format.Uint64ToString(msg.Commit))

	info.WriteString(", raft-msg-entries: ")
	info.WriteString(format.Uint64ToString(uint64(len(msg.Entries))))
}

func appendError(err errorpb.Error, info *bytes.Buffer) {
	info.WriteString(", error: ")
	info.WriteString(err.Message)
}

func appendPeer(key string, peer metapb.Peer, info *bytes.Buffer, first bool) {
	if !first {
		info.WriteString(", ")
	}
	if key != "" {
		info.WriteString(key)
		info.WriteString(": ")
	}
	doAppendPeer(peer, info)
}

func appendPeers(key string, peers []metapb.Peer, info *bytes.Buffer, first bool) {
	if !first {
		info.WriteString(", ")
	}
	if key != "" {
		info.WriteString(key)
		info.WriteString(": ")
	}

	info.WriteString("[")
	n := len(peers)
	for idx, peer := range peers {
		doAppendPeer(peer, info)
		if idx != (n - 1) {
			info.WriteString(" ")
		}
	}
	info.WriteString("]")
}

func doAppendPeer(peer metapb.Peer, info *bytes.Buffer) {
	info.WriteString(format.Uint64ToString(peer.ID))
	info.WriteString("/")
	info.WriteString(format.Uint64ToString(peer.ContainerID))
}

func appendResourceEpoch(key string, epoch metapb.ResourceEpoch, info *bytes.Buffer, first bool) {
	if !first {
		info.WriteString(", ")
	}
	info.WriteString(key)
	info.WriteString(": ")
	info.WriteString(format.Uint64ToString(epoch.Version))
	info.WriteString("/")
	info.WriteString(format.Uint64ToString(epoch.ConfVer))
}
