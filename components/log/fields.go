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
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

// SnapshotField returns snapshot field
func SnapshotField(ss raftpb.Snapshot) zap.Field {
	return zap.Uint64("index", ss.Metadata.Index)
}

// SourceContainerField returns source container field
func SourceContainerField(id uint64) zap.Field {
	return zap.Uint64("source-container", id)
}

// TargetContainerField returns target container field
func TargetContainerField(id uint64) zap.Field {
	return zap.Uint64("target-container", id)
}

// ResourceField returns resource field
func ResourceField(id uint64) zap.Field {
	return zap.Uint64("resource", id)
}

// NodeField returns zap.StringField
func NodeField(name string) zap.Field {
	return zap.String("node", name)
}

// EntryCountField returns zap.IntField
func EntryCountField(count int) zap.Field {
	return zap.Int("entry-count", count)
}

// IndexField returns zap.Uint64Field
func IndexField(index uint64) zap.Field {
	return zap.Uint64("index", index)
}

// WorkerField returns zap.StringField
func WorkerField(id uint64) zap.Field {
	return zap.Uint64("worker-index", id)
}

// ReasonField returns zap.StringField
func ReasonField(why string) zap.Field {
	return zap.String("reason", why)
}

// ShardIDField returns zap.Uint64Field
func ShardIDField(id uint64) zap.Field {
	return zap.Uint64("shard-id", id)
}

// StoreIDField returns zap.Uint64Field
func StoreIDField(id uint64) zap.Field {
	return zap.Uint64("store-id", id)
}

// ReplicaIDField returns zap.Uint64Field
func ReplicaIDField(id uint64) zap.Field {
	return zap.Uint64("replica-id", id)
}

// ReplicaIDsField returns zap.Uint64Field
func ReplicaIDsField(key string, ids []uint64) zap.Field {
	if len(ids) == 0 {
		return zap.String(key, "")
	}

	var info bytes.Buffer
	appendIDs(ids, &info)
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// RequestIDField returns zap.StringField, use hex.EncodeToString as string value
func RequestIDField(data []byte) zap.Field {
	if len(data) == 0 {
		return zap.String("request-id", "")
	}
	return zap.String("request-id", hex.EncodeToString(data))
}

// HexField returns zap.StringField, use hex.EncodeToString as string value
func HexField(key string, data []byte) zap.Field {
	if len(data) == 0 {
		return zap.String(key, "")
	}
	return zap.String(key, hex.EncodeToString(data))
}

// ListenAddressField return address field
func ListenAddressField(address string) zap.Field {
	return zap.String("listen-address", address)
}

// ResponseBatchField rpc.ResponseBatch zap field
func ResponseBatchField(key string, resp rpc.ResponseBatch) zap.Field {
	return ResponsesField(key, resp.Responses)
}

// ResponsesField []rpc.Response zap field
func ResponsesField(key string, resps []rpc.Response) zap.Field {
	var info bytes.Buffer
	info.WriteString("responses {")
	for _, resp := range resps {
		appendRaftResponse(&resp, &info, true)
	}
	info.WriteString("}")
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// RequestBatchField request batch field
func RequestBatchField(key string, req rpc.RequestBatch) zap.Field {
	return RequestsField(key, req.Requests)
}

// RequestsField []rpc.Request zap field
func RequestsField(key string, reqs []rpc.Request) zap.Field {
	var info bytes.Buffer
	info.WriteString("requests {")
	for _, req := range reqs {
		appendRaftRequest(&req, &info, false)
	}
	info.WriteString("}")
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// RaftMessageField return formated raft message zap string field
func RaftMessageField(key string, msg meta.RaftMessage) zap.Field {
	var info bytes.Buffer

	appendRaftMessage(msg.Message, &info, true)
	appendReplica("from", msg.From, &info, false)
	appendReplica("to", msg.To, &info, false)
	appendShard(meta.Shard{ID: msg.ShardID, Group: msg.Group, Epoch: msg.ShardEpoch, Start: msg.Start, End: msg.End}, &info, false)

	info.WriteString(", tombstone: ")
	info.WriteString(format.BoolToString(msg.IsTombstone))

	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// ShardField return formated shard zap string field
func ShardField(key string, shard meta.Shard) zap.Field {
	var info bytes.Buffer
	appendShard(shard, &info, true)
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// EpochField return formated epoch zap string field
func EpochField(key string, epoch metapb.ResourceEpoch) zap.Field {
	var info bytes.Buffer
	doAppendResourceEpoch(epoch, &info)
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// RaftRequestField return formated raft request zap string field
func RaftRequestField(key string, req *rpc.Request) zap.Field {
	var info bytes.Buffer
	appendRaftRequest(req, &info, true)
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// RaftResponseField return formated raft response zap string field
func RaftResponseField(key string, resp *rpc.Response) zap.Field {
	var info bytes.Buffer
	appendRaftResponse(resp, &info, true)
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// ReplicasField return Replicas zap field
func ReplicasField(key string, Replicas []metapb.Replica) zap.Field {
	var info bytes.Buffer
	appendReplicas("", Replicas, &info, true)
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// ReplicaField returns Replica zap field
func ReplicaField(key string, Replica metapb.Replica) zap.Field {
	var info bytes.Buffer
	appendReplica("", Replica, &info, true)
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// ConfigChangeFieldWithHeartbeatResp return formated change Replica zap string field
func ConfigChangeFieldWithHeartbeatResp(key string, req rpcpb.ResourceHeartbeatRsp) zap.Field {
	var info bytes.Buffer
	doAppendConfigChange(req.ConfigChange.ChangeType, req.ConfigChange.Replica, &info)
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// ConfigChangesFieldWithHeartbeatResp return formated change Replica zap string field
func ConfigChangesFieldWithHeartbeatResp(key string, req rpcpb.ResourceHeartbeatRsp) zap.Field {
	var info bytes.Buffer
	info.WriteString("[")
	for idx, change := range req.ConfigChangeV2.Changes {
		doAppendConfigChange(change.ChangeType, change.Replica, &info)
		if idx < len(req.ConfigChangeV2.Changes)-1 {
			info.WriteString(", ")
		}
	}
	info.WriteString("]")
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// ConfigChangeField return formated change Replica zap string field
func ConfigChangeField(key string, req *rpc.ConfigChangeRequest) zap.Field {
	var info bytes.Buffer
	doAppendConfigChange(req.ChangeType, req.Replica, &info)
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

// ConfigChangesField return formated change Replica zap string field
func ConfigChangesField(key string, changes []rpc.ConfigChangeRequest) zap.Field {
	var info bytes.Buffer
	info.WriteString("[")
	for idx := range changes {
		doAppendConfigChange(changes[idx].ChangeType, changes[idx].Replica, &info)
		if idx < len(changes)-1 {
			info.WriteString(", ")
		}
	}
	info.WriteString("]")
	return zap.String(key, hack.SliceToString(info.Bytes()))
}

func doAppendConfigChange(confType metapb.ConfigChangeType, replica metapb.Replica, info *bytes.Buffer) {
	info.WriteString("{type: ")
	info.WriteString(confType.String())
	appendReplica("replica", replica, info, false)
	info.WriteString("}")
}

func appendRaftResponse(resp *rpc.Response, info *bytes.Buffer, first bool) {
	if !first {
		info.WriteString(", ")
	}

	info.WriteString("id: ")
	info.WriteString(hex.EncodeToString(resp.ID))

	info.WriteString(", pid: ")
	info.WriteString(format.Int64ToString(resp.PID))

	info.WriteString(", type: ")
	info.WriteString(resp.Type.String())

	appendError(resp.Error, info)

	info.WriteString(", value: ")
	info.WriteString(format.Uint64ToString(uint64(len(resp.Value))))
	info.WriteString(" bytes")
}

func appendRaftRequest(req *rpc.Request, info *bytes.Buffer, first bool) {
	if !first {
		info.WriteString(", ")
	}

	info.WriteString("id: ")
	info.WriteString(hex.EncodeToString(req.ID))

	info.WriteString(", key: ")
	info.WriteString(hex.EncodeToString(req.Key))

	info.WriteString(", pid: ")
	info.WriteString(format.Int64ToString(req.PID))

	info.WriteString(", type: ")
	info.WriteString(req.Type.String())

	info.WriteString(", custom-type: ")
	info.WriteString(format.Uint64ToString(req.CustomType))

	info.WriteString(", cmd: ")
	info.WriteString(format.Uint64ToString(uint64(len(req.Cmd))))
	info.WriteString(" bytes")

	info.WriteString(", to-shard: ")
	info.WriteString(format.Uint64ToString(req.ToShard))
}

func appendShard(shard meta.Shard, info *bytes.Buffer, first bool) {
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

	appendReplicas("replicas", shard.Replicas, info, false)

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

func appendReplica(key string, Replica metapb.Replica, info *bytes.Buffer, first bool) {
	if !first {
		info.WriteString(", ")
	}
	if key != "" {
		info.WriteString(key)
		info.WriteString(": ")
	}
	doAppendReplica(Replica, info)
}

func appendReplicas(key string, Replicas []metapb.Replica, info *bytes.Buffer, first bool) {
	if !first {
		info.WriteString(", ")
	}
	if key != "" {
		info.WriteString(key)
		info.WriteString(": ")
	}

	info.WriteString("[")
	n := len(Replicas)
	for idx, Replica := range Replicas {
		doAppendReplica(Replica, info)
		if idx != (n - 1) {
			info.WriteString(" ")
		}
	}
	info.WriteString("]")
}

func appendSplitRequest(req rpc.SplitRequest, info *bytes.Buffer, first bool) {
	if !first {
		info.WriteString(", ")
	}

	info.WriteString("shard-id: ")
	info.WriteString(format.Uint64ToString(req.NewShardID))

	info.WriteString(", shard-range: [")
	info.WriteString(hex.EncodeToString(req.Start))
	info.WriteString(", ")
	info.WriteString(hex.EncodeToString(req.End))
	info.WriteString(")")
	appendReplicas("replicas", req.NewReplicas, info, false)
}

func appendIDs(ids []uint64, info *bytes.Buffer) {
	for idx, id := range ids {
		info.WriteString(format.Uint64ToString(id))
		if idx != len(ids)-1 {
			info.WriteString(",")
		}
	}
}

func doAppendReplica(Replica metapb.Replica, info *bytes.Buffer) {
	info.WriteString(format.Uint64ToString(Replica.ID))
	info.WriteString("/")
	info.WriteString(format.Uint64ToString(Replica.ContainerID))
	info.WriteString("/")
	info.WriteString(Replica.Role.String())
}

func appendResourceEpoch(key string, epoch metapb.ResourceEpoch, info *bytes.Buffer, first bool) {
	if !first {
		info.WriteString(", ")
	}
	info.WriteString(key)
	info.WriteString(": ")
	doAppendResourceEpoch(epoch, info)
}

func doAppendResourceEpoch(epoch metapb.ResourceEpoch, info *bytes.Buffer) {
	info.WriteString(format.Uint64ToString(epoch.Version))
	info.WriteString("/")
	info.WriteString(format.Uint64ToString(epoch.ConfVer))
}
