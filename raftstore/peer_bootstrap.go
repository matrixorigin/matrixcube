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

package raftstore

import (
	"reflect"
	"sort"

	"github.com/fagongzi/util/protoc"
	"go.etcd.io/etcd/raft/v3"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
)

// bootstrap checks the metadata storage to see whether initial peers have been
// set for the specified shard. If true, it checks whether the stored info is
// consistent with the shard parameter. If not previously set, it sets it to
// the metadata storage and boostraps the raft raw node by invoking
// rn.Bootstrap().
func (pr *peerReplica) bootstrap(rn *raft.RawNode, shard *bhmetapb.Shard,
	db logdb.LogDB) error {
	// when bootstrap() is called, we've already confirmed that
	// storage.LastIndex() == 0, i.e. the storage is empty
	bi, err := db.GetBootstrapInfo(pr.shardID, pr.peer.ID)
	if err != nil && err != logdb.ErrNotFound {
		return err
	}

	var toSave bhraftpb.BootstrapInfo
	var peers []raft.Peer
	for _, p := range shard.Peers {
		if p.InitialMember {
			req := &raftcmdpb.RaftCMDRequest{
				Header: &raftcmdpb.RaftRequestHeader{
					IgnoreEpochCheck: true,
					ShardID:          shard.ID,
					Peer:             p,
				},
				AdminRequest: &raftcmdpb.AdminRequest{
					CmdType: raftcmdpb.AdminCmdType_ChangePeer,
					ChangePeer: &raftcmdpb.ChangePeerRequest{
						ChangeType: metapb.ChangePeerType_AddNode,
						Peer:       p,
					},
				},
			}
			data := protoc.MustMarshal(req)
			toSave.Peers = append(toSave.Peers, p.ID)
			peers = append(peers, raft.Peer{ID: p.ID, Context: data})
		}
	}
	// this is not an initial member, nothing to bootstrap
	if len(peers) == 0 {
		return nil
	}
	if err == logdb.ErrNotFound {
		if err := db.SaveBootstrapInfo(pr.shardID, pr.peer.ID, bi); err != nil {
			return nil
		}
	} else {
		// bootstrap info found on disk, but the storage is empty, probably had a
		// crash?
		checkBootstrapInfo(shard, bi)
	}
	return rn.Bootstrap(peers)
}

func checkBootstrapInfo(shard *bhmetapb.Shard, bi bhraftpb.BootstrapInfo) bool {
	var expected bhraftpb.BootstrapInfo
	for _, p := range shard.Peers {
		if p.InitialMember {
			expected.Peers = append(expected.Peers, p.ID)
		}
	}
	sort.Slice(expected.Peers, func(i, j int) bool { return expected.Peers[i] < expected.Peers[j] })
	sort.Slice(bi.Peers, func(i, j int) bool { return bi.Peers[i] < bi.Peers[j] })
	return reflect.DeepEqual(bi.Peers, expected)
}
