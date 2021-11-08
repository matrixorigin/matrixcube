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
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util/uuid"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

type shardCreatorFactory func() *shardCreator

// shardCreator the entry point for all Shards created in the cube, the current scenarios for creating Replica are as below
// 1. the first set of shards created when the cluster is created
// 2. Shards created dynamically through Prophet's API
// 3. After Rebalance, we receive a raft message from leader and create the shard replica.
type shardCreator struct {
	s                   *store
	sync, saveMetadata  bool
	reason              string
	startReplica        bool
	afterStartedFunc    func(*replica)
	replicaRecordGetter func(Shard) Replica
	wc                  *logdb.WorkerContext
	logger              *zap.Logger
}

func newShardCreator(s *store) *shardCreator {
	return &shardCreator{s: s}
}

func (rc *shardCreator) withLogdbContext(wc *logdb.WorkerContext) *shardCreator {
	rc.wc = wc
	return rc
}

func (rc *shardCreator) withReplicaRecordGetter(value func(Shard) Replica) *shardCreator {
	rc.replicaRecordGetter = value
	return rc
}

func (rc *shardCreator) withReason(reason string) *shardCreator {
	rc.reason = reason
	return rc
}

func (rc *shardCreator) withStartReplica(afterStartedFunc func(*replica)) *shardCreator {
	rc.startReplica = true
	rc.afterStartedFunc = afterStartedFunc
	return rc
}

func (rc *shardCreator) withSaveMetadata(sync bool) *shardCreator {
	rc.sync = sync
	rc.saveMetadata = true
	return rc
}

func (rc *shardCreator) adjust() {
	if rc.reason == "" {
		rc.reason = "unknown"
	}
	if rc.saveMetadata && rc.wc == nil {
		rc.wc = rc.s.logdb.NewWorkerContext()
	}
	rc.logger = rc.s.logger.With(log.ReasonField(rc.reason))
}

func (rc *shardCreator) create(shards []Shard) {
	rc.adjust()
	shards, replicas := rc.getTarget(shards)
	rc.maybeSaveMetadata(shards)
	rc.maybeStartReplicas(shards, replicas)
}

func (rc *shardCreator) getTarget(originShards []Shard) ([]Shard, []*replica) {
	if !rc.startReplica {
		return originShards, nil
	}

	var shards []Shard
	var replicas []*replica
	for _, shard := range originShards {
		var replica Replica
		if rc.replicaRecordGetter == nil {
			if v := findReplica(shard, rc.s.Meta().ID); v != nil {
				replica = *v
			}
		} else {
			replica = rc.replicaRecordGetter(shard)
		}

		pr, err := newReplica(rc.s, shard, replica, rc.reason)
		if err != nil {
			rc.logger.Fatal("failed to create shard",
				log.ShardField("shard", shard),
				zap.Error(err))
		}

		if !rc.s.addReplica(pr) {
			rc.logger.Info("create shard skipped, already created",
				log.ShardField("shard", shard),
				zap.Error(err))
			continue
		}

		shards = append(shards, shard)
		replicas = append(replicas, pr)
	}
	return shards, replicas
}

func (rc *shardCreator) maybeSaveMetadata(shards []Shard) {
	if !rc.saveMetadata {
		return
	}

	doWithShardsByGroup(rc.s.DataStorageByGroup, func(ds storage.DataStorage, v []Shard) {
		var sm []meta.ShardMetadata
		var ids []uint64
		for _, shard := range v {
			rc.logger.Info("begin to save shard metadata",
				log.ShardField("shard", shard))

			ids = append(ids, shard.ID)
			sm = append(sm, meta.ShardMetadata{
				ShardID:  shard.ID,
				LogIndex: 1,
				LogTerm:  1,
				Metadata: meta.ShardLocalState{
					State: meta.ReplicaState_Normal,
					Shard: shard,
				},
			})

			err := rc.maybeAddFirstUpdateMetadataLog(sm[len(sm)-1].Metadata, shard.Replicas[0])
			if err != nil {
				rc.logger.Fatal("failed to add 1th raft log",
					log.ShardField("shard", shard),
					zap.Error(err))
			}
		}

		if err := ds.SaveShardMetadata(sm); err != nil {
			rc.logger.Fatal("failed to save shards metadata",
				zap.Error(err))
		}

		if rc.sync {
			if err := ds.Sync(ids); err != nil {
				rc.logger.Fatal("failed to sync shards metadata",
					zap.Error(err))
			}
		}
	}, shards...)
}

func (rc *shardCreator) maybeStartReplicas(shards []Shard, replicas []*replica) {
	if !rc.startReplica {
		return
	}

	// fix issue 166, we must store the initialization state before pr joins the store
	// to avoid data problems caused by concurrent modification of this state.
	if rc.s.cfg.Test.SaveDynamicallyShardInitStateWait > 0 {
		time.Sleep(rc.s.cfg.Test.SaveDynamicallyShardInitStateWait)
	}

	for idx, pr := range replicas {
		pr.start()
		shard := shards[idx]
		for _, p := range shard.Replicas {
			rc.s.replicaRecords.Store(p.ID, p)
		}
		if rc.afterStartedFunc != nil {
			rc.afterStartedFunc(pr)
		}
	}

	groupBy := groupByGroupID(shards)
	for g, v := range groupBy {
		rc.s.updateShardKeyRange(g, v...)
	}
}

// maybeAddFirstUpdateMetadataLog the first log of all shards is a log of updated metadata, and all
// subsequent metadata changes need to correspond to a raft log, which is used to ensure the consistency
// of metadata. Only InitialMember can add.
func (rc *shardCreator) maybeAddFirstUpdateMetadataLog(state meta.ShardLocalState, replica Replica) error {
	// only InitialMember replica need to add this raft log
	if !replica.InitialMember {
		return nil
	}

	rb := rpc.RequestBatch{}
	rb.Header.ShardID = state.Shard.ID
	rb.Header.Replica = replica
	rb.Header.ID = uuid.NewV4().Bytes()
	rb.AdminRequest.CmdType = rpc.AdminCmdType_UpdateMetadata
	rb.AdminRequest.UpdateMetadata = &rpc.UpdateMetadataRequest{
		Metadata: state,
	}

	rc.wc.Reset()
	return rc.s.logdb.SaveRaftState(state.Shard.ID,
		replica.ID,
		raft.Ready{
			Entries: []raftpb.Entry{{Index: 1, Term: 1, Type: raftpb.EntryNormal,
				Data: protoc.MustMarshal(&rb)}},
			HardState: raftpb.HardState{Commit: 1, Term: 1},
		},
		rc.wc)
}

func doWithShardsByGroup(factory func(group uint64) storage.DataStorage, fn func(storage.DataStorage, []Shard), shards ...Shard) {
	groupBy := groupByGroupID(shards)
	for g, v := range groupBy {
		ds := factory(g)
		fn(ds, v)
	}
}

func groupByGroupID(shards []Shard) map[uint64][]Shard {
	groupBy := make(map[uint64][]Shard)
	for _, s := range shards {
		v := groupBy[s.Group]
		v = append(v, s)
		groupBy[s.Group] = v
	}
	return groupBy
}
