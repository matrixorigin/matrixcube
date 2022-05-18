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
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util/uuid"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

type replicaCreatorFactory func() *replicaCreator

// replicaCreator the entry point for all Shards created in the cube, the current scenarios for creating Replica are as below
// 1. the first set of shards created when the cluster is created
// 2. Shards created dynamically through Prophet's API
// 3. After Rebalance, we receive a raft message from leader and create the shard replica.
type replicaCreator struct {
	store                             *store
	sync, saveMetadata                bool
	saveLog                           bool
	leaseStore                        uint64
	reason                            string
	startReplica                      bool
	campaign                          bool
	afterStartedFunc, beforeStartFunc func(*replica)
	replicaRecordGetter               func(Shard) Replica
	wc                                *logdb.WorkerContext
	logger                            *zap.Logger
	shardsMetadata                    []metapb.ShardMetadata
}

func newReplicaCreator(store *store) *replicaCreator {
	return &replicaCreator{store: store}
}

func (rc *replicaCreator) withLogdbContext(wc *logdb.WorkerContext) *replicaCreator {
	rc.wc = wc
	return rc
}

func (rc *replicaCreator) withReplicaRecordGetter(value func(Shard) Replica) *replicaCreator {
	rc.replicaRecordGetter = value
	return rc
}

func (rc *replicaCreator) withReason(reason string) *replicaCreator {
	rc.reason = reason
	return rc
}

func (rc *replicaCreator) withStartReplica(campaign bool,
	beforeStartFunc, afterStartedFunc func(*replica)) *replicaCreator {
	rc.startReplica = true
	rc.afterStartedFunc = afterStartedFunc
	rc.beforeStartFunc = beforeStartFunc
	rc.campaign = campaign
	return rc
}

func (rc *replicaCreator) withSaveMetadata(sync bool) *replicaCreator {
	rc.sync = sync
	rc.saveMetadata = true
	rc.saveLog = true
	return rc
}

func (rc *replicaCreator) withSaveLog() *replicaCreator {
	rc.saveLog = true
	return rc
}

func (rc *replicaCreator) withLeaseStore(storeID uint64) *replicaCreator {
	rc.leaseStore = storeID
	return rc
}

func (rc *replicaCreator) adjust() {
	if rc.reason == "" {
		rc.reason = "unknown"
	}
	if rc.wc == nil {
		rc.wc = rc.store.logdb.NewWorkerContext()
	}
	rc.logger = rc.store.logger.With(log.ReasonField(rc.reason))
}

func (rc *replicaCreator) create(shards []Shard) {
	rc.adjust()
	shards, replicas := rc.getTarget(shards)
	rc.maybeInitReplica(shards)
	rc.maybeStartReplicas(shards, replicas)
}

func (rc *replicaCreator) getShardsMetadata() []metapb.ShardMetadata {
	return rc.shardsMetadata
}

func (rc *replicaCreator) getTarget(originShards []Shard) ([]Shard, []*replica) {
	if !rc.startReplica {
		return originShards, nil
	}

	var shards []Shard
	var replicas []*replica
	for _, shard := range originShards {
		pr, err := newReplica(rc.store, shard, rc.getLocalReplica(shard), rc.reason)
		if err != nil {
			rc.logger.Fatal("failed to create shard",
				log.ShardField("shard", shard),
				zap.Error(err))
		}

		if !rc.store.addReplica(pr) {
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

func (rc *replicaCreator) maybeInitReplica(shards []Shard) {
	if !rc.saveLog && !rc.saveMetadata {
		return
	}

	doWithShardsByGroupID(rc.store.DataStorageByGroup, func(ds storage.DataStorage, v []Shard) {
		var sm []metapb.ShardMetadata
		var ids []uint64
		for _, shard := range v {
			rc.logger.Info("begin to save shard metadata",
				log.ShardField("shard", shard))

			ids = append(ids, shard.ID)
			var lease *metapb.EpochLease
			if rc.leaseStore > 0 {
				lease = &metapb.EpochLease{
					Epoch:     0,
					ReplicaID: findReplica(shard, rc.leaseStore).ID,
				}
			}
			sm = append(sm, metapb.ShardMetadata{
				ShardID:  shard.ID,
				LogIndex: 1,
				Metadata: metapb.ShardLocalState{
					State: metapb.ReplicaState_Normal,
					Shard: shard,
					Lease: lease,
				},
			})

			if rc.saveLog {
				err := rc.maybeInsertBootstrapRaftLog(sm[len(sm)-1].Metadata, rc.getLocalReplica(shard))
				if err != nil {
					rc.logger.Fatal("failed to add 1th raft log",
						log.ShardField("shard", shard),
						zap.Error(err))
				}
			}
		}

		if len(sm) == 0 {
			return
		}

		if len(sm) > 0 {
			rc.shardsMetadata = append(rc.shardsMetadata, sm...)
		}

		if !rc.saveMetadata {
			return
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

func (rc *replicaCreator) maybeStartReplicas(shards []Shard, replicas []*replica) {
	if !rc.startReplica {
		return
	}

	// fix issue 166, we must store the initialization state before pr joins the store
	// to avoid data problems caused by concurrent modification of this state.
	if rc.store.cfg.Test.SaveDynamicallyShardInitStateWait > 0 {
		time.Sleep(rc.store.cfg.Test.SaveDynamicallyShardInitStateWait)
	}

	for idx, pr := range replicas {
		if rc.beforeStartFunc != nil {
			rc.beforeStartFunc(pr)
		}
		pr.start(rc.campaign)
		shard := shards[idx]
		for _, p := range shard.Replicas {
			rc.store.replicaRecords.Store(p.ID, p)
		}
		if rc.afterStartedFunc != nil {
			rc.afterStartedFunc(pr)
		}
	}

	groupBy := groupShardByGroupID(shards)
	for g, shards := range groupBy {
		rc.store.updateShardKeyRange(g, shards...)
	}
}

// maybeInsertBootstrapRaftLog the first log of all shards is a log of updated metadata, and all
// subsequent metadata changes need to correspond to a raft log, which is used to ensure the consistency
// of metadata. Only InitialMember can add.
func (rc *replicaCreator) maybeInsertBootstrapRaftLog(state metapb.ShardLocalState, replica Replica) error {
	// only InitialMember replica need to add this raft log
	if !replica.InitialMember {
		return nil
	}

	rb := rpcpb.RequestBatch{}
	rb.Header.ShardID = state.Shard.ID
	rb.Header.Replica = replica
	rb.Header.ID = uuid.NewV4().Bytes()
	rb.Requests = []rpcpb.Request{
		{
			ID:         uuid.NewV4().Bytes(),
			ToShard:    state.Shard.ID,
			Group:      state.Shard.Group,
			Type:       rpcpb.Admin,
			CustomType: uint64(rpcpb.CmdUpdateMetadata),
			Epoch:      state.Shard.Epoch,
			Cmd: protoc.MustMarshal(&rpcpb.UpdateMetadataRequest{
				// TODO: use leader as lease
				Metadata: state,
			}),
		},
	}

	rc.wc.Reset()
	return rc.store.logdb.SaveRaftState(state.Shard.ID,
		replica.ID,
		raft.Ready{
			Entries: []raftpb.Entry{{Index: 1, Term: 1, Type: raftpb.EntryNormal,
				Data: protoc.MustMarshal(&rb)}},
			HardState: raftpb.HardState{Commit: 1, Term: 1},
		},
		rc.wc)
}

func (rc *replicaCreator) getLocalReplica(shard Shard) Replica {
	if rc.replicaRecordGetter == nil {
		if v := findReplica(shard, rc.store.Meta().ID); v != nil {
			return *v
		}
	} else {
		return rc.replicaRecordGetter(shard)
	}

	return Replica{}
}

func doWithShardsByGroupID(dataStorageGetter func(group uint64) storage.DataStorage, fn func(storage.DataStorage, []Shard), shards ...Shard) {
	groupBy := groupShardByGroupID(shards)
	for g, v := range groupBy {
		ds := dataStorageGetter(g)
		fn(ds, v)
	}
}

func groupShardByGroupID(shards []Shard) map[uint64][]Shard {
	groupBy := make(map[uint64][]Shard)
	for _, s := range shards {
		v := groupBy[s.Group]
		v = append(v, s)
		groupBy[s.Group] = v
	}
	return groupBy
}
