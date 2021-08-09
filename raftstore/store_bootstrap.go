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

package raftstore

import (
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
	"github.com/matrixorigin/matrixcube/util"
	"go.etcd.io/etcd/raft/raftpb"
)

func (s *store) ProphetBecomeLeader() {
	logger.Infof("*********Become prophet leader*********")
	s.bootOnce.Do(func() {
		s.doBootstrapCluster()
		close(s.pdStartedC)
	})
}

func (s *store) ProphetBecomeFollower() {
	logger.Infof("*********Become prophet follower*********")
	s.bootOnce.Do(func() {
		s.doBootstrapCluster()
		close(s.pdStartedC)
	})
}

func (s *store) initMeta() {
	s.meta.meta.Labels = s.cfg.GetLabels()
	s.meta.SetStartTimestamp(time.Now().Unix())
	s.meta.SetDeployPath(s.cfg.DeployPath)
	s.meta.SetVersion(s.cfg.Version, s.cfg.GitHash)
	s.meta.SetAddrs(s.cfg.ClientAddr, s.cfg.RaftAddr)

	logger.Infof("raftstore init with store %s", s.meta.meta.String())
}

func (s *store) doBootstrapCluster() {
	logger.Infof("begin to bootstrap the cluster")
	s.initMeta()

	if s.mustLoadStoreMetadata() {
		return
	}

	logger.Infof("begin to create local store metadata")
	id := s.MustAllocID()
	s.meta.meta.ID = id
	s.mustSaveStoreMetadata()
	logger.Infof("create local store with id %d", id)

	ok, err := s.pd.GetStorage().AlreadyBootstrapped()
	if err != nil {
		logger.Fatal("check the cluster whether bootstrapped failed with %+v", err)
	}
	logger.Infof("the cluster already bootstrap: %+v", ok)

	if !ok {
		logger.Infof("begin to bootstrap the cluster with init shards")
		var initShards []bhmetapb.Shard
		var resources []metadata.Resource
		if s.cfg.Customize.CustomInitShardsFactory != nil {
			shards := s.cfg.Customize.CustomInitShardsFactory()
			for _, shard := range shards {
				s.doCreateInitShard(&shard)
				initShards = append(initShards, shard)
				resources = append(resources, NewResourceAdapterWithShard(shard))
			}
		} else {
			shard := bhmetapb.Shard{}
			s.doCreateInitShard(&shard)
			initShards = append(initShards, shard)
			resources = append(resources, NewResourceAdapterWithShard(shard))
		}
		s.mustSaveShards(initShards...)

		ok, err := s.pd.GetStorage().PutBootstrapped(s.meta, resources...)
		if err != nil {
			s.removeInitShards(initShards...)
			logger.Fatalf("bootstrap cluster failed with %+v", err)
		}
		if !ok {
			logger.Info("the cluster is already bootstrapped")
			s.removeInitShards(initShards...)
		}
	}

	if err := s.pd.GetClient().PutContainer(s.meta); err != nil {
		logger.Fatalf("put container to prophet failed with %+v", err)
	}

	s.startHandleResourceHeartbeat()
}

func (s *store) mustSaveStoreMetadata() {
	count := 0
	err := s.cfg.Storage.MetaStorage.Scan(minKey, maxKey, func([]byte, []byte) (bool, error) {
		count++
		return false, nil
	}, false)
	if err != nil {
		logger.Fatalf("check store metadata failed with %+v", err)
	}
	if count > 0 {
		logger.Fatalf("local store is not empty and has already hard data")
	}

	v := &bhmetapb.StoreIdent{
		StoreID:   s.meta.meta.ID,
		ClusterID: s.pd.GetClusterID(),
	}
	err = s.cfg.Storage.MetaStorage.Set(storeIdentKey, protoc.MustMarshal(v))
	if err != nil {
		logger.Fatal("save local store id failed with %+v", err)
	}
}

func (s *store) mustLoadStoreMetadata() bool {
	data, err := s.cfg.Storage.MetaStorage.Get(storeIdentKey)
	if err != nil {
		logger.Fatalf("load store meta failed with %+v", err)
	}

	if len(data) > 0 {
		v := &bhmetapb.StoreIdent{}
		protoc.MustUnmarshal(v, data)

		if v.ClusterID != s.pd.GetClusterID() {
			logger.Fatalf("unexpect cluster id, want %d, but %d",
				v.ClusterID,
				s.pd.GetClusterID())
		}

		s.meta.meta.ID = v.StoreID
		logger.Infof("load local store %d", s.meta.meta.ID)
		return true
	}

	return false
}

func (s *store) doCreateInitShard(shard *bhmetapb.Shard) {
	shardID := s.MustAllocID()
	peerID := s.MustAllocID()
	shard.ID = shardID
	shard.Epoch.Version = 1
	shard.Epoch.ConfVer = 1
	shard.Peers = append(shard.Peers, metapb.Peer{
		ID:          peerID,
		ContainerID: s.meta.meta.ID,
	})
}

func (s *store) mustSaveShards(shards ...bhmetapb.Shard) {
	wb := util.NewWriteBatch()
	for _, shard := range shards {
		// shard local state
		wb.Set(getStateKey(shard.ID), protoc.MustMarshal(&bhraftpb.ShardLocalState{
			Shard: shard,
		}))

		// shard raft state
		wb.Set(getRaftStateKey(shard.ID), protoc.MustMarshal(&bhraftpb.RaftLocalState{
			LastIndex: raftInitLogIndex,
			HardState: raftpb.HardState{
				Term:   raftInitLogTerm,
				Commit: raftInitLogIndex,
			},
		}))

		// shard raft apply state
		wb.Set(getApplyStateKey(shard.ID), protoc.MustMarshal(&bhraftpb.RaftApplyState{
			AppliedIndex: raftInitLogIndex,
			TruncatedState: bhraftpb.RaftTruncatedState{
				Term:  raftInitLogTerm,
				Index: raftInitLogIndex,
			},
		}))

		logger.Infof("create init shard %+v", shard.String())
	}

	err := s.cfg.Storage.MetaStorage.Write(wb, true)
	if err != nil {
		logger.Fatalf("create init shards failed with %+v", err)
	}
}

func (s *store) removeInitShards(shards ...bhmetapb.Shard) {
	var ids []uint64
	for _, shard := range shards {
		ids = append(ids, shard.ID)
	}

	s.mustRemoveShards(ids...)
	logger.Info("init shards has been removed from store")
}

func (s *store) mustRemoveShards(ids ...uint64) {
	wb := util.NewWriteBatch()
	for _, id := range ids {
		wb.Delete(getStateKey(id))
		wb.Delete(getRaftStateKey(id))
		wb.Delete(getApplyStateKey(id))
	}
	err := s.cfg.Storage.MetaStorage.Write(wb, true)
	if err != nil {
		logger.Fatalf("remove shards failed with %+v", err)
	}
}
