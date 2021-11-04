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
	"math"
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/keys"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage"
	"go.uber.org/zap"
)

func (s *store) ProphetBecomeLeader() {
	s.logger.Info("*********Become prophet leader*********",
		s.storeField())
	s.bootOnce.Do(func() {
		s.doBootstrapCluster()
		close(s.pdStartedC)
	})
}

func (s *store) ProphetBecomeFollower() {
	s.logger.Info("*********Become prophet follower*********",
		s.storeField())
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

	s.logger.Info("store metadata init",
		s.storeField(),
		zap.String("raft-addr", s.Meta().RaftAddr),
		zap.String("client-addr", s.Meta().ClientAddr),
		zap.Any("labels", s.Meta().Labels))
}

func (s *store) doBootstrapCluster() {
	s.logger.Info("begin to bootstrap the cluster",
		s.storeField())
	s.initMeta()

	if s.mustLoadStoreMetadata() {
		return
	}

	s.logger.Info("begin to create local store metadata",
		s.storeField())
	id := s.MustAllocID()
	s.meta.meta.ID = id
	s.mustSaveStoreMetadata()
	s.logger.Info("create local store",
		s.storeField())

	ok, err := s.pd.GetStorage().AlreadyBootstrapped()
	if err != nil {
		s.logger.Fatal("fail to check the cluster whether bootstrapped",
			s.storeField(),
			zap.Error(err))
	}
	s.logger.Info("cluster bootstrap state",
		s.storeField(),
		zap.Bool("bootstrapped", ok))

	if !ok {
		s.logger.Info("begin to bootstrap the cluster with init shards",
			s.storeField())
		var initShards []Shard
		var resources []metadata.Resource
		if s.cfg.Customize.CustomInitShardsFactory != nil {
			shards := s.cfg.Customize.CustomInitShardsFactory()
			for _, shard := range shards {
				s.doCreateInitShard(&shard)
				initShards = append(initShards, shard)
				resources = append(resources, NewResourceAdapterWithShard(shard))
			}
		} else {
			shard := Shard{}
			s.doCreateInitShard(&shard)
			initShards = append(initShards, shard)
			resources = append(resources, NewResourceAdapterWithShard(shard))
		}
		s.mustSaveShards(initShards...)

		ok, err := s.pd.GetStorage().PutBootstrapped(s.meta, resources...)
		if err != nil {
			s.removeInitShards(initShards...)
			s.logger.Fatal("fail to bootstrap cluster",
				s.storeField(),
				zap.Error(err))
		}
		if !ok {
			s.logger.Info("the cluster is already bootstrapped, remove init shards",
				s.storeField())
			s.removeInitShards(initShards...)
		}
	}

	if err := s.pd.GetClient().PutContainer(s.meta); err != nil {
		s.logger.Fatal("fail to put container to prophet",
			s.storeField(),
			zap.Error(err))
	}

	s.startHandleResourceHeartbeat()
}

func (s *store) mustSaveStoreMetadata() {
	count := 0
	err := s.kvStorage.Scan(keys.GetRaftPrefix(0), keys.GetRaftPrefix(math.MaxUint64), func([]byte, []byte) (bool, error) {
		count++
		return false, nil
	}, false)
	if err != nil {
		s.logger.Fatal("fail to check store metadata",
			s.storeField(),
			zap.Error(err))
	}
	if count > 0 {
		s.logger.Fatal("local store is not empty and has already hard data",
			s.storeField())
	}

	v := &meta.StoreIdent{
		StoreID:   s.meta.meta.ID,
		ClusterID: s.pd.GetClusterID(),
	}
	err = s.kvStorage.Set(keys.GetStoreIdentKey(), protoc.MustMarshal(v), true)
	if err != nil {
		s.logger.Fatal("fail to save local store id",
			s.storeField(),
			zap.Error(err))
	}
}

func (s *store) mustLoadStoreMetadata() bool {
	data, err := s.kvStorage.Get(keys.GetStoreIdentKey())
	if err != nil {
		s.logger.Fatal("fail to load store metadata",
			s.storeField(),
			zap.Error(err))
	}

	if len(data) > 0 {
		v := &meta.StoreIdent{}
		protoc.MustUnmarshal(v, data)

		if v.ClusterID != s.pd.GetClusterID() {
			s.logger.Fatal("cluster metadata mismatch",
				s.storeField(),
				zap.Uint64("local", v.ClusterID),
				zap.Uint64("prophet", s.pd.GetClusterID()))
		}

		s.meta.meta.ID = v.StoreID
		s.logger.Info("load local store metadata",
			s.storeField())
		return true
	}

	return false
}

func (s *store) doCreateInitShard(shard *Shard) {
	shardID := s.MustAllocID()
	peerID := s.MustAllocID()
	shard.ID = shardID
	shard.Epoch.Version = 1
	shard.Epoch.ConfVer = 1
	shard.Replicas = append(shard.Replicas, Replica{
		ID:            peerID,
		ContainerID:   s.meta.meta.ID,
		InitialMember: true,
	})
}

func (s *store) mustSaveShards(shards ...Shard) {
	s.doWithShardsByGroup(func(ds storage.DataStorage, v []Shard) {
		var sm []meta.ShardMetadata
		var ids []uint64
		for _, shard := range v {
			ids = append(ids, shard.ID)
			sm = append(sm, meta.ShardMetadata{
				ShardID:  shard.ID,
				LogIndex: 1,
				Metadata: meta.ShardLocalState{
					State: meta.ReplicaState_Normal,
					Shard: shard,
				},
			})

			err := addFirstUpdateMetadataLog(s.logdb, sm[len(sm)-1].Metadata, shard.Replicas[0], nil)
			if err != nil {
				s.logger.Fatal("fail to create init shards",
					s.storeField(),
					zap.Error(err))
			}
		}

		if err := ds.SaveShardMetadata(sm); err != nil {
			s.logger.Fatal("fail to create init shards",
				s.storeField(),
				zap.Error(err))
		}

		if err := ds.Sync(ids); err != nil {
			s.logger.Fatal("fail to create init shards",
				s.storeField(),
				zap.Error(err))
		}
	}, shards...)
}

func (s *store) removeInitShards(shards ...Shard) {
	s.doWithShardsByGroup(func(ds storage.DataStorage, v []Shard) {
		for _, shard := range v {
			if err := ds.RemoveShard(shard, true); err != nil {
				s.logger.Fatal("fail to remove init shards",
					s.storeField(),
					zap.Error(err))
			}
		}
	}, shards...)
	s.logger.Info("init shards removed from store")
}

func (s *store) doWithShardsByGroup(fn func(storage.DataStorage, []Shard), shards ...Shard) {
	shardsByGroup := make(map[uint64][]Shard)
	for _, s := range shards {
		v := shardsByGroup[s.Group]
		v = append(v, s)
		shardsByGroup[s.Group] = v
	}

	for g, v := range shardsByGroup {
		ds := s.DataStorageByGroup(g)
		fn(ds, v)
	}
}
