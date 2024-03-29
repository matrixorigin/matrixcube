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
	"github.com/matrixorigin/matrixcube/keys"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/storage"
	"go.uber.org/zap"
)

func (s *store) ProphetBecomeLeader() {
	s.logger.Info("*********become prophet leader*********",
		s.storeField())
	s.bootOnce.Do(func() {
		go func() {
			s.doBootstrapCluster(true)
		}()
	})
}

func (s *store) ProphetBecomeFollower() {
	s.logger.Info("*********become prophet follower*********",
		s.storeField())
	s.bootOnce.Do(func() {
		go func() {
			s.doBootstrapCluster(false)
		}()
	})
}

func (s *store) initMeta() {
	s.meta.SetLabels(s.cfg.GetLabels())
	s.meta.SetStartTime(time.Now().Unix())
	s.meta.SetDeployPath(s.cfg.DeployPath)
	s.meta.SetVersionAndCommitID(s.cfg.Version, s.cfg.GitHash)
	s.meta.SetAddrs(s.cfg.AdvertiseClientAddr, s.cfg.AdvertiseRaftAddr)

	s.logger.Info("store metadata init",
		s.storeField(),
		zap.String("raft-addr", s.Meta().RaftAddress),
		zap.String("client-addr", s.Meta().ClientAddress),
		zap.Any("labels", s.Meta().Labels))
}

func (s *store) doBootstrapCluster(bootstrap bool) {
	defer s.postBootstrapped()

	s.logger.Info("begin to bootstrap the cluster",
		s.storeField())
	s.initMeta()

	if s.mustLoadStoreMetadata() {
		return
	}

	s.logger.Info("begin to create local store metadata",
		s.storeField())
	s.Lock()
	s.meta.SetID(s.MustAllocID())
	s.Unlock()
	s.mustSaveStoreMetadata()
	s.logger.Info("create local store",
		s.storeField())

	if bootstrap {
		ok, err := s.pd.GetStorage().AlreadyBootstrapped()
		if err != nil {
			s.logger.Fatal("failed to check the cluster whether bootstrapped",
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
			var resources []*Shard
			if s.cfg.Customize.CustomInitShardsFactory != nil {
				shards := s.cfg.Customize.CustomInitShardsFactory()
				for _, shard := range shards {
					s.doCreateInitShard(&shard)
					initShards = append(initShards, shard)
					resources = append(resources, shard.Clone())
				}
			} else {
				shard := metapb.NewShard()
				s.doCreateInitShard(shard)
				initShards = append(initShards, *shard)
				resources = append(resources, shard)
			}

			newReplicaCreator(s).
				withReason("bootstrap init").
				withSaveMetadata(true).
				create(initShards)

			ok, err := s.pd.GetStorage().PutBootstrapped(s.meta, resources...)
			if err != nil {
				s.removeInitShards(initShards...)
				s.logger.Fatal("failed to bootstrap cluster",
					s.storeField(),
					zap.Error(err))
			}
			if !ok {
				s.logger.Info("the cluster is already bootstrapped, remove init shards",
					s.storeField())
				s.removeInitShards(initShards...)
			}
		}
	}
}

func (s *store) postBootstrapped() {
	s.mustPutStore()
	s.startHandleShardHeartbeat()
	close(s.pdStartedC)
}

func (s *store) mustPutStore() {
	for {
		if err := s.pd.GetClient().PutStore(s.meta); err != nil {
			s.logger.Info("failed to put container to prophet",
				s.storeField(),
				zap.Error(err),
			)
			time.Sleep(time.Second)
			continue
		}
		break
	}
}

func (s *store) mustSaveStoreMetadata() {
	count := 0
	err := s.kvStorage.Scan(keys.GetRaftPrefix(0), keys.GetRaftPrefix(math.MaxUint64), func([]byte, []byte) (bool, error) {
		count++
		return false, nil
	}, false)
	if err != nil {
		s.logger.Fatal("failed to check store metadata",
			s.storeField(),
			zap.Error(err))
	}
	if count > 0 {
		s.logger.Fatal("local store is not empty and has already hard data",
			s.storeField())
	}

	v := &metapb.StoreIdent{
		StoreID:   s.meta.GetID(),
		ClusterID: s.pd.GetClusterID(),
	}
	err = s.kvStorage.Set(keys.GetStoreIdentKey(), protoc.MustMarshal(v), true)
	if err != nil {
		s.logger.Fatal("failed to save local store id",
			s.storeField(),
			zap.Error(err))
	}
}

func (s *store) mustLoadStoreMetadata() bool {
	data, err := s.kvStorage.Get(keys.GetStoreIdentKey())
	if err != nil {
		s.logger.Fatal("failed to load store metadata",
			s.storeField(),
			zap.Error(err))
	}

	if len(data) > 0 {
		v := &metapb.StoreIdent{}
		protoc.MustUnmarshal(v, data)

		if v.ClusterID != s.pd.GetClusterID() {
			s.logger.Fatal("cluster metadata mismatch",
				s.storeField(),
				zap.Uint64("local", v.ClusterID),
				zap.Uint64("prophet", s.pd.GetClusterID()))
		}

		s.meta.SetID(v.StoreID)
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
	shard.Epoch.Generation = 1
	shard.Epoch.ConfigVer = 1
	shard.Replicas = append(shard.Replicas, Replica{
		ID:            peerID,
		StoreID:       s.meta.GetID(),
		InitialMember: true,
	})
}

func (s *store) removeInitShards(shards ...Shard) {
	doWithShardsByGroupID(s.DataStorageByGroup, func(ds storage.DataStorage, v []Shard) {
		for _, shard := range v {
			if err := ds.RemoveShard(shard, true); err != nil {
				s.logger.Fatal("failed to remove init shards",
					s.storeField(),
					zap.Error(err))
			}
		}
	}, shards...)
	s.logger.Info("init shards removed from store")
}
