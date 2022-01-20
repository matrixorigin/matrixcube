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
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util"
	"go.uber.org/zap"
)

type resourceAdapter struct {
	sync.RWMutex

	meta Shard
}

func newResourceAdapter() metadata.Resource {
	return &resourceAdapter{}
}

// NewResourceAdapterWithShard create a prophet resource use shard
func NewResourceAdapterWithShard(meta Shard) metadata.Resource {
	return &resourceAdapter{meta: meta}
}

func (ra *resourceAdapter) ID() uint64 {
	ra.RLock()
	defer ra.RUnlock()

	return ra.meta.ID
}

func (ra *resourceAdapter) SetID(id uint64) {
	ra.Lock()
	defer ra.Unlock()

	ra.meta.ID = id
}

func (ra *resourceAdapter) Group() uint64 {
	ra.RLock()
	defer ra.RUnlock()

	return ra.meta.Group
}

func (ra *resourceAdapter) SetGroup(group uint64) {
	ra.meta.Group = group
}

func (ra *resourceAdapter) Peers() []Replica {
	ra.RLock()
	defer ra.RUnlock()

	return ra.meta.Replicas
}

func (ra *resourceAdapter) SetPeers(peers []Replica) {
	ra.Lock()
	defer ra.Unlock()

	ra.meta.Replicas = peers
}

func (ra *resourceAdapter) Range() ([]byte, []byte) {
	ra.RLock()
	defer ra.RUnlock()

	return ra.meta.Start, ra.meta.End
}

func (ra *resourceAdapter) SetStartKey(value []byte) {
	ra.Lock()
	defer ra.Unlock()

	ra.meta.Start = value
}

func (ra *resourceAdapter) SetEndKey(value []byte) {
	ra.Lock()
	defer ra.Unlock()

	ra.meta.End = value
}

func (ra *resourceAdapter) Epoch() metapb.ResourceEpoch {
	ra.RLock()
	defer ra.RUnlock()

	return ra.meta.Epoch
}

func (ra *resourceAdapter) SetEpoch(value metapb.ResourceEpoch) {
	ra.Lock()
	defer ra.Unlock()

	ra.meta.Epoch = value
}

func (ra *resourceAdapter) State() metapb.ResourceState {
	ra.RLock()
	defer ra.RUnlock()

	return ra.meta.State
}

func (ra *resourceAdapter) SetState(state metapb.ResourceState) {
	ra.Lock()
	defer ra.Unlock()

	ra.meta.State = state
}

func (ra *resourceAdapter) Unique() string {
	ra.RLock()
	defer ra.RUnlock()

	return ra.meta.Unique
}

func (ra *resourceAdapter) SetUnique(value string) {
	ra.Lock()
	defer ra.Unlock()

	ra.meta.Unique = value
}

func (ra *resourceAdapter) Data() []byte {
	ra.RLock()
	defer ra.RUnlock()

	return ra.meta.Data
}

func (ra *resourceAdapter) SetData(value []byte) {
	ra.Lock()
	defer ra.Unlock()

	ra.meta.Data = value
}

func (ra *resourceAdapter) RuleGroups() []string {
	ra.RLock()
	defer ra.RUnlock()

	return ra.meta.RuleGroups
}

func (ra *resourceAdapter) SetRuleGroups(values ...string) {
	ra.Lock()
	defer ra.Unlock()

	ra.meta.RuleGroups = values
}

func (ra *resourceAdapter) Labels() []metapb.Pair {
	ra.RLock()
	defer ra.RUnlock()

	return ra.meta.Labels
}

func (ra *resourceAdapter) SetLabels(labels []metapb.Pair) {
	ra.Lock()
	defer ra.Unlock()

	ra.meta.Labels = labels
}

func (ra *resourceAdapter) Marshal() ([]byte, error) {
	ra.RLock()
	defer ra.RUnlock()

	return protoc.MustMarshal(&ra.meta), nil
}

func (ra *resourceAdapter) Unmarshal(data []byte) error {
	ra.Lock()
	defer ra.Unlock()

	protoc.MustUnmarshal(&ra.meta, data)
	return nil
}

func (ra *resourceAdapter) Clone() metadata.Resource {
	value := &resourceAdapter{}
	data, _ := ra.Marshal()
	value.Unmarshal(data)
	return value
}

type containerAdapter struct {
	sync.RWMutex
	meta meta.Store
}

func newContainerAdapter() metadata.Container {
	return &containerAdapter{}
}

func (ca *containerAdapter) SetAddrs(addr, shardAddr string) {
	ca.Lock()
	defer ca.Unlock()

	ca.meta.ClientAddr = addr
	ca.meta.RaftAddr = shardAddr
}

func (ca *containerAdapter) Addr() string {
	ca.RLock()
	defer ca.RUnlock()

	return ca.meta.ClientAddr
}

func (ca *containerAdapter) ShardAddr() string {
	ca.RLock()
	defer ca.RUnlock()

	return ca.meta.RaftAddr
}

func (ca *containerAdapter) SetID(id uint64) {
	ca.Lock()
	defer ca.Unlock()

	ca.meta.ID = id
}

func (ca *containerAdapter) ID() uint64 {
	ca.RLock()
	defer ca.RUnlock()

	return ca.meta.ID
}

func (ca *containerAdapter) Labels() []metapb.Pair {
	ca.RLock()
	defer ca.RUnlock()

	return ca.meta.Labels
}

func (ca *containerAdapter) SetLabels(labels []metapb.Pair) {
	ca.Lock()
	defer ca.Unlock()

	ca.meta.Labels = labels
}

func (ca *containerAdapter) StartTimestamp() int64 {
	ca.RLock()
	defer ca.RUnlock()

	return ca.meta.StartTime
}

func (ca *containerAdapter) SetStartTimestamp(value int64) {
	ca.Lock()
	defer ca.Unlock()

	ca.meta.StartTime = value
}

func (ca *containerAdapter) Version() (string, string) {
	ca.RLock()
	defer ca.RUnlock()

	return ca.meta.Version, ca.meta.GitHash
}

func (ca *containerAdapter) SetVersion(version string, githash string) {
	ca.Lock()
	defer ca.Unlock()

	ca.meta.Version = version
	ca.meta.GitHash = githash
}

func (ca *containerAdapter) DeployPath() string {
	ca.RLock()
	defer ca.RUnlock()

	return ca.meta.DeployPath
}

func (ca *containerAdapter) SetDeployPath(value string) {
	ca.Lock()
	defer ca.Unlock()

	ca.meta.DeployPath = value
}

func (ca *containerAdapter) State() metapb.ContainerState {
	ca.RLock()
	defer ca.RUnlock()

	return ca.meta.State
}

func (ca *containerAdapter) SetState(value metapb.ContainerState) {
	ca.Lock()
	defer ca.Unlock()

	ca.meta.State = value
}

func (ca *containerAdapter) LastHeartbeat() int64 {
	ca.RLock()
	defer ca.RUnlock()

	return ca.meta.LastHeartbeatTime
}

func (ca *containerAdapter) SetLastHeartbeat(value int64) {
	ca.Lock()
	defer ca.Unlock()

	ca.meta.LastHeartbeatTime = value
}

func (ca *containerAdapter) PhysicallyDestroyed() bool {
	ca.RLock()
	defer ca.RUnlock()

	return ca.meta.PhysicallyDestroyed
}

func (ca *containerAdapter) SetPhysicallyDestroyed(v bool) {
	ca.Lock()
	defer ca.Unlock()

	ca.meta.PhysicallyDestroyed = v
}

func (ca *containerAdapter) Marshal() ([]byte, error) {
	ca.RLock()
	defer ca.RUnlock()

	return protoc.MustMarshal(&ca.meta), nil
}

func (ca *containerAdapter) Unmarshal(data []byte) error {
	ca.Lock()
	defer ca.Unlock()

	protoc.MustUnmarshal(&ca.meta, data)
	return nil
}

func (ca *containerAdapter) Clone() metadata.Container {
	value := &containerAdapter{}
	data, _ := ca.Marshal()
	value.Unmarshal(data)
	return value
}

type prophetAdapter struct {
}

func newProphetAdapter() metadata.Adapter {
	return &prophetAdapter{}
}

func (pa *prophetAdapter) NewResource() metadata.Resource {
	return newResourceAdapter()
}

func (pa *prophetAdapter) NewContainer() metadata.Container {
	return newContainerAdapter()
}

func (s *store) getStoreHeartbeat(last time.Time) (rpcpb.ContainerHeartbeatReq, error) {
	stats := metapb.ContainerStats{}
	stats.ContainerID = s.Meta().ID

	v, err := s.storageStatsReader.stats()
	if err != nil {
		s.logger.Error("fail to get storage capacity status",
			s.storeField(),
			zap.Error(err))
		return rpcpb.ContainerHeartbeatReq{}, err
	}
	stats.Capacity = v.capacity
	stats.UsedSize = v.usedSize
	stats.Available = v.available

	if s.cfg.Capacity > 0 {
		stats.Capacity = uint64(s.cfg.Capacity)
		// If `Capacity` set, calculate `Available` using `Capacity`
		stats.Available = stats.Capacity - stats.UsedSize
	}

	// cpu usages
	usages, err := util.CpuUsages()
	if err != nil {
		s.logger.Error("fail to get cpu status",
			s.storeField(),
			zap.Error(err))
		return rpcpb.ContainerHeartbeatReq{}, err
	}
	for i, v := range usages {
		stats.CpuUsages = append(stats.CpuUsages, metapb.RecordPair{
			Key:   fmt.Sprintf("cpu:%d", i),
			Value: uint64(v * 100),
		})
	}

	// io rates
	rates, err := util.IORates(s.cfg.DataPath)
	if err != nil {
		s.logger.Error("fail to get io status",
			s.storeField(),
			zap.Error(err))
		return rpcpb.ContainerHeartbeatReq{}, err
	}
	for name, v := range rates {
		stats.WriteIORates = append(stats.WriteIORates, metapb.RecordPair{
			Key:   name,
			Value: v.WriteBytes,
		})
		stats.ReadIORates = append(stats.ReadIORates, metapb.RecordPair{
			Key:   name,
			Value: v.ReadBytes,
		})
	}

	s.forEachReplica(func(pr *replica) bool {
		// TODO: re-enable this
		//if pr.ps.isApplyingSnapshot() {
		//	stats.ApplyingSnapCount++
		//}

		stats.ResourceCount++
		return true
	})
	// FIXME: provide this count from the new implementation
	// stats.ReceivingSnapCount = s.snapshotManager.ReceiveSnapCount()
	stats.SendingSnapCount = s.trans.SendingSnapshotCount()
	stats.StartTime = uint64(s.Meta().StartTime)

	s.cfg.Storage.ForeachDataStorageFunc(func(db storage.DataStorage) {
		st := db.Stats()
		stats.WrittenBytes += st.WrittenBytes
		stats.WrittenKeys += st.WrittenKeys
		stats.ReadKeys += st.ReadKeys
		stats.ReadBytes += st.ReadBytes
	})

	// TODO: is busy
	stats.IsBusy = false
	stats.Interval = &metapb.TimeInterval{
		Start: uint64(last.Unix()),
		End:   uint64(time.Now().Unix()),
	}

	var data []byte
	if s.cfg.Customize.CustomStoreHeartbeatDataProcessor != nil {
		data = s.cfg.Customize.CustomStoreHeartbeatDataProcessor.CollectData()
	}
	return rpcpb.ContainerHeartbeatReq{Stats: stats, Data: data}, nil
}

func (s *store) startHandleResourceHeartbeat() {
	c, err := s.pd.GetClient().GetResourceHeartbeatRspNotifier()
	if err != nil {
		s.logger.Fatal("tail to start handle resource heartbeat resp task",
			s.storeField(),
			zap.Error(err))
	}
	s.stopper.RunWorker(func() {
		for {
			select {
			case <-s.stopper.ShouldStop():
				s.logger.Info("handle resource heartbeat resp task stopped",
					s.storeField())
				return
			case rsp, ok := <-c:
				if ok {
					s.doResourceHeartbeatRsp(rsp)
				}
			}
		}
	})
}

func (s *store) doResourceHeartbeatRsp(rsp rpcpb.ResourceHeartbeatRsp) {
	if rsp.DestroyDirectly {
		s.destroyReplica(rsp.ResourceID, true, true, "remove by pd")
		return
	}

	pr := s.getReplica(rsp.ResourceID, true)
	if pr == nil {
		s.logger.Info("skip heartbeat resp",
			s.storeField(),
			log.ShardIDField(rsp.ResourceID),
			log.ReasonField("not leader"))
		return
	}

	if rsp.ConfigChange != nil {
		s.logger.Info("send conf change request",
			s.storeField(),
			log.ShardIDField(rsp.ResourceID),
			log.ConfigChangeFieldWithHeartbeatResp("change", rsp))
		pr.addAdminRequest(rpc.AdminCmdType_ConfigChange, &rpc.ConfigChangeRequest{
			ChangeType: rsp.ConfigChange.ChangeType,
			Replica:    rsp.ConfigChange.Replica,
		})
	} else if rsp.ConfigChangeV2 != nil {
		s.logger.Info("send conf change request",
			s.storeField(),
			log.ShardIDField(rsp.ResourceID),
			log.ConfigChangesFieldWithHeartbeatResp("changes", rsp))
		panic("ConfigChangeV2 request from prophet")
	} else if rsp.TransferLeader != nil {
		s.logger.Info("send transfer leader request",
			s.storeField(),
			log.ShardIDField(rsp.ResourceID))
		pr.addAdminRequest(rpc.AdminCmdType_TransferLeader, &rpc.TransferLeaderRequest{
			Replica: rsp.TransferLeader.Replica,
		})
	} else if rsp.SplitResource != nil {
		// currently, pd only support use keys to splits
		switch rsp.SplitResource.Policy {
		case metapb.CheckPolicy_USEKEY:
			splitIDs, err := pr.store.pd.GetClient().AskBatchSplit(NewResourceAdapterWithShard(pr.getShard()),
				uint32(len(rsp.SplitResource.Keys)))
			if err != nil {
				s.logger.Error("fail to ask batch split",
					s.storeField(),
					log.ShardIDField(rsp.ResourceID),
					zap.Error(err))
				return
			}
			pr.addAction(action{
				epoch:      rsp.ResourceEpoch,
				actionType: splitAction,
				splitCheckData: splitCheckData{
					splitKeys: rsp.SplitResource.Keys,
					splitIDs:  splitIDs,
				},
			})
		}
	}
}

type storageStatsReader interface {
	stats() (storageStats, error)
}

type storageStats struct {
	capacity  uint64
	available uint64
	usedSize  uint64
}

type memoryStorageStatsReader struct {
}

func newMemoryStorageStatsReader() storageStatsReader {
	return &memoryStorageStatsReader{}
}

func (s *memoryStorageStatsReader) stats() (storageStats, error) {
	ms, err := util.MemStats()
	if err != nil {
		return storageStats{}, err
	}

	return storageStats{
		capacity:  ms.Total,
		usedSize:  ms.Total - ms.Available,
		available: ms.Available,
	}, nil
}

type diskStorageStatsReader struct {
	dir string
}

func newDiskStorageStatsReader(dir string) storageStatsReader {
	return &diskStorageStatsReader{dir: dir}
}

func (s *diskStorageStatsReader) stats() (storageStats, error) {
	ms, err := util.DiskStats(s.dir)
	if err != nil {
		return storageStats{}, err
	}

	return storageStats{
		capacity:  ms.Total,
		usedSize:  ms.Total - ms.Free,
		available: ms.Free,
	}, nil
}
