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
	return ra.meta.ID
}

func (ra *resourceAdapter) SetID(id uint64) {
	ra.meta.ID = id
}

func (ra *resourceAdapter) Group() uint64 {
	return ra.meta.Group
}

func (ra *resourceAdapter) SetGroup(group uint64) {
	ra.meta.Group = group
}

func (ra *resourceAdapter) Peers() []Replica {
	return ra.meta.Replicas
}

func (ra *resourceAdapter) SetPeers(peers []Replica) {
	ra.meta.Replicas = peers
}

func (ra *resourceAdapter) Range() ([]byte, []byte) {
	return ra.meta.Start, ra.meta.End
}

func (ra *resourceAdapter) SetStartKey(value []byte) {
	ra.meta.Start = value
}

func (ra *resourceAdapter) SetEndKey(value []byte) {
	ra.meta.End = value
}

func (ra *resourceAdapter) Epoch() metapb.ResourceEpoch {
	return ra.meta.Epoch
}

func (ra *resourceAdapter) SetEpoch(value metapb.ResourceEpoch) {
	ra.meta.Epoch = value
}

func (ra *resourceAdapter) State() metapb.ResourceState {
	return ra.meta.State
}

func (ra *resourceAdapter) SetState(state metapb.ResourceState) {
	ra.meta.State = state
}

func (ra *resourceAdapter) Unique() string {
	return ra.meta.Unique
}

func (ra *resourceAdapter) SetUnique(value string) {
	ra.meta.Unique = value
}

func (ra *resourceAdapter) Data() []byte {
	return ra.meta.Data
}

func (ra *resourceAdapter) SetData(value []byte) {
	ra.meta.Data = value
}

func (ra *resourceAdapter) RuleGroups() []string {
	return ra.meta.RuleGroups
}

func (ra *resourceAdapter) SetRuleGroups(values ...string) {
	ra.meta.RuleGroups = values
}

func (ra *resourceAdapter) Marshal() ([]byte, error) {
	return protoc.MustMarshal(&ra.meta), nil
}

func (ra *resourceAdapter) Unmarshal(data []byte) error {
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
	meta meta.Store
}

func newContainerAdapter() metadata.Container {
	return &containerAdapter{}
}

func (ca *containerAdapter) SetAddrs(addr, shardAddr string) {
	ca.meta.ClientAddr = addr
	ca.meta.RaftAddr = shardAddr
}

func (ca *containerAdapter) Addr() string {
	return ca.meta.ClientAddr
}

func (ca *containerAdapter) ShardAddr() string {
	return ca.meta.RaftAddr
}

func (ca *containerAdapter) SetID(id uint64) {
	ca.meta.ID = id
}

func (ca *containerAdapter) ID() uint64 {
	return ca.meta.ID
}

func (ca *containerAdapter) Labels() []metapb.Pair {
	return ca.meta.Labels
}

func (ca *containerAdapter) SetLabels(labels []metapb.Pair) {
	ca.meta.Labels = labels
}

func (ca *containerAdapter) StartTimestamp() int64 {
	return ca.meta.StartTime
}

func (ca *containerAdapter) SetStartTimestamp(value int64) {
	ca.meta.StartTime = value
}

func (ca *containerAdapter) Version() (string, string) {
	return ca.meta.Version, ca.meta.GitHash
}

func (ca *containerAdapter) SetVersion(version string, githash string) {
	ca.meta.Version = version
	ca.meta.GitHash = githash
}

func (ca *containerAdapter) DeployPath() string {
	return ca.meta.DeployPath
}

func (ca *containerAdapter) SetDeployPath(value string) {
	ca.meta.DeployPath = value
}

func (ca *containerAdapter) State() metapb.ContainerState {
	return ca.meta.State
}

func (ca *containerAdapter) SetState(value metapb.ContainerState) {
	ca.meta.State = value
}

func (ca *containerAdapter) LastHeartbeat() int64 {
	return ca.meta.LastHeartbeatTime
}

func (ca *containerAdapter) SetLastHeartbeat(value int64) {
	ca.meta.LastHeartbeatTime = value
}

func (ca *containerAdapter) PhysicallyDestroyed() bool {
	return ca.meta.PhysicallyDestroyed
}

func (ca *containerAdapter) SetPhysicallyDestroyed(v bool) {
	ca.meta.PhysicallyDestroyed = v
}

func (ca *containerAdapter) Marshal() ([]byte, error) {
	return protoc.MustMarshal(&ca.meta), nil
}

func (ca *containerAdapter) Unmarshal(data []byte) error {
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

func (s *store) doShardHeartbeat() {
	s.forEachReplica(func(pr *replica) bool {
		if pr.isLeader() {
			pr.addAction(action{actionType: heartbeatAction})
		}
		return true
	})
}

func (s *store) doStoreHeartbeat(last time.Time) {
	req, err := s.getStoreHeartbeat(last)
	if err != nil {
		return
	}

	rsp, err := s.pd.GetClient().ContainerHeartbeat(req)
	if err != nil {
		s.logger.Error("fail to send store heartbeat",
			s.storeField(),
			zap.Error(err))
		return
	}
	if s.cfg.Customize.CustomStoreHeartbeatDataProcessor != nil {
		err := s.cfg.Customize.CustomStoreHeartbeatDataProcessor.HandleHeartbeatRsp(rsp.Data)
		if err != nil {
			s.logger.Error("fail to handle store heartbeat rsp data",
				s.storeField(),
				zap.Error(err))
		}
	}
}

func (s *store) getStoreHeartbeat(last time.Time) (rpcpb.ContainerHeartbeatReq, error) {
	stats := metapb.ContainerStats{}
	stats.ContainerID = s.Meta().ID
	if s.cfg.UseMemoryAsStorage {
		ms, err := util.MemStats()
		if err != nil {
			s.logger.Error("fail to get storage capacity status",
				s.storeField(),
				zap.Error(err))
			return rpcpb.ContainerHeartbeatReq{}, err
		}
		stats.Capacity = ms.Total
		stats.UsedSize = ms.Total - ms.Available
		stats.Available = ms.Available
	} else {
		ms, err := util.DiskStats(s.cfg.DataPath)
		if err != nil {
			s.logger.Error("fail to get storage capacity status",
				s.storeField(),
				zap.Error(err))
			return rpcpb.ContainerHeartbeatReq{}, err
		}
		stats.Capacity = ms.Total
		stats.UsedSize = ms.Total - ms.Free
		stats.Available = ms.Free
	}
	if s.cfg.Capacity > 0 && stats.Capacity > uint64(s.cfg.Capacity) {
		stats.Capacity = uint64(s.cfg.Capacity)
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
	stats.ReceivingSnapCount = s.snapshotManager.ReceiveSnapCount()
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
	if rsp.DestoryDirectly {
		s.destroyReplica(rsp.ResourceID, true, "remove by pd")
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
		pr.onAdmin(newConfigChangeAdminReq(rsp))
	} else if rsp.ConfigChangeV2 != nil {
		s.logger.Info("send conf change request",
			s.storeField(),
			log.ShardIDField(rsp.ResourceID),
			log.ConfigChangesFieldWithHeartbeatResp("changes", rsp))
		pr.onAdmin(newConfigChangeV2AdminReq(rsp))
	} else if rsp.TransferLeader != nil {
		pr.onAdmin(newTransferLeaderAdminReq(rsp))
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
				actionType: doSplitAction,
				splitKeys:  rsp.SplitResource.Keys,
				splitIDs:   splitIDs,
			})
		}
	}
}

func newConfigChangeAdminReq(rsp rpcpb.ResourceHeartbeatRsp) rpc.AdminRequest {
	return rpc.AdminRequest{
		CmdType: rpc.AdminCmdType_ConfigChange,
		ConfigChange: &rpc.ConfigChangeRequest{
			ChangeType: rsp.ConfigChange.ChangeType,
			Replica:    rsp.ConfigChange.Replica,
		},
	}
}

func newConfigChangeV2AdminReq(rsp rpcpb.ResourceHeartbeatRsp) rpc.AdminRequest {
	req := rpc.AdminRequest{
		CmdType:        rpc.AdminCmdType_ConfigChangeV2,
		ConfigChangeV2: &rpc.ConfigChangeV2Request{},
	}

	for _, ch := range rsp.ConfigChangeV2.Changes {
		req.ConfigChangeV2.Changes = append(req.ConfigChangeV2.Changes, rpc.ConfigChangeRequest{
			ChangeType: ch.ChangeType,
			Replica:    ch.Replica,
		})
	}
	return req
}

func newTransferLeaderAdminReq(rsp rpcpb.ResourceHeartbeatRsp) rpc.AdminRequest {
	return rpc.AdminRequest{
		CmdType: rpc.AdminCmdType_TransferLeader,
		TransferLeader: &rpc.TransferLeaderRequest{
			Replica: rsp.TransferLeader.Replica,
		},
	}
}
