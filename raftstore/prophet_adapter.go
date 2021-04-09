package raftstore

import (
	"context"
	"fmt"
	"time"

	"github.com/deepfabric/beehive/pb/bhmetapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/prophet/metadata"
	"github.com/deepfabric/prophet/pb/metapb"
	"github.com/deepfabric/prophet/pb/rpcpb"
	"github.com/fagongzi/util/protoc"
)

type resourceAdapter struct {
	meta bhmetapb.Shard
}

func newResourceAdapter() metadata.Resource {
	return &resourceAdapter{}
}

func newResourceAdapterWithShard(meta bhmetapb.Shard) metadata.Resource {
	return &resourceAdapter{meta: meta}
}

func (ra *resourceAdapter) SetID(id uint64) {
	ra.meta.ID = id
}
func (ra *resourceAdapter) ID() uint64 {
	return ra.meta.ID
}

func (ra *resourceAdapter) Peers() []metapb.Peer {
	return ra.meta.Peers
}

func (ra *resourceAdapter) SetPeers(peers []metapb.Peer) {
	ra.meta.Peers = peers
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
	meta bhmetapb.Store
}

func newContainer() metadata.Container {
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

func (ca *containerAdapter) ActionOnJoinCluster() metapb.Action {
	return metapb.Action_None
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
	return newContainer()
}

func (s *store) startStoreHeartbeat() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(s.cfg.Replication.StoreHeartbeatDuration.Duration)
		defer ticker.Stop()

		last := time.Now()
		for {
			select {
			case <-ctx.Done():
				logger.Infof("store heartbeat task stopped")
				return
			case <-ticker.C:
				s.doStoreHeartbeat(last)
				last = time.Now()
			}
		}
	})
}

func (s *store) doStoreHeartbeat(last time.Time) {
	stats := rpcpb.ContainerStats{}
	stats.ContainerID = s.Meta().ID
	if s.cfg.UseMemoryAsStorage {
		ms, err := util.MemStats()
		if err != nil {
			logger.Errorf("get storage capacity status failed with %+v", err)
			return
		}
		stats.Capacity = ms.Total
		stats.UsedSize = ms.Total - ms.Available
	} else {
		ms, err := util.DiskStats(s.cfg.DataPath)
		if err != nil {
			logger.Errorf("get storage capacity status failed with %+v", err)
			return
		}
		stats.Capacity = ms.Total
		stats.UsedSize = ms.Total - ms.Free
	}
	if s.cfg.Capacity > 0 && stats.Capacity > uint64(s.cfg.Capacity) {
		stats.Capacity = uint64(s.cfg.Capacity)
	}

	// cpu usages
	usages, err := util.CpuUsages()
	if err != nil {
		logger.Errorf("get cpu usages failed with %+v", err)
		return
	}
	for i, v := range usages {
		stats.CpuUsages = append(stats.CpuUsages, rpcpb.RecordPair{
			Key:   fmt.Sprintf("cpu:%d", i),
			Value: uint64(v * 100),
		})
	}

	// io rates
	rates, err := util.IORates(s.cfg.DataPath)
	if err != nil {
		logger.Errorf("get io rates failed with %+v", err)
		return
	}
	for name, v := range rates {
		stats.WriteIORates = append(stats.WriteIORates, rpcpb.RecordPair{
			Key:   name,
			Value: v.WriteBytes,
		})
		stats.ReadIORates = append(stats.ReadIORates, rpcpb.RecordPair{
			Key:   name,
			Value: v.ReadBytes,
		})
	}

	s.foreachPR(func(pr *peerReplica) bool {
		if pr.ps.isApplyingSnapshot() {
			stats.ApplyingSnapCount++
		}

		stats.ResourceCount++
		return true
	})
	stats.ReceivingSnapCount = s.snapshotManager.ReceiveSnapCount()
	stats.SendingSnapCount = s.trans.SendingSnapshotCount()
	stats.StartTime = uint64(s.Meta().StartTime)

	s.cfg.Storage.ForeachDataStorageFunc(func(db storage.DataStorage) {
		st := db.Stats()
		stats.BytesWritten += st.WrittenBytes
		stats.KeysWritten += st.WrittenKeys
		stats.KeysRead += st.ReadKeys
		stats.BytesRead += st.ReadBytes
	})

	// TODO: is busy
	stats.IsBusy = false
	stats.Interval = &rpcpb.TimeInterval{
		Start: uint64(last.Unix()),
		End:   uint64(time.Now().Unix()),
	}

	_, err = s.pd.GetClient().ContainerHeartbeat(rpcpb.ContainerHeartbeatReq{Stats: stats})
	if err != nil {
		logger.Errorf("send store heartbeat failed with %+v", err)
	}
}

func (s *store) startHandleResourceHeartbeat() {
	c, err := s.pd.GetClient().GetResourceHeartbeatRspNotifier()
	if err != nil {
		logger.Fatalf("start handle resource heartbeat resp task failed with %+v", err)
	}
	s.runner.RunCancelableTask(func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				logger.Infof("handle resource heartbeat resp task stopped")
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
	pr := s.getPR(rsp.ResourceID, true)
	if pr == nil {
		logger.Infof("shard-%d is not leader, skip heartbeat resp",
			rsp.ResourceID)
		return
	}

	if rsp.ChangePeer != nil {
		logger.Infof("shard-%d %s peer %+v",
			rsp.ResourceID,
			rsp.ChangePeer.ChangeType.String(),
			rsp.ChangePeer.Peer)
		pr.onAdmin(newChangePeerAdminReq(rsp))
	} else if rsp.ChangePeerV2 != nil {
		pr.onAdmin(newChangePeerV2AdminReq(rsp))
	} else if rsp.TransferLeader != nil {
		pr.onAdmin(newTransferLeaderAdminReq(rsp))
	} else if rsp.SplitResource != nil {
		// currently, pd only support use keys to splits
		switch rsp.SplitResource.Policy {
		case metapb.CheckPolicy_USEKEY:
			splitIDs, err := pr.store.pd.GetClient().AskBatchSplit(newResourceAdapterWithShard(pr.ps.shard),
				uint32(len(rsp.SplitResource.Keys)))
			if err != nil {
				logger.Errorf("shard-%d ask batch split failed with %+v",
					rsp.ResourceID,
					err)
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

func newChangePeerAdminReq(rsp rpcpb.ResourceHeartbeatRsp) *raftcmdpb.AdminRequest {
	req := &raftcmdpb.AdminRequest{
		CmdType: raftcmdpb.AdminCmdType_ChangePeer,
		ChangePeer: &raftcmdpb.ChangePeerRequest{
			ChangeType: rsp.ChangePeer.ChangeType,
			Peer:       rsp.ChangePeer.Peer,
		},
	}

	req.ChangePeerV2.Changes = append(req.ChangePeerV2.Changes, raftcmdpb.ChangePeerRequest{
		ChangeType: rsp.ChangePeer.ChangeType,
		Peer:       rsp.ChangePeer.Peer,
	})
	return req
}

func newChangePeerV2AdminReq(rsp rpcpb.ResourceHeartbeatRsp) *raftcmdpb.AdminRequest {
	req := &raftcmdpb.AdminRequest{
		CmdType:      raftcmdpb.AdminCmdType_ChangePeerV2,
		ChangePeerV2: &raftcmdpb.ChangePeerV2Request{},
	}

	for _, ch := range rsp.ChangePeerV2.Changes {
		req.ChangePeerV2.Changes = append(req.ChangePeerV2.Changes, raftcmdpb.ChangePeerRequest{
			ChangeType: ch.ChangeType,
			Peer:       ch.Peer,
		})
	}
	return req
}

func newTransferLeaderAdminReq(rsp rpcpb.ResourceHeartbeatRsp) *raftcmdpb.AdminRequest {
	req := &raftcmdpb.AdminRequest{
		CmdType: raftcmdpb.AdminCmdType_TransferLeader,
		TransferLeader: &raftcmdpb.TransferLeaderRequest{
			Peer: rsp.TransferLeader.Peer,
		},
	}

	req.ChangePeerV2.Changes = append(req.ChangePeerV2.Changes, raftcmdpb.ChangePeerRequest{
		ChangeType: rsp.ChangePeer.ChangeType,
		Peer:       rsp.ChangePeer.Peer,
	})
	return req
}
