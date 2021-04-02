package raftstore

import (
	"context"
	"time"

	"github.com/deepfabric/beehive/pb/bhmetapb"
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

		for {
			select {
			case <-ctx.Done():
				logger.Infof("store heartbeat task stopped")
				return
			case <-ticker.C:
				s.doStoreHeartbeat()
			}
		}
	})
}

func (s *store) getShardCount() uint64 {
	c := uint64(0)
	s.replicas.Range(func(key, value interface{}) bool {
		c++
		return true
	})
	return c
}

func (s *store) doStoreHeartbeat() {
	stats := &rpcpb.ContainerStats{}
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

	s.foreachPR(func(pr *peerReplica) bool {
		if pr.ps.isApplyingSnapshot() {
			stats.ApplyingSnapCount++
		}

		stats.ResourceCount++
		return true
	})
	stats.ResourceCount = s.getShardCount()
	stats.ReceivingSnapCount = s.snapshotManager.ReceiveSnapCount()

	// let snap_stats = self.ctx.snap_mgr.stats();
	// stats.set_sending_snap_count(snap_stats.sending_count as u32);
	// stats.set_receiving_snap_count(snap_stats.receiving_count as u32);
	// STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
	// 	.with_label_values(&["sending"])
	// 	.set(snap_stats.sending_count as i64);
	// STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
	// 	.with_label_values(&["receiving"])
	// 	.set(snap_stats.receiving_count as i64);

	// let apply_snapshot_count = self.ctx.applying_snap_count.load(Ordering::SeqCst);
	// stats.set_applying_snap_count(apply_snapshot_count as u32);
	// STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
	// 	.with_label_values(&["applying"])
	// 	.set(apply_snapshot_count as i64);

	// stats.set_start_time(self.fsm.store.start_time.unwrap().sec as u32);

	// // report store write flow to pd
	// stats.set_bytes_written(
	// 	self.ctx
	// 		.global_stat
	// 		.stat
	// 		.engine_total_bytes_written
	// 		.swap(0, Ordering::SeqCst),
	// );
	// stats.set_keys_written(
	// 	self.ctx
	// 		.global_stat
	// 		.stat
	// 		.engine_total_keys_written
	// 		.swap(0, Ordering::SeqCst),
	// );

	// stats.set_is_busy(
	// 	self.ctx
	// 		.global_stat
	// 		.stat
	// 		.is_busy
	// 		.swap(false, Ordering::SeqCst),
	// );

	// let store_info = StoreInfo {
	// 	engine: self.ctx.engines.kv.clone(),
	// 	capacity: self.ctx.cfg.capacity.0,
	// };

	// let task = PdTask::StoreHeartbeat { stats, store_info };
	// if let Err(e) = self.ctx.pd_scheduler.schedule(task) {
	// 	error!("notify pd failed";
	// 		"store_id" => self.fsm.store.id,
	// 		"err" => ?e
	// 	);
	// }
}
