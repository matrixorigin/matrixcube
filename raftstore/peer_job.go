package raftstore

import (
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
	sn "github.com/matrixorigin/matrixcube/snapshot"
	"go.etcd.io/etcd/raft/raftpb"
)

func (pr *peerReplica) startApplyingSnapJob() {
	pr.ps.applySnapJobLock.Lock()
	err := pr.store.addApplyJob(pr.applyWorker, "doApplyingSnapshotJob", pr.doApplyingSnapshotJob, pr.ps.setApplySnapJob)
	if err != nil {
		logger.Fatalf("shard %d add apply snapshot task failed with %+v",
			pr.shardID,
			err)
	}
	pr.ps.applySnapJobLock.Unlock()
}

func (pr *peerReplica) startRegistrationJob() {
	delegate := &applyDelegate{
		store:            pr.store,
		ps:               pr.ps,
		peerID:           pr.peer.ID,
		shard:            pr.ps.shard,
		term:             pr.getCurrentTerm(),
		applyState:       pr.ps.applyState,
		appliedIndexTerm: pr.ps.appliedIndexTerm,
		ctx:              newApplyContext(pr.shardID, pr.store),
	}

	err := pr.store.addApplyJob(pr.applyWorker, "doRegistrationJob", func() error {
		return pr.doRegistrationJob(delegate)
	}, nil)

	if err != nil {
		logger.Fatalf("shard %d add registration job failed with %+v",
			pr.ps.shard.ID,
			err)
	}
}

func (pr *peerReplica) startApplyCommittedEntriesJob(shardID uint64, term uint64, commitedEntries []raftpb.Entry) error {
	err := pr.store.addApplyJob(pr.applyWorker, "doApplyCommittedEntries", func() error {
		return pr.doApplyCommittedEntries(shardID, term, commitedEntries)
	}, nil)
	return err
}

func (pr *peerReplica) startCompactRaftLogJob(shardID, startIndex, endIndex uint64) error {
	err := pr.store.addApplyJob(pr.applyWorker, "doCompactRaftLog", func() error {
		return pr.doCompactRaftLog(shardID, startIndex, endIndex)
	}, nil)

	return err
}

func (s *store) startDestroyJob(shardID uint64, peer metapb.Peer) error {
	pr := s.getPR(shardID, false)
	if pr != nil {
		err := s.addApplyJob(pr.applyWorker, "doDestroy", func() error {
			s.doDestroy(shardID, false)
			return nil
		}, nil)
		return err
	}

	return nil
}

func (pr *peerReplica) startProposeJob(c cmd, isConfChange bool) error {
	err := pr.store.addApplyJob(pr.applyWorker, "doPropose", func() error {
		return pr.doPropose(c, isConfChange)
	}, nil)

	return err
}

func (pr *peerReplica) startSplitCheckJob() error {
	shard := pr.ps.shard
	epoch := shard.Epoch
	startKey := encStartKey(&shard)
	endKey := encEndKey(&shard)

	logger.Infof("shard %d start split check job from %+v to %+v",
		pr.ps.shard.ID,
		startKey,
		endKey)
	err := pr.store.addSplitJob(func() error {
		return pr.doSplitCheck(epoch, startKey, endKey)
	})

	return err
}

func (ps *peerStorage) cancelApplyingSnapJob() bool {
	ps.applySnapJobLock.RLock()
	if ps.applySnapJob == nil {
		ps.applySnapJobLock.RUnlock()
		return true
	}

	ps.applySnapJob.Cancel()

	if ps.applySnapJob.IsCancelled() {
		ps.applySnapJobLock.RUnlock()
		return true
	}

	succ := !ps.isApplyingSnapshot()
	ps.applySnapJobLock.RUnlock()
	return succ
}

func (ps *peerStorage) resetApplyingSnapJob() {
	ps.applySnapJobLock.Lock()
	ps.applySnapJob = nil
	ps.applySnapJobLock.Unlock()
}

func (ps *peerStorage) resetGenSnapJob() {
	ps.genSnapJob = nil
}

func (pr *peerReplica) doPropose(c cmd, isConfChange bool) error {
	value, ok := pr.store.delegates.Load(pr.shardID)
	if !ok {
		c.respShardNotFound(pr.shardID)
		return nil
	}

	delegate := value.(*applyDelegate)
	if delegate.shard.ID != pr.shardID {
		logger.Fatal("BUG: delegate id not match")
	}

	if isConfChange {
		changeC := delegate.pendingChangePeerCMD
		if changeC.req != nil && changeC.req.Header != nil {
			delegate.notifyStaleCMD(changeC)
		}
		delegate.pendingChangePeerCMD = c
	} else {
		delegate.appendPendingCmd(c)
	}

	return nil
}

func (ps *peerStorage) doGenerateSnapshotJob() error {
	start := time.Now()

	if ps.genSnapJob == nil {
		logger.Fatalf("shard %d generating snapshot job is nil", ps.shard.ID)
	}

	applyState, err := ps.loadApplyState()
	if err != nil {
		logger.Fatalf("shard %d load snapshot failed with %+v",
			ps.shard.ID,
			err)
		return nil
	} else if nil == applyState {
		logger.Fatalf("shard %d could not load snapshot", ps.shard.ID)
		return nil
	}

	var term uint64
	if applyState.AppliedIndex == applyState.TruncatedState.Index {
		term = applyState.TruncatedState.Term
	} else {
		entry, err := ps.loadLogEntry(applyState.AppliedIndex)
		if err != nil {
			return nil
		}

		term = entry.Term
	}

	state, err := ps.loadLocalState(nil)
	if err != nil {
		return nil
	}

	if state.State != bhraftpb.PeerState_Normal {
		logger.Errorf("shard %d snap seems stale, skip", ps.shard.ID)
		return nil
	}

	msg := &bhraftpb.SnapshotMessage{}
	msg.Header = bhraftpb.SnapshotMessageHeader{
		Shard: state.Shard,
		Term:  term,
		Index: applyState.AppliedIndex,
	}

	snapshot := raftpb.Snapshot{}
	snapshot.Metadata.Term = msg.Header.Term
	snapshot.Metadata.Index = msg.Header.Index

	confState := raftpb.ConfState{}
	for _, peer := range ps.shard.Peers {
		confState.Voters = append(confState.Voters, peer.ID)
	}
	snapshot.Metadata.ConfState = confState

	if ps.store.snapshotManager.Register(msg, sn.Creating) {
		defer ps.store.snapshotManager.Deregister(msg, sn.Creating)

		err = ps.store.snapshotManager.Create(msg)
		if err != nil {
			logger.Errorf("shard %d create snapshot failed with %+v",
				ps.shard.ID,
				err)
			return nil
		}
	}

	snapshot.Data = protoc.MustMarshal(msg)
	ps.genSnapJob.SetResult(snapshot)

	metric.ObserveSnapshotBuildingDuration(start)
	logger.Infof("shard %d snapshot created, epoch=<%s> term=<%d> index=<%d> ",
		ps.shard.ID,
		msg.Header.Shard.Epoch.String(),
		msg.Header.Term,
		msg.Header.Index)
	return nil
}

func (pr *peerReplica) doSplitCheck(epoch metapb.ResourceEpoch, startKey, endKey []byte) error {
	if !pr.isLeader() {
		return nil
	}

	var size uint64
	var keys uint64
	var splitKeys [][]byte
	var err error

	useDefault := true
	if pr.store.cfg.Customize.CustomSplitCheckFuncFactory != nil {
		if fn := pr.store.cfg.Customize.CustomSplitCheckFuncFactory(pr.ps.shard.Group); fn != nil {
			size, keys, splitKeys, err = fn(pr.ps.shard)
			useDefault = false
		}
	}

	if useDefault {
		size, keys, splitKeys, err = pr.store.DataStorageByGroup(pr.ps.shard.Group, pr.ps.shard.ID).SplitCheck(startKey, endKey, uint64(pr.store.cfg.Replication.ShardCapacityBytes))
	}

	logger.Debugf("shard %d split check result, total size %d(%d), total keys %d, split keys %+v",
		pr.shardID,
		size,
		uint64(pr.store.cfg.Replication.ShardCapacityBytes),
		keys,
		splitKeys)

	if err != nil {
		logger.Errorf("shard %d scan split key failed with %+v",
			pr.shardID,
			err)
		return err
	}

	pr.approximateSize = size
	pr.approximateKeys = keys
	if len(splitKeys) == 0 {
		pr.sizeDiffHint = size
		return nil
	}

	logger.Infof("shard %d try to split, size %d bytes splitKeys %+v",
		pr.shardID,
		size,
		splitKeys)

	current := pr.ps.shard
	if current.Epoch.Version != epoch.Version {
		logger.Infof("shard %d epoch changed, need re-check later, current=<%+v> split=<%+v>",
			pr.shardID,
			current.Epoch,
			epoch)
		return nil
	}

	newIDs, err := pr.store.pd.GetClient().AskBatchSplit(NewResourceAdapterWithShard(current), uint32(len(splitKeys)))
	if err != nil {
		logger.Errorf("shard %d ask batch split failed with %+v",
			pr.shardID,
			err)
		return err
	}

	pr.addAction(action{actionType: doSplitAction, splitKeys: splitKeys, splitIDs: newIDs, epoch: epoch})
	return nil
}
