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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
	"github.com/matrixorigin/matrixcube/storage"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

const (
	// When we create a shard peer, we should initialize its log term/index > 0,
	// so that we can force the follower peer to sync the snapshot first.
	raftInitLogTerm  = 5
	raftInitLogIndex = 5
)

var (
	emptyEntry = raftpb.Entry{}
)

type peerStorage struct {
	store *store
	shard bhmetapb.Shard

	lastTerm         uint64
	appliedIndexTerm uint64
	lastReadyIndex   uint64
	lastCompactIndex uint64
	raftLocalState   bhraftpb.RaftLocalState
	raftApplyState   bhraftpb.RaftApplyState

	genSnapJob       *task.Job
	applySnapJob     *task.Job
	applySnapJobLock sync.RWMutex

	pendingReads *readIndexQueue
}

func newPeerStorage(store *store, shard bhmetapb.Shard) (*peerStorage, error) {
	s := new(peerStorage)
	s.store = store
	s.shard = shard
	s.appliedIndexTerm = raftInitLogTerm

	err := s.initRaftLocalState()
	if err != nil {
		return nil, err
	}
	logger.Infof("shard %d init with raft local state %+v",
		shard.ID,
		s.raftLocalState)

	err = s.initRaftApplyState()
	if err != nil {
		return nil, err
	}
	logger.Infof("shard %d init raft apply state, state=<%+v>",
		shard.ID,
		s.raftApplyState)

	err = s.initLastTerm()
	if err != nil {
		return nil, err
	}
	logger.Infof("shard %d init last term, last term=<%d>",
		shard.ID,
		s.lastTerm)

	s.lastReadyIndex = s.getAppliedIndex()
	s.pendingReads = new(readIndexQueue)

	return s, nil
}

func (ps *peerStorage) initRaftLocalState() error {
	v, err := ps.store.MetadataStorage().Get(getRaftLocalStateKey(ps.shard.ID))
	if err != nil {
		return err
	}
	if len(v) > 0 {
		s := &bhraftpb.RaftLocalState{}
		err = s.Unmarshal(v)
		if err != nil {
			return err
		}

		ps.raftLocalState = *s
		return nil
	}

	s := &bhraftpb.RaftLocalState{}
	if len(ps.shard.Peers) > 0 {
		s.LastIndex = raftInitLogIndex
		s.HardState.Commit = raftInitLogIndex
		s.HardState.Term = raftInitLogTerm
	}

	ps.raftLocalState = *s
	return nil
}

func (ps *peerStorage) initRaftApplyState() error {
	v, err := ps.store.MetadataStorage().Get(getRaftApplyStateKey(ps.shard.ID))
	if err != nil {
		return err
	}
	if len(v) > 0 && len(ps.shard.Peers) > 0 {
		s := &bhraftpb.RaftApplyState{}
		err = s.Unmarshal(v)
		if err != nil {
			return err
		}

		ps.raftApplyState = *s
		appliedIndex := ps.raftApplyState.AppliedIndex
		if ps.store.cfg.Customize.CustomAdjustInitAppliedIndexFactory != nil {
			factory := ps.store.cfg.Customize.CustomAdjustInitAppliedIndexFactory(ps.shard.Group)
			if factory != nil {
				newAppliedIndex := factory(ps.shard, appliedIndex)
				if newAppliedIndex < raftInitLogIndex {
					if newAppliedIndex == 0 {
						newAppliedIndex = appliedIndex
					} else {
						logger.Fatalf("shard %d unexpect adjust applied index, ajdust index %d must >= init applied index %d",
							ps.shard.ID,
							newAppliedIndex,
							raftInitLogIndex)
					}
				}
				if newAppliedIndex > appliedIndex {
					logger.Fatalf("shard %d unexpect adjust applied index, ajdust index %d must <= real applied index %d",
						ps.shard.ID,
						newAppliedIndex,
						appliedIndex)
				}

				logger.Infof("shard %d change init applied index from %d to %d",
					ps.shard.ID, appliedIndex, newAppliedIndex)
				appliedIndex = newAppliedIndex
			}
		}
		ps.raftApplyState.AppliedIndex = appliedIndex

		return nil
	}

	if len(ps.shard.Peers) > 0 {
		ps.raftApplyState.AppliedIndex = raftInitLogIndex
		ps.raftApplyState.TruncatedState.Index = raftInitLogIndex
		ps.raftApplyState.TruncatedState.Term = raftInitLogTerm
	}

	return nil
}

func (ps *peerStorage) initLastTerm() error {
	lastIndex := ps.raftLocalState.LastIndex

	if lastIndex == 0 {
		ps.lastTerm = lastIndex
		return nil
	} else if lastIndex == raftInitLogIndex {
		ps.lastTerm = raftInitLogTerm
		return nil
	} else if lastIndex == ps.raftApplyState.TruncatedState.Index {
		ps.lastTerm = ps.raftApplyState.TruncatedState.Term
		return nil
	} else if lastIndex < raftInitLogIndex {
		logger.Fatalf("shard %d error raft last index %d",
			ps.shard.ID,
			lastIndex)
		return nil
	}

	v, err := ps.store.MetadataStorage().Get(getRaftLogKey(ps.shard.ID, lastIndex))
	if err != nil {
		return err
	}

	if nil == v {
		return fmt.Errorf("shard %d entry at index<%d> doesn't exist, may lose data",
			ps.shard.ID,
			lastIndex)
	}

	s := &raftpb.Entry{}
	err = s.Unmarshal(v)
	if err != nil {
		return err
	}

	ps.lastTerm = s.Term
	return nil
}

func (ps *peerStorage) isApplyComplete() bool {
	return ps.getCommittedIndex() == ps.getAppliedIndex()
}

func (ps *peerStorage) getAppliedIndex() uint64 {
	return ps.raftApplyState.AppliedIndex
}

func (ps *peerStorage) getCommittedIndex() uint64 {
	return ps.raftLocalState.HardState.Commit
}

func (ps *peerStorage) getTruncatedIndex() uint64 {
	return ps.raftApplyState.TruncatedState.Index
}

func (ps *peerStorage) getTruncatedTerm() uint64 {
	return ps.raftApplyState.TruncatedState.Term
}

func (ps *peerStorage) validateSnap(snap *raftpb.Snapshot) bool {
	snapData := &bhraftpb.SnapshotMessage{}
	err := snapData.Unmarshal(snap.Data)
	if err != nil {
		logger.Errorf("shard %d decode snapshot failed with %+v",
			ps.shard.ID,
			err)
		return false
	}

	snapEpoch := snapData.Header.Shard.Epoch
	lastEpoch := ps.shard.Epoch

	if snapEpoch.ConfVer < lastEpoch.ConfVer {
		logger.Infof("shard %d snapshot epoch %d/%d stale, generate again.",
			ps.shard.ID,
			snapEpoch.ConfVer,
			lastEpoch.ConfVer)
		return false
	}

	return true
}

func (ps *peerStorage) isInitialized() bool {
	return len(ps.shard.Peers) != 0
}

func (ps *peerStorage) isApplyingSnapshot() bool {
	return ps.applySnapJob != nil && ps.applySnapJob.IsNotComplete()
}

func (ps *peerStorage) checkRange(low, high uint64) error {
	if low > high {
		return fmt.Errorf("shard %d low %d is greater that high %d",
			ps.shard.ID,
			low,
			high)
	} else if low <= ps.getTruncatedIndex() {
		return raft.ErrCompacted
	} else {
		i, err := ps.LastIndex()
		if err != nil {
			return err
		}

		if high > i+1 {
			return fmt.Errorf("shard %d entries' high is out of bound lastindex, hight=<%d> lastindex=<%d>",
				ps.shard.ID,
				high,
				i)
		}
	}

	return nil
}

func (ps *peerStorage) loadLogEntry(index uint64) (raftpb.Entry, error) {
	key := getRaftLogKey(ps.shard.ID, index)
	v, err := ps.store.MetadataStorage().Get(key)
	if err != nil {
		logger.Errorf("shard %d load entry failed at %d with %+v",
			ps.shard.ID,
			index,
			err)
		return emptyEntry, err
	} else if len(v) == 0 {
		logger.Errorf("shard %d entry %d not found",
			ps.shard.ID,
			index)
		return emptyEntry, fmt.Errorf("log entry at %d not found", index)
	}

	return ps.unmarshal(v, index)
}

func (ps *peerStorage) loadShardLocalState(job *task.Job) (*bhraftpb.ShardLocalState, error) {
	if nil != job &&
		job.IsCancelling() {
		return nil, task.ErrJobCancelled
	}

	return loadShardLocalState(ps.shard.ID, ps.store.MetadataStorage(), false)
}

func (ps *peerStorage) applySnapshot(job *task.Job) error {
	if nil != job &&
		job.IsCancelling() {
		return task.ErrJobCancelled
	}

	snap := &bhraftpb.SnapshotMessage{}
	snap.Header = bhraftpb.SnapshotMessageHeader{
		Shard: ps.shard,
		Term:  ps.raftApplyState.TruncatedState.Term,
		Index: ps.raftApplyState.TruncatedState.Index,
	}

	return ps.store.snapshotManager.Apply(snap)
}

func (ps *peerStorage) loadRaftApplyState() (*bhraftpb.RaftApplyState, error) {
	key := getRaftApplyStateKey(ps.shard.ID)
	v, err := ps.store.MetadataStorage().Get(key)
	if err != nil {
		logger.Errorf("shard %d load apply state failed with %d",
			ps.shard.ID,
			err)
		return nil, err
	}

	if len(v) == 0 {
		return nil, errors.New("shard apply state not found")
	}

	applyState := &bhraftpb.RaftApplyState{}
	err = applyState.Unmarshal(v)
	return applyState, err
}

func (ps *peerStorage) unmarshal(v []byte, expectIndex uint64) (raftpb.Entry, error) {
	e := raftpb.Entry{}
	protoc.MustUnmarshal(&e, v)
	if e.Index != expectIndex {
		logger.Fatalf("shard %d raft log index not match, logIndex %d expect %d",
			ps.shard.ID,
			e.Index,
			expectIndex)
	}

	return e, nil
}

func compactRaftLog(shardID uint64, state *bhraftpb.RaftApplyState, compactIndex, compactTerm uint64) error {
	logger.Debugf("shard %d compact log entries to index %d",
		shardID,
		compactIndex)
	if compactIndex <= state.TruncatedState.Index {
		return errors.New("try to truncate compacted entries")
	} else if compactIndex > state.AppliedIndex {
		return fmt.Errorf("compact index %d > applied index %d", compactIndex, state.AppliedIndex)
	}

	// we don't actually delete the logs now, we add an async task to do it.
	state.TruncatedState.Index = compactIndex
	state.TruncatedState.Term = compactTerm

	return nil
}

func loadShardLocalState(shardID uint64, driver storage.MetadataStorage, allowNotFound bool) (*bhraftpb.ShardLocalState, error) {
	key := getShardLocalStateKey(shardID)
	v, err := driver.Get(key)
	if err != nil {
		logger.Errorf("shard %d load raft state failed with %+v",
			shardID,
			err)
		return nil, err
	} else if len(v) == 0 {
		if allowNotFound {
			return nil, nil
		}

		return nil, errors.New("shard state not found")
	}

	stat := &bhraftpb.ShardLocalState{}
	err = stat.Unmarshal(v)

	return stat, err
}

func (ps *peerStorage) isGeneratingSnap() bool {
	return ps.genSnapJob != nil && ps.genSnapJob.IsNotComplete()
}

func (ps *peerStorage) isGenSnapJobComplete() bool {
	return ps.genSnapJob != nil && ps.genSnapJob.IsComplete()
}

// ============================ etcd raft storage interface method
func (ps *peerStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	confState := raftpb.ConfState{}
	if ps.raftLocalState.HardState.Commit == 0 &&
		ps.raftLocalState.HardState.Term == 0 &&
		ps.raftLocalState.HardState.Vote == 0 {
		if ps.isInitialized() {
			logger.Fatalf("shard %d shard is initialized but local state has empty hard state, hardState=<%v>",
				ps.shard.ID,
				ps.raftLocalState.HardState)
		}

		return ps.raftLocalState.HardState, confState, nil
	}

	for _, p := range ps.shard.Peers {
		if p.Role == metapb.PeerRole_Voter {
			confState.Voters = append(confState.Voters, p.ID)
		} else if p.Role == metapb.PeerRole_Learner {
			confState.Learners = append(confState.Learners, p.ID)
		}
	}

	return ps.raftLocalState.HardState, confState, nil
}

func (ps *peerStorage) Entries(low, high, maxSize uint64) ([]raftpb.Entry, error) {
	err := ps.checkRange(low, high)
	if err != nil {
		return nil, err
	}

	var ents []raftpb.Entry
	if low == high {
		return ents, nil
	}

	var totalSize uint64
	nextIndex := low
	exceededMaxSize := false

	startKey := getRaftLogKey(ps.shard.ID, low)

	if low+1 == high {
		// If election happens in inactive shards, they will just try
		// to fetch one empty log.
		v, err := ps.store.MetadataStorage().Get(startKey)
		if err != nil {
			return nil, err
		}

		if len(v) == 0 {
			return nil, raft.ErrUnavailable
		}

		e, err := ps.unmarshal(v, low)
		if err != nil {
			return nil, err
		}

		ents = append(ents, e)
		return ents, nil
	}

	endKey := getRaftLogKey(ps.shard.ID, high)
	err = ps.store.MetadataStorage().Scan(startKey, endKey, func(key, value []byte) (bool, error) {
		e := raftpb.Entry{}
		protoc.MustUnmarshal(&e, value)

		// May meet gap or has been compacted.
		if e.Index != nextIndex {
			return false, nil
		}

		nextIndex++
		totalSize += uint64(len(value))

		exceededMaxSize = totalSize > maxSize
		if !exceededMaxSize || len(ents) == 0 {
			ents = append(ents, e)
		}

		return !exceededMaxSize, nil
	}, false)

	if err != nil {
		return nil, err
	}

	// If we get the correct number of entries the total size exceeds max_size, returns.
	if len(ents) == int(high-low) || exceededMaxSize {
		return ents, nil
	}

	return nil, raft.ErrUnavailable
}

func (ps *peerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.getTruncatedIndex() {
		return ps.getTruncatedTerm(), nil
	}

	err := ps.checkRange(idx, idx+1)
	if err != nil {
		return 0, err
	}

	lastIdx, err := ps.LastIndex()
	if err != nil {
		return 0, err
	}

	if ps.getTruncatedTerm() == ps.lastTerm || idx == lastIdx {
		return ps.lastTerm, nil
	}

	key := getRaftLogKey(ps.shard.ID, idx)
	v, err := ps.store.MetadataStorage().Get(key)
	if err != nil {
		return 0, err
	}

	if v == nil {
		return 0, raft.ErrUnavailable
	}

	e, err := ps.unmarshal(v, idx)
	if err != nil {
		return 0, err
	}

	return e.Term, nil
}

func (ps *peerStorage) LastIndex() (uint64, error) {
	return atomic.LoadUint64(&ps.raftLocalState.LastIndex), nil
}

func (ps *peerStorage) FirstIndex() (uint64, error) {
	return ps.getTruncatedIndex() + 1, nil
}

func (ps *peerStorage) Snapshot() (raftpb.Snapshot, error) {
	if ps.isGeneratingSnap() {
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}

	if ps.isGenSnapJobComplete() {
		result := ps.genSnapJob.GetResult()
		// snapshot failure, we will continue try do snapshot
		if nil == result {
			logger.Warningf("shard %d snapshot generating failed",
				ps.shard.ID)
		} else {
			snap := result.(raftpb.Snapshot)
			if ps.validateSnap(&snap) {
				ps.resetGenSnapJob()
				return snap, nil
			}
		}
	}

	logger.Infof("shard %d start snapshot, epoch=<%+v>",
		ps.shard.ID,
		ps.shard.Epoch)

	err := ps.store.addSnapJob(ps.shard.Group, ps.doGenerateSnapshotJob, ps.setGenSnapJob)
	if err != nil {
		logger.Fatalf("shard %d add generate job failed with %+v",
			ps.shard.ID,
			err)
	}

	return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
}

func (ps *peerStorage) setGenSnapJob(job *task.Job) {
	ps.genSnapJob = job
}

func (ps *peerStorage) setApplySnapJob(job *task.Job) {
	if ps.store.cfg.Test.PeerReplicaSetSnapshotJobWait > 0 {
		time.Sleep(ps.store.cfg.Test.PeerReplicaSetSnapshotJobWait)
	}
	ps.applySnapJob = job
}
