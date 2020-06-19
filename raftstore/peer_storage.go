package raftstore

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/coreos/etcd/raft"
	etcdPB "github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftpb"
	"github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/util"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
)

const (
	// When we create a shard peer, we should initialize its log term/index > 0,
	// so that we can force the follower peer to sync the snapshot first.
	raftInitLogTerm  = 5
	raftInitLogIndex = 5

	maxSnapTryCnt = 5
)

type snapshotState int

var (
	relax        = snapshotState(1)
	generating   = snapshotState(2)
	applying     = snapshotState(3)
	applyAborted = snapshotState(4)
)

const (
	pending = iota
	running
	cancelling
	cancelled
	finished
	failed
)

var (
	emptyEntry = etcdPB.Entry{}
)

type peerStorage struct {
	store *store
	shard metapb.Shard

	lastTerm         uint64
	appliedIndexTerm uint64
	lastReadyIndex   uint64
	lastCompactIndex uint64
	raftState        raftpb.RaftLocalState
	raftHardState    etcdPB.HardState
	applyState       raftpb.RaftApplyState

	genSnapJob       *task.Job
	applySnapJob     *task.Job
	applySnapJobLock sync.RWMutex

	pendingReads *readIndexQueue
}

func newPeerStorage(store *store, shard metapb.Shard) (*peerStorage, error) {
	s := new(peerStorage)
	s.store = store
	s.shard = shard
	s.appliedIndexTerm = raftInitLogTerm

	err := s.initRaftState()
	if err != nil {
		return nil, err
	}
	logger.Infof("shard %d init with raft state %+v ,%+v",
		shard.ID,
		s.raftState,
		s.raftHardState)

	err = s.initApplyState()
	if err != nil {
		return nil, err
	}
	logger.Infof("shard %d init apply state, state=<%+v>",
		shard.ID,
		s.applyState)

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

func (ps *peerStorage) initRaftState() error {
	v, err := ps.store.MetadataStorage().Get(getRaftStateKey(ps.shard.ID))
	if err != nil {
		return err
	}

	if len(v) > 0 {
		s := &raftpb.RaftLocalState{}
		err = s.Unmarshal(v)
		if err != nil {
			return err
		}

		ps.raftState = *s
		protoc.MustUnmarshal(&ps.raftHardState, s.HardState)
		return nil
	}

	s := &raftpb.RaftLocalState{}
	if len(ps.shard.Peers) > 0 {
		s.LastIndex = raftInitLogIndex
	}

	ps.raftState = *s
	return nil
}

func (ps *peerStorage) initApplyState() error {
	v, err := ps.store.MetadataStorage().Get(getApplyStateKey(ps.shard.ID))
	if err != nil {
		return err
	}

	if len(v) > 0 && len(ps.shard.Peers) > 0 {
		s := &raftpb.RaftApplyState{}
		err = s.Unmarshal(v)
		if err != nil {
			return err
		}

		ps.applyState = *s
		return nil
	}

	if len(ps.shard.Peers) > 0 {
		ps.applyState.AppliedIndex = raftInitLogIndex
		ps.applyState.TruncatedState.Index = raftInitLogIndex
		ps.applyState.TruncatedState.Term = raftInitLogTerm
	}

	return nil
}

func (ps *peerStorage) initLastTerm() error {
	lastIndex := ps.raftState.LastIndex

	if lastIndex == 0 {
		ps.lastTerm = lastIndex
		return nil
	} else if lastIndex == raftInitLogIndex {
		ps.lastTerm = raftInitLogTerm
		return nil
	} else if lastIndex == ps.applyState.TruncatedState.Index {
		ps.lastTerm = ps.applyState.TruncatedState.Term
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

	s := &etcdPB.Entry{}
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
	return ps.applyState.AppliedIndex
}

func (ps *peerStorage) getCommittedIndex() uint64 {
	return ps.raftHardState.Commit
}

func (ps *peerStorage) getTruncatedIndex() uint64 {
	return ps.applyState.TruncatedState.Index
}

func (ps *peerStorage) getTruncatedTerm() uint64 {
	return ps.applyState.TruncatedState.Term
}

func (ps *peerStorage) validateSnap(snap *etcdPB.Snapshot) bool {
	snapData := &raftpb.SnapshotMessage{}
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

func (ps *peerStorage) loadLogEntry(index uint64) (etcdPB.Entry, error) {
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

func (ps *peerStorage) loadLocalState(job *task.Job) (*raftpb.ShardLocalState, error) {
	if nil != job &&
		job.IsCancelling() {
		return nil, task.ErrJobCancelled
	}

	return loadLocalState(ps.shard.ID, ps.store.MetadataStorage(), false)
}

func (ps *peerStorage) applySnapshot(job *task.Job) error {
	if nil != job &&
		job.IsCancelling() {
		return task.ErrJobCancelled
	}

	snap := &raftpb.SnapshotMessage{}
	snap.Header = raftpb.SnapshotMessageHeader{
		Shard: ps.shard,
		Term:  ps.applyState.TruncatedState.Term,
		Index: ps.applyState.TruncatedState.Index,
	}

	return ps.store.snapshotManager.Apply(snap)
}

func (ps *peerStorage) loadApplyState() (*raftpb.RaftApplyState, error) {
	key := getApplyStateKey(ps.shard.ID)
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

	applyState := &raftpb.RaftApplyState{}
	err = applyState.Unmarshal(v)
	return applyState, err
}

func (ps *peerStorage) unmarshal(v []byte, expectIndex uint64) (etcdPB.Entry, error) {
	e := etcdPB.Entry{}
	protoc.MustUnmarshal(&e, v)
	if e.Index != expectIndex {
		logger.Fatalf("shard %d raft log index not match, logIndex %d expect %d",
			ps.shard.ID,
			e.Index,
			expectIndex)
	}

	return e, nil
}

/// Delete all data belong to the shard.
/// If return Err, data may get partial deleted.
func (ps *peerStorage) clearData() error {
	shard := ps.shard

	shardID := shard.ID
	startKey := encStartKey(&shard)
	endKey := encEndKey(&shard)

	err := ps.store.addSnapJob(ps.shard.Group, func() error {
		logger.Infof("shard %d deleting data in [%+v, %+v)",
			shardID,
			startKey,
			endKey)
		err := ps.deleteAllInRange(startKey, endKey, nil)
		if err != nil {
			logger.Errorf("shard %d failed to delete data in [%+v, %+v) with %+v",
				shardID,
				startKey,
				endKey,
				err)
		}

		return err
	}, nil)

	return err
}

// Delete all data that is not covered by `newShard`.
func (ps *peerStorage) clearExtraData(newShard metapb.Shard) error {
	shard := ps.shard

	oldStartKey := encStartKey(&shard)
	oldEndKey := encEndKey(&shard)

	newStartKey := encStartKey(&newShard)
	newEndKey := encEndKey(&newShard)

	if bytes.Compare(oldStartKey, newStartKey) < 0 {
		err := ps.startDestroyDataJob(newShard.ID, oldStartKey, newStartKey)

		if err != nil {
			return err
		}
	}

	if bytes.Compare(newEndKey, oldEndKey) < 0 {
		err := ps.startDestroyDataJob(newShard.ID, newEndKey, oldEndKey)

		if err != nil {
			return err
		}
	}

	return nil
}

func (ps *peerStorage) startDestroyDataJob(shardID uint64, start, end []byte) error {
	pr := ps.store.getPR(shardID, false)
	if pr != nil {
		err := ps.store.addApplyJob(pr.applyWorker, "doDestroyDataJob", func() error {
			return ps.doDestroyDataJob(shardID, start, end)
		}, nil)

		return err
	}

	return nil
}

func (ps *peerStorage) doDestroyDataJob(shardID uint64, startKey, endKey []byte) error {
	logger.Infof("shard %d deleting data, start=<%v>, end=<%v>",
		shardID,
		startKey,
		endKey)

	err := ps.deleteAllInRange(startKey, endKey, nil)
	if err != nil {
		logger.Errorf("shard %d failed to delete data, start=<%v> end=<%v> errors:\n %+v",
			shardID,
			startKey,
			endKey,
			err)
	}

	return err
}

func (ps *peerStorage) updatePeerState(shard metapb.Shard, state raftpb.PeerState, wb *util.WriteBatch) error {
	shardState := &raftpb.ShardLocalState{}
	shardState.State = state
	shardState.Shard = shard

	if wb != nil {
		return wb.Set(getStateKey(shard.ID), protoc.MustMarshal(shardState))
	}

	return ps.store.MetadataStorage().Set(getStateKey(shard.ID), protoc.MustMarshal(shardState))
}

func (ps *peerStorage) writeInitialState(shardID uint64, wb *util.WriteBatch) error {
	hard := etcdPB.HardState{}
	hard.Term = raftInitLogTerm
	hard.Commit = raftInitLogIndex

	raftState := new(raftpb.RaftLocalState)
	raftState.LastIndex = raftInitLogIndex
	raftState.HardState = protoc.MustMarshal(&hard)

	applyState := new(raftpb.RaftApplyState)
	applyState.AppliedIndex = raftInitLogIndex
	applyState.TruncatedState.Index = raftInitLogIndex
	applyState.TruncatedState.Term = raftInitLogTerm

	err := wb.Set(getRaftStateKey(shardID), protoc.MustMarshal(raftState))
	if err != nil {
		return err
	}

	return wb.Set(getApplyStateKey(shardID), protoc.MustMarshal(applyState))
}

func (ps *peerStorage) deleteAllInRange(start, end []byte, job *task.Job) error {
	if job != nil &&
		job.IsCancelling() {
		return task.ErrJobCancelled
	}

	return ps.store.DataStorageByGroup(ps.shard.Group).RangeDelete(start, end)
}

func compactRaftLog(shardID uint64, state *raftpb.RaftApplyState, compactIndex, compactTerm uint64) error {
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

func loadLocalState(shardID uint64, driver storage.MetadataStorage, allowNotFound bool) (*raftpb.ShardLocalState, error) {
	key := getStateKey(shardID)
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

	stat := &raftpb.ShardLocalState{}
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
func (ps *peerStorage) InitialState() (etcdPB.HardState, etcdPB.ConfState, error) {
	hardState := etcdPB.HardState{}
	protoc.MustUnmarshal(&hardState, ps.raftState.HardState)
	confState := etcdPB.ConfState{}

	if hardState.Commit == 0 &&
		hardState.Term == 0 &&
		hardState.Vote == 0 {
		if ps.isInitialized() {
			logger.Fatalf("shard %d shard is initialized but local state has empty hard state, hardState=<%v>",
				ps.shard.ID,
				hardState)
		}

		return hardState, confState, nil
	}

	for _, p := range ps.shard.Peers {
		confState.Nodes = append(confState.Nodes, p.ID)
	}

	return hardState, confState, nil
}

func (ps *peerStorage) Entries(low, high, maxSize uint64) ([]etcdPB.Entry, error) {
	err := ps.checkRange(low, high)
	if err != nil {
		return nil, err
	}

	var ents []etcdPB.Entry
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
		e := etcdPB.Entry{}
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
	return atomic.LoadUint64(&ps.raftState.LastIndex), nil
}

func (ps *peerStorage) FirstIndex() (uint64, error) {
	return ps.getTruncatedIndex() + 1, nil
}

func (ps *peerStorage) Snapshot() (etcdPB.Snapshot, error) {
	if ps.isGeneratingSnap() {
		return etcdPB.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}

	if ps.isGenSnapJobComplete() {
		result := ps.genSnapJob.GetResult()
		// snapshot failure, we will continue try do snapshot
		if nil == result {
			logger.Warningf("shard %d snapshot generating failed",
				ps.shard.ID)
		} else {
			snap := result.(etcdPB.Snapshot)
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
		logger.Fatalf("shard %d add generate job failed, errors:\n %+v",
			ps.shard.ID,
			err)
	}

	return etcdPB.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
}

func (ps *peerStorage) setGenSnapJob(job *task.Job) {
	ps.genSnapJob = job
}

func (ps *peerStorage) setApplySnapJob(job *task.Job) {
	ps.applySnapJob = job
}
