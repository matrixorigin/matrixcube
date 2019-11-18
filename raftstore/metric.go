package raftstore

import (
	"github.com/deepfabric/beehive/metric"
)

type applyMetrics struct {
	// an inaccurate difference in shard size since last reset.
	sizeDiffHint uint64
	// delete keys' count since last reset.
	deleteKeysHint uint64
	writtenBytes   uint64
	writtenKeys    uint64

	admin raftAdminMetrics
}

type localMetrics struct {
	ready   raftReadyMetrics
	message raftMessageMetrics
	propose raftProposeMetrics
	admin   raftAdminMetrics
}

func (m *localMetrics) flush() {
	m.ready.flush()
	m.message.flush()
	m.propose.flush()
	m.admin.flush()
}

type raftReadyMetrics struct {
	message   uint64
	commit    uint64
	append    uint64
	snapshort uint64
}

func (m *raftReadyMetrics) flush() {
	if m.message > 0 {
		metric.AddRaftReadySendCount(m.message)
		m.message = 0
	}

	if m.commit > 0 {
		metric.AddRaftReadyCommitCount(m.commit)
		m.commit = 0
	}

	if m.append > 0 {
		metric.AddRaftReadyAppendCount(m.append)
		m.append = 0
	}

	if m.snapshort > 0 {
		metric.AddRaftReadySnapshotCount(m.snapshort)
		m.snapshort = 0
	}
}

type raftMessageMetrics struct {
	append        uint64
	appendResp    uint64
	vote          uint64
	voteResp      uint64
	snapshot      uint64
	heartbeat     uint64
	heartbeatResp uint64
	transfeLeader uint64
}

func (m *raftMessageMetrics) flush() {
	if m.append > 0 {
		metric.AddRaftAppendMsgsCount(m.append)
		m.append = 0
	}

	if m.appendResp > 0 {
		metric.AddRaftAppendRespMsgsCount(m.append)
		m.appendResp = 0
	}

	if m.vote > 0 {
		metric.AddRaftVoteMsgsCount(m.vote)
		m.vote = 0
	}

	if m.voteResp > 0 {
		metric.AddRaftVoteRespMsgsCount(m.voteResp)
		m.voteResp = 0
	}

	if m.snapshot > 0 {
		metric.AddRaftSnapshotMsgsCount(m.snapshot)
		m.snapshot = 0
	}

	if m.heartbeat > 0 {
		metric.AddRaftHeartbeatMsgsCount(m.heartbeat)
		m.heartbeat = 0
	}

	if m.heartbeatResp > 0 {
		metric.AddRaftHeartbeatRespMsgsCount(m.heartbeatResp)
		m.heartbeatResp = 0
	}

	if m.transfeLeader > 0 {
		metric.AddRaftTransferLeaderMsgsCount(m.transfeLeader)
		m.transfeLeader = 0
	}
}

type raftProposeMetrics struct {
	readLocal      uint64
	readIndex      uint64
	normal         uint64
	transferLeader uint64
	confChange     uint64
}

func (m *raftProposeMetrics) flush() {
	if m.readLocal > 0 {
		metric.AddRaftProposalReadLocalCount(m.readLocal)
		m.readLocal = 0
	}

	if m.readIndex > 0 {
		metric.AddRaftProposalReadIndexCount(m.readIndex)
		m.readIndex = 0
	}

	if m.normal > 0 {
		metric.AddRaftProposalNormalCount(m.normal)
		m.normal = 0
	}

	if m.transferLeader > 0 {
		metric.AddRaftProposalTransferLeaderCount(m.transferLeader)
		m.transferLeader = 0
	}

	if m.confChange > 0 {
		metric.AddRaftProposalConfChangeCount(m.confChange)
		m.confChange = 0
	}
}

type raftAdminMetrics struct {
	confChange uint64
	split      uint64
	compact    uint64

	confChangeReject uint64

	confChangeSucceed uint64
	addPeerSucceed    uint64
	removePeerSucceed uint64
	splitSucceed      uint64
	compactSucceed    uint64
}

func (m *raftAdminMetrics) incBy(by raftAdminMetrics) {
	m.confChange += by.confChange
	m.confChangeSucceed += by.confChangeSucceed
	m.confChangeReject += by.confChangeReject
	m.addPeerSucceed += by.addPeerSucceed
	m.removePeerSucceed += by.removePeerSucceed
	m.split += by.split
	m.splitSucceed += by.splitSucceed
	m.compact += by.compact
	m.compactSucceed += by.compactSucceed
}

func (m *raftAdminMetrics) flush() {
	if m.confChange > 0 {
		metric.AddRaftAdminCommandConfChangeCount(m.confChange)
		m.confChange = 0
	}
	if m.confChangeSucceed > 0 {
		metric.AddRaftAdminCommandConfChangeSucceedCount(m.confChangeSucceed)
		m.confChangeSucceed = 0
	}
	if m.confChangeReject > 0 {
		metric.AddRaftAdminCommandConfChangeRejectCount(m.confChangeReject)
		m.confChangeReject = 0
	}

	if m.split > 0 {
		metric.AddRaftAdminCommandSplitCount(m.split)
		m.split = 0
	}
	if m.splitSucceed > 0 {
		metric.AddRaftAdminCommandSplitSucceedCount(m.splitSucceed)
		m.splitSucceed = 0
	}

	if m.compact > 0 {
		metric.AddRaftAdminCommandCompactCount(m.compact)
		m.compact = 0
	}
	if m.compactSucceed > 0 {
		metric.AddRaftAdminCommandCompactSucceedCount(m.compactSucceed)
		m.compactSucceed = 0
	}
}
