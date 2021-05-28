package operator

import (
	"errors"
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/filter"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
)

// Builder is used to create operators. Usage:
//     op, err := NewBuilder(desc, cluster, resource).
//                 RemovePeer(container1).
//                 AddPeer(peer1).
//                 SetLeader(container2).
//                 Build(kind)
// The generated Operator will choose the most appropriate execution order
// according to various constraints.
type Builder struct {
	// basic info
	desc          string
	cluster       opt.Cluster
	resourceID    uint64
	resourceEpoch metapb.ResourceEpoch
	rules         []*placement.Rule
	expectedRoles map[uint64]placement.PeerRoleType

	// operation record
	originPeers             peersMap
	unhealthyPeers          peersMap
	originLeaderContainerID uint64
	targetPeers             peersMap
	targetLeaderContainerID uint64
	err                     error

	// skip origin check flags
	skipOriginJointStateCheck bool

	// build flags
	allowDemote       bool
	useJointConsensus bool
	lightWeight       bool
	forceTargetLeader bool

	// intermediate states
	currentPeers                         peersMap
	currentLeaderContainerID             uint64
	toAdd, toRemove, toPromote, toDemote peersMap       // pending tasks.
	steps                                []OpStep       // generated steps.
	peerAddStep                          map[uint64]int // record at which step a peer is created.

	// comparison function
	stepPlanPreferFuncs []func(stepPlan) int // for buildStepsWithoutJointConsensus
}

// BuilderOption is used to create operator builder.
type BuilderOption func(*Builder)

// SkipOriginJointStateCheck lets the builder skip the joint state check for origin peers.
func SkipOriginJointStateCheck(b *Builder) {
	b.skipOriginJointStateCheck = true
}

// NewBuilder creates a Builder.
func NewBuilder(desc string, cluster opt.Cluster, res *core.CachedResource, opts ...BuilderOption) *Builder {
	b := &Builder{
		desc:          desc,
		cluster:       cluster,
		resourceID:    res.Meta.ID(),
		resourceEpoch: res.Meta.Epoch(),
	}

	// options
	for _, option := range opts {
		option(b)
	}

	// origin peers
	err := b.err
	originPeers := newPeersMap()
	unhealthyPeers := newPeersMap()

	for _, p := range res.Meta.Peers() {
		if p.ContainerID == 0 {
			err = errors.New("cannot build operator for resource with nil peer")
			break
		}
		originPeers.Set(p)
	}

	for _, p := range res.GetPendingPeers() {
		unhealthyPeers.Set(p)
	}

	for _, p := range res.GetDownPeers() {
		unhealthyPeers.Set(p.Peer)
	}

	// origin leader
	originLeaderContainerID := uint64(0)
	if res.GetLeader() == nil {
		err = errors.New("cannot build operator for resource with no leader")
	} else {
		originLeaderContainerID = res.GetLeader().GetContainerID()
		if _, ok := originPeers[originLeaderContainerID]; err == nil && !ok {
			err = errors.New("cannot build operator for resource with no leader")
		}
	}

	// placement rules
	var rules []*placement.Rule
	if err == nil && cluster.GetOpts().IsPlacementRulesEnabled() {
		fit := cluster.FitResource(res)
		for _, rf := range fit.RuleFits {
			rules = append(rules, rf.Rule)
		}
		if len(rules) == 0 {
			err = errors.New("cannot build operator for resource match no placement rule")
		}
	}

	// joint state check
	if err == nil && !b.skipOriginJointStateCheck && metadata.IsInJointState(res.Meta.Peers()...) {
		err = errors.New("cannot build operator for resource which is in joint state")
	}

	b.rules = rules
	b.originPeers = originPeers
	b.unhealthyPeers = unhealthyPeers
	b.originLeaderContainerID = originLeaderContainerID
	b.targetPeers = originPeers.Copy()
	b.allowDemote = cluster.JointConsensusEnabled()
	b.useJointConsensus = cluster.JointConsensusEnabled() && cluster.GetOpts().IsUseJointConsensus()
	b.err = err
	return b
}

// AddPeer records an add Peer operation in Builder. If peer.Id is 0, the builder
// will allocate a new peer ID later.
func (b *Builder) AddPeer(peer metapb.Peer) *Builder {
	if b.err != nil {
		return b
	}
	if peer.ContainerID == 0 {
		b.err = fmt.Errorf("cannot add nil peer")
	} else if metadata.IsInJointState(peer) {
		b.err = fmt.Errorf("cannot add peer %+v: is in joint state", peer)
	} else if old, ok := b.targetPeers[peer.ContainerID]; ok {
		b.err = fmt.Errorf("cannot add peer %+v: already have peer %+v", peer, old)
	} else {
		b.targetPeers.Set(peer)
	}
	return b
}

// RemovePeer records a remove peer operation in Builder.
func (b *Builder) RemovePeer(containerID uint64) *Builder {
	if b.err != nil {
		return b
	}
	if _, ok := b.targetPeers[containerID]; !ok {
		b.err = fmt.Errorf("cannot remove peer from %d: not found", containerID)
	} else if b.targetLeaderContainerID == containerID {
		b.err = fmt.Errorf("cannot remove peer from %d: is target leader", containerID)
	} else {
		delete(b.targetPeers, containerID)
	}
	return b
}

// PromoteLearner records a promote learner operation in Builder.
func (b *Builder) PromoteLearner(containerID uint64) *Builder {
	if b.err != nil {
		return b
	}
	if peer, ok := b.targetPeers[containerID]; !ok {
		b.err = fmt.Errorf("cannot promote peer %d: not found", containerID)
	} else if !metadata.IsLearner(peer) {
		b.err = fmt.Errorf("cannot promote peer %d: is not learner", containerID)
	} else if _, ok := b.unhealthyPeers[containerID]; ok {
		b.err = fmt.Errorf("cannot promote peer %d: unhealthy", containerID)
	} else {
		b.targetPeers.Set(metapb.Peer{
			ID:          peer.ID,
			ContainerID: peer.ContainerID,
			Role:        metapb.PeerRole_Voter,
		})
	}
	return b
}

// DemoteVoter records a demote voter operation in Builder.
func (b *Builder) DemoteVoter(containerID uint64) *Builder {
	if b.err != nil {
		return b
	}
	if peer, ok := b.targetPeers[containerID]; !ok {
		b.err = fmt.Errorf("cannot demote voter %d: not found", containerID)
	} else if metadata.IsLearner(peer) {
		b.err = fmt.Errorf("cannot demote voter %d: is already learner", containerID)
	} else {
		b.targetPeers.Set(metapb.Peer{
			ID:          peer.ID,
			ContainerID: peer.ContainerID,
			Role:        metapb.PeerRole_Learner,
		})
	}
	return b
}

// SetLeader records the target leader in Builder.
func (b *Builder) SetLeader(containerID uint64) *Builder {
	if b.err != nil {
		return b
	}
	if peer, ok := b.targetPeers[containerID]; !ok {
		b.err = fmt.Errorf("cannot transfer leader to %d: not found", containerID)
	} else if metadata.IsLearner(peer) {
		b.err = fmt.Errorf("cannot transfer leader to %d: not voter", containerID)
	} else if _, ok := b.unhealthyPeers[containerID]; ok {
		b.err = fmt.Errorf("cannot transfer leader to %d: unhealthy", containerID)
	} else {
		b.targetLeaderContainerID = containerID
	}
	return b
}

// SetPeers resets the target peer list.
//
// If peer's ID is 0, the builder will allocate a new ID later. If current
// target leader does not exist in peers, it will be reset.
func (b *Builder) SetPeers(peers map[uint64]metapb.Peer) *Builder {
	if b.err != nil {
		return b
	}

	for key, peer := range peers {
		if key == 0 || peer.ContainerID != key || metadata.IsInJointState(peer) {
			b.err = fmt.Errorf("setPeers with mismatch peers: %v", peers)
			return b
		}
	}

	if _, ok := peers[b.targetLeaderContainerID]; !ok {
		b.targetLeaderContainerID = 0
	}

	b.targetPeers = peersMap(peers).Copy()
	return b
}

// SetExpectedRoles records expected roles of target peers.
// It may update `targetLeaderContainerID` if there is a peer has role `leader` or `follower`.
func (b *Builder) SetExpectedRoles(roles map[uint64]placement.PeerRoleType) *Builder {
	if b.err != nil {
		return b
	}
	var leaderCount, voterCount int
	for id, role := range roles {
		switch role {
		case placement.Leader:
			if leaderCount > 0 {
				b.err = fmt.Errorf("resource cannot have multiple leaders")
				return b
			}
			b.targetLeaderContainerID = id
			leaderCount++
		case placement.Voter:
			voterCount++
		case placement.Follower, placement.Learner:
			if b.targetLeaderContainerID == id {
				b.targetLeaderContainerID = 0
			}
		}
	}
	if leaderCount+voterCount == 0 {
		b.err = fmt.Errorf("resource need at least 1 voter or leader")
		return b
	}
	b.expectedRoles = roles
	return b
}

// EnableLightWeight marks the resource as light weight. It is used for scatter resources.
func (b *Builder) EnableLightWeight() *Builder {
	b.lightWeight = true
	return b
}

// EnableForceTargetLeader marks the step of transferring leader to target is forcible. It is used for grant leader.
func (b *Builder) EnableForceTargetLeader() *Builder {
	b.forceTargetLeader = true
	return b
}

// Build creates the Operator.
func (b *Builder) Build(kind OpKind) (*Operator, error) {
	var brief string

	if b.err != nil {
		return nil, b.err
	}

	if brief, b.err = b.prepareBuild(); b.err != nil {
		return nil, b.err
	}

	if b.useJointConsensus {
		kind, b.err = b.buildStepsWithJointConsensus(kind)
	} else {
		kind, b.err = b.buildStepsWithoutJointConsensus(kind)
	}
	if b.err != nil {
		return nil, b.err
	}

	return NewOperator(b.desc, brief, b.resourceID, b.resourceEpoch, kind, b.steps...), nil
}

// Initialize intermediate states.
// TODO: simplify the code
func (b *Builder) prepareBuild() (string, error) {
	b.toAdd = newPeersMap()
	b.toRemove = newPeersMap()
	b.toPromote = newPeersMap()
	b.toDemote = newPeersMap()

	voterCount := 0
	for _, peer := range b.targetPeers {
		if !metadata.IsLearner(peer) {
			voterCount++
		}
	}
	if voterCount == 0 {
		return "", errors.New("cannot create operator: target peers have no voter")
	}

	// Diff `originPeers` and `targetPeers` to initialize `toAdd`, `toRemove`, `toPromote`, `toDemote`.
	// Note: Use `toDemote` only when `allowDemote` is true. Otherwise use `toAdd`, `toRemove` instead.
	for _, o := range b.originPeers {
		n, ok := b.targetPeers[o.ContainerID]
		if !ok {
			b.toRemove.Set(o)
			continue
		}

		// If the peer id in the target is different from that in the origin,
		// modify it to the peer id of the origin.
		if o.ID != n.ID {
			n = metapb.Peer{
				ID:          o.ID,
				ContainerID: o.ContainerID,
				Role:        n.Role,
			}
		}

		if metadata.IsLearner(o) {
			if !metadata.IsLearner(n) {
				// learner -> voter
				b.toPromote.Set(n)
			}
		} else {
			if metadata.IsLearner(n) {
				// voter -> learner
				if b.allowDemote {
					b.toDemote.Set(n)
				} else {
					b.toRemove.Set(o)
					// Need to add `b.toAdd.Set(n)` in the later targetPeers loop
				}
			}
		}
	}
	for _, n := range b.targetPeers {
		// old peer not exists, or target is learner while old one is voter.
		o, ok := b.originPeers[n.ContainerID]
		if !ok || (!b.allowDemote && !metadata.IsLearner(o) && metadata.IsLearner(n)) {
			if n.ID == 0 {
				// Allocate peer ID if need.
				id, err := b.cluster.AllocID()
				if err != nil {
					return "", err
				}
				n = metapb.Peer{
					ID:          id,
					ContainerID: n.ContainerID,
					Role:        n.Role,
				}
			}
			// It is a pair with `b.toRemove.Set(o)` when `o != nil`.
			b.toAdd.Set(n)
		}
	}

	// If the target leader does not exist or is a Learner, the target is cancelled.
	if peer, ok := b.targetPeers[b.targetLeaderContainerID]; !ok || metadata.IsLearner(peer) {
		b.targetLeaderContainerID = 0
	}

	b.currentPeers, b.currentLeaderContainerID = b.originPeers.Copy(), b.originLeaderContainerID

	if b.targetLeaderContainerID != 0 {
		targetLeader := b.targetPeers[b.targetLeaderContainerID]
		if !b.allowLeader(targetLeader, b.forceTargetLeader) {
			return "", errors.New("cannot create operator: target leader is not allowed")
		}
	}

	if len(b.toAdd)+len(b.toRemove)+len(b.toPromote)+len(b.toDemote) <= 1 {
		// If only one peer changed, joint consensus is not used.
		b.useJointConsensus = false
	}

	b.peerAddStep = make(map[uint64]int)

	return b.brief(), nil
}

// generate brief description of the operator.
func (b *Builder) brief() string {
	switch {
	case len(b.toAdd) > 0 && len(b.toRemove) > 0:
		op := "mv peer"
		if b.lightWeight {
			op = "mv light peer"
		}
		return fmt.Sprintf("%s: container %s to %s", op, b.toRemove, b.toAdd)
	case len(b.toAdd) > 0:
		return fmt.Sprintf("add peer: container %s", b.toAdd)
	case len(b.toRemove) > 0:
		return fmt.Sprintf("rm peer: container %s", b.toRemove)
	case len(b.toPromote) > 0:
		return fmt.Sprintf("promote peer: container %s", b.toPromote)
	case len(b.toDemote) > 0:
		return fmt.Sprintf("demote peer: container %s", b.toDemote)
	case b.originLeaderContainerID != b.targetLeaderContainerID:
		return fmt.Sprintf("transfer leader: container %d to %d", b.originLeaderContainerID, b.targetLeaderContainerID)
	default:
		return ""
	}
}

// Using Joint Consensus can ensure the replica safety and reduce the number of steps.
func (b *Builder) buildStepsWithJointConsensus(kind OpKind) (OpKind, error) {
	// Add all the peers as Learner first. Split `Add Voter` to `Add Learner + Promote`
	for _, add := range b.toAdd.IDs() {
		peer := b.toAdd[add]
		if !metadata.IsLearner(peer) {
			b.execAddPeer(metapb.Peer{
				ID:          peer.ID,
				ContainerID: peer.ContainerID,
				Role:        metapb.PeerRole_Learner,
			})
			b.toPromote.Set(peer)
		} else {
			b.execAddPeer(peer)
		}
		kind |= OpResource
	}

	b.setTargetLeaderIfNotExist()
	if b.targetLeaderContainerID == 0 {
		return kind, errors.New("no valid leader")
	}

	// Split `Remove Voter` to `Demote + Remove Learner`
	for _, remove := range b.toRemove.IDs() {
		peer := b.toRemove[remove]
		if !metadata.IsLearner(peer) {
			b.toDemote.Set(metapb.Peer{
				ID:          peer.ID,
				ContainerID: peer.ContainerID,
				Role:        metapb.PeerRole_Learner,
			})
		}
	}

	if targetLeaderBefore, ok := b.originPeers[b.targetLeaderContainerID]; ok && !metadata.IsLearner(targetLeaderBefore) {
		// target leader is a voter in `originPeers`, transfer leader first.
		if b.originLeaderContainerID != b.targetLeaderContainerID {
			b.execTransferLeader(b.targetLeaderContainerID)
			kind |= OpLeader
		}
		b.execChangePeerV2(true, false)
	} else if originLeaderAfter, ok := b.targetPeers[b.originLeaderContainerID]; b.originLeaderContainerID == 0 ||
		(ok && !metadata.IsLearner(originLeaderAfter)) {
		// origin leader is none or a voter in `targetPeers`, change peers first.
		b.execChangePeerV2(true, false)
		if b.originLeaderContainerID != b.targetLeaderContainerID {
			b.execTransferLeader(b.targetLeaderContainerID)
			kind |= OpLeader
		}
	} else {
		// both demote origin leader and promote target leader, transfer leader in joint state.
		b.execChangePeerV2(true, true)
		kind |= OpLeader
	}

	// Finally, remove all the peers as Learner
	for _, remove := range b.toRemove.IDs() {
		b.execRemovePeer(b.toRemove[remove])
		kind |= OpResource
	}

	return kind, nil
}

func (b *Builder) setTargetLeaderIfNotExist() {
	if b.targetLeaderContainerID != 0 {
		return
	}

	leaderPreferFuncs := []func(uint64) int{
		b.preferLeaderRoleAsLeader,
		b.preferUPContainerAsLeader,
		b.preferCurrentLeader,
		b.preferKeepVoterAsLeader,
		b.preferOldPeerAsLeader,
	}

	for _, targetLeaderContainerID := range b.targetPeers.IDs() {
		peer := b.targetPeers[targetLeaderContainerID]
		if !b.allowLeader(peer, b.forceTargetLeader) {
			continue
		}
		// if role info is given, container having role follower should not be target leader.
		if role, ok := b.expectedRoles[targetLeaderContainerID]; ok && role == placement.Follower {
			continue
		}
		if b.targetLeaderContainerID == 0 {
			b.targetLeaderContainerID = targetLeaderContainerID
			continue
		}
		for _, f := range leaderPreferFuncs {
			if best, next := f(b.targetLeaderContainerID), f(targetLeaderContainerID); best < next {
				b.targetLeaderContainerID = targetLeaderContainerID
				break
			} else if best > next {
				break
			}
		}
	}
}

func (b *Builder) preferLeaderRoleAsLeader(targetLeaderContainerID uint64) int {
	role, ok := b.expectedRoles[targetLeaderContainerID]
	return typeutil.BoolToInt(ok && role == placement.Leader)
}

func (b *Builder) preferUPContainerAsLeader(targetLeaderContainerID uint64) int {
	container := b.cluster.GetContainer(targetLeaderContainerID)
	return typeutil.BoolToInt(container != nil && container.IsUp())
}

func (b *Builder) preferCurrentLeader(targetLeaderContainerID uint64) int {
	return typeutil.BoolToInt(targetLeaderContainerID == b.currentLeaderContainerID)
}

func (b *Builder) preferKeepVoterAsLeader(targetLeaderContainerID uint64) int {
	_, ok := b.toPromote[targetLeaderContainerID]
	return typeutil.BoolToInt(!ok)
}

func (b *Builder) preferOldPeerAsLeader(targetLeaderContainerID uint64) int {
	return -b.peerAddStep[targetLeaderContainerID]
}

// Some special cases, and containers that do not support using joint consensus.
func (b *Builder) buildStepsWithoutJointConsensus(kind OpKind) (OpKind, error) {
	b.initStepPlanPreferFuncs()

	for len(b.toAdd) > 0 || len(b.toRemove) > 0 || len(b.toPromote) > 0 || len(b.toDemote) > 0 {
		plan := b.peerPlan()
		if plan.IsEmpty() {
			return kind, errors.New("fail to build operator: plan is empty, maybe no valid leader")
		}
		if plan.leaderBeforeAdd != 0 && plan.leaderBeforeAdd != b.currentLeaderContainerID {
			b.execTransferLeader(plan.leaderBeforeAdd)
			kind |= OpLeader
		}
		if plan.add != nil {
			b.execAddPeer(*plan.add)
			kind |= OpResource
		}
		if plan.promote != nil {
			b.execPromoteLearner(*plan.promote)
		}
		if plan.leaderBeforeRemove != 0 && plan.leaderBeforeRemove != b.currentLeaderContainerID {
			b.execTransferLeader(plan.leaderBeforeRemove)
			kind |= OpLeader
		}
		if plan.demote != nil {
			b.execDemoteFollower(*plan.demote)
		}
		if plan.remove != nil {
			b.execRemovePeer(*plan.remove)
			kind |= OpResource
		}
	}

	b.setTargetLeaderIfNotExist()

	if _, ok := b.currentPeers[b.targetLeaderContainerID]; ok &&
		b.targetLeaderContainerID != 0 &&
		b.currentLeaderContainerID != b.targetLeaderContainerID {
		// Transfer only when target leader is legal.
		b.execTransferLeader(b.targetLeaderContainerID)
		kind |= OpLeader
	}

	if len(b.steps) == 0 {
		return kind, errors.New("no operator step is built")
	}
	return kind, nil
}

func (b *Builder) execTransferLeader(id uint64) {
	b.steps = append(b.steps, TransferLeader{FromContainer: b.currentLeaderContainerID, ToContainer: id})
	b.currentLeaderContainerID = id
}

func (b *Builder) execPromoteLearner(peer metapb.Peer) {
	b.steps = append(b.steps, PromoteLearner{ToContainer: peer.ContainerID, PeerID: peer.ID})
	b.currentPeers.Set(peer)
	delete(b.toPromote, peer.ContainerID)
}

func (b *Builder) execDemoteFollower(peer metapb.Peer) {
	b.steps = append(b.steps, DemoteFollower{ToContainer: peer.ContainerID, PeerID: peer.ID})
	b.currentPeers.Set(peer)
	delete(b.toDemote, peer.ContainerID)
}

func (b *Builder) execAddPeer(peer metapb.Peer) {
	if b.lightWeight {
		b.steps = append(b.steps, AddLightLearner{ToContainer: peer.ContainerID, PeerID: peer.ID})
	} else {
		b.steps = append(b.steps, AddLearner{ToContainer: peer.ContainerID, PeerID: peer.ID})
	}
	if !metadata.IsLearner(peer) {
		b.steps = append(b.steps, PromoteLearner{ToContainer: peer.ContainerID, PeerID: peer.ID})
	}
	b.currentPeers.Set(peer)
	b.peerAddStep[peer.ContainerID] = len(b.steps)
	delete(b.toAdd, peer.ContainerID)
}

func (b *Builder) execRemovePeer(peer metapb.Peer) {
	b.steps = append(b.steps, RemovePeer{FromContainer: peer.ContainerID, PeerID: peer.ID})
	delete(b.currentPeers, peer.ContainerID)
	delete(b.toRemove, peer.ContainerID)
}

func (b *Builder) execChangePeerV2(needEnter bool, needTransferLeader bool) {
	// Enter
	step := ChangePeerV2Enter{
		PromoteLearners: make([]PromoteLearner, 0, len(b.toPromote)),
		DemoteVoters:    make([]DemoteVoter, 0, len(b.toDemote)),
	}

	for _, p := range b.toPromote.IDs() {
		peer := b.toPromote[p]
		step.PromoteLearners = append(step.PromoteLearners, PromoteLearner{ToContainer: peer.ContainerID, PeerID: peer.ID})
		b.currentPeers.Set(peer)
	}
	b.toPromote = newPeersMap()

	for _, d := range b.toDemote.IDs() {
		peer := b.toDemote[d]
		step.DemoteVoters = append(step.DemoteVoters, DemoteVoter{ToContainer: peer.ContainerID, PeerID: peer.ID})
		b.currentPeers.Set(peer)
	}
	b.toDemote = newPeersMap()

	if needEnter {
		b.steps = append(b.steps, step)
	}
	// Transfer Leader
	if needTransferLeader && b.originLeaderContainerID != b.targetLeaderContainerID {
		b.execTransferLeader(b.targetLeaderContainerID)
	}
	// Leave
	b.steps = append(b.steps, ChangePeerV2Leave(step))
}

// check if the peer is allowed to become the leader.
func (b *Builder) allowLeader(peer metapb.Peer, ignoreClusterLimit bool) bool {
	// these peer roles are not allowed to become leader.
	switch peer.Role {
	case metapb.PeerRole_Learner, metapb.PeerRole_DemotingVoter:
		return false
	}

	// container does not exist
	if peer.ContainerID == b.currentLeaderContainerID {
		return true
	}
	container := b.cluster.GetContainer(peer.ContainerID)
	if container == nil {
		return false
	}

	if ignoreClusterLimit {
		return true
	}

	stateFilter := &filter.ContainerStateFilter{ActionScope: "operator-builder", TransferLeader: true}
	// container state filter
	if !stateFilter.Target(b.cluster.GetOpts(), container) {
		return false
	}

	// placement rules
	if len(b.rules) == 0 {
		return true
	}
	for _, r := range b.rules {
		if (r.Role == placement.Leader || r.Role == placement.Voter) &&
			placement.MatchLabelConstraints(container, r.LabelConstraints) {
			return true
		}
	}

	return false
}

// stepPlan is exec step. It can be:
// 1. promote learner + demote voter.
// 2. add voter + remove voter.
// 3. add learner + remove learner.
// 4. add learner + promote learner + remove voter.
// 5. add voter + demote voter + remove learner.
// 6. promote learner.
// 7. demote voter.
// 8. remove voter/learner.
// 9. add voter/learner.
// Plan 1-5 (replace plans) do not change voter/learner count, so they have higher priority.
type stepPlan struct {
	leaderBeforeAdd    uint64 // leader before adding peer.
	leaderBeforeRemove uint64 // leader before removing peer.
	add                *metapb.Peer
	remove             *metapb.Peer
	promote            *metapb.Peer
	demote             *metapb.Peer
}

func (p stepPlan) String() string {
	return fmt.Sprintf("stepPlan{leaderBeforeAdd=%v,add={%s},promote={%s},leaderBeforeRemove=%v,demote={%s},remove={%s}}",
		p.leaderBeforeAdd, p.add, p.promote, p.leaderBeforeRemove, p.demote, p.remove)
}

func (p stepPlan) IsEmpty() bool {
	return p.promote == nil && p.demote == nil && p.add == nil && p.remove == nil
}

func (b *Builder) peerPlan() stepPlan {
	// Replace has the highest priority because it does not change resource's
	// voter/learner count.
	if p := b.planReplace(); !p.IsEmpty() {
		return p
	}
	if p := b.planPromotePeer(); !p.IsEmpty() {
		return p
	}
	if p := b.planDemotePeer(); !p.IsEmpty() {
		return p
	}
	if p := b.planRemovePeer(); !p.IsEmpty() {
		return p
	}
	if p := b.planAddPeer(); !p.IsEmpty() {
		return p
	}
	return stepPlan{}
}

func (b *Builder) planReplace() stepPlan {
	var best stepPlan
	// promote learner + demote voter
	for _, i := range b.toDemote.IDs() {
		demote := b.toDemote[i]
		for _, j := range b.toPromote.IDs() {
			promote := b.toPromote[j]
			best = b.planReplaceLeaders(best, stepPlan{promote: &promote, demote: &demote})
		}
	}
	// add voter + remove voter OR add learner + remove learner.
	for _, i := range b.toAdd.IDs() {
		add := b.toAdd[i]
		for _, j := range b.toRemove.IDs() {
			remove := b.toRemove[j]
			if metadata.IsLearner(remove) == metadata.IsLearner(add) {
				best = b.planReplaceLeaders(best, stepPlan{add: &add, remove: &remove})
			}
		}
	}
	// add learner + promote learner + remove voter
	for _, i := range b.toPromote.IDs() {
		promote := b.toPromote[i]
		for _, j := range b.toAdd.IDs() {
			if add := b.toAdd[j]; metadata.IsLearner(add) {
				for _, k := range b.toRemove.IDs() {
					if remove := b.toRemove[k]; !metadata.IsLearner(remove) && j != k {
						best = b.planReplaceLeaders(best, stepPlan{promote: &promote, add: &add, remove: &remove})
					}
				}
			}
		}
	}
	// add voter + demote voter + remove learner
	for _, i := range b.toDemote.IDs() {
		demote := b.toDemote[i]
		for _, j := range b.toRemove.IDs() {
			if remove := b.toRemove[j]; metadata.IsLearner(remove) {
				for _, k := range b.toAdd.IDs() {
					if add := b.toAdd[k]; !metadata.IsLearner(add) && j != k {
						best = b.planReplaceLeaders(best, stepPlan{demote: &demote, add: &add, remove: &remove})
					}
				}
			}
		}
	}
	return best
}

func (b *Builder) planReplaceLeaders(best, next stepPlan) stepPlan {
	// Brute force all possible leader combinations to find the best plan.
	for _, leaderBeforeAdd := range b.currentPeers.IDs() {
		if !b.allowLeader(b.currentPeers[leaderBeforeAdd], false) {
			continue
		}
		next.leaderBeforeAdd = leaderBeforeAdd
		for _, leaderBeforeRemove := range b.currentPeers.IDs() {
			if leaderBeforeRemove != next.demote.GetContainerID() &&
				leaderBeforeRemove != next.remove.GetContainerID() &&
				b.allowLeader(b.currentPeers[leaderBeforeRemove], false) {
				// leaderBeforeRemove does not select nodes to be demote or removed.
				next.leaderBeforeRemove = leaderBeforeRemove
				best = b.comparePlan(best, next)
			}
		}
		if next.promote != nil &&
			next.promote.GetContainerID() != next.demote.GetContainerID() &&
			next.promote.GetContainerID() != next.remove.GetContainerID() &&
			b.allowLeader(*next.promote, false) {
			// leaderBeforeRemove does not select nodes to be demote or removed.
			next.leaderBeforeRemove = next.promote.GetContainerID()
			best = b.comparePlan(best, next)
		}
		if next.add != nil &&
			next.add.GetContainerID() != next.demote.GetContainerID() &&
			next.add.GetContainerID() != next.remove.GetContainerID() &&
			b.allowLeader(*next.add, false) {
			// leaderBeforeRemove does not select nodes to be demote or removed.
			next.leaderBeforeRemove = next.add.GetContainerID()
			best = b.comparePlan(best, next)
		}
	}
	return best
}

func (b *Builder) planPromotePeer() stepPlan {
	for _, i := range b.toPromote.IDs() {
		peer := b.toPromote[i]
		return stepPlan{promote: &peer}
	}
	return stepPlan{}
}

func (b *Builder) planDemotePeer() stepPlan {
	var best stepPlan
	for _, i := range b.toDemote.IDs() {
		d := b.toDemote[i]
		for _, leader := range b.currentPeers.IDs() {
			if b.allowLeader(b.currentPeers[leader], false) && leader != d.ContainerID {
				best = b.comparePlan(best, stepPlan{demote: &d, leaderBeforeRemove: leader})
			}
		}
	}
	return best
}

func (b *Builder) planRemovePeer() stepPlan {
	var best stepPlan
	for _, i := range b.toRemove.IDs() {
		r := b.toRemove[i]
		for _, leader := range b.currentPeers.IDs() {
			if b.allowLeader(b.currentPeers[leader], false) && leader != r.ContainerID {
				best = b.comparePlan(best, stepPlan{remove: &r, leaderBeforeRemove: leader})
			}
		}
	}
	return best
}

func (b *Builder) planAddPeer() stepPlan {
	var best stepPlan
	for _, i := range b.toAdd.IDs() {
		a := b.toAdd[i]
		for _, leader := range b.currentPeers.IDs() {
			if b.allowLeader(b.currentPeers[leader], false) {
				best = b.comparePlan(best, stepPlan{add: &a, leaderBeforeAdd: leader})
			}
		}
	}
	return best
}

func (b *Builder) initStepPlanPreferFuncs() {
	b.stepPlanPreferFuncs = []func(stepPlan) int{
		b.planPreferReplaceByNearest, // 1. violate it affects replica safety.
		// 2-3 affects operator execution speed.
		b.planPreferUPContainerAsLeader, // 2. compare to 3, it is more likely to affect execution speed.
		b.planPreferOldPeerAsLeader,     // 3. violate it may or may not affect execution speed.
		// 4-6 are less important as they are only trying to build the
		// operator with less leader transfer steps.
		b.planPreferAddOrPromoteTargetLeader, // 4. it is precondition of 5 so goes first.
		b.planPreferTargetLeader,             // 5. it may help 6 in later steps.
		b.planPreferLessLeaderTransfer,       // 6. trivial optimization to make the operator more tidy.
	}
}

// Pick the better plan from 2 candidates.
func (b *Builder) comparePlan(best, next stepPlan) stepPlan {
	if best.IsEmpty() {
		return next
	}
	for _, f := range b.stepPlanPreferFuncs {
		if scoreBest, scoreNext := f(best), f(next); scoreBest > scoreNext {
			return best
		} else if scoreBest < scoreNext {
			return next
		}
	}
	return best
}

func (b *Builder) labelMatch(x, y uint64) int {
	sx, sy := b.cluster.GetContainer(x), b.cluster.GetContainer(y)
	if sx == nil || sy == nil {
		return 0
	}
	labels := b.cluster.GetOpts().GetLocationLabels()
	for i, l := range labels {
		if sx.GetLabelValue(l) != sy.GetLabelValue(l) {
			return i
		}
	}
	return len(labels)
}

// return matched label count.
func (b *Builder) planPreferReplaceByNearest(p stepPlan) int {
	m := 0
	if p.add != nil && p.remove != nil {
		m = b.labelMatch(p.add.ContainerID, p.remove.ContainerID)
		if p.promote != nil {
			// add learner + promote learner + remove voter
			if m2 := b.labelMatch(p.promote.ContainerID, p.add.ContainerID); m2 < m {
				return m2
			}
		} else if p.demote != nil {
			// demote voter + remove learner + add voter
			if m2 := b.labelMatch(p.demote.ContainerID, p.remove.ContainerID); m2 < m {
				return m2
			}
		}
	}
	return m
}

// Avoid generating snapshots from offline containers.
func (b *Builder) planPreferUPContainerAsLeader(p stepPlan) int {
	if p.add != nil {
		container := b.cluster.GetContainer(p.leaderBeforeAdd)
		return typeutil.BoolToInt(container != nil && container.IsUp())
	}
	return 1
}

// Newly created peer may reject the leader.
func (b *Builder) planPreferOldPeerAsLeader(p stepPlan) int {
	ret := -b.peerAddStep[p.leaderBeforeAdd]
	if p.add != nil && p.add.ContainerID == p.leaderBeforeRemove {
		ret -= len(b.steps) + 1
	} else {
		ret -= b.peerAddStep[p.leaderBeforeRemove]
	}
	return ret
}

// It is better to avoid transferring leader.
func (b *Builder) planPreferLessLeaderTransfer(p stepPlan) int {
	if p.leaderBeforeAdd == 0 || p.leaderBeforeAdd == b.currentLeaderContainerID {
		// 3: current == leaderBeforeAdd == leaderBeforeRemove
		// 2: current == leaderBeforeAdd != leaderBeforeRemove
		return 2 + typeutil.BoolToInt(p.leaderBeforeRemove == 0 || p.leaderBeforeRemove == b.currentLeaderContainerID)
	}
	// 1: current != leaderBeforeAdd == leaderBeforeRemove
	// 0: current != leaderBeforeAdd != leaderBeforeRemove
	return typeutil.BoolToInt(p.leaderBeforeRemove == 0 || p.leaderBeforeRemove == p.leaderBeforeAdd)
}

// It is better to transfer leader to the target leader.
func (b *Builder) planPreferTargetLeader(p stepPlan) int {
	return typeutil.BoolToInt(b.targetLeaderContainerID == 0 ||
		(p.leaderBeforeRemove != 0 && p.leaderBeforeRemove == b.targetLeaderContainerID) ||
		(p.leaderBeforeRemove == 0 && p.leaderBeforeAdd == b.targetLeaderContainerID))
}

// It is better to add target leader as early as possible.
func (b *Builder) planPreferAddOrPromoteTargetLeader(p stepPlan) int {
	if b.targetLeaderContainerID == 0 {
		return 0
	}
	addTarget := p.add != nil && !metadata.IsLearner(*p.add) && p.add.ContainerID == b.targetLeaderContainerID
	promoteTarget := p.promote != nil && p.promote.ContainerID == b.targetLeaderContainerID
	return typeutil.BoolToInt(addTarget || promoteTarget)
}

// Peers indexed by containerID.
type peersMap map[uint64]metapb.Peer

func newPeersMap() peersMap {
	return make(map[uint64]metapb.Peer)
}

// IDs is used for iteration in order.
func (pm peersMap) IDs() []uint64 {
	ids := make([]uint64, 0, len(pm))
	for id := range pm {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (pm peersMap) Set(peer metapb.Peer) {
	pm[peer.ContainerID] = peer
}

func (pm peersMap) String() string {
	ids := make([]uint64, 0, len(pm))
	for _, p := range pm {
		ids = append(ids, p.ContainerID)
	}
	return fmt.Sprintf("%v", ids)
}

func (pm peersMap) Copy() peersMap {
	var pm2 peersMap = make(map[uint64]metapb.Peer, len(pm))
	for _, p := range pm {
		pm2.Set(p)
	}
	return pm2
}
