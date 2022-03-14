// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
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

package schedule

import (
	"container/heap"
	"container/list"
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/hbstream"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/util/cache"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"go.uber.org/zap"
)

// The source of dispatched resource.
const (
	DispatchFromHeartBeat     = "heartbeat"
	DispatchFromNotifierQueue = "active push"
	DispatchFromCreate        = "create"
)

var (
	historyKeepTime    = 5 * time.Minute
	slowNotifyInterval = 5 * time.Second
	fastNotifyInterval = 2 * time.Second
	// PushOperatorTickInterval is the interval try to push the operator.
	PushOperatorTickInterval = 500 * time.Millisecond
	// StoreBalanceBaseTime represents the base time of balance rate.
	StoreBalanceBaseTime float64 = 60
)

// OperatorController is used to limit the speed of scheduling.
type OperatorController struct {
	sync.RWMutex
	ctx             context.Context
	cluster         opt.Cluster
	operators       map[uint64]*operator.Operator
	hbStreams       *hbstream.HeartbeatStreams
	histories       *list.List
	counts          map[operator.OpKind]uint64
	opRecords       *OperatorRecords
	containersLimit map[uint64]map[limit.Type]*limit.StoreLimit
	wop             WaitingOperator
	wopStatus       *WaitingOperatorStatus
	opNotifierQueue operatorQueue
}

// NewOperatorController creates a OperatorController.
func NewOperatorController(ctx context.Context, cluster opt.Cluster, hbStreams *hbstream.HeartbeatStreams) *OperatorController {
	return &OperatorController{
		ctx:             ctx,
		cluster:         cluster,
		operators:       make(map[uint64]*operator.Operator),
		hbStreams:       hbStreams,
		histories:       list.New(),
		counts:          make(map[operator.OpKind]uint64),
		opRecords:       NewOperatorRecords(ctx),
		containersLimit: make(map[uint64]map[limit.Type]*limit.StoreLimit),
		wop:             NewRandBuckets(),
		wopStatus:       NewWaitingOperatorStatus(),
		opNotifierQueue: make(operatorQueue, 0),
	}
}

// Ctx returns a context which will be canceled once RaftCluster is stopped.
// For now, it is only used to control the lifetime of TTL cache in schedulers.
func (oc *OperatorController) Ctx() context.Context {
	return oc.ctx
}

// GetCluster exports cluster to evict-scheduler for check container status.
func (oc *OperatorController) GetCluster() opt.Cluster {
	oc.RLock()
	defer oc.RUnlock()
	return oc.cluster
}

// DispatchDestroyDirectly send DestroyDirect cmd to the current container, because
// the resource has been removed.
func (oc *OperatorController) DispatchDestroyDirectly(res *core.CachedShard, source string) {
	oc.SendScheduleCommand(res, operator.DestroyDirectly{}, source)
}

// Dispatch is used to dispatch the operator of a resource.
func (oc *OperatorController) Dispatch(res *core.CachedShard, source string) {
	// Check existed operator.
	if op := oc.GetOperator(res.Meta.GetID()); op != nil {
		// Update operator status:
		// The operator status should be STARTED.
		// Check will call CheckSuccess and CheckTimeout.
		step := op.Check(res)

		switch op.Status() {
		case operator.STARTED:
			operatorCounter.WithLabelValues(op.Desc(), "check").Inc()
			if source == DispatchFromHeartBeat && oc.checkStaleOperator(op, step, res) {
				return
			}
			oc.SendScheduleCommand(res, step, source)
		case operator.SUCCESS:
			oc.pushHistory(op)
			if oc.RemoveOperator(op, "") {
				operatorWaitCounter.WithLabelValues(op.Desc(), "promote-success").Inc()
				oc.PromoteWaitingOperator()
			}
		case operator.TIMEOUT:
			if oc.RemoveOperator(op, "") {
				operatorCounter.WithLabelValues(op.Desc(), "promote-timeout").Inc()
				oc.PromoteWaitingOperator()
			}
		default:
			if oc.removeOperatorWithoutBury(op) {
				// CREATED, EXPIRED must not appear.
				// CANCELED, REPLACED must remove before transition.
				oc.cluster.GetLogger().Error("resource dispatching operator with unexpected status",
					log.ResourceField(op.ShardID()),
					zap.Stringer("op", op),
					zap.String("status", operator.OpStatusToString(op.Status())))

				operatorWaitCounter.WithLabelValues(op.Desc(), "unexpected").Inc()
				_ = op.Cancel()
				oc.buryOperator(op, "")
				operatorWaitCounter.WithLabelValues(op.Desc(), "promote-unexpected").Inc()
				oc.PromoteWaitingOperator()
			}
		}
	}
}

func (oc *OperatorController) checkStaleOperator(op *operator.Operator, step operator.OpStep, res *core.CachedShard) bool {
	err := step.CheckSafety(res)
	if err != nil {
		if oc.RemoveOperator(op, err.Error()) {
			operatorCounter.WithLabelValues(op.Desc(), "stale").Inc()
			operatorWaitCounter.WithLabelValues(op.Desc(), "promote-stale").Inc()
			oc.PromoteWaitingOperator()
			return true
		}
	}
	// When the "source" is heartbeat, the resource may have a newer
	// confver than the resource that the operator holds. In this case,
	// the operator is stale, and will not be executed even we would
	// have sent it to your storage applications. Here, we just cancel it.
	origin := op.ShardEpoch()
	latest := res.Meta.GetEpoch()
	changes := latest.GetConfigVer() - origin.GetConfigVer()
	if changes > op.ConfVerChanged(res) {
		if oc.RemoveOperator(
			op,
			"stale operator, confver does not meet expectations",
		) {
			operatorCounter.WithLabelValues(op.Desc(), "stale").Inc()
			operatorWaitCounter.WithLabelValues(op.Desc(), "promote-stale").Inc()
			oc.PromoteWaitingOperator()
			return true
		}
	}

	return false
}

func (oc *OperatorController) getNextPushOperatorTime(step operator.OpStep, now time.Time) time.Time {
	nextTime := slowNotifyInterval
	switch step.(type) {
	case operator.TransferLeader, operator.PromoteLearner, operator.DemoteFollower, operator.ChangePeerV2Enter, operator.ChangePeerV2Leave:
		nextTime = fastNotifyInterval
	}
	return now.Add(nextTime)
}

// pollNeedDispatchShard returns the resource need to dispatch,
// "next" is true to indicate that it may exist in next attempt,
// and false is the end for the poll.
func (oc *OperatorController) pollNeedDispatchShard() (r *core.CachedShard, next bool) {
	oc.Lock()
	defer oc.Unlock()
	if oc.opNotifierQueue.Len() == 0 {
		return nil, false
	}
	item := heap.Pop(&oc.opNotifierQueue).(*operatorWithTime)
	resID := item.op.ShardID()
	op, ok := oc.operators[resID]
	if !ok || op == nil {
		return nil, true
	}
	r = oc.cluster.GetShard(resID)
	if r == nil {
		_ = oc.removeOperatorLocked(op)
		if op.Cancel() {
			oc.cluster.GetLogger().Warn("remove operator because resource disappeared",
				log.ResourceField(op.ShardID()),
				zap.Stringer("op", op))
			operatorCounter.WithLabelValues(op.Desc(), "disappear").Inc()
		}
		oc.buryOperator(op, "")
		return nil, true
	}
	step := op.Check(r)
	if step == nil {
		return r, true
	}
	now := time.Now()
	if now.Before(item.time) {
		heap.Push(&oc.opNotifierQueue, item)
		return nil, false
	}

	// pushes with new notify time.
	item.time = oc.getNextPushOperatorTime(step, now)
	heap.Push(&oc.opNotifierQueue, item)
	return r, true
}

// PushOperators periodically pushes the unfinished operator to the executor(your storage application).
func (oc *OperatorController) PushOperators() {
	for {
		r, next := oc.pollNeedDispatchShard()
		if !next {
			break
		}
		if r == nil {
			continue
		}

		oc.Dispatch(r, DispatchFromNotifierQueue)
	}
}

// AddWaitingOperator adds operators to waiting operators.
func (oc *OperatorController) AddWaitingOperator(ops ...*operator.Operator) int {
	oc.Lock()
	added := 0

	for i := 0; i < len(ops); i++ {
		op := ops[i]
		desc := op.Desc()
		isMerge := false
		if op.Kind()&operator.OpMerge != 0 {
			if i+1 >= len(ops) {
				// should not be here forever
				oc.cluster.GetLogger().Error("orphan merge operators found",
					zap.String("desc", desc))
				oc.Unlock()
				return added
			}
			if ops[i+1].Kind()&operator.OpMerge == 0 {
				oc.cluster.GetLogger().Error("merge operator should be paired",
					zap.String("desc", ops[i+1].Desc()))
				oc.Unlock()
				return added
			}
			isMerge = true
		}
		if !oc.checkAddOperator(op) {
			_ = op.Cancel()
			oc.buryOperator(op, "")
			if isMerge {
				// Merge operation have two operators, cancel them all
				next := ops[i+1]
				_ = next.Cancel()
				oc.buryOperator(next, "")
			}
			oc.Unlock()
			return added
		}
		oc.wop.PutOperator(op)
		if isMerge {
			// count two merge operators as one, so wopStatus.ops[desc] should
			// not be updated here
			i++
			added++
			oc.wop.PutOperator(ops[i])
		}
		operatorWaitCounter.WithLabelValues(desc, "put").Inc()
		oc.wopStatus.ops[desc]++
		added++
	}

	oc.Unlock()
	operatorWaitCounter.WithLabelValues(ops[0].Desc(), "promote-add").Inc()
	oc.PromoteWaitingOperator()
	return added
}

// AddOperator adds operators to the running operators.
func (oc *OperatorController) AddOperator(ops ...*operator.Operator) bool {
	oc.Lock()
	defer oc.Unlock()

	if oc.exceedStoreLimitLocked(ops...) || !oc.checkAddOperator(ops...) {
		for _, op := range ops {
			_ = op.Cancel()
			oc.buryOperator(op, "")
		}
		return false
	}
	for _, op := range ops {
		if !oc.addOperatorLocked(op) {
			return false
		}
	}
	return true
}

// PromoteWaitingOperator promotes operators from waiting operators.
func (oc *OperatorController) PromoteWaitingOperator() {
	oc.Lock()
	defer oc.Unlock()
	var ops []*operator.Operator
	for {
		// GetOperator returns one operator or two merge operators
		ops = oc.wop.GetOperator()
		if ops == nil {
			return
		}
		operatorWaitCounter.WithLabelValues(ops[0].Desc(), "get").Inc()

		if oc.exceedStoreLimitLocked(ops...) || !oc.checkAddOperator(ops...) {
			for _, op := range ops {
				operatorWaitCounter.WithLabelValues(op.Desc(), "promote-canceled").Inc()
				_ = op.Cancel()
				oc.buryOperator(op, "")
			}
			oc.wopStatus.ops[ops[0].Desc()]--
			continue
		}
		oc.wopStatus.ops[ops[0].Desc()]--
		break
	}

	for _, op := range ops {
		if !oc.addOperatorLocked(op) {
			break
		}
	}
}

// checkAddOperator checks if the operator can be added.
// There are several situations that cannot be added:
// - There is no such resource in the cluster
// - The epoch of the operator and the epoch of the corresponding resource are no longer consistent.
// - The resource already has a higher priority or same priority operator.
// - Exceed the max number of waiting operators
// - At least one operator is expired.
func (oc *OperatorController) checkAddOperator(ops ...*operator.Operator) bool {
	for _, op := range ops {
		res := oc.cluster.GetShard(op.ShardID())
		if res == nil {
			oc.cluster.GetLogger().Debug("resource not found, cancel add operator",
				log.ResourceField(op.ShardID()))
			operatorWaitCounter.WithLabelValues(op.Desc(), "not-found").Inc()
			return false
		}
		epoch := res.Meta.GetEpoch()
		if epoch.GetGeneration() != op.ShardEpoch().Generation ||
			epoch.GetConfigVer() != op.ShardEpoch().ConfigVer {
			oc.cluster.GetLogger().Debug("resource epoch not match, cancel add operator",
				log.ResourceField(op.ShardID()),
				log.EpochField("old", epoch),
				log.EpochField("new", op.ShardEpoch()))
			operatorWaitCounter.WithLabelValues(op.Desc(), "epoch-not-match").Inc()
			return false
		}
		if old := oc.operators[op.ShardID()]; old != nil && !isHigherPriorityOperator(op, old) {
			oc.cluster.GetLogger().Debug("resource already have operator, cancel add operator",
				log.ResourceField(op.ShardID()),
				zap.Stringer("old", old))
			operatorWaitCounter.WithLabelValues(op.Desc(), "already-have").Inc()
			return false
		}
		if op.Status() != operator.CREATED {
			oc.cluster.GetLogger().Error("resource trying to add operator with unexpected status",
				log.ResourceField(op.ShardID()),
				zap.Stringer("op", op),
				zap.String("status", operator.OpStatusToString(op.Status())))
			operatorWaitCounter.WithLabelValues(op.Desc(), "unexpected-status").Inc()
			return false
		}
		if oc.wopStatus.ops[op.Desc()] >= oc.cluster.GetOpts().GetSchedulerMaxWaitingOperator() {
			oc.cluster.GetLogger().Debug("waiting exceed max return false",
				log.ResourceField(oc.wopStatus.ops[op.Desc()]),
				zap.Uint64("max", oc.cluster.GetOpts().GetSchedulerMaxWaitingOperator()),
				zap.String("desc", op.Desc()))
			operatorWaitCounter.WithLabelValues(op.Desc(), "exceed-max").Inc()
			return false
		}
	}
	expired := false
	for _, op := range ops {
		if op.CheckExpired() {
			expired = true
			operatorWaitCounter.WithLabelValues(op.Desc(), "expired").Inc()
		}
	}
	return !expired
}

func isHigherPriorityOperator(new, old *operator.Operator) bool {
	return new.GetPriorityLevel() > old.GetPriorityLevel()
}

func (oc *OperatorController) addOperatorLocked(op *operator.Operator) bool {
	resID := op.ShardID()

	oc.cluster.GetLogger().Info("resource add operator",
		log.ResourceField(resID),
		zap.Stringer("op", op),
		zap.String("info", op.GetAdditionalInfo()))

	// If there is an old operator, replace it. The priority should be checked
	// already.
	if old, ok := oc.operators[resID]; ok {
		_ = oc.removeOperatorLocked(old)
		_ = old.Replace()
		oc.buryOperator(old, "")
	}

	if !op.Start() {
		oc.cluster.GetLogger().Error("resource adding operator with unexpected status",
			log.ResourceField(resID),
			zap.Stringer("op", op),
			zap.String("status", operator.OpStatusToString(op.Status())))
		operatorCounter.WithLabelValues(op.Desc(), "unexpected").Inc()
		return false
	}
	oc.operators[resID] = op
	operatorCounter.WithLabelValues(op.Desc(), "start").Inc()
	operatorWaitDuration.WithLabelValues(op.Desc()).Observe(op.ElapsedTime().Seconds())
	opInfluence := NewTotalOpInfluence([]*operator.Operator{op}, oc.cluster)
	for containerID := range opInfluence.StoresInfluence {
		if oc.containersLimit[containerID] == nil {
			continue
		}
		for n, v := range limit.TypeNameValue {
			containerLimit := oc.containersLimit[containerID][v]
			if containerLimit == nil {
				continue
			}
			stepCost := opInfluence.GetStoreInfluence(containerID).GetStepCost(v)
			if stepCost == 0 {
				continue
			}
			containerLimit.Take(stepCost)
			containerLimitCostCounter.WithLabelValues(strconv.FormatUint(containerID, 10), n).Add(float64(stepCost) / float64(limit.ShardInfluence[v]))
		}
	}
	oc.updateCounts(oc.operators)

	var step operator.OpStep
	if res := oc.cluster.GetShard(op.ShardID()); res != nil {
		if step = op.Check(res); step != nil {
			oc.SendScheduleCommand(res, step, DispatchFromCreate)
		}
	}

	heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op, time: oc.getNextPushOperatorTime(step, time.Now())})
	operatorCounter.WithLabelValues(op.Desc(), "create").Inc()
	for _, counter := range op.Counters {
		counter.Inc()
	}
	return true
}

// RemoveOperator removes a operator from the running operators.
func (oc *OperatorController) RemoveOperator(op *operator.Operator, extra string) bool {
	oc.Lock()
	removed := oc.removeOperatorLocked(op)
	oc.Unlock()
	if removed {
		if op.Cancel() {
			oc.cluster.GetLogger().Info("resource operator removed",
				log.ResourceField(op.ShardID()),
				zap.Stringer("op", op),
				zap.Duration("takes", op.RunningTime()))
		}
		oc.buryOperator(op, extra)
	}
	return removed
}

func (oc *OperatorController) removeOperatorWithoutBury(op *operator.Operator) bool {
	oc.Lock()
	defer oc.Unlock()
	return oc.removeOperatorLocked(op)
}

func (oc *OperatorController) removeOperatorLocked(op *operator.Operator) bool {
	resID := op.ShardID()
	if cur := oc.operators[resID]; cur == op {
		delete(oc.operators, resID)
		oc.updateCounts(oc.operators)
		operatorCounter.WithLabelValues(op.Desc(), "remove").Inc()
		return true
	}
	return false
}

func (oc *OperatorController) buryOperator(op *operator.Operator, extra string) {
	st := op.Status()

	if !operator.IsEndStatus(st) {
		oc.cluster.GetLogger().Error("resource burying operator with non-end status",
			log.ResourceField(op.ShardID()),
			zap.Stringer("op", op),
			zap.String("status", operator.OpStatusToString(op.Status())))
		operatorCounter.WithLabelValues(op.Desc(), "unexpected").Inc()
		_ = op.Cancel()
	}

	switch st {
	case operator.SUCCESS:
		oc.cluster.GetLogger().Info("resource operator finish",
			log.ResourceField(op.ShardID()),
			zap.Stringer("op", op),
			zap.Duration("takes", op.RunningTime()),
			zap.String("info", op.GetAdditionalInfo()))
		operatorCounter.WithLabelValues(op.Desc(), "finish").Inc()
		operatorDuration.WithLabelValues(op.Desc()).Observe(op.RunningTime().Seconds())
		for _, counter := range op.FinishedCounters {
			counter.Inc()
		}
	case operator.REPLACED:
		oc.cluster.GetLogger().Info("resource replace old operator",
			log.ResourceField(op.ShardID()),
			zap.Stringer("op", op),
			zap.Duration("takes", op.RunningTime()))
		operatorCounter.WithLabelValues(op.Desc(), "replace").Inc()
	case operator.EXPIRED:
		oc.cluster.GetLogger().Info("resource operator expired",
			log.ResourceField(op.ShardID()),
			zap.Stringer("op", op),
			zap.Duration("lives", op.ElapsedTime()))
		operatorCounter.WithLabelValues(op.Desc(), "expire").Inc()
	case operator.TIMEOUT:
		oc.cluster.GetLogger().Info("resource operator timeout",
			log.ResourceField(op.ShardID()),
			zap.Stringer("op", op),
			zap.Duration("takes", op.RunningTime()))
		operatorCounter.WithLabelValues(op.Desc(), "timeout").Inc()
	case operator.CANCELED:
		oc.cluster.GetLogger().Info("resource operator canceled",
			log.ResourceField(op.ShardID()),
			zap.Stringer("op", op),
			zap.Duration("takes", op.RunningTime()),
			zap.String("extra", extra))
		operatorCounter.WithLabelValues(op.Desc(), "cancel").Inc()
	}

	oc.opRecords.Put(op)
}

// GetOperatorStatus gets the operator and its status with the specify id.
func (oc *OperatorController) GetOperatorStatus(id uint64) *OperatorWithStatus {
	oc.Lock()
	defer oc.Unlock()
	if op, ok := oc.operators[id]; ok {
		return NewOperatorWithStatus(op)
	}
	return oc.opRecords.Get(id)
}

// GetOperator gets a operator from the given resource.
func (oc *OperatorController) GetOperator(resID uint64) *operator.Operator {
	oc.RLock()
	defer oc.RUnlock()
	return oc.operators[resID]
}

// GetOperators gets operators from the running operators.
func (oc *OperatorController) GetOperators() []*operator.Operator {
	oc.RLock()
	defer oc.RUnlock()

	operators := make([]*operator.Operator, 0, len(oc.operators))
	for _, op := range oc.operators {
		operators = append(operators, op)
	}

	return operators
}

// GetWaitingOperators gets operators from the waiting operators.
func (oc *OperatorController) GetWaitingOperators() []*operator.Operator {
	oc.RLock()
	defer oc.RUnlock()
	return oc.wop.ListOperator()
}

// SendScheduleCommand sends a command to the resource.
func (oc *OperatorController) SendScheduleCommand(res *core.CachedShard, step operator.OpStep, source string) {
	oc.cluster.GetLogger().Info("resource send schedule command",
		log.ResourceField(res.Meta.GetID()),
		zap.Stringer("step", step),
		zap.String("source", source))

	var cmd *rpcpb.ShardHeartbeatRsp
	switch st := step.(type) {
	case operator.DestroyDirectly:
		cmd = &rpcpb.ShardHeartbeatRsp{
			DestroyDirectly: true,
		}
	case operator.TransferLeader:
		p, _ := res.GetStorePeer(st.ToStore)
		cmd = &rpcpb.ShardHeartbeatRsp{
			TransferLeader: &rpcpb.TransferLeader{
				Replica: p,
			},
		}
	case operator.AddPeer:
		if _, ok := res.GetStorePeer(st.ToStore); ok {
			// The newly added peer is pending.
			return
		}
		cmd = &rpcpb.ShardHeartbeatRsp{
			ConfigChange: &rpcpb.ConfigChange{
				ChangeType: metapb.ConfigChangeType_AddNode,
				Replica: metapb.Replica{
					ID:      st.PeerID,
					StoreID: st.ToStore,
					Role:    metapb.ReplicaRole_Voter,
				},
			},
		}
	case operator.AddLightPeer:
		if _, ok := res.GetStorePeer(st.ToStore); ok {
			// The newly added peer is pending.
			return
		}
		cmd = &rpcpb.ShardHeartbeatRsp{
			ConfigChange: &rpcpb.ConfigChange{
				ChangeType: metapb.ConfigChangeType_AddNode,
				Replica: metapb.Replica{
					ID:      st.PeerID,
					StoreID: st.ToStore,
					Role:    metapb.ReplicaRole_Voter,
				},
			},
		}
	case operator.AddLearner:
		if _, ok := res.GetStorePeer(st.ToStore); ok {
			// The newly added peer is pending.
			return
		}
		cmd = &rpcpb.ShardHeartbeatRsp{
			ConfigChange: &rpcpb.ConfigChange{
				ChangeType: metapb.ConfigChangeType_AddLearnerNode,
				Replica: metapb.Replica{
					ID:      st.PeerID,
					StoreID: st.ToStore,
					Role:    metapb.ReplicaRole_Learner,
				},
			},
		}
	case operator.AddLightLearner:
		if _, ok := res.GetStorePeer(st.ToStore); ok {
			// The newly added peer is pending.
			return
		}
		cmd = &rpcpb.ShardHeartbeatRsp{
			ConfigChange: &rpcpb.ConfigChange{
				ChangeType: metapb.ConfigChangeType_AddLearnerNode,
				Replica: metapb.Replica{
					ID:      st.PeerID,
					StoreID: st.ToStore,
					Role:    metapb.ReplicaRole_Learner,
				},
			},
		}
	case operator.PromoteLearner:
		cmd = &rpcpb.ShardHeartbeatRsp{
			ConfigChange: &rpcpb.ConfigChange{
				// reuse AddNode type
				ChangeType: metapb.ConfigChangeType_AddNode,
				Replica: metapb.Replica{
					ID:      st.PeerID,
					StoreID: st.ToStore,
					Role:    metapb.ReplicaRole_Voter,
				},
			},
		}
	case operator.DemoteFollower:
		cmd = &rpcpb.ShardHeartbeatRsp{
			ConfigChange: &rpcpb.ConfigChange{
				// reuse AddLearnerNode type
				ChangeType: metapb.ConfigChangeType_AddLearnerNode,
				Replica: metapb.Replica{
					ID:      st.PeerID,
					StoreID: st.ToStore,
					Role:    metapb.ReplicaRole_Learner,
				},
			},
		}
	case operator.RemovePeer:
		p, _ := res.GetStorePeer(st.FromStore)
		cmd = &rpcpb.ShardHeartbeatRsp{
			ConfigChange: &rpcpb.ConfigChange{
				ChangeType: metapb.ConfigChangeType_RemoveNode,
				Replica:    p,
			},
		}
	case operator.MergeShard:
		if st.IsPassive {
			return
		}

		data, _ := st.ToShard.Marshal()
		cmd = &rpcpb.ShardHeartbeatRsp{
			Merge: &rpcpb.Merge{
				Target: data,
			},
		}
	case operator.SplitShard:
		cmd = &rpcpb.ShardHeartbeatRsp{
			SplitShard: &rpcpb.SplitShard{
				Policy: st.Policy,
				Keys:   st.SplitKeys,
			},
		}
	case operator.ChangePeerV2Enter:
		cmd = &rpcpb.ShardHeartbeatRsp{
			ConfigChangeV2: st.GetRequest(),
		}
	case operator.ChangePeerV2Leave:
		cmd = &rpcpb.ShardHeartbeatRsp{
			ConfigChangeV2: &rpcpb.ConfigChangeV2{},
		}
	default:
		oc.cluster.GetLogger().Error("unknown operator step", zap.Stringer("step", step))
		return
	}

	oc.hbStreams.SendMsg(res, cmd)
}

func (oc *OperatorController) pushHistory(op *operator.Operator) {
	oc.Lock()
	defer oc.Unlock()
	for _, h := range op.History() {
		oc.histories.PushFront(h)
	}
}

// PruneHistory prunes a part of operators' history.
func (oc *OperatorController) PruneHistory() {
	oc.Lock()
	defer oc.Unlock()
	p := oc.histories.Back()
	for p != nil && time.Since(p.Value.(operator.OpHistory).FinishTime) > historyKeepTime {
		prev := p.Prev()
		oc.histories.Remove(p)
		p = prev
	}
}

// GetHistory gets operators' history.
func (oc *OperatorController) GetHistory(start time.Time) []operator.OpHistory {
	oc.RLock()
	defer oc.RUnlock()
	histories := make([]operator.OpHistory, 0, oc.histories.Len())
	for p := oc.histories.Front(); p != nil; p = p.Next() {
		history := p.Value.(operator.OpHistory)
		if history.FinishTime.Before(start) {
			break
		}
		histories = append(histories, history)
	}
	return histories
}

// updateCounts updates resource counts using current pending operators.
func (oc *OperatorController) updateCounts(operators map[uint64]*operator.Operator) {
	for k := range oc.counts {
		delete(oc.counts, k)
	}
	for _, op := range operators {
		oc.counts[op.Kind()]++
	}
}

// OperatorCount gets the count of operators filtered by mask.
func (oc *OperatorController) OperatorCount(mask operator.OpKind) uint64 {
	oc.RLock()
	defer oc.RUnlock()
	var total uint64
	for k, count := range oc.counts {
		if k&mask != 0 {
			total += count
		}
	}
	return total
}

// GetOpInfluence gets OpInfluence.
func (oc *OperatorController) GetOpInfluence(cluster opt.Cluster) operator.OpInfluence {
	influence := operator.OpInfluence{
		StoresInfluence: make(map[uint64]*operator.StoreInfluence),
	}
	oc.RLock()
	defer oc.RUnlock()
	for _, op := range oc.operators {
		if !op.CheckTimeout() && !op.CheckSuccess() {
			res := cluster.GetShard(op.ShardID())
			if res != nil {
				op.UnfinishedInfluence(influence, res)
			}
		}
	}
	return influence
}

// NewTotalOpInfluence creates a OpInfluence.
func NewTotalOpInfluence(operators []*operator.Operator, cluster opt.Cluster) operator.OpInfluence {
	influence := operator.OpInfluence{
		StoresInfluence: make(map[uint64]*operator.StoreInfluence),
	}

	for _, op := range operators {
		res := cluster.GetShard(op.ShardID())
		if res != nil {
			op.TotalInfluence(influence, res)
		}
	}

	return influence
}

// SetOperator is only used for test.
func (oc *OperatorController) SetOperator(op *operator.Operator) {
	oc.Lock()
	defer oc.Unlock()
	oc.operators[op.ShardID()] = op
	oc.updateCounts(oc.operators)
}

// OperatorWithStatus records the operator and its status.
type OperatorWithStatus struct {
	Op     *operator.Operator
	Status metapb.OperatorStatus
}

// NewOperatorWithStatus creates an OperatorStatus from an operator.
func NewOperatorWithStatus(op *operator.Operator) *OperatorWithStatus {
	return &OperatorWithStatus{
		Op:     op,
		Status: operator.OpStatusToPDPB(op.Status()),
	}
}

// MarshalJSON returns the status of operator as a JSON string
func (o *OperatorWithStatus) MarshalJSON() ([]byte, error) {
	return []byte(`"` + fmt.Sprintf("status: %s, operator: %s", o.Status.String(), o.Op.String()) + `"`), nil
}

// OperatorRecords remains the operator and its status for a while.
type OperatorRecords struct {
	ttl *cache.TTLUint64
}

const operatorStatusRemainTime = 10 * time.Minute

// NewOperatorRecords returns a OperatorRecords.
func NewOperatorRecords(ctx context.Context) *OperatorRecords {
	return &OperatorRecords{
		ttl: cache.NewIDTTL(ctx, time.Minute, operatorStatusRemainTime),
	}
}

// Get gets the operator and its status.
func (o *OperatorRecords) Get(id uint64) *OperatorWithStatus {
	v, exist := o.ttl.Get(id)
	if !exist {
		return nil
	}
	return v.(*OperatorWithStatus)
}

// Put puts the operator and its status.
func (o *OperatorRecords) Put(op *operator.Operator) {
	id := op.ShardID()
	record := NewOperatorWithStatus(op)
	o.ttl.Put(id, record)
}

// ExceedStoreLimit returns true if the container exceeds the cost limit after adding the operator. Otherwise, returns false.
func (oc *OperatorController) ExceedStoreLimit(ops ...*operator.Operator) bool {
	oc.Lock()
	defer oc.Unlock()
	return oc.exceedStoreLimitLocked(ops...)
}

// exceedStoreLimitLocked returns true if the container exceeds the cost limit after adding the operator. Otherwise, returns false.
func (oc *OperatorController) exceedStoreLimitLocked(ops ...*operator.Operator) bool {
	opInfluence := NewTotalOpInfluence(ops, oc.cluster)
	for containerID := range opInfluence.StoresInfluence {
		for _, v := range limit.TypeNameValue {
			stepCost := opInfluence.GetStoreInfluence(containerID).GetStepCost(v)
			if stepCost == 0 {
				continue
			}
			if oc.getOrCreateStoreLimit(containerID, v).Available() < stepCost {
				return true
			}
		}
	}
	return false
}

// newStoreLimit is used to create the limit of a container.
func (oc *OperatorController) newStoreLimit(containerID uint64, ratePerSec float64, limitType limit.Type) {
	oc.cluster.GetLogger().Info("create or update a container",
		zap.Uint64("container", containerID),
		zap.Stringer("limit", limitType),
		zap.Float64("rate", ratePerSec))

	if oc.containersLimit[containerID] == nil {
		oc.containersLimit[containerID] = make(map[limit.Type]*limit.StoreLimit)
	}
	oc.containersLimit[containerID][limitType] = limit.NewStoreLimit(ratePerSec, limit.ShardInfluence[limitType])
}

// getOrCreateStoreLimit is used to get or create the limit of a container.
func (oc *OperatorController) getOrCreateStoreLimit(containerID uint64, limitType limit.Type) *limit.StoreLimit {
	if oc.containersLimit[containerID][limitType] == nil {
		ratePerSec := oc.cluster.GetOpts().GetStoreLimitByType(containerID, limitType) / StoreBalanceBaseTime
		oc.newStoreLimit(containerID, ratePerSec, limitType)
		oc.cluster.AttachAvailableFunc(containerID, limitType, func() bool {
			oc.RLock()
			defer oc.RUnlock()
			if oc.containersLimit[containerID][limitType] == nil {
				return true
			}
			return oc.containersLimit[containerID][limitType].Available() >= limit.ShardInfluence[limitType]
		})
	}
	ratePerSec := oc.cluster.GetOpts().GetStoreLimitByType(containerID, limitType) / StoreBalanceBaseTime
	if ratePerSec != oc.containersLimit[containerID][limitType].Rate() {
		oc.newStoreLimit(containerID, ratePerSec, limitType)
	}
	return oc.containersLimit[containerID][limitType]
}

// GetLeaderSchedulePolicy is to get leader schedule policy.
func (oc *OperatorController) GetLeaderSchedulePolicy() core.SchedulePolicy {
	if oc.cluster == nil {
		return core.ByCount
	}
	return oc.cluster.GetOpts().GetLeaderSchedulePolicy()
}

// CollectStoreLimitMetrics collects the metrics about container limit
func (oc *OperatorController) CollectStoreLimitMetrics() {
	oc.RLock()
	defer oc.RUnlock()
	if oc.containersLimit == nil {
		return
	}
	containers := oc.cluster.GetStores()
	for _, container := range containers {
		if container != nil {
			containerID := container.Meta.GetID()
			containerIDStr := strconv.FormatUint(containerID, 10)
			for n, v := range limit.TypeNameValue {
				var containerLimit *limit.StoreLimit
				if oc.containersLimit[containerID] == nil || oc.containersLimit[containerID][v] == nil {
					// Set to 0 to represent the container limit of the specific type is not initialized.
					containerLimitRateGauge.WithLabelValues(containerIDStr, n).Set(0)
					continue
				}
				containerLimit = oc.containersLimit[containerID][v]
				containerLimitAvailableGauge.WithLabelValues(containerIDStr, n).Set(float64(containerLimit.Available()) / float64(limit.ShardInfluence[v]))
				containerLimitRateGauge.WithLabelValues(containerIDStr, n).Set(containerLimit.Rate() * StoreBalanceBaseTime)
			}
		}
	}
}
