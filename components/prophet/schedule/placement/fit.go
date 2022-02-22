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

package placement

import (
	"math"
	"sort"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

// ShardFit is the result of fitting a resource's peers to rule list.
// All peers are divided into corresponding rules according to the matching
// rules, and the remaining Replicas are placed in the OrphanPeers list.
type ShardFit struct {
	RuleFits    []*RuleFit
	OrphanPeers []metapb.Replica
}

// IsSatisfied returns if the rules are properly satisfied.
// It means all Rules are fulfilled and there is no orphan peers.
func (f *ShardFit) IsSatisfied() bool {
	if len(f.RuleFits) == 0 {
		return false
	}
	for _, r := range f.RuleFits {
		if !r.IsSatisfied() {
			return false
		}
	}
	return len(f.OrphanPeers) == 0
}

// GetRuleFit returns the RuleFit that contains the peer.
func (f *ShardFit) GetRuleFit(peerID uint64) *RuleFit {
	for _, rf := range f.RuleFits {
		for _, p := range rf.Peers {
			if p.ID == peerID {
				return rf
			}
		}
	}
	return nil
}

// CompareShardFit determines the superiority of 2 fits.
// It returns 1 when the first fit result is better.
func CompareShardFit(a, b *ShardFit) int {
	for i := range a.RuleFits {
		if i >= len(b.RuleFits) {
			break
		}
		if cmp := compareRuleFit(a.RuleFits[i], b.RuleFits[i]); cmp != 0 {
			return cmp
		}
	}
	switch {
	case len(a.OrphanPeers) < len(b.OrphanPeers):
		return 1
	case len(a.OrphanPeers) > len(b.OrphanPeers):
		return -1
	default:
		return 0
	}
}

// RuleFit is the result of fitting status of a Rule.
type RuleFit struct {
	Rule *Rule
	// Peers of the Shard that are divided to this Rule.
	Peers []metapb.Replica
	// PeersWithDifferentRole is subset of `Replicas`. It contains all Peers that have
	// different Role from configuration (the Role can be migrated to target role
	// by scheduling).
	PeersWithDifferentRole []metapb.Replica
	// IsolationScore indicates at which level of labeling these Peers are
	// isolated. A larger value is better.
	IsolationScore float64
}

// IsSatisfied returns if the rule is properly satisfied.
func (f *RuleFit) IsSatisfied() bool {
	return len(f.Peers) == f.Rule.Count && len(f.PeersWithDifferentRole) == 0
}

func compareRuleFit(a, b *RuleFit) int {
	switch {
	case len(a.Peers) < len(b.Peers):
		return -1
	case len(a.Peers) > len(b.Peers):
		return 1
	case len(a.PeersWithDifferentRole) > len(b.PeersWithDifferentRole):
		return -1
	case len(a.PeersWithDifferentRole) < len(b.PeersWithDifferentRole):
		return 1
	case a.IsolationScore < b.IsolationScore:
		return -1
	case a.IsolationScore > b.IsolationScore:
		return 1
	default:
		return 0
	}
}

// StoreSet represents the container.
type StoreSet interface {
	GetStores() []*core.CachedStore
	GetStore(id uint64) *core.CachedStore
}

// FitShard tries to fit peers of a resource to the rules.
func FitShard(containers StoreSet, res *core.CachedShard, rules []*Rule) *ShardFit {
	w := newFitWorker(containers, res, rules)
	w.run()
	return &w.bestFit
}

type fitWorker struct {
	containers []*core.CachedStore
	bestFit    ShardFit   // update during execution
	peers      []*fitPeer // p.selected is updated during execution.
	rules      []*Rule
}

func newFitWorker(containers StoreSet, res *core.CachedShard, rules []*Rule) *fitWorker {
	var peers []*fitPeer
	for _, p := range res.Meta.Replicas() {
		peers = append(peers, &fitPeer{
			Replica:   p,
			container: containers.GetStore(p.StoreID),
			isLeader:  res.GetLeader().GetID() == p.ID,
		})
	}
	// Sort peers to keep the match result deterministic.
	sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })

	return &fitWorker{
		containers: containers.GetStores(),
		bestFit:    ShardFit{RuleFits: make([]*RuleFit, len(rules))},
		peers:      peers,
		rules:      rules,
	}
}

func (w *fitWorker) run() {
	w.fitRule(0)
	w.updateOrphanPeers(0) // All peers go to orphanList when RuleList is empty.
}

// Pick the most suitable peer combination for the rule.
// Index specifies the position of the rule.
// returns true if it replaces `bestFit` with a better alternative.
func (w *fitWorker) fitRule(index int) bool {
	if index >= len(w.rules) {
		return false
	}

	var candidates []*fitPeer
	if checkRule(w.rules[index], w.containers) {
		// Only consider containers:
		// 1. Match label constraints
		// 2. Role match, or can match after transformed.
		// 3. Not selected by other rules.
		for _, p := range w.peers {
			if MatchLabelConstraints(p.container, w.rules[index].LabelConstraints) &&
				p.matchRoleLoose(w.rules[index].Role) &&
				!p.selected {
				candidates = append(candidates, p)
			}
		}
	}

	count := w.rules[index].Count
	if len(candidates) < count {
		count = len(candidates)
	}
	return w.enumPeers(candidates, nil, index, count)
}

// Recursively traverses all feasible peer combinations.
// For each combination, call `compareBest` to determine whether it is better
// than the existing option.
// Returns true if it replaces `bestFit` with a better alternative.
func (w *fitWorker) enumPeers(candidates, selected []*fitPeer, index int, count int) bool {
	if len(selected) == count {
		// We collect enough peers. End recursive.
		return w.compareBest(selected, index)
	}

	var better bool
	for i, p := range candidates {
		p.selected = true
		better = w.enumPeers(candidates[i+1:], append(selected, p), index, count) || better
		p.selected = false
	}
	return better
}

// compareBest checks if the selected peers is better then previous best.
// Returns true if it replaces `bestFit` with a better alternative.
func (w *fitWorker) compareBest(selected []*fitPeer, index int) bool {
	rf := newRuleFit(w.rules[index], selected)
	cmp := 1
	if best := w.bestFit.RuleFits[index]; best != nil {
		cmp = compareRuleFit(rf, best)
	}

	switch cmp {
	case 1:
		w.bestFit.RuleFits[index] = rf
		// Reset previous result after position index.
		for i := index + 1; i < len(w.rules); i++ {
			w.bestFit.RuleFits[i] = nil
		}
		w.fitRule(index + 1)
		w.updateOrphanPeers(index + 1)
		return true
	case 0:
		if w.fitRule(index + 1) {
			w.bestFit.RuleFits[index] = rf
			return true
		}
	}
	return false
}

// determine the orphanPeers list based on fitPeer.selected flag.
func (w *fitWorker) updateOrphanPeers(index int) {
	if index != len(w.rules) {
		return
	}
	w.bestFit.OrphanPeers = w.bestFit.OrphanPeers[:0]
	for _, p := range w.peers {
		if !p.selected {
			w.bestFit.OrphanPeers = append(w.bestFit.OrphanPeers, p.Replica)
		}
	}
}

func newRuleFit(rule *Rule, peers []*fitPeer) *RuleFit {
	rf := &RuleFit{Rule: rule, IsolationScore: isolationScore(peers, rule.LocationLabels)}
	for _, p := range peers {
		rf.Peers = append(rf.Peers, p.Replica)
		if !p.matchRoleStrict(rule.Role) {
			rf.PeersWithDifferentRole = append(rf.PeersWithDifferentRole, p.Replica)
		}
	}
	return rf
}

type fitPeer struct {
	metapb.Replica
	container *core.CachedStore
	isLeader  bool
	selected  bool
}

func (p *fitPeer) matchRoleStrict(role ReplicaRoleType) bool {
	switch role {
	case Voter: // Voter matches either Leader or Follower.
		return !metadata.IsLearner(p.Replica)
	case Leader:
		return p.isLeader
	case Follower:
		return !metadata.IsLearner(p.Replica) && !p.isLeader
	case Learner:
		return metadata.IsLearner(p.Replica)
	}
	return false
}

func (p *fitPeer) matchRoleLoose(role ReplicaRoleType) bool {
	// non-learner cannot become learner. All other roles can migrate to
	// others by scheduling. For example, Leader->Follower, Learner->Leader
	// are possible, but Voter->Learner is impossible.
	return role != Learner || metadata.IsLearner(p.Replica)
}

func isolationScore(peers []*fitPeer, labels []string) float64 {
	var score float64
	if len(labels) == 0 || len(peers) <= 1 {
		return 0
	}
	// NOTE: following loop is partially duplicated with `core.DistinctScore`.
	// The reason not to call it directly is that core.DistinctScore only
	// accepts `[]CachedStore` not `[]*fitPeer` and I don't want alloc slice
	// here because it is kind of hot path.
	// After Go supports generics, we will be enable to do some refactor and
	// reuse `core.DistinctScore`.
	const replicaBaseScore = 100
	for i, p1 := range peers {
		for _, p2 := range peers[i+1:] {
			if index := p1.container.CompareLocation(p2.container, labels); index != -1 {
				score += math.Pow(replicaBaseScore, float64(len(labels)-index-1))
			}
		}
	}
	return score
}
