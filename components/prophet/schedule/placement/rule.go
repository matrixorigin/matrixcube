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
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
)

// ReplicaRoleType is the expected peer type of the placement rule.
type ReplicaRoleType string

const (
	// Voter can either match a leader peer or follower peer
	Voter ReplicaRoleType = "voter"
	// Leader matches a leader.
	Leader ReplicaRoleType = "leader"
	// Follower matches a follower.
	Follower ReplicaRoleType = "follower"
	// Learner matches a learner.
	Learner ReplicaRoleType = "learner"
)

func getReplicaRoleTypeFromRPC(tpe rpcpb.ReplicaRoleType) ReplicaRoleType {
	switch tpe {
	case rpcpb.Voter:
		return Voter
	case rpcpb.Leader:
		return Leader
	case rpcpb.Follower:
		return Follower
	case rpcpb.Learner:
		return Learner
	}
	return Voter
}

func validateRole(s ReplicaRoleType) bool {
	return s == Voter || s == Leader || s == Follower || s == Learner
}

// MetaPeerRole converts placement.ReplicaRoleType to metapb.PeerRole.
func (s ReplicaRoleType) MetaPeerRole() metapb.ReplicaRole {
	if s == Learner {
		return metapb.ReplicaRole_Learner
	}
	return metapb.ReplicaRole_Voter
}

// RPCPeerRole converts placement.ReplicaRoleType to rpcpb.ReplicaRoleType.
func (s ReplicaRoleType) RPCPeerRole() rpcpb.ReplicaRoleType {
	switch s {
	case Voter:
		return rpcpb.Voter
	case Leader:
		return rpcpb.Leader
	case Follower:
		return rpcpb.Follower
	case Learner:
		return rpcpb.Learner
	}
	return rpcpb.Voter
}

// Rule is the placement rule that can be checked against a resource. When
// applying rules (apply means schedule resources to match selected rules), the
// apply order is defined by the tuple [GroupIndex, GroupID, Index, ID].
type Rule struct {
	GroupID          string            `json:"group_id"`                    // mark the source that add the rule
	ID               string            `json:"id"`                          // unique ID within a group
	Index            int               `json:"index,omitempty"`             // rule apply order in a group, rule with less ID is applied first when indexes are equal
	Override         bool              `json:"override,omitempty"`          // when it is true, all rules with less indexes are disabled
	StartKey         []byte            `json:"-"`                           // range start key
	StartKeyHex      string            `json:"start_key"`                   // hex format start key, for marshal/unmarshal
	EndKey           []byte            `json:"-"`                           // range end key
	EndKeyHex        string            `json:"end_key"`                     // hex format end key, for marshal/unmarshal
	Role             ReplicaRoleType   `json:"role"`                        // expected role of the peers
	Count            int               `json:"count"`                       // expected count of the peers
	LabelConstraints []LabelConstraint `json:"label_constraints,omitempty"` // used to select containers to place peers
	LocationLabels   []string          `json:"location_labels,omitempty"`   // used to make peers isolated physically
	IsolationLevel   string            `json:"isolation_level,omitempty"`   // used to isolate replicas explicitly and forcibly

	group *RuleGroup // only set at runtime, no need to {,un}marshal or persist.
}

// RPCRules convert rules to rpc placement rules
func RPCRules(rules []*Rule) []rpcpb.PlacementRule {
	var values []rpcpb.PlacementRule
	for _, rule := range rules {
		values = append(values, rpcpb.PlacementRule{
			GroupID:          rule.GroupID,
			ID:               rule.ID,
			Index:            uint32(rule.Index),
			Override:         rule.Override,
			StartKey:         rule.StartKey,
			EndKey:           rule.EndKey,
			Role:             rule.Role.RPCPeerRole(),
			Count:            uint32(rule.Count),
			LabelConstraints: toRPCLabelConstraints(rule.LabelConstraints),
			LocationLabels:   rule.LocationLabels,
			IsolationLevel:   rule.IsolationLevel,
		})
	}
	return values
}

// NewRuleFromRPC creates the rule from rpc
func NewRuleFromRPC(rule rpcpb.PlacementRule) *Rule {
	return &Rule{
		GroupID:          rule.GroupID,
		ID:               rule.ID,
		Index:            int(rule.Index),
		Override:         rule.Override,
		StartKeyHex:      hex.EncodeToString(rule.StartKey),
		EndKeyHex:        hex.EncodeToString(rule.EndKey),
		Role:             getReplicaRoleTypeFromRPC(rule.Role),
		Count:            int(rule.Count),
		LabelConstraints: newLabelConstraintsFromRPC(rule.LabelConstraints),
		LocationLabels:   rule.LocationLabels,
		IsolationLevel:   rule.IsolationLevel,
	}
}

func (r *Rule) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

// Key returns (groupID, ID) as the global unique key of a rule.
func (r *Rule) Key() [2]string {
	return [2]string{r.GroupID, r.ID}
}

// StoreKey returns the rule's key for persistent container.
func (r *Rule) StoreKey() string {
	return hex.EncodeToString([]byte(r.GroupID)) + "-" + hex.EncodeToString([]byte(r.ID))
}

func (r *Rule) groupIndex() int {
	if r.group != nil {
		return r.group.Index
	}
	return 0
}

// RuleGroup defines properties of a rule group.
type RuleGroup struct {
	ID       string `json:"id,omitempty"`
	Index    int    `json:"index,omitempty"`
	Override bool   `json:"override,omitempty"`
}

func (g *RuleGroup) isDefault() bool {
	return g.Index == 0 && !g.Override
}

func (g *RuleGroup) String() string {
	b, _ := json.Marshal(g)
	return string(b)
}

// Rules are ordered by (GroupID, Index, ID).
func compareRule(a, b *Rule) int {
	switch {
	case a.groupIndex() < b.groupIndex():
		return -1
	case a.groupIndex() > b.groupIndex():
		return 1
	case a.GroupID < b.GroupID:
		return -1
	case a.GroupID > b.GroupID:
		return 1
	case a.Index < b.Index:
		return -1
	case a.Index > b.Index:
		return 1
	case a.ID < b.ID:
		return -1
	case a.ID > b.ID:
		return 1
	default:
		return 0
	}
}

func sortRules(rules []*Rule) {
	sort.Slice(rules, func(i, j int) bool { return compareRule(rules[i], rules[j]) < 0 })
}

// prepareRulesForApply search the target rules from the given rules.
// it will filter the rules depends on the interval index override in the same group or the
// group-index override between different groups
// For example, given rules:
// ruleA: group_id: 4, id: 2, override: true
// ruleB: group_id: 4, id: 1, override: true
// ruleC: group_id: 3
// ruleD: group_id: 2
// RuleGroupA: id:4, override: false
// RuleGroupB: id:3, override: true
// RuleGroupC: id:2, override: false
// Finally only ruleA and ruleC will be selected.
func prepareRulesForApply(rules []*Rule) []*Rule {
	var res []*Rule
	var i, j int
	for i = 1; i < len(rules); i++ {
		if rules[j].GroupID != rules[i].GroupID {
			if rules[i].group != nil && rules[i].group.Override {
				res = res[:0] // override all previous groups
			} else {
				res = append(res, rules[j:i]...) // save rules belong to previous group
			}
			j = i
		}
		if rules[i].Override {
			j = i // skip all previous rules in the same group
		}
	}
	return append(res, rules[j:]...)
}

// GroupBundle represents a rule group and all rules belong to the group.
type GroupBundle struct {
	ID       string  `json:"group_id"`
	Index    int     `json:"group_index"`
	Override bool    `json:"group_override"`
	Rules    []*Rule `json:"rules"`
}

func (g GroupBundle) String() string {
	b, _ := json.Marshal(g)
	return string(b)
}
