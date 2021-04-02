package placement

import (
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/deepfabric/prophet/pb/metapb"
)

// PeerRoleType is the expected peer type of the placement rule.
type PeerRoleType string

const (
	// Voter can either match a leader peer or follower peer
	Voter PeerRoleType = "voter"
	// Leader matches a leader.
	Leader PeerRoleType = "leader"
	// Follower matches a follower.
	Follower PeerRoleType = "follower"
	// Learner matches a learner.
	Learner PeerRoleType = "learner"
)

func validateRole(s PeerRoleType) bool {
	return s == Voter || s == Leader || s == Follower || s == Learner
}

// MetaPeerRole converts placement.PeerRoleType to metapb.PeerRole.
func (s PeerRoleType) MetaPeerRole() metapb.PeerRole {
	if s == Learner {
		return metapb.PeerRole_Learner
	}
	return metapb.PeerRole_Voter
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
	Role             PeerRoleType      `json:"role"`                        // expected role of the peers
	Count            int               `json:"count"`                       // expected count of the peers
	LabelConstraints []LabelConstraint `json:"label_constraints,omitempty"` // used to select containers to place peers
	LocationLabels   []string          `json:"location_labels,omitempty"`   // used to make peers isolated physically
	IsolationLevel   string            `json:"isolation_level,omitempty"`   // used to isolate replicas explicitly and forcibly

	group *RuleGroup // only set at runtime, no need to {,un}marshal or persist.
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
