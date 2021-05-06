package placement

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTrim(t *testing.T) {
	rc := newRuleConfig()
	rc.setRule(&Rule{GroupID: "g1", ID: "id1"})
	rc.setRule(&Rule{GroupID: "g1", ID: "id2"})
	rc.setRule(&Rule{GroupID: "g2", ID: "id3"})
	rc.setGroup(&RuleGroup{ID: "g1", Index: 1})
	rc.setGroup(&RuleGroup{ID: "g2", Index: 2})

	testCases := []struct {
		ops       func(p *ruleConfigPatch)
		mutRules  map[[2]string]*Rule
		mutGroups map[string]*RuleGroup
	}{
		{
			func(p *ruleConfigPatch) {
				p.setRule(&Rule{GroupID: "g1", ID: "id1", Index: 100})
				p.setRule(&Rule{GroupID: "g1", ID: "id2"})
				p.setGroup(&RuleGroup{ID: "g1", Index: 100})
				p.setGroup(&RuleGroup{ID: "g2", Index: 2})
			},
			map[[2]string]*Rule{{"g1", "id1"}: {GroupID: "g1", ID: "id1", Index: 100}},
			map[string]*RuleGroup{"g1": {ID: "g1", Index: 100}},
		},
		{
			func(p *ruleConfigPatch) {
				p.deleteRule("g1", "id1")
				p.deleteGroup("g2")
				p.deleteRule("g3", "id3")
				p.deleteGroup("g3")
			},
			map[[2]string]*Rule{{"g1", "id1"}: nil},
			map[string]*RuleGroup{"g2": {ID: "g2"}},
		},
		{
			func(p *ruleConfigPatch) {
				p.setRule(&Rule{GroupID: "g1", ID: "id2", Index: 200})
				p.setRule(&Rule{GroupID: "g1", ID: "id2"})
				p.setRule(&Rule{GroupID: "g3", ID: "id3"})
				p.deleteRule("g3", "id3")
				p.setGroup(&RuleGroup{ID: "g1", Index: 100})
				p.setGroup(&RuleGroup{ID: "g1", Index: 1})
				p.setGroup(&RuleGroup{ID: "g3", Index: 3})
				p.deleteGroup("g3")
			},
			map[[2]string]*Rule{},
			map[string]*RuleGroup{},
		},
	}

	for _, tc := range testCases {
		p := rc.beginPatch()
		tc.ops(p)
		p.trim()
		assert.True(t, reflect.DeepEqual(tc.mutRules, p.mut.rules))
		assert.True(t, reflect.DeepEqual(tc.mutGroups, p.mut.groups))
	}
}
