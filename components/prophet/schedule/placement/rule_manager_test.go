package placement

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/stretchr/testify/assert"
)

type testManager struct {
	storage storage.Storage
	manager *RuleManager
}

func (s *testManager) setup(t *testing.T) {
	s.storage = storage.NewTestStorage()
	var err error
	s.manager = NewRuleManager(s.storage)
	err = s.manager.Initialize(3, []string{"zone", "rack", "host"})
	assert.NoError(t, err)
}

func (s *testManager) dhex(hk string) []byte {
	k, err := hex.DecodeString(hk)
	if err != nil {
		panic("decode fail")
	}
	return k
}

func TestDefault(t *testing.T) {
	s := &testManager{}
	s.setup(t)

	rules := s.manager.GetAllRules()
	assert.Equal(t, 1, len(rules))
	assert.Equal(t, "prophet", rules[0].GroupID)
	assert.Equal(t, "default", rules[0].ID)
	assert.Equal(t, 0, rules[0].Index)
	assert.Empty(t, rules[0].StartKey)
	assert.Empty(t, rules[0].EndKey)
	assert.Equal(t, Voter, rules[0].Role)
	assert.True(t, reflect.DeepEqual([]string{"zone", "rack", "host"}, rules[0].LocationLabels))
}

func TestAdjustRule(t *testing.T) {
	s := &testManager{}
	s.setup(t)

	rules := []Rule{
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: "voter", Count: 3},
		{GroupID: "", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: "voter", Count: 3},
		{GroupID: "group", ID: "", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: "voter", Count: 3},
		{GroupID: "group", ID: "id", StartKeyHex: "123ab", EndKeyHex: "123abf", Role: "voter", Count: 3},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "1123abf", Role: "voter", Count: 3},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123aaa", Role: "voter", Count: 3},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: "master", Count: 3},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: "voter", Count: 0},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: "voter", Count: -1},
		{GroupID: "group", ID: "id", StartKeyHex: "123abc", EndKeyHex: "123abf", Role: "voter", Count: 3, LabelConstraints: []LabelConstraint{{Op: "foo"}}},
	}
	assert.Nil(t, s.manager.adjustRule(&rules[0]))
	assert.True(t, reflect.DeepEqual([]byte{0x12, 0x3a, 0xbc}, rules[0].StartKey))
	assert.True(t, reflect.DeepEqual([]byte{0x12, 0x3a, 0xbf}, rules[0].EndKey))
	for i := 1; i < len(rules); i++ {
		assert.NotNil(t, s.manager.adjustRule(&rules[i]))
	}
}

func TestLeaderCheck(t *testing.T) {
	s := &testManager{}
	s.setup(t)

	assert.Regexp(t, "^.*needs at least one leader or voter.*$", s.manager.SetRule(&Rule{GroupID: "prophet", ID: "default", Role: "learner", Count: 3}).Error())
	assert.Regexp(t, "^.*define multiple leaders by count 2.*$", s.manager.SetRule(&Rule{GroupID: "g2", ID: "33", Role: "leader", Count: 2}).Error())
	assert.Regexp(t, "^.*multiple leader replicas.*$", s.manager.Batch([]RuleOp{
		{
			Rule:   &Rule{GroupID: "g2", ID: "foo1", Role: "leader", Count: 1},
			Action: RuleOpAdd,
		},
		{
			Rule:   &Rule{GroupID: "g2", ID: "foo2", Role: "leader", Count: 1},
			Action: RuleOpAdd,
		},
	}).Error())
}

func TestSaveLoad(t *testing.T) {
	s := &testManager{}
	s.setup(t)

	rules := []*Rule{
		{GroupID: "prophet", ID: "default", Role: "voter", Count: 5},
		{GroupID: "foo", ID: "baz", StartKeyHex: "", EndKeyHex: "abcd", Role: "voter", Count: 1},
		{GroupID: "foo", ID: "bar", Role: "learner", Count: 1},
	}
	for _, r := range rules {
		assert.Nil(t, s.manager.SetRule(r))
	}

	m2 := NewRuleManager(s.storage)
	err := m2.Initialize(3, []string{"no", "labels"})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(m2.GetAllRules()))
	assert.True(t, reflect.DeepEqual(rules[0], m2.GetRule("prophet", "default")))
	assert.True(t, reflect.DeepEqual(rules[1], m2.GetRule("foo", "baz")))
	assert.True(t, reflect.DeepEqual(rules[2], m2.GetRule("foo", "bar")))
}

func TestKeys(t *testing.T) {
	s := &testManager{}
	s.setup(t)

	rules := []*Rule{
		{GroupID: "1", ID: "1", Role: "voter", Count: 1, StartKeyHex: "", EndKeyHex: ""},
		{GroupID: "2", ID: "2", Role: "voter", Count: 1, StartKeyHex: "11", EndKeyHex: "ff"},
		{GroupID: "2", ID: "3", Role: "voter", Count: 1, StartKeyHex: "22", EndKeyHex: "dd"},
	}

	toDelete := []RuleOp{}
	for _, r := range rules {
		s.manager.SetRule(r)
		toDelete = append(toDelete, RuleOp{
			Rule:             r,
			Action:           RuleOpDel,
			DeleteByIDPrefix: false,
		})
	}
	checkRules(t, s.manager.GetAllRules(), [][2]string{{"1", "1"}, {"2", "2"}, {"2", "3"}, {"prophet", "default"}})
	s.manager.Batch(toDelete)
	checkRules(t, s.manager.GetAllRules(), [][2]string{{"prophet", "default"}})

	rules = append(rules, &Rule{GroupID: "3", ID: "4", Role: "voter", Count: 1, StartKeyHex: "44", EndKeyHex: "ee"},
		&Rule{GroupID: "3", ID: "5", Role: "voter", Count: 1, StartKeyHex: "44", EndKeyHex: "dd"})
	s.manager.SetRules(rules)
	checkRules(t, s.manager.GetAllRules(), [][2]string{{"1", "1"}, {"2", "2"}, {"2", "3"}, {"3", "4"}, {"3", "5"}, {"prophet", "default"}})

	s.manager.DeleteRule("prophet", "default")
	checkRules(t, s.manager.GetAllRules(), [][2]string{{"1", "1"}, {"2", "2"}, {"2", "3"}, {"3", "4"}, {"3", "5"}})

	splitKeys := [][]string{
		{"", "", "11", "22", "44", "dd", "ee", "ff"},
		{"44", "", "dd", "ee", "ff"},
		{"44", "dd"},
		{"22", "ef", "44", "dd", "ee"},
	}
	for _, keys := range splitKeys {
		splits := s.manager.GetSplitKeys(s.dhex(keys[0]), s.dhex(keys[1]))
		assert.Equal(t, len(keys)-2, len(splits))
		for i := range splits {
			assert.True(t, reflect.DeepEqual(s.dhex(keys[i+2]), splits[i]))
		}
	}

	resourceKeys := [][][2]string{
		{{"", ""}},
		{{"aa", "bb"}, {"", ""}, {"11", "ff"}, {"22", "dd"}, {"44", "ee"}, {"44", "dd"}},
		{{"11", "22"}, {"", ""}, {"11", "ff"}},
		{{"11", "33"}},
	}
	for _, keys := range resourceKeys {
		resource := core.NewCachedResource(&metadata.TestResource{Start: s.dhex(keys[0][0]), End: s.dhex(keys[0][1])}, nil)
		rules := s.manager.GetRulesForApplyResource(resource)
		assert.Equal(t, len(keys)-1, len(rules))
		for i := range rules {
			assert.Equal(t, keys[i+1][0], rules[i].StartKeyHex)
			assert.Equal(t, keys[i+1][1], rules[i].EndKeyHex)
		}
	}

	ruleByKeys := [][]string{ // first is query key, rests are rule keys.
		{"", "", ""},
		{"11", "", "", "11", "ff"},
		{"33", "", "", "11", "ff", "22", "dd"},
	}
	for _, keys := range ruleByKeys {
		rules := s.manager.GetRulesByKey(s.dhex(keys[0]))
		assert.Equal(t, (len(keys)-1)/2, len(rules))
		for i := range rules {
			assert.Equal(t, keys[i*2+1], rules[i].StartKeyHex)
			assert.Equal(t, keys[i*2+2], rules[i].EndKeyHex)
		}
	}

	rulesByGroup := [][]string{ // first is group, rests are rule keys.
		{"1", "", ""},
		{"2", "11", "ff", "22", "dd"},
		{"3", "44", "ee", "44", "dd"},
		{"4"},
	}
	for _, keys := range rulesByGroup {
		rules := s.manager.GetRulesByGroup(keys[0])
		assert.Equal(t, (len(keys)-1)/2, len(rules))
		for i := range rules {
			assert.Equal(t, keys[i*2+1], rules[i].StartKeyHex)
			assert.Equal(t, keys[i*2+2], rules[i].EndKeyHex)
		}
	}
}

func TestDeleteByIDPrefix(t *testing.T) {
	s := &testManager{}
	s.setup(t)

	s.manager.SetRules([]*Rule{
		{GroupID: "g1", ID: "foo1", Role: "voter", Count: 1},
		{GroupID: "g2", ID: "foo1", Role: "voter", Count: 1},
		{GroupID: "g2", ID: "foobar", Role: "voter", Count: 1},
		{GroupID: "g2", ID: "baz2", Role: "voter", Count: 1},
	})
	s.manager.DeleteRule("prophet", "default")
	checkRules(t, s.manager.GetAllRules(), [][2]string{{"g1", "foo1"}, {"g2", "baz2"}, {"g2", "foo1"}, {"g2", "foobar"}})

	s.manager.Batch([]RuleOp{{
		Rule:             &Rule{GroupID: "g2", ID: "foo"},
		Action:           RuleOpDel,
		DeleteByIDPrefix: true,
	}})
	checkRules(t, s.manager.GetAllRules(), [][2]string{{"g1", "foo1"}, {"g2", "baz2"}})
}

func TestRangeGap(t *testing.T) {
	s := &testManager{}
	s.setup(t)

	// |--  default  --|
	// cannot delete the last rule
	err := s.manager.DeleteRule("prophet", "default")
	assert.Error(t, err)

	err = s.manager.SetRule(&Rule{GroupID: "prophet", ID: "foo", StartKeyHex: "", EndKeyHex: "abcd", Role: "voter", Count: 1})
	assert.NoError(t, err)

	// |-- default --|
	// |-- foo --|
	// still cannot delete default since it will cause ("abcd", "") has no rules inside.
	err = s.manager.DeleteRule("prophet", "default")
	assert.Error(t, err)
	err = s.manager.SetRule(&Rule{GroupID: "prophet", ID: "bar", StartKeyHex: "abcd", EndKeyHex: "", Role: "voter", Count: 1})
	assert.NoError(t, err)
	// now default can be deleted.
	err = s.manager.DeleteRule("prophet", "default")
	assert.NoError(t, err)
	// cannot change range since it will cause ("abaa", "abcd") has no rules inside.
	err = s.manager.SetRule(&Rule{GroupID: "prophet", ID: "foo", StartKeyHex: "", EndKeyHex: "abaa", Role: "voter", Count: 1})
	assert.Error(t, err)
}

func TestGroupConfig(t *testing.T) {
	s := &testManager{}
	s.setup(t)

	// group prophet
	pd1 := &RuleGroup{ID: "prophet"}
	assert.True(t, reflect.DeepEqual(pd1, s.manager.GetRuleGroup("prophet")))

	// update group pd
	pd2 := &RuleGroup{ID: "prophet", Index: 100, Override: true}
	err := s.manager.SetRuleGroup(pd2)
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(pd2, s.manager.GetRuleGroup("prophet")))

	// new group g without config
	err = s.manager.SetRule(&Rule{GroupID: "g", ID: "1", Role: "voter", Count: 1})
	assert.NoError(t, err)
	g1 := &RuleGroup{ID: "g"}
	assert.True(t, reflect.DeepEqual(g1, s.manager.GetRuleGroup("g")))
	assert.True(t, reflect.DeepEqual([]*RuleGroup{g1, pd2}, s.manager.GetRuleGroups()))

	// update group g
	g2 := &RuleGroup{ID: "g", Index: 2, Override: true}
	err = s.manager.SetRuleGroup(g2)
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual([]*RuleGroup{g2, pd2}, s.manager.GetRuleGroups()))

	// delete pd group, restore to default config
	err = s.manager.DeleteRuleGroup("prophet")
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual([]*RuleGroup{pd1, g2}, s.manager.GetRuleGroups()))

	// delete rule, the group is removed too
	err = s.manager.DeleteRule("prophet", "default")
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual([]*RuleGroup{g2}, s.manager.GetRuleGroups()))
}

func TestCheckApplyRules(t *testing.T) {
	s := &testManager{}
	s.setup(t)

	err := checkApplyRules([]*Rule{
		{
			Role:  Leader,
			Count: 1,
		},
	})
	assert.NoError(t, err)

	err = checkApplyRules([]*Rule{
		{
			Role:  Voter,
			Count: 1,
		},
	})
	assert.NoError(t, err)

	err = checkApplyRules([]*Rule{
		{
			Role:  Leader,
			Count: 1,
		},
		{
			Role:  Voter,
			Count: 1,
		},
	})
	assert.NoError(t, err)

	err = checkApplyRules([]*Rule{
		{
			Role:  Leader,
			Count: 3,
		},
	})
	assert.Regexp(t, "multiple leader replicas", err.Error())

	err = checkApplyRules([]*Rule{
		{
			Role:  Leader,
			Count: 1,
		},
		{
			Role:  Leader,
			Count: 1,
		},
	})
	assert.Regexp(t, "multiple leader replicas", err.Error())

	err = checkApplyRules([]*Rule{
		{
			Role:  Learner,
			Count: 1,
		},
		{
			Role:  Follower,
			Count: 1,
		},
	})
	assert.Regexp(t, "needs at least one leader or voter", err.Error())
}

func checkRules(t *testing.T, rules []*Rule, expect [][2]string) {
	assert.Equal(t, len(expect), len(rules))
	for i := range rules {
		assert.True(t, reflect.DeepEqual(expect[i], rules[i].Key()))
	}
}
