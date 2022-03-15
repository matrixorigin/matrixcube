// Copyright 2020 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain alloc copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/id"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

const (
	aoeGroup     uint64 = 1
	kvGroup      uint64 = 2
	ruleName            = "RuleTable"
	labelTable          = "LabelTable"
	labelAnother        = "LabelAnother"
)

var ruleID uint64 = 100

func TestScheduleGroupRuleCache(t *testing.T) {
	ruleCache := NewScheduleGroupRuleCache()
	assert.Equal(t, 0, ruleCache.RuleCount())

	var rule metapb.ScheduleGroupRule
	var err error

	// 1. add a rule
	rule = makeScheduleGroupRule(aoeGroup, labelTable)
	shouldAdd, ruleID := ruleCache.Precheck(rule)
	assert.True(t, shouldAdd)
	assert.Equal(t, ruleID, id.UninitializedID)

	err = ruleCache.AddRule(rule)
	assert.NoError(t, err)
	assert.Equal(t, 1, ruleCache.RuleCount())

	// 2. add the same rule
	rule = makeScheduleGroupRule(aoeGroup, labelTable)
	shouldAdd, _ = ruleCache.Precheck(rule)
	assert.False(t, shouldAdd)

	err = ruleCache.AddRule(rule)
	assert.NoError(t, err)
	assert.Equal(t, 1, ruleCache.RuleCount())

	// 3. add the same rule but with different label
	rule = makeScheduleGroupRule(aoeGroup, labelAnother)
	shouldAdd, _ = ruleCache.Precheck(rule)
	assert.True(t, shouldAdd)

	err = ruleCache.AddRule(rule)
	assert.NoError(t, err)
	assert.Equal(t, 1, ruleCache.RuleCount())

	// 4. add another rule
	rule = makeScheduleGroupRule(kvGroup, labelTable)
	shouldAdd, _ = ruleCache.Precheck(rule)
	assert.True(t, shouldAdd)

	err = ruleCache.AddRule(rule)
	assert.NoError(t, err)
	assert.Equal(t, 2, ruleCache.RuleCount())

	// 5. get rules from cache
	rules, err := ruleCache.ListRules()
	assert.NoError(t, err)
	assert.Equal(t, len(rules), ruleCache.RuleCount())

	// Changing the result of ListRules shouldn't affect the cached rules
	rules = append(rules, metapb.ScheduleGroupRule{})
	assert.Equal(t, 2, ruleCache.RuleCount())

	// 6. clear cached rules
	ruleCache.Clear()
	assert.Equal(t, 0, ruleCache.RuleCount())
}

func TestScheduleGroupRuleCacheConcurrecy(t *testing.T) {
	var wg sync.WaitGroup
	ruleCache := NewScheduleGroupRuleCache()
	ruleChan := make(chan metapb.ScheduleGroupRule)
	totalWorker := 5
	totalRule := 100

	// 1. launch goroutine in order to add rule
	wg.Add(totalWorker)
	for i := 0; i < totalWorker; i++ {
		go func() {
			defer wg.Done()

			for {
				select {
				case rule, ok := <-ruleChan:
					if !ok {
						return
					}
					err := ruleCache.AddRule(rule)
					assert.NoError(t, err)
				}
			}
		}()
	}

	// 2. produce duplicated rules
	for id := 1; id <= totalRule; id++ {
		ruleChan <- makeScheduleGroupRule(uint64(id), labelTable)
		ruleChan <- makeScheduleGroupRule(uint64(id), labelTable)
		ruleChan <- makeScheduleGroupRule(uint64(id), labelTable)
	}
	close(ruleChan)

	wg.Wait()

	// 3. assert
	assert.Equal(t, ruleCache.RuleCount(), totalRule)
}

func makeScheduleGroupRule(groupID uint64, labelName string) metapb.ScheduleGroupRule {
	defer func() {
		atomic.AddUint64(&ruleID, 1)
	}()

	return metapb.ScheduleGroupRule{
		ID:           ruleID,
		GroupID:      groupID,
		Name:         ruleName,
		GroupByLabel: labelName,
	}
}
