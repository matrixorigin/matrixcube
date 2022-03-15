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

	"github.com/matrixorigin/matrixcube/components/prophet/id"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

const (
	InvalidSliceIndex int = -1
)

// ScheduleGroupRuleCache defines common behavior for cached scheduling rules.
// NB: implementation should be concurrent-safe
type ScheduleGroupRuleCache interface {
	// RuleCount get the number of cached rules.
	RuleCount() int

	// Precheck checks whether a rule is fresh or not.
	// A rule should be added in the following conditions:
	//	1. for a non-exit rule;
	//  2. rule exist but with a different `GroupByLabel`;
	Precheck(rule metapb.ScheduleGroupRule) (shouldAdd bool, ruleID uint64)

	// AddRule would add a rule into the cache.
	// NB: Precheck and AddRule should be aotmic if in joint use.
	AddRule(rule metapb.ScheduleGroupRule) error

	// ListRules would list all cached rules via copy semantics.
	ListRules() ([]metapb.ScheduleGroupRule, error)

	// Clear would clear all cached rules in order to release resource.
	Clear()
}

// scheduleGroupRuleCache cache all scheduling ruels in a slice.
type scheduleGroupRuleCache struct {
	sync.RWMutex
	rules []metapb.ScheduleGroupRule
}

// NewScheduleGroupRules construcs a cache for schedule group rules.
func NewScheduleGroupRuleCache() ScheduleGroupRuleCache {
	return &scheduleGroupRuleCache{}
}

// RuleCount get the number of cached rules.
func (cache *scheduleGroupRuleCache) RuleCount() int {
	cache.RLock()
	defer cache.RUnlock()

	return len(cache.rules)
}

// Precheck checks whether a rule is fresh or not.
// A rule should be added in the following conditions:
//	1. for a non-exit rule;
//  2. rule exist but with a different `GroupByLabel`;
func (cache *scheduleGroupRuleCache) Precheck(rule metapb.ScheduleGroupRule) (bool, uint64) {
	cache.RLock()
	defer cache.RUnlock()

	ruleID := id.UninitializedID
	shouldAdd, index := cache.precheckLocked(rule)
	if index != InvalidSliceIndex {
		ruleID = cache.rules[index].ID
	}
	return shouldAdd, ruleID
}

// AddRule would add a rule into the cache.
func (cache *scheduleGroupRuleCache) AddRule(rule metapb.ScheduleGroupRule) error {
	cache.Lock()
	defer cache.Unlock()

	shouldAdd, index := cache.precheckLocked(rule)
	if !shouldAdd {
		return nil
	}

	if index == InvalidSliceIndex {
		cache.rules = append(cache.rules, rule)
	} else {
		cache.rules[index] = rule
	}
	return nil
}

// ListRules would list all cached rules via copy semantics.
func (cache *scheduleGroupRuleCache) ListRules() ([]metapb.ScheduleGroupRule, error) {
	cache.RLock()
	defer cache.RUnlock()

	if cache.rules == nil {
		return nil, nil
	}

	// make a copy of the cached rules
	rulesCopy := make([]metapb.ScheduleGroupRule, len(cache.rules))
	for i, r := range cache.rules {
		rulesCopy[i] = r
	}

	return rulesCopy, nil
}

// Clear would clear all cached rules in order to release resource.
func (cache *scheduleGroupRuleCache) Clear() {
	cache.Lock()
	defer cache.Unlock()

	cache.rules = nil
}

// precheckLocked checks whether the rule should be added or not.
// The returned int value is the slot if the rule exist.
func (cache *scheduleGroupRuleCache) precheckLocked(rule metapb.ScheduleGroupRule) (bool, int) {
	isAnother, toUpdate := true, true
	ruleIndex := InvalidSliceIndex

	for i, cached := range cache.rules {
		if withIdenticalFingerprint(cached, rule) {
			isAnother = false
			ruleIndex = i
			if cached.GroupByLabel == rule.GroupByLabel {
				toUpdate = false
				break
			}
		}
	}

	shouldAdd := isAnother || toUpdate
	return shouldAdd, ruleIndex
}

// withIdenticalFingerprint return true if two rules have the same fingerprint.
func withIdenticalFingerprint(left, right metapb.ScheduleGroupRule) bool {
	return left.GroupID == right.GroupID && left.Name == right.Name
}
