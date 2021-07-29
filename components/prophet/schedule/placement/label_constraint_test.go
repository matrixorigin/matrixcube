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
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/stretchr/testify/assert"
)

func TestLabelConstraint(t *testing.T) {
	containers := []map[string]string{
		{"zone": "zone1", "rack": "rack1"},                    // 1
		{"zone": "zone1", "rack": "rack2"},                    // 2
		{"zone": "zone2"},                                     // 3
		{"zone": "zone1", "engine": "rocksdb", "disk": "hdd"}, // 4
		{"rack": "rack1", "disk": "ssd"},                      // 5
		{"zone": "zone3"},                                     // 6
	}
	constraints := []LabelConstraint{
		{Key: "zone", Op: "in", Values: []string{"zone1"}},
		{Key: "zone", Op: "in", Values: []string{"zone1", "zone2"}},
		{Key: "zone", Op: "notIn", Values: []string{"zone1"}},
		{Key: "zone", Op: "notIn", Values: []string{"zone1", "zone2"}},
		{Key: "zone", Op: "exists"},
		{Key: "disk", Op: "notExists"},
	}
	expect := [][]int{
		{1, 2, 4},
		{1, 2, 3, 4},
		{3, 5, 6},
		{5, 6},
		{1, 2, 3, 4, 6},
		{1, 2, 3, 6},
	}
	for i, constraint := range constraints {
		var matched []int
		for j, container := range containers {
			if constraint.MatchContainer(core.NewTestContainerInfoWithLabel(uint64(j), 0, container)) {
				matched = append(matched, j+1)
			}
		}
		assert.True(t, reflect.DeepEqual(matched, expect[i]))
	}
}

func TestLabelConstraints(t *testing.T) {
	containers := []map[string]string{
		{},                                       // 1
		{"k1": "v1"},                             // 2
		{"k1": "v2"},                             // 3
		{"k2": "v1"},                             // 4
		{"k2": "v2"},                             // 5
		{"engine": "e1"},                         // 6
		{"engine": "e2"},                         // 7
		{"k1": "v1", "k2": "v1"},                 // 8
		{"k2": "v2", "engine": "e1"},             // 9
		{"k1": "v1", "k2": "v2", "engine": "e2"}, // 10
		{"$foo": "bar"},                          // 11
	}
	constraints := [][]LabelConstraint{
		{},
		{{Key: "engine", Op: "in", Values: []string{"e1", "e2"}}},
		{{Key: "k1", Op: "notExists"}, {Key: "k2", Op: "exists"}},
		{{Key: "engine", Op: "exists"}, {Key: "k1", Op: "in", Values: []string{"v1", "v2"}}, {Key: "k2", Op: "notIn", Values: []string{"v3"}}},
		{{Key: "$foo", Op: "in", Values: []string{"bar", "baz"}}},
	}
	expect := [][]int{
		{1, 2, 3, 4, 5, 8},
		{6, 7, 9, 10},
		{4, 5},
		{10},
		{11},
	}
	for i, cs := range constraints {
		var matched []int
		for j, container := range containers {
			if MatchLabelConstraints(core.NewTestContainerInfoWithLabel(uint64(j), 0, container), cs) {
				matched = append(matched, j+1)
			}
		}
		assert.True(t, reflect.DeepEqual(matched, expect[i]))
	}
}
