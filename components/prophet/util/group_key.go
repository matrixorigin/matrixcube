// Copyright 2021 MatrixOrigin.
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

package util

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

// EncodeGroupKey encode group key
func EncodeGroupKey(group uint64, rules []metapb.ScheduleGroupRule, labels []metapb.Label) string {
	var buf bytes.Buffer
	buf.Write(format.Uint64ToBytes(group))
	if len(rules) == 0 {
		return buf.String()
	}

	sort.Slice(rules, func(i, j int) bool {
		return rules[i].ID < rules[j].ID
	})
	labelMap := make(map[string]string)
	for _, label := range labels {
		labelMap[label.Key] = label.Value
	}

	for _, r := range rules {
		buf.WriteByte(0)
		if v, ok := labelMap[r.GroupByLabel]; ok {
			buf.WriteString(v)
		}
	}
	return buf.String()
}

// DecodeGroupKey decode group key, returns group id
func DecodeGroupKey(key string) uint64 {
	if key == "" {
		return 0
	}

	v := []byte(key)
	if len(v) < 8 {
		panic(fmt.Sprintf("invalid group key: %+v", v))
	}

	return format.MustBytesToUint64(v[:8])
}
