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

package operator

import (
	"fmt"
	"strings"
)

// OpKind is a bit field to identify operator types.
type OpKind uint32

// Flags for operators.
const (
	// Include leader transfer.
	OpLeader OpKind = 1 << iota
	// Include peer addition or removal. This means that this operator may take a long time.
	OpResource
	// Include resource split. Initiated by rule checker if `kind & OpAdmin == 0`.
	OpSplit
	// Initiated by admin.
	OpAdmin
	// Initiated by hot resource scheduler.
	OpHotResource
	// Initiated by replica checker.
	OpReplica
	// Initiated by merge checker or merge scheduler. Note that it may not include resource merge.
	OpMerge
	// Initiated by range scheduler.
	OpRange
	opMax
)

var flagToName = map[OpKind]string{
	OpLeader:      "leader",
	OpResource:    "resource",
	OpSplit:       "split",
	OpAdmin:       "admin",
	OpHotResource: "hot-resource",
	OpReplica:     "replica",
	OpMerge:       "merge",
	OpRange:       "range",
}

var nameToFlag = map[string]OpKind{
	"leader":       OpLeader,
	"resource":     OpResource,
	"split":        OpSplit,
	"admin":        OpAdmin,
	"hot-resource": OpHotResource,
	"replica":      OpReplica,
	"merge":        OpMerge,
	"range":        OpRange,
}

func (k OpKind) String() string {
	var flagNames []string
	for flag := OpKind(1); flag < opMax; flag <<= 1 {
		if k&flag != 0 {
			flagNames = append(flagNames, flagToName[flag])
		}
	}
	if len(flagNames) == 0 {
		return "unknown"
	}
	return strings.Join(flagNames, ",")
}

// ParseOperatorKind converts string (flag name list concat by ',') to OpKind.
func ParseOperatorKind(str string) (OpKind, error) {
	var k OpKind
	for _, flagName := range strings.Split(str, ",") {
		flag, ok := nameToFlag[flagName]
		if !ok {
			return 0, fmt.Errorf("unknown flag name: %s", flagName)
		}
		k |= flag
	}
	return k, nil
}
