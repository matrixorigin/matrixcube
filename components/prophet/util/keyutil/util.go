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

package keyutil

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/fagongzi/util/format"
)

// BuildKeyRangeKey build key for a keyRange
func BuildKeyRangeKey(group uint64, startKey, endKey []byte) string {
	return fmt.Sprintf("%d-%s-%s", group, hex.EncodeToString(startKey), hex.EncodeToString(endKey))
}

// GetGroupFromRangeKey return group from range key
func GetGroupFromRangeKey(key string) uint64 {
	return format.MustParseStringUint64(strings.Split(key, "-")[0])
}
