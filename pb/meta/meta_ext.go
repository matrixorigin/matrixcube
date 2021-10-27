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

package meta

import (
	"github.com/fagongzi/util/protoc"
)

// Clone clone the shard
func (m Shard) Clone() Shard {
	var value Shard
	protoc.MustUnmarshal(&value, protoc.MustMarshal(&m))
	return value
}
