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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsEndStatus(t *testing.T) {
	for st := OpStatus(0); st < firstEndStatus; st++ {
		assert.False(t, IsEndStatus(st))
	}
	for st := firstEndStatus; st < statusCount; st++ {
		assert.True(t, IsEndStatus(st))
	}
	for st := statusCount; st < statusCount+100; st++ {
		assert.False(t, IsEndStatus(st))
	}
}
