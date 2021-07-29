// Copyright 2020 MatrixOrigin.
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

package prophet

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSingleTransport(t *testing.T) {
	p := newTestSingleProphet(t, nil)
	defer p.Stop()

	id, err := p.GetClient().AllocID()
	assert.NoError(t, err)
	assert.True(t, id > 0)
}

func TestClusterTransport(t *testing.T) {
	cluster := newTestClusterProphet(t, 3, nil)
	defer func() {
		for _, p := range cluster {
			p.Stop()
		}
	}()

	id, err := cluster[0].GetClient().AllocID()
	assert.NoError(t, err)
	assert.True(t, id > 0)

	id, err = cluster[1].GetClient().AllocID()
	assert.NoError(t, err)
	assert.True(t, id > 0)

	id, err = cluster[2].GetClient().AllocID()
	assert.NoError(t, err)
	assert.True(t, id > 0)
}
