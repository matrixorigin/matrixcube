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

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/stretchr/testify/assert"
)

type testStoreHeartbeatDataProcessor struct {
	started bool
}

func (p *testStoreHeartbeatDataProcessor) Start(storage.Storage) error {
	p.started = true
	return nil
}
func (p *testStoreHeartbeatDataProcessor) Stop(storage.Storage) error {
	p.started = false
	return nil
}

func (p *testStoreHeartbeatDataProcessor) HandleHeartbeatReq(id uint64, data []byte, store storage.Storage) ([]byte, error) {
	return nil, nil
}

func TestCustomStartAndStop(t *testing.T) {
	h := &testStoreHeartbeatDataProcessor{}
	p := newTestSingleProphet(t, func(c *config.Config) {
		c.StoreHeartbeatDataProcessor = h
		c.TestContext = config.NewTestContext()
	})
	defer p.Stop()

	assert.True(t, h.started)
	p.(*defaultProphet).stopCustom()
	assert.False(t, h.started)
}
