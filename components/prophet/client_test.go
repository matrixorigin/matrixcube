package prophet

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientLeaderChange(t *testing.T) {
	cluster := newTestClusterProphet(t, 3)
	defer func() {
		for _, p := range cluster {
			p.Stop()
		}
	}()

	id, err := cluster[2].GetClient().AllocID()
	assert.NoError(t, err)
	assert.True(t, id > 0)

	// stop current leader
	cluster[0].Stop()

	for _, c := range cluster {
		c.GetConfig().DisableResponse = true
	}

	// rpc timeout error
	_, err = cluster[2].GetClient().AllocID()
	assert.Error(t, err)

	for _, c := range cluster {
		c.GetConfig().DisableResponse = false
	}

	id, err = cluster[2].GetClient().AllocID()
	assert.NoError(t, err)
	assert.True(t, id > 0)
}

func TestClientGetContainer(t *testing.T) {
	p := newTestSingleProphet(t)
	defer p.Stop()

	c := p.GetClient()
	value, err := c.GetContainer(0)
	assert.Error(t, err)
	assert.Nil(t, value)
}
