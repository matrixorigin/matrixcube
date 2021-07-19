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
