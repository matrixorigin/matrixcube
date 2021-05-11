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
