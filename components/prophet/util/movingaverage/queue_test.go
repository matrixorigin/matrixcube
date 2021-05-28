package movingaverage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	sq := NewSafeQueue()
	sq.PushBack(1)
	sq.PushBack(2)
	v1 := sq.PopFront()
	v2 := sq.PopFront()
	assert.Equal(t, 1, v1)
	assert.Equal(t, 2, v2)
}
