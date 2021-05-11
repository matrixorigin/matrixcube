package prophet

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSystimeMonitor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var jumpForward int32

	trigged := false
	go StartMonitor(ctx,
		func() time.Time {
			if !trigged {
				trigged = true
				return time.Now()
			}

			return time.Now().Add(-2 * time.Second)
		}, func() {
			atomic.StoreInt32(&jumpForward, 1)
		})

	time.Sleep(1 * time.Second)

	assert.Equal(t, int32(1), atomic.LoadInt32(&jumpForward), "TestSystimeMonitor failed")
}
