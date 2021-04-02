package prophet

import (
	"context"
	"time"

	"github.com/deepfabric/prophet/util"
)

// StartMonitor calls systimeErrHandler if system time jump backward.
func StartMonitor(ctx context.Context, now func() time.Time, systimeErrHandler func()) {
	util.GetLogger().Info("start system time monitor")
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		last := now().UnixNano()
		select {
		case <-tick.C:
			if now().UnixNano() < last {
				util.GetLogger().Errorf("system time jump backward, last %+v", last)
				systimeErrHandler()
			}
		case <-ctx.Done():
			return
		}
	}
}
