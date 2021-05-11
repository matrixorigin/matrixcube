package operator

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreate(t *testing.T) {
	before := time.Now()
	trk := NewOpStatusTracker()
	assert.Equal(t, CREATED, trk.Status())
	assert.True(t, reflect.DeepEqual(trk.ReachTimeOf(CREATED), trk.ReachTime()))
	checkTimeOrder(t, before, trk.ReachTime(), time.Now())
	checkReachTime(t, &trk, CREATED)
}

func TestNonEndTrans(t *testing.T) {
	{
		trk := NewOpStatusTracker()
		checkInvalidTrans(t, &trk, SUCCESS, REPLACED, TIMEOUT)
		checkValidTrans(t, &trk, STARTED)
		checkInvalidTrans(t, &trk, EXPIRED)
		checkValidTrans(t, &trk, SUCCESS)
		checkReachTime(t, &trk, CREATED, STARTED, SUCCESS)
	}
	{
		trk := NewOpStatusTracker()
		checkValidTrans(t, &trk, CANCELED)
		checkReachTime(t, &trk, CREATED, CANCELED)
	}
	{
		trk := NewOpStatusTracker()
		checkValidTrans(t, &trk, STARTED)
		checkValidTrans(t, &trk, CANCELED)
		checkReachTime(t, &trk, CREATED, STARTED, CANCELED)
	}
	{
		trk := NewOpStatusTracker()
		checkValidTrans(t, &trk, STARTED)
		checkValidTrans(t, &trk, REPLACED)
		checkReachTime(t, &trk, CREATED, STARTED, REPLACED)
	}
	{
		trk := NewOpStatusTracker()
		checkValidTrans(t, &trk, EXPIRED)
		checkReachTime(t, &trk, CREATED, EXPIRED)
	}
	{
		trk := NewOpStatusTracker()
		checkValidTrans(t, &trk, STARTED)
		checkValidTrans(t, &trk, TIMEOUT)
		checkReachTime(t, &trk, CREATED, STARTED, TIMEOUT)
	}
}

func TestEndStatusTrans(t *testing.T) {
	allStatus := make([]OpStatus, 0, statusCount)
	for st := OpStatus(0); st < statusCount; st++ {
		allStatus = append(allStatus, st)
	}
	for from := firstEndStatus; from < statusCount; from++ {
		trk := NewOpStatusTracker()
		trk.current = from
		assert.True(t, trk.IsEnd())
		checkInvalidTrans(t, &trk, allStatus...)
	}
}

func TestStatusCheckExpired(t *testing.T) {
	{
		// Not expired
		before := time.Now()
		trk := NewOpStatusTracker()
		after := time.Now()
		assert.False(t, trk.CheckExpired(10*time.Second))
		assert.Equal(t, CREATED, trk.Status())
		checkTimeOrder(t, before, trk.ReachTime(), after)
	}
	{
		// Expired but status not changed
		trk := NewOpStatusTracker()
		trk.setTime(CREATED, time.Now().Add(-10*time.Second))
		assert.True(t, trk.CheckExpired(5*time.Second))
		assert.Equal(t, EXPIRED, trk.Status())
	}
	{
		// Expired and status changed
		trk := NewOpStatusTracker()
		before := time.Now()
		assert.True(t, trk.To(EXPIRED))
		after := time.Now()
		assert.True(t, trk.CheckExpired(0))
		assert.Equal(t, EXPIRED, trk.Status())
		checkTimeOrder(t, before, trk.ReachTime(), after)
	}
}

func TestStatusCheckTimeout(t *testing.T) {
	{
		// Not timeout
		trk := NewOpStatusTracker()
		before := time.Now()
		assert.True(t, trk.To(STARTED))
		after := time.Now()
		assert.False(t, trk.CheckTimeout(10*time.Second))
		assert.Equal(t, STARTED, trk.Status())
		checkTimeOrder(t, before, trk.ReachTime(), after)
	}
	{
		// Timeout but status not changed
		trk := NewOpStatusTracker()
		assert.True(t, trk.To(STARTED))
		trk.setTime(STARTED, time.Now().Add(-10*time.Second))
		assert.True(t, trk.CheckTimeout(5*time.Second))
		assert.Equal(t, TIMEOUT, trk.Status())
	}
	{
		// Timeout and status changed
		trk := NewOpStatusTracker()
		assert.True(t, trk.To(STARTED))
		before := time.Now()
		assert.True(t, trk.To(TIMEOUT))
		after := time.Now()
		assert.True(t, trk.CheckTimeout(0))
		assert.Equal(t, TIMEOUT, trk.Status())
		checkTimeOrder(t, before, trk.ReachTime(), after)
	}
}

func checkTimeOrder(t *testing.T, t1, t2, t3 time.Time) {
	assert.True(t, t1.Before(t2))
	assert.True(t, t3.After(t2))
}

func checkValidTrans(t *testing.T, trk *OpStatusTracker, st OpStatus) {
	before := time.Now()
	assert.True(t, trk.To(st))
	assert.Equal(t, st, trk.Status())
	assert.True(t, reflect.DeepEqual(trk.ReachTimeOf(st), trk.ReachTime()))
	checkTimeOrder(t, before, trk.ReachTime(), time.Now())
}

func checkInvalidTrans(t *testing.T, trk *OpStatusTracker, sts ...OpStatus) {
	origin := trk.Status()
	originTime := trk.ReachTime()
	sts = append(sts, statusCount, statusCount+1, statusCount+10)
	for _, st := range sts {
		assert.False(t, trk.To(st))
		assert.Equal(t, origin, trk.Status())
		assert.True(t, reflect.DeepEqual(originTime, trk.ReachTime()))
	}
}

func checkReachTime(t *testing.T, trk *OpStatusTracker, reached ...OpStatus) {
	reachedMap := make(map[OpStatus]struct{}, len(reached))
	for _, st := range reached {
		assert.False(t, trk.ReachTimeOf(st).IsZero())
		reachedMap[st] = struct{}{}
	}
	for st := OpStatus(0); st <= statusCount+10; st++ {
		if _, ok := reachedMap[st]; ok {
			continue
		}
		assert.True(t, trk.ReachTimeOf(st).IsZero())
	}
}
