package schedule

import (
	"time"

	"github.com/deepfabric/prophet/schedule/operator"
)

type operatorWithTime struct {
	op   *operator.Operator
	time time.Time
}

type operatorQueue []*operatorWithTime

func (opn operatorQueue) Len() int { return len(opn) }

func (opn operatorQueue) Less(i, j int) bool {
	return opn[i].time.Before(opn[j].time)
}

func (opn operatorQueue) Swap(i, j int) {
	opn[i], opn[j] = opn[j], opn[i]
}

func (opn *operatorQueue) Push(x interface{}) {
	item := x.(*operatorWithTime)
	*opn = append(*opn, item)
}

func (opn *operatorQueue) Pop() interface{} {
	old := *opn
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	*opn = old[0 : n-1]
	return item
}
