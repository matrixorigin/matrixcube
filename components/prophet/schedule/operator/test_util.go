package operator

import (
	"time"
)

// SetOperatorStatusReachTime sets the reach time of the operator.
// NOTE: Should only use in test.
func SetOperatorStatusReachTime(op *Operator, st OpStatus, t time.Time) {
	op.status.setTime(st, t)
}
