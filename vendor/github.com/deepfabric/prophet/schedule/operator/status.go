package operator

import (
	"github.com/deepfabric/prophet/pb/metapb"
)

// OpStatus represents the status of an Operator.
type OpStatus = uint32

// Status list
const (
	// Status list
	// Just created. Next status: {RUNNING, CANCELED, EXPIRED}.
	CREATED OpStatus = iota
	// Started and not finished. Next status: {SUCCESS, CANCELED, REPLACED, TIMEOUT}.
	STARTED
	// Followings are end status, i.e. no next status.
	SUCCESS  // Finished successfully
	CANCELED // Canceled due to some reason
	REPLACED // Replaced by an higher priority operator
	EXPIRED  // Didn't start to run for too long
	TIMEOUT  // Running for too long
	// Status list end
	statusCount    // Total count of status
	firstEndStatus = SUCCESS
)

type transition [statusCount][statusCount]bool

// Valid status transition
var validTrans = transition{
	CREATED: {
		STARTED:  true,
		CANCELED: true,
		EXPIRED:  true,
	},
	STARTED: {
		SUCCESS:  true,
		CANCELED: true,
		REPLACED: true,
		TIMEOUT:  true,
	},
	SUCCESS:  {},
	CANCELED: {},
	REPLACED: {},
	EXPIRED:  {},
	TIMEOUT:  {},
}

var statusString = [statusCount]string{
	CREATED:  "Created",
	STARTED:  "Started",
	SUCCESS:  "Success",
	CANCELED: "Canceled",
	REPLACED: "Replaced",
	EXPIRED:  "Expired",
	TIMEOUT:  "Timeout",
}

const invalid = metapb.OperatorStatus_RUNNING + 1

var pdpbStatus = [statusCount]metapb.OperatorStatus{
	// FIXME: use a valid status
	CREATED:  invalid,
	STARTED:  metapb.OperatorStatus_RUNNING,
	SUCCESS:  metapb.OperatorStatus_SUCCESS,
	CANCELED: metapb.OperatorStatus_CANCEL,
	REPLACED: metapb.OperatorStatus_REPLACE,
	// FIXME: use a better status
	EXPIRED: metapb.OperatorStatus_TIMEOUT,
	TIMEOUT: metapb.OperatorStatus_TIMEOUT,
}

// IsEndStatus checks whether s is an end status.
func IsEndStatus(s OpStatus) bool {
	return firstEndStatus <= s && s < statusCount
}

// OpStatusToPDPB converts OpStatus to metapb.OperatorStatus.
func OpStatusToPDPB(s OpStatus) metapb.OperatorStatus {
	if s < statusCount {
		return pdpbStatus[s]
	}
	return invalid
}

// OpStatusToString converts Status to string.
func OpStatusToString(s OpStatus) string {
	if s < statusCount {
		return statusString[s]
	}
	return "Unknown"
}
