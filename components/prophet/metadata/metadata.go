// Copyright 2020 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package metadata

import (
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

// IsLearner judges whether the Peer's Role is Learner.
func IsLearner(peer metapb.Replica) bool {
	return peer.Role == metapb.ReplicaRole_Learner
}

// IsVoterOrIncomingVoter judges whether peer role will become Voter.
// The peer is not nil and the role is equal to IncomingVoter or Voter.
func IsVoterOrIncomingVoter(peer metapb.Replica) bool {
	switch peer.Role {
	case metapb.ReplicaRole_IncomingVoter, metapb.ReplicaRole_Voter:
		return true
	}
	return false
}

// IsLearnerOrDemotingVoter judges whether peer role will become Learner.
// The peer is not nil and the role is equal to DemotingVoter or Learner.
func IsLearnerOrDemotingVoter(peer metapb.Replica) bool {
	switch peer.Role {
	case metapb.ReplicaRole_DemotingVoter, metapb.ReplicaRole_Learner:
		return true
	}
	return false
}

// IsInJointState judges whether the Peer is in joint state.
func IsInJointState(peers ...metapb.Replica) bool {
	for _, peer := range peers {
		switch peer.Role {
		case metapb.ReplicaRole_IncomingVoter, metapb.ReplicaRole_DemotingVoter:
			return true
		default:
		}
	}
	return false
}

// CountInJointState count the peers are in joint state.
func CountInJointState(peers ...metapb.Replica) int {
	count := 0
	for _, peer := range peers {
		switch peer.Role {
		case metapb.ReplicaRole_IncomingVoter, metapb.ReplicaRole_DemotingVoter:
			count++
		default:
		}
	}
	return count
}
