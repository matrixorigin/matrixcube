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

// RoleChangeHandler prophet role change handler
type RoleChangeHandler interface {
	ProphetBecomeLeader()
	ProphetBecomeFollower()
}

type testRoleChangeHandler struct {
	leaderCB, followerCB func()
}

// NewTestRoleHandler create test adapter
func NewTestRoleHandler(leaderCB, followerCB func()) RoleChangeHandler {
	return &testRoleChangeHandler{leaderCB: leaderCB, followerCB: followerCB}
}

func (ta *testRoleChangeHandler) ProphetBecomeLeader() {
	if ta.leaderCB != nil {
		ta.leaderCB()
	}
}

func (ta *testRoleChangeHandler) ProphetBecomeFollower() {
	if ta.followerCB != nil {
		ta.followerCB()
	}
}
