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

type testAdapter struct {
	leaderCB, followerCB func()
}

// NewTestAdapter create test adapter
func NewTestAdapter() Adapter {
	return &testAdapter{}
}

// NewTestRoleHandler create test adapter
func NewTestRoleHandler(leaderCB, followerCB func()) RoleChangeHandler {
	return &testAdapter{leaderCB: leaderCB, followerCB: followerCB}
}

func (ta *testAdapter) NewShard() Shard {
	return NewTestShard(0)
}

func (ta *testAdapter) NewStore() Store {
	return NewTestStore(0)
}

func (ta *testAdapter) ProphetBecomeLeader() {
	if ta.leaderCB != nil {
		ta.leaderCB()
	}
}

func (ta *testAdapter) ProphetBecomeFollower() {
	if ta.followerCB != nil {
		ta.followerCB()
	}
}
