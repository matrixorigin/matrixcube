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

func (ta *testAdapter) NewResource() Resource {
	return NewTestResource(0)
}

func (ta *testAdapter) NewContainer() Container {
	return NewTestContainer(0)
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
