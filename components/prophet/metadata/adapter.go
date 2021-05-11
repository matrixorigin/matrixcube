package metadata

// Adapter metadata adapter
type Adapter interface {
	// NewResource return a new resource
	NewResource() Resource
	// NewContainer return a new container
	NewContainer() Container
}

// RoleChangeHandler prophet role change handler
type RoleChangeHandler interface {
	ProphetBecomeLeader()
	ProphetBecomeFollower()
}
