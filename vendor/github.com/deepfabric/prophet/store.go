package prophet

// Store meta store
type Store interface {
	// PutResource puts the meta to the store
	PutResource(meta Resource) error
	// GetResource returns the spec resource
	GetResource(id uint64) (Resource, error)
	// PutContainer puts the meta to the store
	PutContainer(meta Container) error
	// GetContainer returns the spec container
	GetContainer(id uint64) (Container, error)
	// LoadResources load all resources
	LoadResources(limit int64, do func(Resource)) error
	// LoadContainers load all containers
	LoadContainers(limit int64, do func(Container)) error

	// AllocID returns the alloc id
	AllocID() (uint64, error)
	// AlreadyBootstrapped returns the cluster was already bootstrapped
	AlreadyBootstrapped() (bool, error)
	// PutBootstrapped put cluster is bootstrapped
	PutBootstrapped(container Container, resources ...Resource) (bool, error)

	// PutIfNotExists put the value at path
	// returns true, nil, nil if created
	// returns false, exists, nil if not created
	PutIfNotExists(path string, value []byte) (bool, []byte, error)
	// RemoveIfValueMatched returns true if the expect value is and the exists value are matched
	RemoveIfValueMatched(path string, expect []byte) (bool, error)
}
