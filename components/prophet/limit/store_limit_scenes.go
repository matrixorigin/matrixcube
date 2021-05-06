package limit

// Scene defines the container limitation on difference
// scenes
// Idle/Low/Normal/High indicates the load of the cluster, it is defined
// in cluster.State. See the details there for how to calculate the
// load.
// The values here defines the container-limit for each load. For example:
// Idle = 60, means that change the container-limit to 60 when the cluster is
// idle.
type Scene struct {
	Idle   int
	Low    int
	Normal int
	High   int
}

// DefaultScene returns Scene object with default values
func DefaultScene(limitType Type) *Scene {
	defaultScene := &Scene{
		Idle:   100,
		Low:    50,
		Normal: 32,
		High:   12,
	}

	// change this if different type rate limit has different default scene
	switch limitType {
	case AddPeer:
		return defaultScene
	case RemovePeer:
		return defaultScene
	default:
		return nil
	}
}
