package statistics

// ContainerStatInformer provides access to a shared informer of statistics.
type ContainerStatInformer interface {
	GetContainersLoads() map[uint64][]float64
}
