package statistics

// ContainerStatInformer provides access to a shared informer of statistics.
type ContainerStatInformer interface {
	GetContainersStats() *ContainersStats
}
