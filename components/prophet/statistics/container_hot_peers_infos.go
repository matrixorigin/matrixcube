package statistics

// ContainerHotPeersInfos is used to get human-readable description for hot resources.
type ContainerHotPeersInfos struct {
	AsPeer   ContainerHotPeersStat `json:"as_peer"`
	AsLeader ContainerHotPeersStat `json:"as_leader"`
}

// ContainerHotPeersStat is used to record the hot resource statistics group by container.
type ContainerHotPeersStat map[uint64]*HotPeersStat

// GetContainerStatAsPeer returns stat as peer from the corresponding container.
func (info *ContainerHotPeersInfos) GetContainerStatAsPeer(containerID uint64) (string, *HotPeersStat) {
	stat, ok := info.AsPeer[containerID]
	if !ok {
		stat = &HotPeersStat{}
	}
	return "as_peer", stat
}

// GetContainerStatAsLeader returns stat stat as leader from the corresponding container.
func (info *ContainerHotPeersInfos) GetContainerStatAsLeader(containerID uint64) (string, *HotPeersStat) {
	stat, ok := info.AsLeader[containerID]
	if !ok {
		stat = &HotPeersStat{}
	}
	return "as_leader", stat
}
