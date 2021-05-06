package statistics

// HotPeersStat records all hot resources statistics
type HotPeersStat struct {
	TotalBytesRate float64       `json:"total_flow_bytes"`
	TotalKeysRate  float64       `json:"total_flow_keys"`
	Count          int           `json:"resources_count"`
	Stats          []HotPeerStat `json:"statistics"`
}
