package client

// BatchSender BatchRequest sender.
type BatchSender interface {
	// Send based on the routing information in the BatchRequest, a BatchRequest is split
	// into multiple BatchRequests and distributed to the appropriate Shards, and the results
	// are combined and returned.
	// Send(context.Context, BatchRequest) (BatchResponse, error)
}
