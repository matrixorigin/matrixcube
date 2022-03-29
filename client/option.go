package client

import (
	"github.com/matrixorigin/matrixcube/raftstore"
	"go.uber.org/zap"
)

// CreateOption option for create cube client
type CreateOption func(c *client)

// CreateWithLogger set cube client logger
func CreateWithLogger(logger *zap.Logger) CreateOption {
	return func(c *client) {
		c.logger = logger
	}
}

// CreateWithShardsProxy set shardsProxy for client
func CreateWithShardsProxy(shardsProxy raftstore.ShardsProxy) CreateOption {
	return func(c *client) {
		c.shardsProxy = shardsProxy
	}
}
