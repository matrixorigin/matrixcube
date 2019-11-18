package server

import (
	"github.com/deepfabric/beehive/raftstore"
)

// Cfg cfg
type Cfg struct {
	RedisAddr string
	RaftCfg   raftstore.Cfg
	Options   []raftstore.Option
}
