package server

import (
	"github.com/deepfabric/beehive/raftstore"
)

// Cfg cfg
type Cfg struct {
	Addr           string
	Store          raftstore.Store
	Handler        Handler
	ExternalServer bool
}
