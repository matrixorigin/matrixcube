package server

import (
	"github.com/matrixorigin/matrixcube/raftstore"
)

// Cfg cfg
type Cfg struct {
	Addr           string
	Store          raftstore.Store
	Handler        Handler
	ExternalServer bool
}
