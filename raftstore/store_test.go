package raftstore

import (
	"testing"
)

func TestStart(t *testing.T) {
	s, closer := newTestStore()
	defer closer()

	s.Start()
}
