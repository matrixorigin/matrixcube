package redis

import (
	"errors"
)

var (
	// ErrCMDNotSupport command not support error
	ErrCMDNotSupport = errors.New("The command is not support")
)
