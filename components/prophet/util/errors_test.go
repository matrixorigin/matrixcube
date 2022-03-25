package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsJobProcessorNotFoundErr(t *testing.T) {
	msg := fmt.Sprintf("type = %d", 1)
	wrappedErr := WrappedError(ErrJobProcessorNotFound, msg)
	assert.True(
		t,
		IsJobProcessorNotFoundErr(wrappedErr.Error()),
	)
}
