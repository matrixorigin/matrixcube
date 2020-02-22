package grafana

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreate(t *testing.T) {
	c := NewDashboardCreator("http://127.0.0.1:3000",
		"eyJrIjoiSm5XNDByb0l2MklYZzZyZUVMR2J3SzBuc29tMTZyWTkiLCJuIjoiMTExIiwiaWQiOjF9",
		"Prometheus")
	err := c.Create()
	assert.NoError(t, err, "TestCreate failed")
}
