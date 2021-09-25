package testutil

import (
	"sync"
)

var (
	mutex sync.Mutex
	port  = 40000
)

// GenTestPorts gen and return ports
func GenTestPorts(n int) []int {
	mutex.Lock()
	defer mutex.Unlock()
	ports := make([]int, n)
	for i := 0; i < n; i++ {
		ports[i] = port
		port++
	}
	return ports
}
