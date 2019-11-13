package util

import (
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
)

// DiskStats returns the disk usage stats
func DiskStats(path string) (*disk.UsageStat, error) {
	stats, err := disk.Usage(path)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

// MemStats returns the mem usage stats
func MemStats() (*mem.VirtualMemoryStat, error) {
	return mem.VirtualMemory()
}
