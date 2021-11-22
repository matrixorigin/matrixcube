// Copyright 2020 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
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

// CpuUsages returns cpu usages
func CpuUsages() ([]float64, error) {
	return cpu.Percent(0, true)
}

// IORates io rates
func IORates(path string) (map[string]disk.IOCountersStat, error) {
	return disk.IOCounters(path)
}
