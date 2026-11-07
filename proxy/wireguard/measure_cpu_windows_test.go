package wireguard

import (
	"syscall"
	"time"
)

// cpuTime is the user and system time the process has taken.
func cpuTime() time.Duration {
	var creation, exit, kernel, user syscall.Filetime
	if p, err := syscall.GetCurrentProcess(); err == nil {
		syscall.GetProcessTimes(p, &creation, &exit, &kernel, &user)
	}
	// Filetime counts 100 ns ticks.
	ticks := func(f syscall.Filetime) int64 { return int64(f.HighDateTime)<<32 | int64(f.LowDateTime) }
	return time.Duration(ticks(kernel)+ticks(user)) * 100
}
