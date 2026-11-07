//go:build !windows

package wireguard

import (
	"syscall"
	"time"
)

// cpuTime is the user and system time the process has taken.
func cpuTime() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
