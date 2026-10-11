//go:build linux && !386

package health

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

func qualityGetsockopt(fd uintptr, level, opt int, value unsafe.Pointer, length *uint32) syscall.Errno {
	_, _, errno := unix.Syscall6(unix.SYS_GETSOCKOPT, fd, uintptr(level), uintptr(opt), uintptr(value), uintptr(unsafe.Pointer(length)), 0)
	return errno
}
