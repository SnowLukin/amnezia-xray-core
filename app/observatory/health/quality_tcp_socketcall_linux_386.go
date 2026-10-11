package health

import (
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

func qualityGetsockopt(fd uintptr, level, opt int, value unsafe.Pointer, length *uint32) syscall.Errno {
	// Linux i386 acquired a direct getsockopt syscall in 4.3. socketcall also
	// works on older kernels, matching x/sys/unix's linux/386 implementation.
	const getSockOpt = 15
	args := [5]uintptr{fd, uintptr(level), uintptr(opt), uintptr(value), uintptr(unsafe.Pointer(length))}
	_, _, errno := unix.Syscall(unix.SYS_SOCKETCALL, getSockOpt, uintptr(unsafe.Pointer(&args[0])), 0)
	// The argument array contains integer representations of these pointers.
	runtime.KeepAlive(value)
	runtime.KeepAlive(length)
	return errno
}
