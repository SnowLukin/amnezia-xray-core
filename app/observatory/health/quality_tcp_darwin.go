package health

import (
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

func kernelTCPQuality(rc syscall.RawConn) tcpQualitySnapshot {
	if rc == nil {
		return tcpQualitySnapshot{}
	}
	var b [unix.SizeofTCPConnectionInfo]byte
	n := uint32(len(b))
	var errno syscall.Errno
	err := rc.Control(func(fd uintptr) {
		_, _, errno = qualityLibcSyscall6(qualityLibcGetsockoptTrampoline, fd, unix.IPPROTO_TCP, unix.TCP_CONNECTION_INFO,
			uintptr(unsafe.Pointer(&b[0])), uintptr(unsafe.Pointer(&n)), 0)
	})
	runtime.KeepAlive(&b)
	runtime.KeepAlive(&n)
	if err != nil || errno != 0 || n > uint32(len(b)) {
		return tcpQualitySnapshot{}
	}
	return decodeDarwinTCPQuality(b[:n])
}

// Call the public libSystem getsockopt entry point, as x/sys/unix does, while
// retaining its returned socklen_t. A direct Darwin kernel trap would bypass
// libSystem's forward compatibility for macOS and iOS (Go 1.12 release notes).
// https://go.dev/doc/go1.12#darwin
// syscall.syscall6 explicitly exports its linkname in the Go runtime; no
// unexported x/sys symbol or cgo dependency is needed here.
//
//go:linkname qualityLibcSyscall6 syscall.syscall6
func qualityLibcSyscall6(fn, a1, a2, a3, a4, a5, a6 uintptr) (r1, r2 uintptr, errno syscall.Errno)

var qualityLibcGetsockoptTrampoline uintptr

//go:cgo_import_dynamic quality_libc_getsockopt getsockopt "/usr/lib/libSystem.B.dylib"
