package health

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// kernelTCPQuality must be called before taking the observer's locks. Control
// keeps the descriptor open for the syscall; no descriptor escapes its callback.
func kernelTCPQuality(rc syscall.RawConn) tcpQualitySnapshot {
	if rc == nil {
		return tcpQualitySnapshot{}
	}
	var b [unix.SizeofTCPInfo]byte
	n := uint32(len(b))
	var errno syscall.Errno
	err := rc.Control(func(fd uintptr) {
		errno = qualityGetsockopt(fd, unix.IPPROTO_TCP, unix.TCP_INFO, unsafe.Pointer(&b[0]), &n)
	})
	if err != nil || errno != 0 || n > uint32(len(b)) {
		return tcpQualitySnapshot{}
	}
	return decodeLinuxTCPQuality(b[:n])
}
