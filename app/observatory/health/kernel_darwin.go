package health

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// kernelReceived returns how many bytes the kernel has received on the TCP socket.
func kernelReceived(rc syscall.RawConn) (uint64, bool) {
	var n uint64
	var ok bool
	_ = rc.Control(func(fd uintptr) {
		info, err := unix.GetsockoptTCPConnectionInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_CONNECTION_INFO)
		if err == nil {
			n, ok = info.Rxbytes, true
		}
	})
	return n, ok
}
