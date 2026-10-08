package health

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// kernelReceived returns how many bytes the kernel has received on the TCP
// socket, which keeps counting while a splice moves data past the counters.
func kernelReceived(rc syscall.RawConn) (uint64, bool) {
	var n uint64
	var ok bool
	_ = rc.Control(func(fd uintptr) {
		info, err := unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
		if err == nil {
			n, ok = info.Bytes_received, true
		}
	})
	return n, ok
}
