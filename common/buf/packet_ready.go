package buf

import (
	gonet "net"
	"syscall"
)

// ReadyRawConn returns the RawConn that ReadPacketReady reads from, if c is a plain UDP socket and the system lets it.
// Only a plain socket: a wrapper that changes what ReadFrom gives would be read past
func ReadyRawConn(c interface{}) syscall.RawConn {
	if !ReadyReadable {
		return nil
	}
	u, ok := c.(*gonet.UDPConn)
	if !ok {
		return nil
	}
	rc, err := u.SyscallConn()
	if err != nil {
		return nil
	}
	return rc
}
