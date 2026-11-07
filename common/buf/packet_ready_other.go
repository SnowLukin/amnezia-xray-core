//go:build !unix

package buf

import (
	"errors"
	gonet "net"
	"syscall"
)

// ReadPacketReady is not done here: callers read into a buffer they take before waiting
func ReadPacketReady(syscall.RawConn) (*Buffer, *gonet.UDPAddr, error) {
	return nil, nil, errors.ErrUnsupported
}

// ReadyReadable reports whether ReadPacketReady works on this system
const ReadyReadable = false
