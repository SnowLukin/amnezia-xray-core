//go:build unix

package buf

import (
	gonet "net"
	"syscall"
)

// ReadPacketReady reads one datagram from the socket of rc into a buffer that is taken only once the socket has one:
// it goes back to the pool every time the socket has nothing yet, so a socket that waits for its peer does not hold it.
// A datagram larger than Size is cut to Size, as a read into a Buffer cuts it
func ReadPacketReady(rc syscall.RawConn) (*Buffer, *gonet.UDPAddr, error) {
	var b *Buffer
	var from syscall.Sockaddr
	var rerr error
	err := rc.Read(func(fd uintptr) bool {
		for {
			b = New()
			var n int
			n, from, rerr = syscall.Recvfrom(int(fd), b.v[:Size], 0)
			switch rerr {
			case syscall.EINTR:
				b.Release()
				continue
			case syscall.EAGAIN:
				b.Release()
				b = nil
				return false
			case nil:
				b.end = int32(n)
			}
			return true
		}
	})
	if err == nil {
		err = rerr
	}
	if err != nil {
		if b != nil {
			b.Release()
		}
		return nil, nil, err
	}
	return b, udpAddrOf(from), nil
}

// ReadyReadable reports whether ReadPacketReady works on this system
const ReadyReadable = true

func udpAddrOf(sa syscall.Sockaddr) *gonet.UDPAddr {
	switch sa := sa.(type) {
	case *syscall.SockaddrInet4:
		return &gonet.UDPAddr{IP: gonet.IP(append([]byte(nil), sa.Addr[:]...)), Port: sa.Port}
	case *syscall.SockaddrInet6:
		addr := &gonet.UDPAddr{IP: gonet.IP(append([]byte(nil), sa.Addr[:]...)), Port: sa.Port}
		if sa.ZoneId != 0 {
			if ifi, err := gonet.InterfaceByIndex(int(sa.ZoneId)); err == nil {
				addr.Zone = ifi.Name
			}
		}
		return addr
	}
	return &gonet.UDPAddr{}
}
