//go:build unix

package buf

import (
	gonet "net"
	"strconv"
	"sync"
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
			addr.Zone = zoneName(int(sa.ZoneId))
		}
		return addr
	}
	return &gonet.UDPAddr{}
}

// zoneNames are the names of the interfaces of the zones that datagrams came from. A name is looked up once: the
// index alone would cost a lookup of all the interfaces on every write to the address, as net looks a zone up by name
var zoneNames sync.Map

func zoneName(index int) string {
	if name, ok := zoneNames.Load(index); ok {
		return name.(string)
	}
	ifi, err := gonet.InterfaceByIndex(index)
	if err != nil {
		// net takes the index for a zone as well
		return strconv.Itoa(index)
	}
	zoneNames.Store(index, ifi.Name)
	return ifi.Name
}
