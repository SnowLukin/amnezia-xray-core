package health

import (
	"net"
	"syscall"

	"github.com/xtls/xray-core/transport/internet/stat"
)

// rawConnOf digs through TLS, REALITY and counter wrappers to the socket of a
// dialed connection. It returns nil for transports without one socket per
// connection, such as XHTTP or gRPC streams.
func rawConnOf(c net.Conn) syscall.RawConn {
	for i := 0; i < 8 && c != nil; i++ {
		if sc, ok := c.(syscall.Conn); ok {
			rc, err := sc.SyscallConn()
			if err != nil {
				return nil
			}
			return rc
		}
		if cc, ok := c.(*stat.CounterConnection); ok {
			c = cc.Connection
			continue
		}
		if nc, ok := c.(interface{ NetConn() net.Conn }); ok {
			c = nc.NetConn()
			continue
		}
		return nil
	}
	return nil
}
