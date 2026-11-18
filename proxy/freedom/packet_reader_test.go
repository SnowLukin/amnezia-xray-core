package freedom

import (
	"context"
	gonet "net"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
	_ "github.com/xtls/xray-core/transport/internet/udp"
)

func TestPacketReaderReadsOnceDataCame(t *testing.T) {
	if !buf.ReadyReadable {
		t.Skip("a datagram is read into a buffer taken before it waits here")
	}
	peer, err := gonet.ListenUDP("udp", &gonet.UDPAddr{IP: gonet.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	dest := net.UDPDestination(net.LocalHostIP, net.Port(peer.LocalAddr().(*gonet.UDPAddr).Port))
	for _, c := range []struct {
		name string
		wrap func(stat.Connection) stat.Connection
	}{
		{"plain", func(c stat.Connection) stat.Connection { return c }},
		// as the outbound handler gives it when it counts traffic
		{"counted", func(c stat.Connection) stat.Connection { return &stat.CounterConnection{Connection: c} }},
	} {
		t.Run(c.name, func(t *testing.T) {
			// Process takes its socket from the outbound handler, which dials it so. It is bound to 127.0.0.1 as with
			// sendThrough: on darwin a socket on any address of IPv6 loses datagrams to 127.0.0.1 to an IPv4 socket
			// that has the same port
			ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{Gateway: net.LocalHostIP}})
			conn, err := internet.Dial(ctx, dest, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			r, ok := NewPacketReader(c.wrap(conn), &Handler{}, nil, net.Destination{}, dest).(*PacketReader)
			if !ok || r.rawConn == nil {
				t.Fatalf("%T from internet.Dial is read into a buffer taken before it waits", conn)
			}
			if _, err := peer.WriteTo([]byte("pong"), conn.LocalAddr()); err != nil {
				t.Fatal(err)
			}
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			mb, err := r.ReadMultiBuffer()
			if err != nil {
				t.Fatal(err)
			}
			defer buf.ReleaseMulti(mb)
			if mb.String() != "pong" {
				t.Errorf("read %q instead of pong", mb.String())
			}
		})
	}
}
