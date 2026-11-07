//go:build unix

package socks

import (
	"context"
	"net"
	"testing"

	"github.com/xtls/xray-core/transport/internet"
)

func TestAssociationSocketIsReadOnceDataCame(t *testing.T) {
	// the socket of an association comes from here, see ServerSession.handshake5
	pc, err := internet.ListenSystemPacket(context.Background(), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tcpA, tcpB := net.Pipe()
	defer tcpB.Close()
	conn := NewTempUDPConn(pc, tcpA, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	defer conn.Close()
	if conn.rawConn == nil {
		t.Fatalf("%T from ListenSystemPacket is read into a buffer taken before it waits", pc)
	}
}
