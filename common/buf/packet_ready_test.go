//go:build unix

package buf_test

import (
	"net"
	"testing"
	"time"

	. "github.com/xtls/xray-core/common/buf"
)

func TestReadPacketReadyReadsDatagramAndSource(t *testing.T) {
	server, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := net.DialUDP("udp", nil, server.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	rc := ReadyRawConn(server)
	if rc == nil {
		t.Fatal("no RawConn for a plain UDP socket")
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		client.Write([]byte("ping"))
	}()
	b, from, err := ReadPacketReady(rc)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	if string(b.Bytes()) != "ping" {
		t.Errorf("read %q instead of ping", b.Bytes())
	}
	if want := client.LocalAddr().(*net.UDPAddr); !from.IP.Equal(want.IP) || from.Port != want.Port {
		t.Errorf("the datagram came from %v, not from %v", from, want)
	}
}

func TestReadPacketReadyHoldsNoBufferWhileWaiting(t *testing.T) {
	const n = 256
	sockets := make([]*net.UDPConn, n)
	for i := range n {
		var err error
		if sockets[i], err = net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}); err != nil {
			t.Fatal(err)
		}
		defer sockets[i].Close()
	}
	got := make(chan error, n)
	cost := idleCost(t, n, func(i int) {
		rc := ReadyRawConn(sockets[i])
		go func() {
			b, _, err := ReadPacketReady(rc)
			if err == nil {
				b.Release()
			}
			got <- err
		}()
	})
	if cost > 2048 {
		t.Errorf("a socket that waits holds %.0f bytes of the heap", cost)
	}
	for i := range n {
		sockets[i].SetReadDeadline(time.Now())
	}
	for range n {
		if err := <-got; err == nil {
			t.Fatal("read a datagram that nobody sent")
		}
	}
}

func TestReadyRawConnOnlyForPlainUDPSockets(t *testing.T) {
	server, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	type wrapper struct{ *net.UDPConn }
	if ReadyRawConn(wrapper{server}) != nil {
		t.Error("a wrapper of a socket would be read past")
	}
}
