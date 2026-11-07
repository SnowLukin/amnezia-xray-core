package socks

import (
	"net"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/features/stats"
)

type testCounter struct{ n int64 }

func (c *testCounter) Value() int64      { return c.n }
func (c *testCounter) Set(v int64) int64 { c.n = v; return v }
func (c *testCounter) Add(d int64) int64 { c.n += d; return c.n }

var _ stats.Counter = (*testCounter)(nil)

func newAssociation(t *testing.T, expectedPort int) (*TempUDPConn, *net.UDPConn, *net.UDPConn) {
	t.Helper()
	hub, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	tcpA, tcpB := net.Pipe()
	t.Cleanup(func() { tcpB.Close() })
	client, err := net.DialUDP("udp", nil, hub.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	other, err := net.DialUDP("udp", nil, hub.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.Close()
		other.Close()
	})
	expected := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: expectedPort}
	if expectedPort < 0 {
		expected.Port = client.LocalAddr().(*net.UDPAddr).Port
	}
	conn := NewTempUDPConn(hub, tcpA, expected)
	conn.SetTimeout(time.Minute)
	t.Cleanup(func() { conn.Close() })
	return conn, client, other
}

func TestTempUDPConnReadsOnlyItsClient(t *testing.T) {
	conn, client, other := newAssociation(t, -1)
	counter := &testCounter{}
	conn.ReadCounter = counter
	other.Write([]byte("not the client"))
	time.Sleep(20 * time.Millisecond)
	client.Write([]byte("ping"))
	mb, err := conn.ReadMultiBuffer()
	if err != nil {
		t.Fatal(err)
	}
	defer buf.ReleaseMulti(mb)
	if mb.String() != "ping" {
		t.Errorf("read %q instead of the packet of the client", mb.String())
	}
	if counter.n != 4 {
		t.Errorf("counted %d bytes instead of 4", counter.n)
	}
}

func TestTempUDPConnLearnsPortOfClient(t *testing.T) {
	conn, client, _ := newAssociation(t, 0)
	client.Write([]byte("ping"))
	mb, err := conn.ReadMultiBuffer()
	if err != nil {
		t.Fatal(err)
	}
	buf.ReleaseMulti(mb)
	if got, want := conn.ExpectedRemote.Load().Port, client.LocalAddr().(*net.UDPAddr).Port; got != want {
		t.Errorf("expects the client at port %d instead of %d", got, want)
	}
}

func TestTempUDPConnReadEndsWhenClosed(t *testing.T) {
	conn, _, _ := newAssociation(t, -1)
	done := make(chan error, 1)
	go func() {
		_, err := conn.ReadMultiBuffer()
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	conn.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Error("read nothing without an error")
		}
	case <-time.After(time.Second):
		t.Fatal("read goes on after the association was closed")
	}
}
