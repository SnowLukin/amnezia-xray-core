package socks

import (
	"context"
	"io"
	gonet "net"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/policy"
)

// heapObjects is the heap that live objects take, after what is garbage is gone
func heapObjects() uint64 {
	runtime.GC()
	runtime.GC()
	debug.FreeOSMemory()
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

// associate asks for a UDP association on c as a SOCKS 5 client
func associate(c gonet.Conn) error {
	if _, err := c.Write([]byte{5, 1, 0}); err != nil {
		return err
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(c, reply); err != nil {
		return err
	}
	if _, err := c.Write([]byte{5, 3, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		return err
	}
	// the address of the association is IPv4 since the server listens on 127.0.0.1
	reply = make([]byte, 10)
	_, err := io.ReadFull(c, reply)
	return err
}

func TestIdleUDPAssociationHoldsNoBuffer(t *testing.T) {
	if !buf.ReadyReadable {
		t.Skip("a datagram is read into a buffer taken before it waits here")
	}
	const n = 128
	s := &Server{
		config:        &ServerConfig{UdpEnabled: true},
		policyManager: policy.DefaultManager{},
	}
	ln, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	gateway := net.DestinationFromAddr(ln.Addr())
	clients := make([]gonet.Conn, n)
	servers := make([]gonet.Conn, n)
	for i := range n {
		if clients[i], err = gonet.Dial("tcp", ln.Addr().String()); err != nil {
			t.Fatal(err)
		}
		defer clients[i].Close()
		if servers[i], err = ln.Accept(); err != nil {
			t.Fatal(err)
		}
		defer servers[i].Close()
	}
	done := make(chan error, n)
	before := heapObjects()
	for i := range n {
		ctx := session.ContextWithInbound(context.Background(), &session.Inbound{Gateway: gateway})
		go func() {
			done <- s.Process(ctx, net.Network_TCP, servers[i], nil)
		}()
		if err := associate(clients[i]); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(300 * time.Millisecond)
	cost := (float64(heapObjects()) - float64(before)) / n
	// both of its connections wait for what may never come, and a buffer of the pool taken by either would be 8 KiB
	if cost > buf.Size {
		t.Errorf("an idle UDP association holds %.0f bytes of the heap", cost)
	}
	for i := range n {
		clients[i].Close()
	}
	for range n {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("an association goes on after its client closed the connection")
		}
	}
}
