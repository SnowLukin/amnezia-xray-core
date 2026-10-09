package wireguard

import (
	"context"
	"io"
	stdnet "net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/amnezia-vpn/amneziawg-go/v3/tun"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
)

var (
	linkedServerAddr = netip.MustParseAddr("10.7.0.1")
	linkedClientAddr = netip.MustParseAddr("10.7.0.2")
)

// linkedStacks returns two stacks of CreateNetTUN that pass their packets to
// each other, as two devices and the tunnel between them would, and a TCP
// listener on port 80 of the first.
func linkedStacks(t *testing.T) (l *gonet.TCPListener, client *Net, clientStack *stack.Stack) {
	t.Helper()
	st, _, sstk, err := CreateNetTUN([]netip.Addr{linkedServerAddr}, nil, 1420, false)
	if err != nil {
		t.Fatal(err)
	}
	ct, cnet, cstk, err := CreateNetTUN([]netip.Addr{linkedClientAddr}, nil, 1420, false)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	pump := func(from, to tun.Device) {
		defer wg.Done()
		bufs, sizes := [][]byte{make([]byte, 2048)}, []int{0}
		for {
			n, err := from.Read(bufs, sizes, 0)
			if err != nil {
				return
			}
			if n == 1 {
				to.Write([][]byte{bufs[0][:sizes[0]]}, 0)
			}
		}
	}
	wg.Add(2)
	go pump(st, ct)
	go pump(ct, st)
	l, err = gonet.ListenTCP(sstk, tcpip.FullAddress{NIC: 1, Addr: tcpip.AddrFrom4(linkedServerAddr.As4()), Port: 80}, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		l.Close()
		st.Close()
		ct.Close()
		wg.Wait()
	})
	return l, cnet, cstk
}

func dialLinked(t *testing.T, client *Net) stdnet.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.DialContextTCPAddrPort(ctx, netip.AddrPortFrom(linkedServerAddr, 80))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// tcpEndpoints returns the TCP endpoints of s.
func tcpEndpoints(s *stack.Stack) []*tcp.Endpoint {
	var eps []*tcp.Endpoint
	for _, ep := range s.RegisteredEndpoints() {
		if te, ok := ep.(*tcp.Endpoint); ok {
			eps = append(eps, te)
		}
	}
	return eps
}

// A connection whose reader stalls holds no more than the window its
// starting receive buffer allows, not the megabyte of gVisor's default.
func TestStalledConnectionHoldsItsStartingWindow(t *testing.T) {
	l, client, cstk := linkedStacks(t)
	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		// Blocks once the client's window is full, until the cleanup
		// closes the stacks.
		c.Write(make([]byte, 4<<20))
	}()
	c := dialLinked(t, client)
	defer c.Close()

	eps := tcpEndpoints(cstk)
	if len(eps) != 1 {
		t.Fatalf("%d TCP endpoints in the client stack, want 1", len(eps))
	}
	queued := func() int {
		v, err := eps[0].GetSockOptInt(tcpip.ReceiveQueueSizeOption)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	// The queue stops growing once the window is full.
	last, held := 0, time.Now()
	for deadline := time.Now().Add(10 * time.Second); last == 0 || time.Since(held) < 300*time.Millisecond; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the receive queue does not settle: %d bytes", last)
		}
		if n := queued(); n != last {
			last, held = n, time.Now()
		}
	}
	// gVisor advertises half the receive buffer as the window.
	if last > tcpReceiveBufferDefault/2 {
		t.Fatalf("a stalled connection holds %d bytes, want at most %d", last, tcpReceiveBufferDefault/2)
	}
}

// Connections the client closed leave its stack within tcpCloseTimeout,
// from TIME_WAIT after a peer that closed too and from FIN_WAIT_2 after one
// that never does.
func TestClosedConnectionsLeaveTheStack(t *testing.T) {
	l, client, cstk := linkedStacks(t)
	hold := make(chan struct{})
	defer close(hold)
	go func() {
		for i := 0; ; i++ {
			c, err := l.Accept()
			if err != nil {
				return
			}
			if i == 0 {
				go func() {
					io.Copy(io.Discard, c)
					c.Close()
				}()
			} else {
				go func() {
					<-hold
					c.Close()
				}()
			}
		}
	}()
	// One at a time, so that the first is the one the server closes.
	for range 2 {
		c := dialLinked(t, client)
		if _, err := c.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		c.Close()
	}
	states := map[string]bool{}
	start := time.Now()
	for {
		eps := tcpEndpoints(cstk)
		if len(eps) == 0 {
			break
		}
		for _, ep := range eps {
			states[tcp.EndpointState(ep.State()).String()] = true
		}
		if time.Since(start) > tcpCloseTimeout+3*time.Second {
			t.Fatalf("closed connections stay %v after the close: %v", time.Since(start).Round(time.Second), states)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !states["TIME-WAIT"] || !states["FIN-WAIT2"] {
		t.Fatalf("closed connections went through %v, want TIME-WAIT and FIN-WAIT2", states)
	}
}
