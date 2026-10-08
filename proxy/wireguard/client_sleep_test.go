package wireguard

import (
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/outbound"
)

func TestClientSleep(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	inst := newTestInstance(t)
	base := goroutineBase(t)

	oh, h := addTestClient(t, inst, "wg", s, priv, addr, testParams)
	sleeper, ok := oh.(outbound.Sleeper)
	if !ok {
		t.Fatal("the outbound handler is not a Sleeper")
	}
	if !sleeper.Sleep() {
		t.Fatal("an outbound that never connected refused to sleep")
	}

	for i := range 3 {
		c := dial(oh, net.TCPDestination(net.IPAddress(testServerAddr.AsSlice()), 7))
		if err := c.echo([]byte("ping "+strconv.Itoa(i)), 15*time.Second); err != nil {
			t.Fatal(err)
		}
		if sleeper.Sleep() {
			t.Fatal("the outbound slept during a connection")
		}
		if !h.holds() {
			t.Fatal("a refused Sleep released the stack")
		}
		if err := c.close(); err != nil {
			t.Fatal(err)
		}
		s.waitConns(t)
		if !sleeper.Sleep() {
			t.Fatal("an idle outbound refused to sleep")
		}
		if h.holds() {
			t.Fatal("the outbound holds a stack or a device after Sleep")
		}
		waitGoroutines(t, base)
		time.Sleep(handshakeGap)
	}

	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if !sleeper.Sleep() {
		t.Fatal("a closed outbound refused to sleep")
	}
	if c := dial(oh, net.TCPDestination(net.IPAddress(testServerAddr.AsSlice()), 7)); c.wait() != nil || h.holds() {
		t.Fatal("Sleep reopened a closed outbound")
	}
	waitGoroutines(t, base)
}

// TestClientWakesWithPadding sends one UDP datagram right after each wake.
// UDP is not retried, so the datagram must leave the new device with the
// padding the server expects.
func TestClientWakesWithPadding(t *testing.T) {
	params := &AmneziaParameters{
		Jc: "3", Jmin: "50", Jmax: "1000",
		S1: "20", S2: "78", S3: "15", S4: "25",
		H1: "39131278", H2: "832138185", H3: "1436957857", H4: "1635877746",
	}
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, params, testPeer{pub, addr})
	inst := newTestInstance(t)
	oh, h := addTestClient(t, inst, "wg", s, priv, addr, params)

	for i := range 5 {
		c := dial(oh, net.UDPDestination(net.IPAddress(testServerAddr.AsSlice()), 7))
		err := c.echo([]byte("ping "+strconv.Itoa(i)), 15*time.Second)
		if cerr := c.close(); cerr != nil {
			t.Fatal(cerr)
		}
		if err != nil {
			t.Fatalf("wake %d: %v", i, err)
		}
		if !h.Sleep() {
			t.Fatal("an idle outbound refused to sleep")
		}
		time.Sleep(handshakeGap)
	}
}

// TestClientSleepRace runs connections, Sleep and finally Close at once; the
// race detector and the goroutine count judge the outcome.
func TestClientSleepRace(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	inst := newTestInstance(t)
	base := goroutineBase(t)
	oh, h := addTestClient(t, inst, "wg", s, priv, addr, testParams)

	stop := make(chan struct{})
	var slept atomic.Int32
	var sleeper sync.WaitGroup
	sleeper.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
			}
			if h.Sleep() {
				slept.Add(1)
			}
		}
	})
	var conns sync.WaitGroup
	for i := range 3 {
		conns.Go(func() {
			for j := range 3 {
				if err := tcpEcho(oh, "ping "+strconv.Itoa(i)+"."+strconv.Itoa(j)); err != nil {
					t.Error(err)
					return
				}
				time.Sleep(handshakeGap)
			}
		})
	}
	conns.Wait()
	close(stop)
	sleeper.Wait()
	t.Logf("slept %d times", slept.Load())

	// A Sleep right after a connection ends may drop its FIN on the way.
	s.closeConns()
	s.waitConns(t)

	// Close ends a connection in flight.
	c := dial(oh, net.TCPDestination(net.IPAddress(testServerAddr.AsSlice()), 7))
	if err := c.echo([]byte("last"), 15*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.wait(); err != nil {
		t.Fatal(err)
	}
	if !h.Sleep() || h.holds() {
		t.Fatal("a closed outbound holds a stack or a device")
	}
	c.close()
	// The closed stack sent no FIN for the connection.
	s.closeConns()
	s.waitConns(t)
	waitGoroutines(t, base)
}
