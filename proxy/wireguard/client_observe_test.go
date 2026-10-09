package wireguard

import (
	"context"
	"encoding/json"
	goerrors "errors"
	stdnet "net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amnezia-vpn/amneziawg-go/v3/device"
	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/app/policy"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/stats"
)

// testRelay stands between a client and its server, as the network does,
// and drops datagrams the way a censor or a dead server would.
type testRelay struct {
	addr  string
	front *stdnet.UDPConn
	back  *stdnet.UDPConn
	// Where the client sends from.
	client atomic.Pointer[stdnet.UDPAddr]
	// Datagrams to drop, either way, before any passes again.
	drop atomic.Int64
	// Datagrams still let through, either way; below zero, all of them.
	allow atomic.Int64
	// Datagrams from the client, whatever became of them.
	sent atomic.Int64
	wg   sync.WaitGroup
}

func startTestRelay(t testing.TB, server string) *testRelay {
	t.Helper()
	front, err := stdnet.ListenUDP("udp", &stdnet.UDPAddr{IP: stdnet.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	to, err := stdnet.ResolveUDPAddr("udp", server)
	if err != nil {
		t.Fatal(err)
	}
	back, err := stdnet.DialUDP("udp", nil, to)
	if err != nil {
		t.Fatal(err)
	}
	r := &testRelay{addr: front.LocalAddr().String(), front: front, back: back}
	r.allow.Store(-1)
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		b := make([]byte, 2048)
		for {
			n, from, err := front.ReadFromUDP(b)
			if err != nil {
				return
			}
			r.client.Store(from)
			r.sent.Add(1)
			if r.pass() {
				back.Write(b[:n])
			}
		}
	}()
	go func() {
		defer r.wg.Done()
		b := make([]byte, 2048)
		for {
			n, err := back.Read(b)
			if err != nil {
				return
			}
			if c := r.client.Load(); c != nil && r.pass() {
				front.WriteToUDP(b[:n], c)
			}
		}
	}()
	t.Cleanup(func() {
		front.Close()
		back.Close()
		r.wg.Wait()
	})
	return r
}

func (r *testRelay) pass() bool {
	for {
		n := r.drop.Load()
		if n <= 0 {
			break
		}
		if r.drop.CompareAndSwap(n, n-1) {
			return false
		}
	}
	for {
		n := r.allow.Load()
		if n < 0 {
			return true
		}
		if n == 0 {
			return false
		}
		if r.allow.CompareAndSwap(n, n-1) {
			return true
		}
	}
}

// freeze drops everything from now on, as a censor that blocks a flow
// it has seen for a while.
func (r *testRelay) freeze() { r.allow.Store(0) }

// testPassive judges with little traffic and much patience: under load, the
// race detector slows the cryptography of the tunnel down to seconds a
// handshake. A test shortens the timeout it is about. A path never turns
// suspect, so the observer ends no connection of the tests.
func testPassive() *health.PassiveConfig {
	return &health.PassiveConfig{
		DialTimeout:      int64(8 * time.Second),
		FirstByteTimeout: int64(10 * time.Second),
		StallTimeout:     int64(10 * time.Second),
		SuccessBytes:     testSuccessBytes,
		SweepInterval:    int64(50 * time.Millisecond),
		Failures:         100,
	}
}

const testSuccessBytes = 4000

// newObservedInstance runs a core whose health observatory follows the
// outbounds tagged wg*.
func newObservedInstance(t testing.TB, passive *health.PassiveConfig, apps ...*serial.TypedMessage) (*core.Instance, *health.Observer) {
	t.Helper()
	inst, err := core.New(&core.Config{App: append([]*serial.TypedMessage{
		serial.ToTypedMessage(&dispatcher.Config{}),
		serial.ToTypedMessage(&proxyman.OutboundConfig{}),
		serial.ToTypedMessage(&health.Config{SubjectSelector: []string{"wg"}, Passive: passive}),
	}, apps...)})
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { inst.Close() })
	return inst, health.FromInstance(inst)
}

// handshakeTimeout is a policy that gives up a dial inside the tunnel
// after d.
func handshakeTimeout(d time.Duration) *serial.TypedMessage {
	return serial.ToTypedMessage(&policy.Config{Level: map[uint32]*policy.Policy{
		0: {Timeout: &policy.Policy_Timeout{Handshake: &policy.Second{Value: uint32(d / time.Second)}}},
	}})
}

type observedPath struct {
	Failures       int              `json:"failures"`
	FailuresByKind map[string]int64 `json:"failures_by_kind"`
	ConnsOK        int64            `json:"conns_ok"`
	BytesDown      int64            `json:"bytes_down"`
}

func pathOf(t testing.TB, o *health.Observer, tag string) observedPath {
	t.Helper()
	var st struct {
		Paths []struct {
			Outbound string `json:"outbound"`
			observedPath
		} `json:"paths"`
	}
	if err := json.Unmarshal(o.Status(), &st); err != nil {
		t.Fatal(err)
	}
	for _, p := range st.Paths {
		if p.Outbound == tag {
			return p.observedPath
		}
	}
	return observedPath{}
}

// kinds lists the failures of tag by kind, such as "closed:1,no_response:1".
func kinds(t testing.TB, o *health.Observer, tag string) string {
	t.Helper()
	var out []string
	for k, n := range pathOf(t, o, tag).FailuresByKind {
		out = append(out, k+":"+strconv.FormatInt(n, 10))
	}
	slices.Sort(out)
	return strings.Join(out, ",")
}

// provenAt is when traffic last proved that tag works, zero if never.
func provenAt(t testing.TB, o *health.Observer, tag string) int64 {
	t.Helper()
	r, err := o.GetObservation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range r.(*observatory.ObservationResult).GetStatus() {
		if s.GetOutboundTag() == tag {
			return s.GetLastSeenTime()
		}
	}
	return 0
}

func waitFor(t testing.TB, within time.Duration, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(within); !cond(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("no %s within %v", what, within)
		}
	}
}

func tcpTo(addr netip.Addr, port uint16) net.Destination {
	return net.TCPDestination(net.IPAddress(addr.AsSlice()), net.Port(port))
}

func udpTo(addr netip.Addr, port uint16) net.Destination {
	return net.UDPDestination(net.IPAddress(addr.AsSlice()), net.Port(port))
}

// A tunnel whose server never answers fails its connections as a dial to
// the server that times out, TCP and UDP alike: nothing is held against
// their destinations.
func TestObservedTunnelThatNeverAnswers(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	r := startTestRelay(t, s.endpoint)
	r.freeze()
	passive := testPassive()
	passive.DialTimeout = int64(2 * time.Second)
	inst, o := newObservedInstance(t, passive)
	conf := testClientConf(s, priv, addr, testParams)
	conf.Peers[0].Endpoint = r.addr
	oh, _ := addTestClientConf(t, inst, "wg", conf)

	tcp := dial(oh, tcpTo(testServerAddr, 7))
	udp := dial(oh, udpTo(testServerAddr, 7))
	if err := udp.echo([]byte("ping"), 100*time.Millisecond); err == nil {
		t.Fatal("an echo came through a dead tunnel")
	}
	waitFor(t, 5*time.Second, "dial timeouts", func() bool {
		return pathOf(t, o, "wg").FailuresByKind["dial_timeout"] == 2
	})
	if err := goerrors.Join(tcp.close(), udp.close()); err != nil {
		t.Fatal(err)
	}
	if got := kinds(t, o, "wg"); got != "dial_timeout:2" {
		t.Fatalf("failures %s, want dial_timeout:2", got)
	}
}

// A working tunnel answers whatever its destinations do: a destination that
// refuses or drops the connection is a failure of that destination only,
// also while nothing else goes through the tunnel and only the handshake a
// waiting connection asks for answers it.
func TestObservedTunnelWithDeadDestinations(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	passive := testPassive()
	// The handshake that answers the dropped connection comes a second
	// after it, and may take seconds under the race detector.
	passive.DialTimeout = int64(6 * time.Second)
	passive.FirstByteTimeout = int64(time.Second)
	inst, o := newObservedInstance(t, passive)
	oh, _ := addTestClient(t, inst, "wg", s, priv, addr, testParams)

	// The stack of the server resets a connection to a closed port.
	refused := dial(oh, tcpTo(testServerAddr, 9))
	if err := refused.wait(); err != nil {
		t.Fatal(err)
	}
	if got := kinds(t, o, "wg"); got != "closed:1" {
		t.Fatalf("a refused destination: failures %s, want closed:1", got)
	}

	// Let the tunnel idle until the device may send a handshake again.
	time.Sleep(device.RekeyTimeout)
	// The server has no such address and drops the connection silently.
	start := time.Now()
	dropped := dial(oh, tcpTo(netip.MustParseAddr("10.9.0.99"), 80))
	waitFor(t, 5*time.Second, "no_response", func() bool {
		return pathOf(t, o, "wg").FailuresByKind["no_response"] == 1
	})
	time.Sleep(time.Until(start.Add(time.Duration(passive.DialTimeout) + time.Second)))
	if err := dropped.close(); err != nil {
		t.Fatal(err)
	}
	if got := kinds(t, o, "wg"); got != "closed:1,no_response:1" {
		t.Fatalf("failures %s, want closed:1,no_response:1 and nothing about the tunnel", got)
	}
}

// A tunnel that freezes after its first kilobytes stalls the connections
// inside it, as the stall of a connection to a server does.
func TestObservedTunnelThatFreezes(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	r := startTestRelay(t, s.endpoint)
	passive := testPassive()
	passive.StallTimeout = int64(time.Second)
	inst, o := newObservedInstance(t, passive)
	conf := testClientConf(s, priv, addr, testParams)
	conf.Peers[0].Endpoint = r.addr
	oh, _ := addTestClientConf(t, inst, "wg", conf)

	c := dial(oh, tcpTo(testServerAddr, 7))
	chunk := make([]byte, 1000)
	for range 3 {
		if err := c.echo(chunk, 15*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	r.freeze()
	if err := c.echo(chunk, 100*time.Millisecond); err == nil {
		t.Fatal("an echo came through a frozen tunnel")
	}
	waitFor(t, 5*time.Second, "a stall", func() bool {
		return pathOf(t, o, "wg").FailuresByKind["stall"] == 1
	})
	if err := c.close(); err != nil {
		t.Fatal(err)
	}
	if got := kinds(t, o, "wg"); got != "stall:1" {
		t.Fatalf("failures %s, want stall:1", got)
	}
}

// Connections that receive enough through a working tunnel prove that it
// works, TCP and UDP alike.
func TestObservedTunnelProvesItself(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	inst, o := newObservedInstance(t, testPassive())
	oh, _ := addTestClient(t, inst, "wg", s, priv, addr, testParams)

	chunk := make([]byte, 1000)
	for _, dest := range []net.Destination{tcpTo(testServerAddr, 7), udpTo(testServerAddr, 7)} {
		c := dial(oh, dest)
		for range testSuccessBytes / len(chunk) {
			if err := c.echo(chunk, 15*time.Second); err != nil {
				t.Fatal(dest, err)
			}
		}
		if err := c.close(); err != nil {
			t.Fatal(err)
		}
	}
	s.waitConns(t)
	if provenAt(t, o, "wg") == 0 {
		t.Fatal("the connections did not prove that the tunnel works")
	}
	p := pathOf(t, o, "wg")
	if p.Failures != 0 || len(p.FailuresByKind) != 0 || p.ConnsOK != 2 || p.BytesDown != 2*testSuccessBytes {
		t.Fatalf("path %+v, want 2 connections without failures that received %d bytes", p, 2*testSuccessBytes)
	}
}

// A wake whose first handshake is lost is no failure: the device repeats
// the handshake after device.RekeyTimeout, within dialTimeout. A connection
// inside the tunnel that gave up before the answer only waited too long.
func TestObservedTunnelWakeLosesItsHandshake(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	r := startTestRelay(t, s.endpoint)
	// testPassive leaves the repeated handshake room under the race
	// detector.
	inst, o := newObservedInstance(t, testPassive(), handshakeTimeout(2*time.Second))
	conf := testClientConf(s, priv, addr, testParams)
	conf.Peers[0].Endpoint = r.addr
	oh, h := addTestClientConf(t, inst, "wg", conf)

	// The junk packets and the initiation of the first handshake.
	jc := 3
	r.drop.Store(int64(jc + 1))
	start := time.Now()
	gaveUp := dial(oh, tcpTo(testServerAddr, 7))
	if err := gaveUp.echo([]byte("ping"), 15*time.Second); err == nil {
		t.Fatal("a connection came through before the handshake")
	}
	if err := gaveUp.wait(); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(start); waited < device.RekeyTimeout {
		t.Fatalf("the connection ended after %v, before the tunnel answered or failed", waited)
	}
	if err := tcpEcho(oh, "after the wake"); err != nil {
		t.Fatal(err)
	}
	s.waitConns(t)
	if got := kinds(t, o, "wg"); got != "" {
		t.Fatalf("a wake that repeated its handshake left failures %s", got)
	}
	if !h.Sleep() {
		t.Fatal("an idle outbound refused to sleep")
	}
}

// dialRecorder is the tracker of one tunnel dial; it hands over what the
// dial ended with.
type dialRecorder struct {
	dialed chan error
}

func (r *dialRecorder) Dialed(_ stdnet.Conn, err error, _, _ stats.Counter) (stats.Counter, stats.Counter) {
	r.dialed <- err
	return nil, nil
}

func (r *dialRecorder) Finished(error) {}

// A connection the observer gave up on asks the server for no more
// handshakes, each of which would start the retries of the device anew,
// even while it still waits inside the tunnel. Its dial ends as one that
// ended before the server answered, not as a server that never did.
func TestAbortedDialAsksForNoHandshake(t *testing.T) {
	rec := &dialRecorder{dialed: make(chan error, 1)}
	ctx, abort := context.WithCancel(context.Background())
	td := &tunnelDial{h: &Handler{}, tracker: rec, ctx: ctx, settled: make(chan struct{})}
	td.entered()
	abort()
	select {
	case err := <-rec.dialed:
		if !goerrors.Is(err, context.Canceled) {
			t.Fatalf("the dial ended with %v, want context.Canceled", err)
		}
	case <-time.After(askHandshakeAfter + time.Second):
		t.Fatal("the dial still asks for handshakes after the abort")
	}
}

// A connection the observer gives up on while it resolves its destination
// inside a dead tunnel ends at once: its lookup sends the server no more
// queries, and nothing holds the outbound back from sleeping, which ends
// what its device would still send.
func TestAbortedLookupLeavesTheDeadTunnel(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	r := startTestRelay(t, s.endpoint)
	inst, _ := newObservedInstance(t, testPassive())
	conf := testClientConf(s, priv, addr, testParams)
	conf.Peers[0].Endpoint = r.addr
	oh, h := addTestClientConf(t, inst, "wg", conf)
	// A UDP echo sets the session up and leaves nothing to close: a TCP
	// connection whose closing exchange the freeze cuts would retransmit
	// its FIN for minutes.
	u := dial(oh, udpTo(testServerAddr, 7))
	if err := u.echo([]byte("ping"), 15*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := u.close(); err != nil {
		t.Fatal(err)
	}

	r.freeze()
	before := r.sent.Load()
	// The name resolves inside the tunnel, whose session still has its
	// keys: each DNS query goes to the server as a datagram.
	c := dial(oh, net.TCPDestination(net.DomainAddress("dead.test"), 80))
	waitFor(t, 5*time.Second, "a DNS query", func() bool { return r.sent.Load() > before })
	sent := r.sent.Load()
	start := time.Now()
	c.cancel()
	if err := c.wait(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("the connection ended %v after the abort", took)
	}
	// A lookup asks the next DNS server once the previous one has not
	// answered for 5 s.
	time.Sleep(6 * time.Second)
	if n := r.sent.Load() - sent; n != 0 {
		t.Fatalf("%d datagrams to the server after the abort", n)
	}
	if !h.Sleep() {
		t.Fatal("the outbound refused to sleep once its only connection ended")
	}
}
