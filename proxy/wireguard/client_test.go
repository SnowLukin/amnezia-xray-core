package wireguard

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	goerrors "errors"
	"fmt"
	"io"
	stdnet "net"
	"net/netip"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amnezia-vpn/amneziawg-go/v3/device"
	"github.com/xtls/xray-core/app/proxyman"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/dns"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	_ "github.com/xtls/xray-core/transport/internet/tcp"
	"github.com/xtls/xray-core/transport/pipe"
	"golang.org/x/crypto/curve25519"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
)

// testParams are AmneziaWG parameters both ends of the tests use.
var testParams = &AmneziaParameters{
	Jc: "3", Jmin: "50", Jmax: "1000",
	S1: "20", S2: "78", S3: "15",
	H1: "39131278", H2: "832138185", H3: "1436957857", H4: "1635877746",
}

// testServerAddr is where the test server echoes, inside the tunnel.
var testServerAddr = netip.MustParseAddr("10.9.0.1")

// handshakeGap separates the handshakes of a device and of the next one: the
// server drops an initiation that comes within 20 ms of the previous one, and
// the client retries only after 5 s.
const handshakeGap = 50 * time.Millisecond

func testKeys(t testing.TB) (priv, pub string) {
	t.Helper()
	var k [32]byte
	if _, err := rand.Read(k[:]); err != nil {
		t.Fatal(err)
	}
	k[0] &= 248
	k[31] = k[31]&127 | 64
	p, err := curve25519.X25519(k[:], curve25519.Basepoint)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(k[:]), hex.EncodeToString(p)
}

type testPeer struct {
	pub  string
	addr netip.Addr
}

// testServer is an AmneziaWG peer on loopback. Behind it, a gVisor stack
// echoes TCP and UDP on port 7 of testServerAddr.
type testServer struct {
	endpoint string
	pub      string

	dev      *device.Device
	listener *gonet.TCPListener
	udp      *gonet.UDPConn
	wg       sync.WaitGroup

	mu    sync.Mutex
	conns map[stdnet.Conn]struct{}
	done  chan struct{} // closed when conns becomes empty
}

func startTestServer(t testing.TB, params *AmneziaParameters, peers ...testPeer) *testServer {
	t.Helper()
	conn, err := stdnet.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tun, _, stk, err := CreateNetTUN([]netip.Addr{testServerAddr}, nil, 1420, false)
	if err != nil {
		t.Fatal(err)
	}
	priv, pub := testKeys(t)
	rt := newReadyTun(tun)
	dev := device.NewDevice(rt, &bind{
		listenFunc: func() (net.PacketConn, error) { return conn, nil },
	}, device.NewLogger(device.LogLevelSilent, ""))
	var cfg strings.Builder
	cfg.WriteString("private_key=" + priv + "\n")
	writeAmneziaParams(&cfg, params)
	for _, p := range peers {
		cfg.WriteString("public_key=" + p.pub + "\n")
		cfg.WriteString("allowed_ip=" + netip.PrefixFrom(p.addr, p.addr.BitLen()).String() + "\n")
	}
	if err := dev.IpcSet(cfg.String()); err != nil {
		t.Fatal(err)
	}
	rt.open()
	if err := dev.Up(); err != nil {
		t.Fatal(err)
	}
	s := &testServer{endpoint: conn.LocalAddr().String(), pub: pub, dev: dev, conns: map[stdnet.Conn]struct{}{}}

	full := tcpip.FullAddress{NIC: 1, Addr: tcpip.AddrFrom4(testServerAddr.As4()), Port: 7}
	s.listener, err = gonet.ListenTCP(stk, full, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatal(err)
	}
	s.udp, err = gonet.DialUDP(stk, &full, nil, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatal(err)
	}
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		for {
			c, err := s.listener.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			if len(s.conns) == 0 {
				s.done = make(chan struct{})
			}
			s.conns[c] = struct{}{}
			s.mu.Unlock()
			s.wg.Go(func() {
				io.Copy(c, c)
				c.Close()
				s.mu.Lock()
				delete(s.conns, c)
				if len(s.conns) == 0 {
					close(s.done)
				}
				s.mu.Unlock()
			})
		}
	}()
	go func() {
		defer s.wg.Done()
		b := make([]byte, 2048)
		for {
			n, addr, err := s.udp.ReadFrom(b)
			if err != nil {
				return
			}
			s.udp.WriteTo(b[:n], addr)
		}
	}()
	t.Cleanup(s.close)
	return s
}

func (s *testServer) close() {
	s.listener.Close()
	s.udp.Close()
	s.closeConns()
	s.dev.Close()
	s.wg.Wait()
}

// closeConns ends the TCP connections whose client went away without
// closing them.
func (s *testServer) closeConns() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		c.Close()
	}
}

// waitConns waits until the clients have closed every TCP connection, so
// nothing is in flight inside the tunnel.
func (s *testServer) waitConns(t testing.TB) {
	t.Helper()
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("server connections did not end")
	}
}

func newTestInstance(t testing.TB) *core.Instance {
	t.Helper()
	inst, err := core.New(&core.Config{
		App: []*serial.TypedMessage{serial.ToTypedMessage(&proxyman.OutboundConfig{})},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { inst.Close() })
	return inst
}

// addTestClient adds a WireGuard outbound that connects to s from addr and
// returns it with its proxy.
func addTestClient(t testing.TB, inst *core.Instance, tag string, s *testServer, priv string, addr netip.Addr, params *AmneziaParameters) (outbound.Handler, *Handler) {
	t.Helper()
	return addTestClientConf(t, inst, tag, testClientConf(s, priv, addr, params))
}

// testClientConf is the config of a gVisor client outbound that connects to
// s from addr.
func testClientConf(s *testServer, priv string, addr netip.Addr, params *AmneziaParameters) *DeviceConfig {
	return &DeviceConfig{
		IsClient:    true,
		SecretKey:   priv,
		Endpoint:    []string{addr.String()},
		Mtu:         1420,
		NoKernelTun: true,
		Peers: []*PeerConfig{{
			PublicKey:  s.pub,
			Endpoint:   s.endpoint,
			AllowedIps: []string{"0.0.0.0/0"},
		}},
		Parameters: params,
	}
}

func addTestClientConf(t testing.TB, inst *core.Instance, tag string, conf *DeviceConfig) (outbound.Handler, *Handler) {
	t.Helper()
	err := core.AddOutboundHandler(inst, &core.OutboundHandlerConfig{
		Tag:            tag,
		SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{}),
		ProxySettings:  serial.ToTypedMessage(conf),
	})
	if err != nil {
		t.Fatal(err)
	}
	oh := inst.GetFeature(outbound.ManagerType()).(outbound.Manager).GetHandler(tag)
	return oh, oh.(proxy.GetOutbound).GetOutbound().(*Handler)
}

// testConn is one connection dispatched to an outbound.
type testConn struct {
	up     *pipe.Writer
	down   *pipe.Reader
	cancel context.CancelFunc
	done   chan struct{}
}

func dial(oh outbound.Handler, dest net.Destination) *testConn {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{Target: dest, Tag: oh.Tag()}})
	upReader, upWriter := pipe.New()
	downReader, downWriter := pipe.New()
	c := &testConn{up: upWriter, down: downReader, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(c.done)
		oh.Dispatch(ctx, &transport.Link{Reader: upReader, Writer: downWriter})
	}()
	return c
}

// echo sends payload and checks that it comes back within timeout.
func (c *testConn) echo(payload []byte, timeout time.Duration) error {
	if err := c.up.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(append([]byte(nil), payload...))}); err != nil {
		return err
	}
	var got []byte
	deadline := time.Now().Add(timeout)
	for len(got) < len(payload) {
		mb, err := c.down.ReadMultiBufferTimeout(time.Until(deadline))
		if err != nil {
			return fmt.Errorf("echo: got %d of %d bytes: %w", len(got), len(payload), err)
		}
		for _, b := range mb {
			got = append(got, b.Bytes()...)
		}
		buf.ReleaseMulti(mb)
	}
	if !bytes.Equal(got, payload) {
		return fmt.Errorf("echo: got %q, want %q", got, payload)
	}
	return nil
}

// close ends the connection and waits until the outbound has finished with it.
func (c *testConn) close() error {
	c.cancel()
	c.up.Close()
	return c.wait()
}

// wait waits until the outbound has finished with the connection.
func (c *testConn) wait() error {
	select {
	case <-c.done:
		return nil
	case <-time.After(15 * time.Second):
		return goerrors.New("outbound did not finish the connection")
	}
}

func tcpEcho(oh outbound.Handler, payload string) error {
	c := dial(oh, net.TCPDestination(net.IPAddress(testServerAddr.AsSlice()), 7))
	err := c.echo([]byte(payload), 15*time.Second)
	return goerrors.Join(err, c.close())
}

// roundTrip carries one TCP echo through the outbound.
func roundTrip(t testing.TB, oh outbound.Handler, s *testServer) {
	t.Helper()
	if err := tcpEcho(oh, "ping "+oh.Tag()); err != nil {
		t.Fatal(err)
	}
	s.waitConns(t)
}

// goroutineBase returns the goroutine count a test comes back to once its
// outbounds are closed. It logs first: the first log line starts a writer
// goroutine that outlives the test. Then it waits for the count to hold
// still, since the stacks of earlier tests may still be stopping.
func goroutineBase(t testing.TB) int {
	t.Helper()
	errors.LogWarning(context.Background(), "goroutine base of ", t.Name())
	n := runtime.NumGoroutine()
	held := time.Now()
	for deadline := held.Add(10 * time.Second); time.Since(held) < 200*time.Millisecond; {
		if time.Now().After(deadline) {
			t.Fatalf("the goroutine count does not settle: %d", n)
		}
		time.Sleep(20 * time.Millisecond)
		if m := runtime.NumGoroutine(); m != n {
			n, held = m, time.Now()
		}
	}
	return n
}

// waitGoroutines waits for the goroutine count to fall to want: stacks and
// devices stop some of their goroutines after Close returns.
func waitGoroutines(t testing.TB, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		n := runtime.NumGoroutine()
		if n <= want {
			return
		}
		if time.Now().After(deadline) {
			b := make([]byte, 1<<20)
			t.Fatalf("%d goroutines, want %d\n%s", n, want, b[:runtime.Stack(b, true)])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (h *Handler) holds() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.tnet != nil || h.dev != nil
}

func TestClientCreatesStackOnFirstUse(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	inst := newTestInstance(t)
	base := goroutineBase(t)

	oh, h := addTestClient(t, inst, "wg", s, priv, addr, testParams)
	if h.holds() {
		t.Fatal("a new outbound holds a stack or a device")
	}
	waitGoroutines(t, base)

	roundTrip(t, oh, s)
	if !h.holds() {
		t.Fatal("the outbound holds no stack after a connection")
	}
	// The stack and the device stay for the next connection.
	roundTrip(t, oh, s)

	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if h.holds() {
		t.Fatal("a closed outbound holds a stack or a device")
	}
	waitGoroutines(t, base)
}

func TestClientConcurrentFirstConnections(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	inst := newTestInstance(t)
	oh, h := addTestClient(t, inst, "wg", s, priv, addr, testParams)

	// Connections that find no stack must create one between them, not one
	// each.
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			if err := tcpEcho(oh, "ping "+strconv.Itoa(i)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	s.waitConns(t)
	if n := h.initCount(); n != 1 {
		t.Fatalf("4 connections created %d devices, want 1", n)
	}
	h.mu.Lock()
	dev := h.dev
	h.mu.Unlock()
	roundTrip(t, oh, s)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.dev != dev || h.inits != 1 {
		t.Fatal("the outbound replaced its device")
	}
}

func (h *Handler) initCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.inits
}

// slowDNS resolves the endpoint of an outbound to loopback after a delay, or
// fails.
type slowDNS struct {
	dns.Client
	delay   time.Duration
	err     error
	lookups atomic.Int32
}

func (d *slowDNS) LookupIP(string, dns.IPOption) ([]net.IP, uint32, error) {
	d.lookups.Add(1)
	time.Sleep(d.delay)
	if d.err != nil {
		return nil, 0, d.err
	}
	return []net.IP{{127, 0, 0, 1}}, 0, nil
}

// newResolvingClient returns a client outbound, outside any instance, that
// reaches s by the name localhost through d.
func newResolvingClient(t testing.TB, s *testServer, priv string, addr netip.Addr, d dns.Client) *Handler {
	t.Helper()
	_, port, err := stdnet.SplitHostPort(s.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{
		conf: &DeviceConfig{
			IsClient:    true,
			SecretKey:   priv,
			Mtu:         1420,
			NoKernelTun: true,
			Peers: []*PeerConfig{{
				PublicKey:  s.pub,
				Endpoint:   "localhost:" + port,
				AllowedIps: []string{"0.0.0.0/0"},
			}},
			Parameters: testParams,
		},
		dns:            d,
		streamSettings: &internet.MemoryStreamConfig{},
		localAddresses: []netip.Addr{addr},
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// waitInit waits until a connection is creating the stack of h.
func waitInit(t testing.TB, h *Handler) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		h.mu.Lock()
		running := h.initDone != nil
		h.mu.Unlock()
		if running {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no connection creates the stack")
		}
	}
}

type acquired struct {
	tnet *Net
	err  error
}

func acquireAsync(h *Handler, ctx context.Context) <-chan acquired {
	ch := make(chan acquired, 1)
	go func() {
		tnet, err := h.acquire(ctx)
		ch <- acquired{tnet, err}
	}()
	return ch
}

// TestClientWakeHoldsNoLock creates the stack with an endpoint that takes a
// second to resolve. Meanwhile, other connections, Sleep and Close must not
// wait for it.
func TestClientWakeHoldsNoLock(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	base := goroutineBase(t)
	d := &slowDNS{delay: time.Second}
	h := newResolvingClient(t, s, priv, addr, d)

	first := acquireAsync(h, context.Background())
	waitInit(t, h)
	start := time.Now()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.acquire(canceled); !goerrors.Is(err, context.Canceled) {
		t.Fatalf("a canceled connection waiting for the stack got %v", err)
	}
	if h.Sleep() {
		t.Fatal("the outbound slept while a connection created its stack")
	}
	if waited := time.Since(start); waited > 500*time.Millisecond {
		t.Fatalf("waited %v for the stack in creation", waited)
	}
	second := acquireAsync(h, context.Background())

	r1, r2 := <-first, <-second
	if r1.err != nil || r2.err != nil {
		t.Fatal(r1.err, r2.err)
	}
	if r1.tnet != r2.tnet || h.initCount() != 1 || d.lookups.Load() != 1 {
		t.Fatalf("2 connections created %d devices with %d lookups, want 1", h.initCount(), d.lookups.Load())
	}
	ctx, cancelDial := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelDial()
	conn, err := r1.tnet.DialContextTCPAddrPort(ctx, netip.AddrPortFrom(testServerAddr, 7))
	if err != nil {
		t.Fatal(err)
	}
	// An echo also makes sure that the server has the connection, which
	// waitConns waits for.
	conn.SetDeadline(time.Now().Add(15 * time.Second))
	got := make([]byte, 4)
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(conn, got); err != nil || string(got) != "ping" {
		t.Fatalf("echo: got %q, %v", got, err)
	}
	conn.Close()
	h.release()
	h.release()
	s.waitConns(t)
	if !h.Sleep() {
		t.Fatal("an idle outbound refused to sleep")
	}
	waitGoroutines(t, base)
}

func TestClientCloseDuringWake(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	base := goroutineBase(t)
	h := newResolvingClient(t, s, priv, addr, &slowDNS{delay: time.Second})

	first := acquireAsync(h, context.Background())
	waitInit(t, h)
	start := time.Now()
	h.Close()
	if waited := time.Since(start); waited > 500*time.Millisecond {
		t.Fatalf("Close waited %v for the stack in creation", waited)
	}
	if r := <-first; r.err == nil {
		t.Fatal("a connection got the stack of a closed outbound")
	}
	if h.holds() {
		t.Fatal("a closed outbound kept the stack created meanwhile")
	}
	waitGoroutines(t, base)
}

// TestClientFailedWakeIsShared checks that the connections waiting for a
// failing init share its error, and that the next ones get it for a while
// instead of trying again at once.
func TestClientFailedWakeIsShared(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	base := goroutineBase(t)
	d := &slowDNS{delay: 300 * time.Millisecond, err: goerrors.New("no such host")}
	h := newResolvingClient(t, s, priv, addr, d)

	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			if _, err := h.acquire(context.Background()); err == nil {
				t.Error("a connection got the stack of a failed init")
			}
		})
	}
	wg.Wait()
	if n := d.lookups.Load(); n != 1 {
		t.Fatalf("3 connections looked the endpoint up %d times, want 1", n)
	}
	if _, err := h.acquire(context.Background()); err == nil || d.lookups.Load() != 1 {
		t.Fatal("a connection right after a failed init tried again")
	}
	time.Sleep(initRetryDelay)
	if _, err := h.acquire(context.Background()); err == nil || d.lookups.Load() != 2 {
		t.Fatal("a connection after initRetryDelay did not try again")
	}
	if h.holds() {
		t.Fatal("a failed init kept a stack or a device")
	}
	waitGoroutines(t, base)
}

func TestClientRefusesConnectionsAfterClose(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	inst := newTestInstance(t)
	base := goroutineBase(t)

	oh, h := addTestClient(t, inst, "wg", s, priv, addr, testParams)
	h.Close()
	c := dial(oh, net.TCPDestination(net.IPAddress(testServerAddr.AsSlice()), 7))
	if err := c.wait(); err != nil {
		t.Fatal("a closed outbound accepted a connection")
	}
	if h.holds() {
		t.Fatal("a closed outbound created a stack")
	}
	waitGoroutines(t, base)
}

func TestClientFailedInitKeepsNothing(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	inst := newTestInstance(t)
	base := goroutineBase(t)

	// The device refuses an invalid parameter only once it is configured,
	// after the stack and the device exist.
	bad := &AmneziaParameters{Jc: "not a number"}
	oh, h := addTestClient(t, inst, "wg", s, priv, addr, bad)
	for range 2 {
		c := dial(oh, net.TCPDestination(net.IPAddress(testServerAddr.AsSlice()), 7))
		if err := c.wait(); err != nil {
			t.Fatal("a misconfigured outbound accepted a connection")
		}
		if h.holds() {
			t.Fatal("a failed init kept a stack or a device")
		}
		waitGoroutines(t, base)
		// Let the next connection try again instead of getting this error.
		time.Sleep(initRetryDelay)
	}
}

func TestNewClientRejectsDuplicateAddress(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	inst := newTestInstance(t)
	err := core.AddOutboundHandler(inst, &core.OutboundHandlerConfig{
		Tag:            "wg",
		SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{}),
		ProxySettings: serial.ToTypedMessage(&DeviceConfig{
			IsClient:    true,
			SecretKey:   priv,
			Endpoint:    []string{"10.9.0.2", "10.9.0.2/32"},
			Mtu:         1420,
			NoKernelTun: true,
			Peers:       []*PeerConfig{{PublicKey: s.pub, Endpoint: s.endpoint, AllowedIps: []string{"0.0.0.0/0"}}},
		}),
	})
	if err == nil {
		t.Fatal("an outbound with a duplicate address was created")
	}
}
