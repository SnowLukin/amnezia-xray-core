package wireguard

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	goerrors "errors"
	"fmt"
	"io"
	stdnet "net"
	"net/netip"
	"reflect"
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
)

// testParams are AmneziaWG parameters both ends of the tests use: testJc
// junk packets before each initiation.
var testParams = &AmneziaParameters{
	Jc: strconv.Itoa(testJc), Jmin: "50", Jmax: "1000",
	S1: "20", S2: "78", S3: "15",
	H1: "39131278", H2: "832138185", H3: "1436957857", H4: "1635877746",
}

const testJc = 3

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

// testServer is an AmneziaWG peer on loopback. Behind it, a stack echoes
// TCP and UDP on port 7 of testServerAddr.
type testServer struct {
	endpoint string
	pub      string

	dev *device.Device
	wg  sync.WaitGroup

	mu     sync.Mutex
	closed bool
	conns  map[stdnet.Conn]struct{}
	done   chan struct{} // closed when conns becomes empty
	flows  map[stdnet.Conn]struct{}
}

func startTestServer(t testing.TB, params *AmneziaParameters, peers ...testPeer) *testServer {
	t.Helper()
	conn, err := stdnet.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	priv, pub := testKeys(t)
	s := &testServer{endpoint: conn.LocalAddr().String(), pub: pub, conns: map[stdnet.Conn]struct{}{}, flows: map[stdnet.Conn]struct{}{}}
	tun, _, err := CreateNetTUN([]netip.Addr{testServerAddr}, nil, 1420, s)
	if err != nil {
		t.Fatal(err)
	}
	rt := newReadyTun(refusingTun{tun.(*netTun)})
	dev := device.NewDevice(rt, &bind{
		ctx:        context.Background(),
		listenFunc: func(context.Context) (net.PacketConn, error) { return conn, nil },
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
	s.dev = dev
	t.Cleanup(s.close)
	return s
}

// HandleConnection echoes what the stack hands it for port 7, TCP or UDP.
// Only the TCP connections count for waitConns: a UDP flow ends only when the
// stack forgets it.
func (s *testServer) HandleConnection(c stdnet.Conn, dest net.Destination) {
	if dest.Port != 7 || dest.Address != net.IPAddress(testServerAddr.AsSlice()) {
		c.Close()
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		c.Close()
		return
	}
	tcp := dest.Network == net.Network_TCP
	if tcp {
		if len(s.conns) == 0 {
			s.done = make(chan struct{})
		}
		s.conns[c] = struct{}{}
	} else {
		s.flows[c] = struct{}{}
	}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()
	if !tcp {
		defer c.Close()
		b := make([]byte, 2048)
		for {
			n, err := c.Read(b)
			if err != nil {
				return
			}
			c.Write(b[:n])
		}
	}
	io.Copy(c, c)
	c.Close()
	s.mu.Lock()
	delete(s.conns, c)
	if len(s.conns) == 0 {
		close(s.done)
	}
	s.mu.Unlock()
}

// refusingTun drops what goes to an address other than testServerAddr and
// resets a TCP connection to any port of it but 7, as a network without that
// host and a host with that port closed do: a stack with a handler takes
// every connection, for any address.
type refusingTun struct {
	*netTun
}

func (t refusingTun) Write(bufs [][]byte, offset int) (int, error) {
	for _, b := range bufs {
		p := b[offset:]
		if len(p) >= 20 && p[0]>>4 == 4 && netip.AddrFrom4([4]byte(p[16:20])) != testServerAddr {
			continue
		}
		if rst := resetFor(p); rst != nil {
			t.WritePacket(rst, nil)
			continue
		}
		t.stack.Input(p)
	}
	return len(bufs), nil
}

// resetFor returns the RST/ACK for p if p is an IPv4 SYN to a port other
// than 7, else nil.
func resetFor(p []byte) []byte {
	if len(p) < 40 || p[0]>>4 != 4 || p[9] != 6 {
		return nil
	}
	ihl := int(p[0]&0x0f) * 4
	if len(p) < ihl+20 {
		return nil
	}
	tcp := p[ihl:]
	if tcp[13]&0x12 != 0x02 || binary.BigEndian.Uint16(tcp[2:]) == 7 {
		return nil
	}
	r := make([]byte, 40)
	r[0], r[8], r[9] = 0x45, 64, 6
	binary.BigEndian.PutUint16(r[2:], 40)
	copy(r[12:16], p[16:20])
	copy(r[16:20], p[12:16])
	h := r[20:]
	copy(h[0:2], tcp[2:4])
	copy(h[2:4], tcp[0:2])
	binary.BigEndian.PutUint32(h[8:], binary.BigEndian.Uint32(tcp[4:])+1)
	h[12], h[13] = 5<<4, 0x14
	binary.BigEndian.PutUint16(r[10:], ^sum16(r[:20], 0))
	var pseudo uint32
	for i := 12; i < 20; i += 2 {
		pseudo += uint32(binary.BigEndian.Uint16(r[i:]))
	}
	pseudo += 6 + 20
	binary.BigEndian.PutUint16(h[16:], ^sum16(h, pseudo))
	return r
}

func sum16(b []byte, s uint32) uint16 {
	for i := 0; i+1 < len(b); i += 2 {
		s += uint32(binary.BigEndian.Uint16(b[i:]))
	}
	for s > 0xffff {
		s = s>>16 + s&0xffff
	}
	return uint16(s)
}

func (s *testServer) close() {
	s.mu.Lock()
	s.closed = true
	for c := range s.flows {
		c.Close()
	}
	s.mu.Unlock()
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
func addTestClient(t testing.TB, inst *core.Instance, tag string, s *testServer, priv string, addr netip.Addr, params *AmneziaParameters) (outbound.Handler, *lazyClient) {
	t.Helper()
	return addTestClientConf(t, inst, tag, testClientConf(s, priv, addr, params))
}

// testClientConf is the config of a client outbound on the built-in stack that connects to
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

func addTestClientConf(t testing.TB, inst *core.Instance, tag string, conf *DeviceConfig) (outbound.Handler, *lazyClient) {
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
	return oh, oh.(proxy.GetOutbound).GetOutbound().(*lazyClient)
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

// sourceDialer records the source address of each system dial.
type sourceDialer struct {
	internet.SystemDialer
	mu      sync.Mutex
	sources []net.Address
}

func (d *sourceDialer) Dial(ctx context.Context, src net.Address, dest net.Destination, sockopt *internet.SocketConfig) (net.Conn, error) {
	d.mu.Lock()
	d.sources = append(d.sources, src)
	d.mu.Unlock()
	return d.SystemDialer.Dial(ctx, src, dest, sockopt)
}

// TestClientSendThrough checks that the tunnel socket of every wake leaves
// from the sendThrough address of the outbound.
func TestClientSendThrough(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	inst := newTestInstance(t)
	d := &sourceDialer{SystemDialer: &internet.DefaultSystemDialer{}}
	internet.UseAlternativeSystemDialer(d)
	t.Cleanup(func() { internet.UseAlternativeSystemDialer(nil) })

	err := core.AddOutboundHandler(inst, &core.OutboundHandlerConfig{
		Tag:            "wg",
		SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{Via: net.NewIPOrDomain(net.LocalHostIP)}),
		ProxySettings:  serial.ToTypedMessage(testClientConf(s, priv, addr, testParams)),
	})
	if err != nil {
		t.Fatal(err)
	}
	oh := inst.GetFeature(outbound.ManagerType()).(outbound.Manager).GetHandler("wg")
	h := oh.(proxy.GetOutbound).GetOutbound().(*lazyClient)

	for range 2 {
		roundTrip(t, oh, s)
		if !h.Sleep() {
			t.Fatal("an idle outbound refused to sleep")
		}
		time.Sleep(handshakeGap)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.sources) == 0 {
		t.Fatal("the outbound made no system dial")
	}
	for i, src := range d.sources {
		if src == nil || src.String() != net.LocalHostIP.String() {
			t.Fatalf("dial %d left from %v, want %v", i, src, net.LocalHostIP)
		}
	}
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

// holds reports whether the outbound holds a stack and a device.
func (c *lazyClient) holds() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.h != nil
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
	awake := h.h
	h.mu.Unlock()
	roundTrip(t, oh, s)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.h != awake || h.inits != 1 {
		t.Fatal("the outbound replaced its device")
	}
}

func (c *lazyClient) initCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inits
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
func newResolvingClient(t testing.TB, s *testServer, priv string, addr netip.Addr, d dns.Client) *lazyClient {
	t.Helper()
	_, port, err := stdnet.SplitHostPort(s.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	return newBareClient(t, &DeviceConfig{
		IsClient:    true,
		SecretKey:   priv,
		Endpoint:    []string{addr.String()},
		Mtu:         1420,
		NoKernelTun: true,
		Peers: []*PeerConfig{{
			PublicKey:  s.pub,
			Endpoint:   "localhost:" + port,
			AllowedIps: []string{"0.0.0.0/0"},
		}},
		Parameters: testParams,
	}, d)
}

// newBareClient returns a client outbound on the built-in stack outside any instance, as
// NewClient would build it, that resolves its endpoint through d.
func newBareClient(t testing.TB, conf *DeviceConfig, d dns.Client) *lazyClient {
	t.Helper()
	addrs := make([]netip.Addr, len(conf.Endpoint))
	for i, a := range conf.Endpoint {
		addrs[i] = netip.MustParseAddr(a)
	}
	tun, _, err := deferNetTUN(addrs, nil, int(conf.Mtu))
	if err != nil {
		t.Fatal(err)
	}
	ob, err := newOutbound(&Handler{conf: conf, dns: d, streamSettings: &internet.MemoryStreamConfig{}, tun: tun})
	if err != nil {
		t.Fatal(err)
	}
	c := ob.(*lazyClient)
	t.Cleanup(func() { c.Close() })
	return c
}

// waitInit waits until a connection is creating the stack of h.
func waitInit(t testing.TB, h *lazyClient) {
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
	h   *Handler
	err error
}

func acquireAsync(c *lazyClient, ctx context.Context) <-chan acquired {
	ch := make(chan acquired, 1)
	go func() {
		h, err := c.acquire(ctx)
		ch <- acquired{h, err}
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
	if r1.h != r2.h || h.initCount() != 1 || d.lookups.Load() != 1 {
		t.Fatalf("2 connections created %d devices with %d lookups, want 1", h.initCount(), d.lookups.Load())
	}
	ctx, cancelDial := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelDial()
	conn, err := r1.h.tnet.DialContextTCPAddrPort(ctx, netip.AddrPortFrom(testServerAddr, 7))
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

// An awake outbound keeps nothing of the connection that woke it, such as
// its inbound, until it sleeps.
func TestWakeKeepsNoValuesOfTheConnection(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	c := newBareClient(t, testClientConf(s, priv, addr, testParams), nil)
	gone := make(chan struct{})
	func() {
		inbound := &session.Inbound{Tag: "first"}
		runtime.AddCleanup(inbound, func(ch chan struct{}) { close(ch) }, gone)
		ctx := session.ContextWithInbound(context.Background(), inbound)
		ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{Target: net.TCPDestination(net.IPAddress(testServerAddr.AsSlice()), 7)}})
		if _, err := c.acquire(ctx); err != nil {
			t.Fatal(err)
		}
		c.release()
	}()
	if !c.holds() {
		t.Fatal("the outbound holds no device after a wake")
	}
	for range 50 {
		runtime.GC()
		select {
		case <-gone:
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatal("the awake outbound keeps the inbound of the connection that woke it")
}

// TestWakeCopiesEveryField fails when the Handler gains a field that wake
// neither copies from the Handler NewClient built nor sets for the wake, as
// a rebase onto a newer upstream may bring.
func TestWakeCopiesEveryField(t *testing.T) {
	known := map[string]bool{
		// Copied by wake.
		"conf": true, "policyManager": true, "dns": true,
		"streamSettings": true, "uplinkCounter": true, "downlinkCounter": true,
		// Of the wake itself.
		"tun": true, "tnet": true, "dev": true, "mu": true, "recv": true,
	}
	ht := reflect.TypeFor[Handler]()
	for i := range ht.NumField() {
		if name := ht.Field(i).Name; !known[name] {
			t.Errorf("Handler.%s is new: copy it in lazyClient.wake, or list it here", name)
		}
	}
}
