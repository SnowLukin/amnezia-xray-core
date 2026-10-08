package wireguard

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	goerrors "errors"
	"fmt"
	stdnet "net"
	"net/netip"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amnezia-vpn/amneziawg-go/v3/device"
	"github.com/xtls/xray-core/app/proxyman"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/transport"
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
// answers TCP and UDP on port 7 of testServerAddr: TCP with one echo of the
// first read, then it closes; UDP with an echo of every datagram.
type testServer struct {
	endpoint string
	pub      string

	dev      *device.Device
	listener *gonet.TCPListener
	udp      *gonet.UDPConn
	wg       sync.WaitGroup
	conns    sync.WaitGroup
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
	dev := device.NewDevice(tun, &bind{
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
	if err := dev.Up(); err != nil {
		t.Fatal(err)
	}
	s := &testServer{endpoint: conn.LocalAddr().String(), pub: pub, dev: dev}

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
			s.conns.Add(1)
			go func() {
				defer s.conns.Done()
				defer c.Close()
				b := make([]byte, 2048)
				n, err := c.Read(b)
				if err == nil {
					c.Write(b[:n])
				}
			}()
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
	s.conns.Wait()
	s.dev.Close()
	s.wg.Wait()
}

// waitConns waits until the server has answered every TCP connection, so
// nothing is in flight inside the tunnel.
func (s *testServer) waitConns(t testing.TB) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		s.conns.Wait()
		close(done)
	}()
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
	conf := &DeviceConfig{
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

// echo sends payload and checks that it comes back.
func (c *testConn) echo(payload []byte) error {
	if err := c.up.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(append([]byte(nil), payload...))}); err != nil {
		return err
	}
	var got []byte
	deadline := time.Now().Add(15 * time.Second)
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
	err := c.echo([]byte(payload))
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
	base := runtime.NumGoroutine()

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
	h.mu.Lock()
	dev := h.dev
	h.mu.Unlock()
	roundTrip(t, oh, s)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.dev != dev {
		t.Fatal("the outbound replaced its device")
	}
}

func TestClientRefusesConnectionsAfterClose(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	inst := newTestInstance(t)
	base := runtime.NumGoroutine()

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
	base := runtime.NumGoroutine()

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
