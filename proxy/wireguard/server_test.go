package wireguard

import (
	"context"
	"encoding/hex"
	stdnet "net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/proxyman"
	_ "github.com/xtls/xray-core/app/proxyman/inbound"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"
)

// testHeaderKeys are header protection keys, as the device takes them.
const (
	testHeaderKey      = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	testOtherHeaderKey = "1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100"
)

// echoDispatcher stands in for the routing of an inbound: it echoes every
// connection the inbound dispatches.
type echoDispatcher struct{}

func (echoDispatcher) Type() interface{} { return routing.DispatcherType() }
func (echoDispatcher) Start() error      { return nil }
func (echoDispatcher) Close() error      { return nil }

func (echoDispatcher) Dispatch(context.Context, net.Destination) (*transport.Link, error) {
	return nil, errors.New("not used by the inbound")
}

func (echoDispatcher) DispatchLink(ctx context.Context, dest net.Destination, link *transport.Link) error {
	if dest.Network == net.Network_UDP {
		// The flow of a datagram ends only when the inbound closes it: answer
		// one, and the inbound closes it on return.
		mb, err := link.Reader.ReadMultiBuffer()
		if err != nil {
			return err
		}
		return link.Writer.WriteMultiBuffer(mb)
	}
	return buf.Copy(link.Reader, link.Writer)
}

// startTestInbound runs a WireGuard inbound in an instance of its own on a
// free loopback port. Behind it, everything is echoed. It returns the
// endpoint and the public key of the inbound.
func startTestInbound(t testing.TB, params *AmneziaParameters, peers ...testPeer) (endpoint, pub string) {
	t.Helper()
	// The inbound listens on a port of its config; take one that is free.
	pc, err := stdnet.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := pc.LocalAddr().(*stdnet.UDPAddr).Port
	pc.Close()

	priv, pub := testKeys(t)
	users := make([]*protocol.User, 0, len(peers))
	for _, p := range peers {
		users = append(users, &protocol.User{
			Email: p.addr.String(),
			Account: serial.ToTypedMessage(&PeerConfig{
				PublicKey:  p.pub,
				AllowedIps: []string{netip.PrefixFrom(p.addr, p.addr.BitLen()).String()},
			}),
		})
	}
	inst, err := core.New(&core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&proxyman.InboundConfig{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { inst.Close() })
	if err := inst.AddFeature(echoDispatcher{}); err != nil {
		t.Fatal(err)
	}
	err = core.AddInboundHandler(inst, &core.InboundHandlerConfig{
		Tag: "wg-in",
		ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
			PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(net.Port(port))}},
			Listen:   net.NewIPOrDomain(net.LocalHostIP),
		}),
		ProxySettings: serial.ToTypedMessage(&DeviceConfig{
			SecretKey:  priv,
			Endpoint:   []string{testServerAddr.String()},
			Mtu:        1420,
			Users:      users,
			Parameters: params,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Start(); err != nil {
		t.Fatal(err)
	}
	return "127.0.0.1:" + net.Port(port).String(), pub
}

// addInboundClient adds an outbound that connects to the inbound at
// endpoint from addr.
func addInboundClient(t testing.TB, endpoint, serverPub, priv string, addr netip.Addr, params *AmneziaParameters) outbound.Handler {
	t.Helper()
	inst := newTestInstance(t)
	oh, _ := addTestClientConf(t, inst, "wg", &DeviceConfig{
		IsClient:    true,
		SecretKey:   priv,
		Endpoint:    []string{addr.String()},
		Mtu:         1420,
		NoKernelTun: true,
		Peers: []*PeerConfig{{
			PublicKey:  serverPub,
			Endpoint:   endpoint,
			AllowedIps: []string{"0.0.0.0/0"},
		}},
		Parameters: params,
	})
	return oh
}

// inboundParams are AmneziaWG parameters away from every default, header
// protection and random trailers included.
func inboundParams(headerKey string) *AmneziaParameters {
	return &AmneziaParameters{
		Jc: "4", Jmin: "40", Jmax: "70",
		S1: "20", S2: "78", S3: "15", S4: "25",
		H1: "100000-100100", H2: "200000-200100", H3: "300000-300100", H4: "400000-400100",
		HeaderProtectionKey: headerKey,
		RandomTrailers:      true,
	}
}

// TestInboundServesAmneziaClient connects an outbound with AmneziaWG
// parameters to an inbound with the same ones. The first exchange is one
// UDP datagram, which nothing retries: its answer is the first packet the
// device of the inbound reads, and must leave with the padding the client
// expects.
func TestInboundServesAmneziaClient(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	endpoint, serverPub := startTestInbound(t, inboundParams(testHeaderKey), testPeer{pub, addr})
	oh := addInboundClient(t, endpoint, serverPub, priv, addr, inboundParams(testHeaderKey))

	c := dial(oh, net.UDPDestination(net.IPAddress(testServerAddr.AsSlice()), 7))
	// UDP is not retried: the wait only bounds the test on a slow machine.
	err := c.echo([]byte("first datagram"), 15*time.Second)
	if cerr := c.close(); cerr != nil {
		t.Fatal(cerr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := tcpEcho(oh, "ping over tcp"); err != nil {
		t.Fatal(err)
	}
}

// TestInboundRefusesOtherHeaderKey changes only the header protection key
// of the inbound: if either side ignored it, the client would get through.
func TestInboundRefusesOtherHeaderKey(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	endpoint, serverPub := startTestInbound(t, inboundParams(testOtherHeaderKey), testPeer{pub, addr})
	oh := addInboundClient(t, endpoint, serverPub, priv, addr, inboundParams(testHeaderKey))

	c := dial(oh, net.UDPDestination(net.IPAddress(testServerAddr.AsSlice()), 7))
	err := c.echo([]byte("lost"), 2*time.Second)
	if cerr := c.close(); cerr != nil {
		t.Fatal(cerr)
	}
	if err == nil {
		t.Fatal("a client with another header protection key got an answer")
	}
}

func TestServerIPC(t *testing.T) {
	var pub [32]byte
	pub[0] = 1
	users := &sync.Map{}
	users.Store(pub, &protocol.MemoryUser{Account: &MemoryAccount{
		Pub:        pub,
		AllowedIPs: []netip.Prefix{netip.MustParsePrefix("10.9.0.2/32")},
		KeepAlive:  "25",
	}})
	s := &Server{conf: &DeviceConfig{SecretKey: "aa"}, users: users}
	peer := "public_key=" + hex.EncodeToString(pub[:]) + "\n" +
		"allowed_ip=10.9.0.2/32\npersistent_keepalive_interval=25\n"
	// An inbound without the awg block writes what it always wrote.
	if got := s.ipc(); got != "private_key=aa\n"+peer {
		t.Errorf("got %q", got)
	}

	s.conf.Parameters = &AmneziaParameters{Jc: "4", S4: "25", HeaderProtectionKey: "bb", DisableCookies: true}
	want := "private_key=aa\njc=4\ns4=25\nheader_protection_key=bb\ndisable_cookies=true\n" + peer
	if got := s.ipc(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
