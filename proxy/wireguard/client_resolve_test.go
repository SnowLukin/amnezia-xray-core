package wireguard

import (
	"context"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/dns"
	"github.com/xtls/xray-core/transport/internet"
)

// fixedDNS resolves every name to ips.
type fixedDNS struct {
	dns.Client
	ips []net.IP
}

func (d fixedDNS) LookupIP(string, dns.IPOption) ([]net.IP, uint32, error) {
	return d.ips, 0, nil
}

// An endpoint name with A and AAAA records resolves as the sockopt
// domainStrategy of the outbound says.
func TestEndpointFollowsSockoptDomainStrategy(t *testing.T) {
	priv, _ := testKeys(t)
	_, peer := testKeys(t)
	conf := &DeviceConfig{
		IsClient:    true,
		SecretKey:   priv,
		Endpoint:    []string{"10.9.0.2"},
		Mtu:         1420,
		NoKernelTun: true,
		Peers: []*PeerConfig{{
			PublicKey:  peer,
			Endpoint:   "localhost:51820",
			AllowedIps: []string{"0.0.0.0/0"},
		}},
	}
	c := newBareClient(t, conf, fixedDNS{ips: []net.IP{net.ParseIP("::1"), {127, 0, 0, 1}}})
	c.template.streamSettings = &internet.MemoryStreamConfig{
		SocketSettings: &internet.SocketConfig{DomainStrategy: internet.DomainStrategy_USE_IP4},
	}
	for range 16 {
		h, err := c.wake(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		got, err := h.dev.IpcGet()
		h.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(got, "endpoint=127.0.0.1:51820\n") {
			t.Fatalf("UseIPv4 left the endpoint at\n%s", got)
		}
	}
}

func TestEndpointWithoutAddressesIsResolveError(t *testing.T) {
	resolve := makeResolveLocal(fixedDNS{}, &internet.MemoryStreamConfig{})
	if ip, err := resolve("empty.test"); err == nil {
		t.Fatalf("resolved to %v", ip)
	}
}

// A destination name inside the tunnel with a dead address and a live one
// connects to the live one, whichever comes first.
func TestDestinationTriesEveryAddress(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	// Each address gets its share of the handshake timeout, at least 2 s.
	inst, err := core.New(&core.Config{App: []*serial.TypedMessage{
		serial.ToTypedMessage(&proxyman.OutboundConfig{}),
		handshakeTimeout(4 * time.Second),
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { inst.Close() })
	oh, c := addTestClient(t, inst, "wg", s, priv, addr, testParams)
	roundTrip(t, oh, s)
	c.mu.Lock()
	h := c.h
	c.mu.Unlock()
	// refusingTun drops what goes to 10.9.0.99.
	h.tnet.cache.Cache("echo.test", []string{"10.9.0.99", testServerAddr.String()}, 300)

	for range 2 {
		conn := dial(oh, net.TCPDestination(net.DomainAddress("echo.test"), 7))
		err := conn.echo([]byte("ping"), 15*time.Second)
		conn.close()
		if err != nil {
			t.Fatal(err)
		}
	}
	s.waitConns(t)
}
