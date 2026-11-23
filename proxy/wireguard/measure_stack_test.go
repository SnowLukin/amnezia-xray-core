package wireguard

import (
	stdnet "net"
	"net/netip"
	"strings"
	"testing"

	"github.com/amnezia-vpn/amneziawg-go/v3/device"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
)

// measure_conns_test.go on a gVisor stack. The server keeps the TCP options of gVisor whatever CreateNetTUN sets.

func measureServeTunnel(t testing.TB, conn stdnet.PacketConn, peerPub string, peerAddr netip.Addr) string {
	t.Helper()
	tun, _, stk, err := CreateNetTUN([]netip.Addr{measureServerAddr}, nil, 1420, false)
	if err != nil {
		t.Fatal(err)
	}
	setGVisorTCPDefaults(t, stk)
	priv, pub := testKeys(t)
	rt := newReadyTun(tun)
	dev := device.NewDevice(rt, &bind{
		listenFunc: func() (net.PacketConn, error) { return conn, nil },
	}, device.NewLogger(device.LogLevelSilent, ""))
	var ipc strings.Builder
	ipc.WriteString("private_key=" + priv + "\n")
	writeAmneziaParams(&ipc, measureParams)
	ipc.WriteString("public_key=" + peerPub + "\n")
	ipc.WriteString("allowed_ip=" + netip.PrefixFrom(peerAddr, peerAddr.BitLen()).String() + "\n")
	if err := dev.IpcSet(ipc.String()); err != nil {
		t.Fatal(err)
	}
	rt.open()
	if err := dev.Up(); err != nil {
		t.Fatal(err)
	}
	full := tcpip.FullAddress{NIC: 1, Addr: tcpip.AddrFrom4(measureServerAddr.As4()), Port: measureTargetPort}
	inside, err := gonet.ListenTCP(stk, full, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatal(err)
	}
	go serveMeasureTarget(inside)
	full.Port = measureUDPPort
	udp, err := gonet.DialUDP(stk, &full, nil, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatal(err)
	}
	go serveMeasurePackets(udp)
	return pub
}

// setGVisorTCPDefaults sets the TCP options CreateNetTUN may tune back to what a new gVisor stack has.
func setGVisorTCPDefaults(t testing.TB, stk *stack.Stack) {
	t.Helper()
	opts := []tcpip.SettableTransportProtocolOption{
		&tcpip.TCPReceiveBufferSizeRangeOption{Min: tcp.MinBufferSize, Default: tcp.DefaultReceiveBufferSize, Max: tcp.MaxBufferSize},
		&tcpip.TCPSendBufferSizeRangeOption{Min: tcp.MinBufferSize, Default: tcp.DefaultSendBufferSize, Max: tcp.MaxBufferSize},
		new(tcpip.TCPModerateReceiveBufferOption(true)),
		new(tcpip.TCPTimeWaitTimeoutOption(tcp.DefaultTCPTimeWaitTimeout)),
		new(tcpip.TCPLingerTimeoutOption(tcp.DefaultTCPLingerTimeout)),
	}
	for _, o := range opts {
		if err := stk.SetTransportProtocolOption(tcp.ProtocolNumber, o); err != nil {
			t.Fatalf("%T: %v", o, err)
		}
	}
}

func measureSetGVisorDefaults(t testing.TB, oh outbound.Handler) bool {
	stk := measuredGVisor(oh)
	if stk == nil {
		return false
	}
	setGVisorTCPDefaults(t, stk)
	return true
}

func measureStackOf(oh outbound.Handler) measureStack {
	if stk := measuredGVisor(oh); stk != nil {
		return gvisorStates{stk}
	}
	return nil
}

func measuredGVisor(oh outbound.Handler) *stack.Stack {
	c, ok := oh.(proxy.GetOutbound).GetOutbound().(*lazyClient)
	if !ok {
		return nil
	}
	c.mu.Lock()
	h := c.h
	c.mu.Unlock()
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if rt, ok := h.tun.(*readyTun); ok {
		if nt, ok := rt.Device.(*netTun); ok {
			return nt.stack
		}
	}
	return nil
}

type gvisorStates struct{ stk *stack.Stack }

func (s gvisorStates) states() map[string]int {
	out := map[string]int{}
	for _, ep := range s.stk.RegisteredEndpoints() {
		if te, ok := ep.(*tcp.Endpoint); ok {
			out[tcp.EndpointState(te.State()).String()]++
		} else {
			out["udp"]++
		}
	}
	return out
}
