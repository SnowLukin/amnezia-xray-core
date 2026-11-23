package wireguard

import (
	"context"
	stdnet "net"
	"net/netip"
	"reflect"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/amnezia-vpn/amneziawg-go/v3/device"
	"github.com/amnezia-vpn/amneziawg-go/v3/tun"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy"
)

// What measure_conns_test.go needs of the outbound on the TCP/IP stack of Xray. The stack keeps what it holds to
// itself, so its connections are counted through reflection: that is for the measurements only.

// measureServeTunnel puts a device with a stack on conn, with a peer of peerPub at peerAddr, and serves the
// measurements behind it. It returns the public key of the device.
func measureServeTunnel(t testing.TB, conn stdnet.PacketConn, peerPub string, peerAddr netip.Addr) string {
	t.Helper()
	dev, _, err := CreateNetTUN([]netip.Addr{measureServerAddr}, nil, 1420, measureTarget{})
	if err != nil {
		t.Fatal(err)
	}
	priv, pub := testKeys(t)
	d := device.NewDevice(dev, &bind{
		listenFunc: func(context.Context) (stdnet.PacketConn, error) { return conn, nil },
	}, device.NewLogger(device.LogLevelSilent, ""))
	var ipc strings.Builder
	ipc.WriteString("private_key=" + priv + "\n")
	writeAmneziaParams(&ipc, measureParams)
	ipc.WriteString("public_key=" + peerPub + "\n")
	ipc.WriteString("allowed_ip=" + netip.PrefixFrom(peerAddr, peerAddr.BitLen()).String() + "\n")
	if err := d.IpcSet(ipc.String()); err != nil {
		t.Fatal(err)
	}
	if err := d.Up(); err != nil {
		t.Fatal(err)
	}
	return pub
}

// measureTarget serves what the stack of the server takes: TCP on measureTargetPort, UDP on measureUDPPort.
type measureTarget struct{}

func (measureTarget) HandleConnection(conn net.Conn, dest net.Destination) {
	switch {
	case dest.Network == net.Network_TCP && dest.Port == measureTargetPort:
		serveMeasureConn(conn)
	case dest.Network == net.Network_UDP && dest.Port == measureUDPPort:
		defer conn.Close()
		b := make([]byte, 2048)
		for {
			n, err := conn.Read(b)
			if err != nil {
				return
			}
			answerMeasureDatagram(b[:n], func(p []byte) error {
				_, err := conn.Write(p)
				return err
			})
		}
	default:
		conn.Close()
	}
}

// measureStackOf returns the stack of the outbound of oh, or nil.
func measureStackOf(oh outbound.Handler) measureStack {
	h, ok := oh.(proxy.GetOutbound).GetOutbound().(*Handler)
	if !ok {
		return nil
	}
	h.mu.Lock()
	dev := h.dev
	h.mu.Unlock()
	if dev == nil {
		return nil
	}
	// the device of amneziawg-go keeps the tun it was made with in tun.device
	f := reflect.ValueOf(dev).Elem().FieldByName("tun").FieldByName("device")
	d, _ := reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Interface().(tun.Device)
	if b, ok := d.(*blockup); ok {
		d = b.Device
	}
	nt, ok := d.(*netTun)
	if !ok {
		return nil
	}
	return reflectedStack{reflect.ValueOf(nt.stack).Elem()}
}

// reflectedStack is the stack of proxy/tun as reflection sees it.
type reflectedStack struct {
	v reflect.Value
}

func (s reflectedStack) field(name string) reflect.Value {
	f := s.v.FieldByName(name)
	return reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
}

func (s reflectedStack) states() map[string]int {
	mu := s.field("mu").Addr().Interface().(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()
	out := map[string]int{}
	for it := s.field("conns").MapRange(); it.Next(); {
		// the fields of a connection are read without its lock: a state may be one segment old
		c := it.Value().Elem()
		b := func(name string) bool { return c.FieldByName(name).Bool() }
		switch {
		case !b("established"):
			out["opening"]++
		case b("lingered"):
			// closed here, its FIN acknowledged or not, waiting for the other side to close: tcpLinger
			out["linger"]++
		case b("closed") && b("rcvFin"):
			out["last-ack"]++
		case b("closed"):
			out["fin-wait"]++
		case b("rcvFin"):
			out["close-wait"]++
		default:
			out["established"]++
		}
	}
	if n := s.field("sockets").Len(); n > 0 {
		out["udp"] = n
	}
	return out
}
