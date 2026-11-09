package wireguard

import (
	"context"
	"net/netip"
	"strings"
	"testing"

	"github.com/amnezia-vpn/amneziawg-go/v3/device"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"google.golang.org/protobuf/proto"
)

// idleDevice is a device that never comes up, for the IPC parser alone.
func idleDevice(t *testing.T) *device.Device {
	t.Helper()
	tn, _, err := CreateNetTUN([]netip.Addr{netip.MustParseAddr("10.9.0.2")}, nil, 1420, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Without its first event the device stays down and never opens the bind.
	<-tn.Events()
	dev := device.NewDevice(tn, &bind{listenFunc: func(context.Context) (net.PacketConn, error) {
		t.Error("the device opened its bind")
		return nil, context.Canceled
	}}, device.NewLogger(device.LogLevelSilent, ""))
	t.Cleanup(dev.Close)
	return dev
}

// TestAmneziaTemplateAgreesWithTheDevice checks ValidateAmneziaTemplate
// against the parser of the library: a tag the library gains or drops, or a
// value it reads differently, fails here.
func TestAmneziaTemplateAgreesWithTheDevice(t *testing.T) {
	dev := idleDevice(t)
	specs := []string{
		"",
		"no tags at all",
		"<b 0xc70000000108><r 16><t>",
		"<b 0x0001><r 24>",
		"<r 32>",
		"<b 0x17030300><r 40>",
		"<b c700>", "<b 0x>", "<b>", "<b 0xc7000>", "<b 0xzz>",
		"<t>", "<t 5>", "<d>", "<ds>", "<dz 2>", "<dz>", "<dz x>",
		"<r>", "<r x>", "<r -1>", "<rc 8>", "<rc>", "<rd 8>", "<rd 1.5>",
		"<c>", "<b 0xc70000000108><r 16><c><t>", "<c><r 8>",
		"<>", "< >", "<r 16", "<r 16> <", "r 16>", "<r 16 extra>",
		"<  r   16  >", "<R 16>", "<b 0x01><x><y>",
	}
	for a := 'a'; a <= 'z'; a++ {
		specs = append(specs, "<"+string(a)+" 10>")
		for b := 'a'; b <= 'z'; b++ {
			specs = append(specs, "<"+string(a)+string(b)+" 10>")
			for c := 'a'; c <= 'z'; c++ {
				specs = append(specs, "<"+string(a)+string(b)+string(c)+" 10>")
			}
		}
	}
	for _, spec := range specs {
		libErr := dev.IpcSet("i1=" + spec + "\n")
		err := ValidateAmneziaTemplate(spec)
		if (libErr == nil) != (err == nil) {
			t.Errorf("%q: the device says %v, ValidateAmneziaTemplate says %v", spec, libErr, err)
		}
	}
}

func TestAmneziaTemplateNamesTheTag(t *testing.T) {
	err := ValidateAmneziaTemplate("<b 0x01><c><r 8>")
	if err == nil || !strings.Contains(err.Error(), "<c>") {
		t.Fatalf("got %v, want an error about <c>", err)
	}
}

// A client whose template the device would refuse fails as the outbound is
// created, not at its first connection.
func TestClientRefusesUnknownTemplateTag(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	inst := newTestInstance(t)
	for _, field := range []string{"i1", "i5"} {
		params := proto.Clone(testParams).(*AmneziaParameters)
		bad := "<b 0xc70000000108><r 16><c><t>"
		if field == "i1" {
			params.I1 = bad
		} else {
			params.I5 = bad
		}
		err := addOutbound(inst, "wg-"+field, testClientConf(s, priv, addr, params))
		if err == nil || !strings.Contains(err.Error(), field) || !strings.Contains(err.Error(), "<c>") {
			t.Errorf("%s: adding the outbound returned %v, want an error about %s and <c>", field, err, field)
		}
	}
	params := proto.Clone(testParams).(*AmneziaParameters)
	params.I1 = "<b 0xc70000000108><r 16><t>"
	if err := addOutbound(inst, "wg-ok", testClientConf(s, priv, addr, params)); err != nil {
		t.Fatal(err)
	}
}

func addOutbound(inst *core.Instance, tag string, conf *DeviceConfig) error {
	return core.AddOutboundHandler(inst, &core.OutboundHandlerConfig{
		Tag:            tag,
		SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{}),
		ProxySettings:  serial.ToTypedMessage(conf),
	})
}
