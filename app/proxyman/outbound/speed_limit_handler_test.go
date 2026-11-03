package outbound_test

import (
	"context"
	"io"
	gonet "net"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/policy"
	"github.com/xtls/xray-core/app/proxyman"
	. "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	core "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

func TestOutboundSpeedLimitDownlink(t *testing.T) {
	const payload = 3 << 20
	listener, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write(make([]byte, payload))
	}()

	v, err := core.New(&core.Config{App: []*serial.TypedMessage{serial.ToTypedMessage(&policy.Config{})}})
	if err != nil {
		t.Fatal(err)
	}
	v.AddFeature(outbound.Manager(new(Manager)))
	ctx := context.WithValue(context.Background(), xrayKey, v)
	h, err := NewHandler(ctx, &core.OutboundHandlerConfig{
		Tag: "limited",
		SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
			SpeedLimit: &proxyman.SpeedLimit{DownlinkBytesPerSecond: 4 << 20},
		}),
		ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
	})
	if err != nil {
		t.Fatal(err)
	}

	port := listener.Addr().(*gonet.TCPAddr).Port
	target := net.TCPDestination(net.LocalHostIP, net.Port(port))
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{Target: target, Tag: "limited"}})
	upReader, upWriter := pipe.New(pipe.WithoutSizeLimit())
	downReader, downWriter := pipe.New(pipe.WithoutSizeLimit())
	defer upWriter.Close()
	go h.Dispatch(ctx, &transport.Link{Reader: upReader, Writer: downWriter})

	start := time.Now()
	var received int
	for received < payload {
		mb, err := downReader.ReadMultiBuffer()
		received += int(mb.Len())
		buf.ReleaseMulti(mb)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	if received != payload {
		t.Fatalf("received %d of %d bytes", received, payload)
	}
	// 4 MiB/s with a 1 MiB burst: the last 2 MiB take half a second.
	if elapsed < 400*time.Millisecond {
		t.Fatalf("3 MiB arrived in %v through a 4 MiB/s outbound", elapsed)
	}
}
