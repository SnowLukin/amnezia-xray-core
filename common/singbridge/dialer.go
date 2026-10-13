package singbridge

import "github.com/xtls/xray-core/common/safe"

import (
	"context"
	"os"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/net/cnc"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/pipe"
)

var _ N.Dialer = (*XrayDialer)(nil)

type XrayDialer struct {
	internet.Dialer
}

func NewDialer(dialer internet.Dialer) *XrayDialer {
	return &XrayDialer{dialer}
}

func (d *XrayDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	dest, err := ToDestination(destination, ToNetwork(network))
	if err != nil {
		return nil, err
	}
	return d.Dialer.Dial(ctx, dest)
}

func (d *XrayDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, os.ErrInvalid
}

type XrayOutboundDialer struct {
	outbound proxy.Outbound
	dialer   internet.Dialer
}

func NewOutboundDialer(outbound proxy.Outbound, dialer internet.Dialer) *XrayOutboundDialer {
	return &XrayOutboundDialer{outbound, dialer}
}

func (d *XrayOutboundDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	dest, err := ToDestination(destination, ToNetwork(network))
	if err != nil {
		return nil, err
	}
	outbounds := session.OutboundsFromContext(ctx)
	if len(outbounds) == 0 {
		outbounds = []*session.Outbound{{}}
		ctx = session.ContextWithOutbounds(ctx, outbounds)
	}
	ob := outbounds[len(outbounds)-1]
	ob.Target = dest

	opts := []pipe.Option{pipe.WithSizeLimit(64 * 1024)}
	uplinkReader, uplinkWriter := pipe.New(opts...)
	downlinkReader, downlinkWriter := pipe.New(opts...)
	conn := cnc.NewConnection(cnc.ConnectionInputMulti(downlinkWriter), cnc.ConnectionOutputMulti(uplinkReader))
	go safe.Run(safe.Connection, "common/singbridge/dialer.go:d.DialContext", safe.Closer(downlinkReader, uplinkWriter), func() func() {
		safeFn := d.outbound.Process
		safeArg0 := ctx
		safeArg1 := &transport.Link{Reader: downlinkReader, Writer: uplinkWriter}
		safeArg2 := d.dialer
		return func() { safeFn(safeArg0, safeArg1, safeArg2) }
	}())
	return conn, nil
}

func (d *XrayOutboundDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, os.ErrInvalid
}
