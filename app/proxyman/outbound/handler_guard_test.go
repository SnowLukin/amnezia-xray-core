package outbound

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/pipe"
)

type dispatchMode struct{ denied atomic.Bool }

func (*dispatchMode) Observes(string) bool { return true }
func (*dispatchMode) StartDial(context.Context, string, net.Destination, func()) outbound.DialTracker {
	return nil
}
func (m *dispatchMode) AllowDispatch(_ context.Context, _ string, handlerProbeOnly bool) bool {
	return !handlerProbeOnly && !m.denied.Load()
}

type countedProxy struct{ calls atomic.Int64 }

func (p *countedProxy) Process(context.Context, *transport.Link, internet.Dialer) error {
	p.calls.Add(1)
	return nil
}

func TestDispatchGuardRejectsSelectedTagAfterModeChange(t *testing.T) {
	mode := &dispatchMode{}
	proxy := &countedProxy{}
	handler := &Handler{tag: "candidate", observer: mode, proxy: proxy}
	mode.denied.Store(true)
	reader, upstream := pipe.New()
	downstream, writer := pipe.New()
	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{Target: net.TCPDestination(net.DomainAddress("user.test"), 443)}})
	handler.Dispatch(ctx, &transport.Link{Reader: reader, Writer: writer})
	if proxy.calls.Load() != 0 {
		t.Fatal("rejected link reached proxy")
	}
	if _, err := downstream.ReadMultiBuffer(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("downlink remains open: %v", err)
	}
	packet := buf.FromBytes([]byte("user data"))
	if err := upstream.WriteMultiBuffer(buf.MultiBuffer{packet}); err == nil {
		t.Fatal("uplink remains open")
	}
}

func TestDispatchGuardPermitsOrdinaryHandler(t *testing.T) {
	proxy := &countedProxy{}
	handler := &Handler{tag: "real", observer: &dispatchMode{}, proxy: proxy}
	reader, upstream := pipe.New()
	downstream, writer := pipe.New()
	defer upstream.Close()
	defer downstream.Interrupt()
	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{Target: net.TCPDestination(net.DomainAddress("user.test"), 443)}})
	handler.Dispatch(ctx, &transport.Link{Reader: reader, Writer: writer})
	if proxy.calls.Load() != 1 {
		t.Fatal("ordinary link did not reach proxy")
	}
}

func TestDispatchGuardRejectsOldProbeCredentialsAfterPromotion(t *testing.T) {
	mode := &dispatchMode{}
	proxy := &countedProxy{}
	old := &Handler{tag: "same-tag", observer: mode, proxy: proxy, isProbeOnly: true}
	reader, upstream := pipe.New()
	downstream, writer := pipe.New()
	defer upstream.Close()
	defer downstream.Interrupt()
	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{Target: net.TCPDestination(net.DomainAddress("user.test"), 443)}})
	old.Dispatch(ctx, &transport.Link{Reader: reader, Writer: writer})
	if proxy.calls.Load() != 0 {
		t.Fatal("old probe credentials received a user link after promotion")
	}
}

func TestDispatchGuardProbeCredentialsFailClosedWithoutObserver(t *testing.T) {
	proxy := &countedProxy{}
	handler := &Handler{tag: "candidate", proxy: proxy, isProbeOnly: true}
	reader, upstream := pipe.New()
	downstream, writer := pipe.New()
	defer upstream.Close()
	defer downstream.Interrupt()
	handler.Dispatch(context.Background(), &transport.Link{Reader: reader, Writer: writer})
	if proxy.calls.Load() != 0 {
		t.Fatal("probe credentials without guard received user link")
	}
}
