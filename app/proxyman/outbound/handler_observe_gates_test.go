package outbound

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/common/net"
	obs "github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
)

type gatedObserver struct {
	enabled bool
	starts  int
}

func (*gatedObserver) Observes(string) bool                            { return true }
func (o *gatedObserver) NeedsObservation(context.Context, string) bool { return o.enabled }
func (o *gatedObserver) StartDial(context.Context, string, net.Destination, func()) obs.DialTracker {
	o.starts++
	return nil
}

type gateProxy struct {
	ctx    context.Context
	dialer internet.Dialer
}

func (p *gateProxy) Process(ctx context.Context, _ *transport.Link, d internet.Dialer) error {
	p.ctx, p.dialer = ctx, d
	return nil
}

func TestHandlerDisabledConsumerBypassesContextAndDialer(t *testing.T) {
	ctx := context.Background()
	p := &gateProxy{}
	o := &gatedObserver{}
	h := &Handler{tag: "path", proxy: p, observer: o}
	if err := h.process(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if p.ctx != ctx || p.dialer != h || o.starts != 0 {
		t.Fatal("disabled consumer still wrapped processing")
	}
	allocations := testing.AllocsPerRun(100, func() {
		if err := h.process(ctx, nil); err != nil {
			panic(err)
		}
	})
	baseline := testing.AllocsPerRun(100, func() {
		if err := p.Process(ctx, nil, h); err != nil {
			panic(err)
		}
	})
	if allocations != baseline {
		t.Fatalf("disabled consumer adds %g allocations over baseline %g", allocations, baseline)
	}
	o.enabled = true
	if err := h.process(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if p.ctx == ctx || p.dialer == h {
		t.Fatal("enabled consumer lost observer context/dialer")
	}
}
