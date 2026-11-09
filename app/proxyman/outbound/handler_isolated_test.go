package outbound

import (
	"context"
	"errors"
	"testing"

	"github.com/xtls/xray-core/common/mux"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

// recordingPicker refuses every worker and records which kind was asked for.
type recordingPicker struct{ shared, isolated int }

func (p *recordingPicker) PickAvailable() (*mux.ClientWorker, error) {
	p.shared++
	return nil, errors.New("no worker in the test")
}

func (p *recordingPicker) PickIsolated(context.Context) (*mux.ClientWorker, error) {
	p.isolated++
	return nil, errors.New("no worker in the test")
}

func TestSkipMuxReuseIsolatesEachNetwork(t *testing.T) {
	for _, dest := range []net.Destination{
		net.TCPDestination(net.DomainAddress("probe.test"), 443),
		net.UDPDestination(net.DomainAddress("probe.test"), 53),
	} {
		muxPicker, xudpPicker := &recordingPicker{}, &recordingPicker{}
		h := &Handler{
			tag:  "mux",
			mux:  &mux.ClientManager{Enabled: true, Picker: muxPicker},
			xudp: &mux.ClientManager{Enabled: true, Picker: xudpPicker},
		}
		ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{Target: dest}})
		ctx = session.ContextWithContent(ctx, &session.Content{SkipMuxReuse: true})
		reader, upstream := pipe.New()
		downstream, writer := pipe.New()
		h.Dispatch(ctx, &transport.Link{Reader: reader, Writer: writer})
		upstream.Close()
		downstream.Interrupt()
		if muxPicker.shared+xudpPicker.shared != 0 || muxPicker.isolated+xudpPicker.isolated != 1 {
			t.Fatalf("%v: mux shared %d isolated %d, xudp shared %d isolated %d", dest.Network,
				muxPicker.shared, muxPicker.isolated, xudpPicker.shared, xudpPicker.isolated)
		}
		if dest.Network == net.Network_UDP && xudpPicker.isolated != 1 {
			t.Fatal("an isolated UDP request left the XUDP manager")
		}
	}
}
