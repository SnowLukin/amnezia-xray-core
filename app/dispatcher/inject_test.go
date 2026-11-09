//go:build !resilience_test

package dispatcher_test

import (
	"context"
	"sync/atomic"
	"testing"

	. "github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

type countedAddress struct {
	net.Address
	formatted *atomic.Int32
}

func (a countedAddress) String() string {
	a.formatted.Add(1)
	return a.Address.String()
}

type noDefaultOutbound struct{ outbound.Manager }

func (noDefaultOutbound) GetDefaultHandler() outbound.Handler { return nil }

func TestDispatchLinkDoesNotFormatDestination(t *testing.T) {
	var formatted atomic.Int32
	d := new(DefaultDispatcher)
	if err := d.Init(&Config{}, noDefaultOutbound{}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	reader, writer := pipe.New()
	destination := net.TCPDestination(countedAddress{net.DomainAddress("example.com"), &formatted}, 443)
	if err := d.DispatchLink(context.Background(), destination, &transport.Link{Reader: reader, Writer: writer}); err != nil {
		t.Fatal(err)
	}
	if n := formatted.Load(); n != 0 {
		t.Fatalf("destination formatted %d times", n)
	}
}
