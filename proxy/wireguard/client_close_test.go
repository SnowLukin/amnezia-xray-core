package wireguard

import (
	"context"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/pipe"
)

// closeCtx is the context of a TCP connection to the echo of the test server.
func closeCtx() context.Context {
	return session.ContextWithOutbounds(context.Background(), []*session.Outbound{{Target: net.TCPDestination(net.IPAddress(testServerAddr.AsSlice()), 7)}})
}

// A connection that took its Handler before the outbound closed fails
// instead of building a device on the closed TUN: the upstream check that
// init finds the Handler closed (#6461) has to survive the lazy wrapper.
func TestProcessAfterCloseFails(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	c := newBareClient(t, testClientConf(s, priv, addr, testParams), nil)
	h, err := c.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	h.policyManager = policy.DefaultManager{}
	c.Close()
	upR, upW := pipe.New()
	upW.Close()
	_, downW := pipe.New()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Process on a closed outbound panicked: %v", r)
		}
	}()
	err = h.Process(closeCtx(), &transport.Link{Reader: upR, Writer: downW}, nil)
	c.release()
	if err == nil {
		t.Fatal("Process on a closed outbound succeeded")
	}
}

// The Handler as NewClient builds it on a kernel TUN takes its first
// connections at once: init creates the device under the lock, and Process
// reads it only through init. A gVisor TUN stands in for the kernel one.
func TestConcurrentFirstConnectionsOnAPlainHandler(t *testing.T) {
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	s := startTestServer(t, testParams, testPeer{pub, addr})
	conf := testClientConf(s, priv, addr, testParams)
	tn, tnet, _, err := CreateNetTUN([]netip.Addr{addr}, nil, 1420, true)
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{conf: conf, policyManager: policy.DefaultManager{}, streamSettings: &internet.MemoryStreamConfig{}, tun: tn, tnet: tnet}
	defer h.Close()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			upR, upW := pipe.New()
			downR, downW := pipe.New()
			go func() {
				upW.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("ping"))})
				downR.ReadMultiBufferTimeout(5 * time.Second)
				upW.Close()
			}()
			ctx, cancel := context.WithTimeout(closeCtx(), 10*time.Second)
			defer cancel()
			h.Process(ctx, &transport.Link{Reader: upR, Writer: downW}, nil)
		}()
	}
	wg.Wait()
}
