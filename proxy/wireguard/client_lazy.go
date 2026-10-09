package wireguard

import (
	"context"
	goerrors "errors"
	"net/netip"
	"slices"
	"sync"
	"time"

	"github.com/amnezia-vpn/amneziawg-go/v3/tun"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

// A client outbound on the gVisor stack creates its stack and its device with
// its first connection, and Sleep releases both while no connection uses
// them; the next connection creates them again. About 65 goroutines and half
// a megabyte on a 14-core machine are held only while the outbound is in use.
//
// This lives here, apart from client.go, so that the Handler keeps following
// upstream. NewClient builds the Handler as upstream does, and on the gVisor
// path wraps it in a lazyClient: each wake copies that Handler into a new one
// with a stack of its own and brings its device up, and only then lets it
// process connections. A Handler thus keeps one stack and one device for its
// whole life. A client on a kernel TUN stays the Handler alone, with its
// interface from load to Close; it does not sleep.

// lazyNetTUN stands in for the TUN of CreateNetTUN in the Handler NewClient
// builds on the gVisor path, and keeps the arguments for the wakes. It is
// never used as a TUN.
type lazyNetTUN struct {
	tun.Device
	localAddresses, dnsServers []netip.Addr
	mtu                        int
	handleLocal                bool
}

// deferNetTUN takes the arguments of CreateNetTUN but leaves the stack to
// the first connection.
func deferNetTUN(localAddresses, dnsServers []netip.Addr, mtu int, handleLocal bool) (tun.Device, *Net, *stack.Stack, error) {
	// The stack refuses an address it already has, whatever its zone, and
	// fails on nothing else: check here to keep failing at load.
	for i, addr := range localAddresses {
		if slices.ContainsFunc(localAddresses[:i], func(a netip.Addr) bool { return a.WithZone("") == addr.WithZone("") }) {
			return nil, nil, nil, errors.New("duplicate address ", addr)
		}
	}
	return &lazyNetTUN{localAddresses: localAddresses, dnsServers: dnsServers, mtu: mtu, handleLocal: handleLocal}, nil, nil, nil
}

// newOutbound returns the Handler NewClient built, or on the gVisor path the
// lazyClient around it.
func newOutbound(h *Handler) (proxy.Outbound, error) {
	args, ok := h.tun.(*lazyNetTUN)
	if !ok {
		return h, nil
	}
	h.tun = nil
	return &lazyClient{template: h, args: args, server: tunnelServer(h.conf)}, nil
}

var errOutboundClosed = goerrors.New("outbound closed")

// initRetryDelay is how long connections fail with the error of the last wake
// before one tries again.
const initRetryDelay = time.Second

// lazyClient is a client outbound on the gVisor stack. It implements
// outbound.Sleeper; the Handler on a kernel TUN does not, so observers leave
// that awake.
type lazyClient struct {
	// template is the Handler NewClient built, without a stack; each wake
	// copies it.
	template *Handler
	args     *lazyNetTUN
	// server is the endpoint the tunnel reaches, for the observer.
	server net.Destination

	// h is the Handler of the current wake, with its stack and its device,
	// nil while the outbound sleeps. active counts the connections using it;
	// Sleep releases it at zero.
	mu     sync.Mutex
	h      *Handler
	active int
	closed bool

	// initDone is closed when the connection waking the outbound is done
	// with it, and is nil while none is. The wake runs outside mu: it can
	// take seconds when the endpoint needs DNS, and the connections waiting
	// for it must be able to give up.
	initDone chan struct{}
	// initErr fails the connections that come shortly after a failed wake,
	// so that each of them does not try again.
	initErr     error
	initErrTime time.Time
	// inits counts the wakes that took a device into use, for tests.
	inits int
}

var _ outbound.Sleeper = (*lazyClient)(nil)

// Process implements proxy.Outbound.Process.
func (c *lazyClient) Process(ctx context.Context, link *transport.Link, dialer internet.Dialer) error {
	// As in Handler.Process: such a connection does not wake the outbound.
	outbounds := session.OutboundsFromContext(ctx)
	if !outbounds[len(outbounds)-1].Target.IsValid() {
		return errors.New("target not specified")
	}
	// The wake is part of the dial the observer is told of, as resolving
	// the server is for other proxies; the Handler takes the dial over.
	td := startTunnelDial(ctx, dialer, c.server)
	h, err := c.acquire(ctx)
	if err != nil {
		td.failed(err)
		return err
	}
	defer c.release()
	return h.Process(ctx, link, startedDial{Dialer: dialer, td: td})
}

// Close ends the connections in flight and releases the stack and the device.
// The outbound accepts no connections afterwards.
func (c *lazyClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.closeLocked()
	return nil
}

// Sleep implements outbound.Sleeper. While no connection uses the outbound or
// wakes it, it closes the device and the stack; the next connection creates
// both again. Unlike Close, it leaves the outbound usable.
//
// A connection counts until Process returns, which is right after it closed
// its socket in the tunnel: the closing exchange may still be under way, and
// Sleep cuts it short. Callers sleep outbounds that have been idle a while.
func (c *lazyClient) Sleep() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active > 0 || c.initDone != nil {
		return false
	}
	c.closeLocked()
	return true
}

func (c *lazyClient) closeLocked() {
	if c.h != nil {
		// The device closes the stack as well.
		c.h.Close()
		c.h = nil
	}
}

// acquire returns the Handler of the current wake for one connection, and
// wakes the outbound when it sleeps. The Handler stays until the connection
// calls release.
func (c *lazyClient) acquire(ctx context.Context) (*Handler, error) {
	c.mu.Lock()
	for c.h == nil {
		if c.closed {
			c.mu.Unlock()
			return nil, errOutboundClosed
		}
		if c.initErr != nil && time.Since(c.initErrTime) < initRetryDelay {
			err := c.initErr
			c.mu.Unlock()
			return nil, err
		}
		if done := c.initDone; done != nil {
			c.mu.Unlock()
			select {
			case <-done:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			c.mu.Lock()
			continue
		}

		done := make(chan struct{})
		c.initDone = done
		c.mu.Unlock()
		// The device keeps the context for reopening its bind long after
		// this connection ends; it must not end with it. This connection
		// finishes the wake even when it is canceled, for those waiting.
		h, err := c.wake(context.WithoutCancel(ctx))
		c.mu.Lock()
		c.initDone = nil
		close(done)
		if c.closed {
			// Close left what the wake created to it.
			c.mu.Unlock()
			if h != nil {
				h.Close()
			}
			return nil, errOutboundClosed
		}
		if err != nil {
			c.initErr, c.initErrTime = err, time.Now()
			c.mu.Unlock()
			return nil, err
		}
		c.initErr = nil
		c.inits++
		c.h = h
	}
	c.active++
	h := c.h
	c.mu.Unlock()
	return h, nil
}

// release ends the use of the Handler that acquire returned.
func (c *lazyClient) release() {
	c.mu.Lock()
	c.active--
	c.mu.Unlock()
}

// wake creates a Handler with a stack and a device of its own, the device up.
// Only one connection runs it at a time, without c.mu. On failure it keeps
// nothing.
func (c *lazyClient) wake(ctx context.Context) (*Handler, error) {
	a := c.args
	t, tnet, _, err := CreateNetTUN(a.localAddresses, a.dnsServers, a.mtu, a.handleLocal)
	if err != nil {
		return nil, err
	}
	// The stack reports itself up at once, and the device would come up on
	// that right away, while init fills in its bind after NewDevice: let
	// init bring the device up once it is configured.
	select {
	case <-t.Events():
	default:
	}
	rt := newReadyTun(t)
	o := c.template
	h := &Handler{
		conf:          o.conf,
		policyManager: o.policyManager,
		dns:           o.dns,

		streamSettings:  o.streamSettings,
		uplinkCounter:   o.uplinkCounter,
		downlinkCounter: o.downlinkCounter,

		tun:  rt,
		tnet: tnet,
	}
	if err := h.init(ctx); err != nil {
		// Closing the TUN ends a device that init left behind.
		h.Close()
		return nil, err
	}
	rt.open()
	return h, nil
}

// startedDial hands the Handler of a wake the dial its lazyClient started.
type startedDial struct {
	internet.Dialer
	td *tunnelDial
}
