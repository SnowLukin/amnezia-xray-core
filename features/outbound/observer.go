package outbound

import (
	"context"
	gonet "net"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/stats"
)

// ConnectionObserver receives the outcome of every connection an outbound
// handler dials to its server. A feature that implements it next to its own
// feature type is picked up by outbound handlers automatically.
//
// Implementations must not block: the calls happen on the dialing goroutine.
type ConnectionObserver interface {
	// Observes reports whether connections of the outbound with the tag are
	// of interest. A handler asks once when it is created and dials without
	// the observer otherwise.
	Observes(tag string) bool
	// StartDial is called right before a handler dials server. It returns nil
	// when the observer is not interested in this connection. abort ends the
	// user connection the dial belongs to, whether it is still dialing or
	// already open; it is nil for dials no user connection waits for.
	StartDial(ctx context.Context, tag string, server net.Destination, abort func()) DialTracker
}

// ContextConnectionObserver may bypass all per-connection wrappers for traffic
// that has no enabled consumer. Core-owned probes can opt in by context even
// while every optional user hook is disabled.
type ContextConnectionObserver interface {
	NeedsObservation(context.Context, string) bool
}

// DispatchGuard rejects a routed link before DNS, mux or proxy processing.
// It is optional for a ConnectionObserver and also protects a handler replaced
// after a router selected its tag. Implementations must not block.
type DispatchGuard interface {
	AllowDispatch(ctx context.Context, tag string, handlerProbeOnly bool) bool
}

// UDPFlowObserver follows a routed UDP link independently of the physical
// connection used by its outbound. A mux worker may carry several such links.
type UDPFlowObserver interface {
	StartUDPFlow(ctx context.Context, tag string, firstPort net.Port) UDPFlowTracker
}

// UDPFlowTracker counts buffers at the routed link boundary. A MultiBuffer
// may contain several UDP packets, so callers pass both bytes and buffers.
type UDPFlowTracker interface {
	Uplink(bytes, packets int64)
	Downlink(bytes, packets int64)
	Finished()
}

// DialTracker follows one dial and the connection it produced.
type DialTracker interface {
	// Dialed reports the dial result and the dialed connection. When err is
	// nil, the returned counters replace read and write for this connection
	// and must forward to them (either may be nil).
	Dialed(conn gonet.Conn, err error, read, write stats.Counter) (stats.Counter, stats.Counter)
	// Finished is called once the handler stops processing the connection,
	// with the error processing ended with, if any.
	Finished(err error)
}

// TunnelDialer is implemented by the dialer an outbound handler gives its
// proxy while a ConnectionObserver follows the outbound. A proxy that keeps a
// tunnel to its server and carries every connection inside it, WireGuard for
// one, reaches the server past the dialer, once for many connections, so the
// observer would see none of them. Such a proxy reports each connection
// through the tunnel here instead, as a dial of its server.
//
// The dial ends when the server answers anything after the connection
// started: that proves the tunnel works for the connection, as a finished
// dial proves the path to the server for other proxies. The proxy calls
// Dialed without an error then, and with one when the tunnel cannot be set
// up or the server does not answer; a connection that ends before either
// passes context.Canceled. The connection inside the tunnel counts its
// traffic through the counters Dialed returns, which forward to read and
// write: a proxy that counts the traffic of its tunnel itself passes nil.
// The proxy calls Dialed before Process returns, and the handler calls
// Finished once processing ends, as for its own dials.
type TunnelDialer interface {
	// StartTunnelDial is called when the proxy starts carrying a connection
	// through its tunnel to server. It returns nil when the observer is not
	// interested in this connection.
	StartTunnelDial(ctx context.Context, server net.Destination) DialTracker
}

// ProcessObserver is implemented by a ConnectionObserver that also wants to
// know when a handler whose proxy can sleep (see Sleeper) processes a
// connection. Such a proxy, WireGuard for one, creates what it holds again
// for its next connection before it reports anything about the connection,
// and the observer follows the connections only of outbounds it judges, so
// only this tells the observer that a connection routed to the outbound by
// any other way than the observer itself, such as a rule straight at its
// tag, woke it.
//
// Implementations must not block: the calls happen on the goroutine of the
// connection.
type ProcessObserver interface {
	// ProcessStarted is called before the proxy of the handler with the tag
	// processes a connection.
	ProcessStarted(tag string)
	// ProcessEnded is called once the proxy stopped processing it, once
	// for every ProcessStarted.
	ProcessEnded(tag string)
}
