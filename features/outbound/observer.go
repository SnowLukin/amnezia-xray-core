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
	// StartDial is called right before a handler dials server. It returns nil
	// when the observer is not interested in this connection.
	StartDial(ctx context.Context, tag string, server net.Destination) DialTracker
}

// DialTracker follows one dial and the connection it produced.
type DialTracker interface {
	// Dialed reports the dial result and the dialed connection. When err is
	// nil, the returned counters replace read and write for this connection
	// and must forward to them (either may be nil).
	Dialed(conn gonet.Conn, err error, read, write stats.Counter) (stats.Counter, stats.Counter)
	// Finished is called once the handler stops processing the connection.
	// Connections multiplexed over a shared one may never get this call.
	Finished()
}
