package outbound

import (
	"context"
	"sync"

	xctx "github.com/xtls/xray-core/common/ctx"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
)

// The observer hook lives here, apart from handler.go, so the handler body
// keeps following upstream without conflicts.

// process runs the proxy. With an observer, the proxy dials through
// observedDialer, which reports every dial of this connection.
func (h *Handler) process(ctx context.Context, link *transport.Link) error {
	if h.observer == nil {
		return h.proxy.Process(ctx, link, h)
	}
	d := &observedDialer{Handler: h}
	defer d.finish()
	return h.proxy.Process(ctx, link, d)
}

// observed makes mux workers dial through the observer as well.
func (h *Handler) observed(p proxy.Outbound) proxy.Outbound {
	if h.observer == nil {
		return p
	}
	return observedProxy{h}
}

type observedProxy struct {
	h *Handler
}

func (p observedProxy) Process(ctx context.Context, link *transport.Link, _ internet.Dialer) error {
	return p.h.process(ctx, link)
}

type observedDialer struct {
	*Handler
	mu       sync.Mutex
	trackers []outbound.DialTracker
	finished bool
	// Stands for the user connection when the context carries no session,
	// so that retries of one dial count once.
	id xctx.ID
}

// Dial implements internet.Dialer.
func (d *observedDialer) Dial(ctx context.Context, dest net.Destination) (stat.Connection, error) {
	d.mu.Lock()
	finished := d.finished
	if xctx.IDFromContext(ctx) == 0 && d.id == 0 {
		d.id = session.NewID()
	}
	id := d.id
	d.mu.Unlock()
	observed := ctx
	switch {
	case finished:
		// A proxy that keeps dialing for later connections, such as VLESS
		// pre-connections: each dial is a connection of its own.
		observed = xctx.ContextWithID(ctx, session.NewID())
	case xctx.IDFromContext(ctx) == 0:
		observed = xctx.ContextWithID(ctx, id)
	}
	tracker := d.observer.StartDial(observed, d.tag, dest)
	if tracker == nil {
		return d.Handler.Dial(ctx, dest)
	}
	conn, err := d.Handler.Dial(ctx, dest)
	// Counters are replaced instead of wrapping Read and Write because readv,
	// writev and splice bypass those but still update counters.
	read, write := tracker.Dialed(conn, err, d.downlinkCounter, d.uplinkCounter)
	if err != nil {
		return conn, err
	}
	if finished {
		// Nobody reports when such a connection ends; only its dial is judged.
		tracker.Finished()
		return conn, nil
	}
	d.mu.Lock()
	d.trackers = append(d.trackers, tracker)
	d.mu.Unlock()
	if read == nil && write == nil {
		return conn, nil
	}
	if c, ok := conn.(*stat.CounterConnection); ok {
		c.ReadCounter, c.WriteCounter = read, write
		return c, nil
	}
	return &stat.CounterConnection{Connection: conn, ReadCounter: read, WriteCounter: write}, nil
}

func (d *observedDialer) finish() {
	d.mu.Lock()
	trackers := d.trackers
	d.trackers = nil
	d.finished = true
	d.mu.Unlock()
	for _, t := range trackers {
		t.Finished()
	}
}
