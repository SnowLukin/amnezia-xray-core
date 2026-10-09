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
// observedDialer, which reports every dial of this connection, and a proxy
// that can sleep reports the connection itself, which may wake it.
func (h *Handler) process(ctx context.Context, link *transport.Link) error {
	if h.observer == nil {
		return h.proxy.Process(ctx, link, h)
	}
	if po := h.processObserver(); po != nil {
		po.ProcessStarted(h.tag)
		defer po.ProcessEnded(h.tag)
	}
	// Proxies end processing when their context is canceled, during the dial
	// and while copying, and close what they dialed: this lets the observer
	// end a connection that will get no answer.
	ctx, abort := context.WithCancel(ctx)
	defer abort()
	d := &observedDialer{Handler: h, abort: abort}
	var err error
	defer func() { d.finish(err) }()
	err = h.proxy.Process(ctx, link, d)
	return err
}

// processObserver returns the observer to tell of every connection the
// proxy processes, or nil. Only a proxy that can sleep needs it: it may
// wake for a connection whose dials pass the handler by.
func (h *Handler) processObserver() outbound.ProcessObserver {
	if _, ok := h.proxy.(outbound.Sleeper); !ok {
		return nil
	}
	po, _ := h.observer.(outbound.ProcessObserver)
	return po
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
	abort    func()
	mu       sync.Mutex
	trackers []outbound.DialTracker
	// Most connections dial once; their tracker needs no slice allocation.
	first    [1]outbound.DialTracker
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
	abort := d.abort
	switch {
	case finished:
		// A proxy that keeps dialing for later connections, such as VLESS
		// pre-connections: each dial is a connection of its own.
		observed = xctx.ContextWithID(ctx, session.NewID())
		abort = nil
	case xctx.IDFromContext(ctx) == 0:
		observed = xctx.ContextWithID(ctx, id)
	}
	tracker := d.observer.StartDial(observed, d.tag, dest, abort)
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
		tracker.Finished(nil)
		return conn, nil
	}
	d.mu.Lock()
	if d.trackers == nil {
		d.trackers = d.first[:0]
	}
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

func (d *observedDialer) finish(err error) {
	d.mu.Lock()
	trackers := d.trackers
	d.trackers = nil
	d.finished = true
	d.mu.Unlock()
	for _, t := range trackers {
		t.Finished(err)
	}
}
