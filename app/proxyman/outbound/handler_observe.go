package outbound

import (
	"context"
	"sync"

	xctx "github.com/xtls/xray-core/common/ctx"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
)

// The observer hook lives here, apart from handler.go, so the handler body
// keeps following upstream without conflicts.

// observerOf returns the observer that follows the outbound with the tag, or
// nil.
func observerOf(v *core.Instance, tag string) outbound.ConnectionObserver {
	if o, ok := v.GetFeature(extension.ObservatoryType()).(outbound.ConnectionObserver); ok && o.Observes(tag) {
		return o
	}
	return nil
}

// process runs the proxy. With an observer, the proxy dials through
// observedDialer, which reports every dial of this connection, or reports
// the connection through its tunnel there (outbound.TunnelDialer), and a
// proxy that can sleep reports the connection itself, which may wake it.
func (h *Handler) process(ctx context.Context, link *transport.Link) error {
	if h.observer == nil {
		return h.proxy.Process(ctx, link, h)
	}
	if o, ok := h.observer.(outbound.ContextConnectionObserver); ok && !o.NeedsObservation(ctx, h.tag) {
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

var _ outbound.TunnelDialer = (*observedDialer)(nil)

type observedDialer struct {
	*Handler
	abort    func()
	mu       sync.Mutex
	trackers []outbound.DialTracker
	// Most connections dial once; their tracker needs no slice allocation.
	first     [1]outbound.DialTracker
	finished  bool
	finishErr error
	// Stands for the user connection when the context carries no session,
	// so that retries of one dial count once.
	id xctx.ID
}

// Dial implements internet.Dialer.
func (d *observedDialer) Dial(ctx context.Context, dest net.Destination) (stat.Connection, error) {
	observed, finished := d.connection(ctx)
	abort := d.abort
	if finished {
		// A proxy that keeps dialing for later connections, such as VLESS
		// pre-connections: each dial is a connection of its own.
		observed = xctx.ContextWithID(ctx, session.NewID())
		abort = nil
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
	d.keep(tracker)
	if read == nil && write == nil {
		return conn, nil
	}
	if c, ok := conn.(*stat.CounterConnection); ok {
		c.ReadCounter, c.WriteCounter = read, write
		return c, nil
	}
	return &stat.CounterConnection{Connection: conn, ReadCounter: read, WriteCounter: write}, nil
}

// StartTunnelDial implements outbound.TunnelDialer. The proxy reports the
// dial itself; the tracker ends with the connection, as those of Dial do.
func (d *observedDialer) StartTunnelDial(ctx context.Context, server net.Destination) outbound.DialTracker {
	observed, finished := d.connection(ctx)
	if finished {
		// A tunnel carries only the connection the proxy processes, and
		// nothing would end a tracker started after that.
		return nil
	}
	tracker := d.observer.StartDial(observed, d.tag, server, d.abort)
	if tracker != nil && !d.keep(tracker) {
		return nil
	}
	return tracker
}

// connection returns ctx with the session the dials of this user connection
// count under, and whether processing has already ended.
func (d *observedDialer) connection(ctx context.Context) (context.Context, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if xctx.IDFromContext(ctx) != 0 {
		return ctx, d.finished
	}
	if d.id == 0 {
		d.id = session.NewID()
	}
	return xctx.ContextWithID(ctx, d.id), d.finished
}

// keep has finish end the tracker with the connection. A dial may return
// after processing ended; complete that tracker immediately, outside the
// lock as in finish, instead of retaining it without a terminal callback.
func (d *observedDialer) keep(tracker outbound.DialTracker) bool {
	d.mu.Lock()
	if d.finished {
		err := d.finishErr
		d.mu.Unlock()
		tracker.Finished(err)
		return false
	}
	if d.trackers == nil {
		d.trackers = d.first[:0]
	}
	d.trackers = append(d.trackers, tracker)
	d.mu.Unlock()
	return true
}

func (d *observedDialer) finish(err error) {
	d.mu.Lock()
	trackers := d.trackers
	d.trackers = nil
	d.finished = true
	d.finishErr = err
	d.mu.Unlock()
	for _, t := range trackers {
		t.Finished(err)
	}
}
