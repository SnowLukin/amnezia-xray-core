// Package health observes outbounds through the connections users already
// make, probes them only when there is a reason, and chooses outbounds for
// balancers with the leastCost strategy.
package health

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	"google.golang.org/protobuf/proto"
)

// Observer is the healthObservatory feature.
type Observer struct {
	config *Config
	s      *settings
	ctx    context.Context

	dispatcher routing.Dispatcher
	ohm        outbound.Manager

	mu      sync.Mutex
	e       *engine
	kick    chan struct{}
	done    chan struct{}
	started bool

	trackMu sync.Mutex
	// Every open connection, for statistics when it ends.
	trackers map[*connTracker]struct{}
	// Connections with a deadline the sweep has to check.
	watching map[*connTracker]struct{}
	sweeping bool
	tracked  uint64

	// Last server address dialed per tag, for outbounds without a server name.
	lastServer sync.Map
	// The engine's network generation, readable without the lock.
	generation atomic.Uint64

	providers []*providerRuntime
	// Ends provider requests still running when the observer closes.
	fetchCtx  context.Context
	stopFetch context.CancelFunc
	// Serializes changes of provider outbounds in the outbound manager.
	applyMu        sync.Mutex
	applyClosed    bool
	parseOutbounds func([]json.RawMessage) ([]*core.OutboundHandlerConfig, error)
	newHandler     func(*core.OutboundHandlerConfig) (outbound.Handler, error)

	clock func() time.Time
	// Connection times count from here on the monotonic clock, so a jump of
	// the wall clock does not time out every open dial at once.
	base time.Time
	once sync.Once
}

// New creates the observer.
func New(ctx context.Context, config *Config) (*Observer, error) {
	o := newObserver(ctx, config)
	err := core.RequireFeatures(ctx, func(dispatcher routing.Dispatcher, ohm outbound.Manager) error {
		o.dispatcher = dispatcher
		o.ohm = ohm
		return nil
	})
	if err != nil {
		return nil, err
	}
	return o, nil
}

func newObserver(ctx context.Context, config *Config) *Observer {
	s := newSettings(config)
	o := &Observer{
		config:         config,
		s:              s,
		ctx:            ctx,
		e:              newEngine(s, time.Now().UnixNano()),
		kick:           make(chan struct{}, 1),
		trackers:       map[*connTracker]struct{}{},
		watching:       map[*connTracker]struct{}{},
		parseOutbounds: loadOutbounds,
		clock:          time.Now,
		done:           make(chan struct{}),
	}
	// A second back, so that zero stays free to mean "not yet".
	o.base = o.clock().Add(-time.Second)
	o.fetchCtx, o.stopFetch = context.WithCancel(ctx)
	o.newHandler = o.createHandler
	for i, ps := range s.providers {
		o.providers = append(o.providers, &providerRuntime{
			p:        o.e.providers[i],
			cfg:      ps,
			built:    map[string]*core.OutboundHandlerConfig{},
			draining: map[outbound.Handler]*drainingOutbound{},
		})
	}
	o.e.serverNameFor = o.serverName
	return o
}

func (o *Observer) now() time.Time { return o.clock() }

func (o *Observer) nowNano() int64 { return int64(o.clock().Sub(o.base)) }

// Type implements common.HasType.
func (o *Observer) Type() interface{} {
	return extension.ObservatoryType()
}

// Start implements common.Runnable.
func (o *Observer) Start() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.started {
		return nil
	}
	o.started = true
	go o.loop(o.done)
	return nil
}

// Close implements common.Closable.
func (o *Observer) Close() error {
	o.once.Do(func() { close(o.done) })
	o.stopFetch()
	o.closeProviders()
	return nil
}

func (o *Observer) wake() {
	select {
	case o.kick <- struct{}{}:
	default:
	}
}

func (o *Observer) loop(done chan struct{}) {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	for {
		o.mu.Lock()
		now := o.now()
		jobs, wake := o.e.tick(now)
		refreshes, refreshWake := o.e.tickProviders(now)
		o.mu.Unlock()
		if !refreshWake.IsZero() && (wake.IsZero() || refreshWake.Before(wake)) {
			wake = refreshWake
		}
		for _, j := range jobs {
			go o.runProbe(j)
		}
		for _, r := range refreshes {
			go o.runRefresh(r)
		}
		if !wake.IsZero() {
			d := wake.Sub(now)
			if d < 10*time.Millisecond {
				d = 10 * time.Millisecond
			}
			timer.Reset(d)
		}
		select {
		case <-o.kick:
		case <-timer.C:
		case <-done:
			timer.Stop()
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

func (o *Observer) runProbe(j *probeJob) {
	var r probeResult
	if j.reason == reasonConnectivity {
		c := o.s.connectivity
		r = o.probe(c.outbound, &probeSettings{
			destination: c.destination, method: "GET", timeout: c.timeout, progressTimeout: c.timeout,
		})
	} else {
		r = o.probe(j.tag, o.s.probeFor(j.tag))
	}
	o.mu.Lock()
	o.e.onProbeResult(j, r, o.now())
	o.mu.Unlock()
	o.wake()
}

// ---- outbound.ConnectionObserver ------------------------------------------

// StartDial implements outbound.ConnectionObserver.
func (o *Observer) StartDial(ctx context.Context, tag string, server net.Destination) outbound.DialTracker {
	if bytes := probeBytesFrom(ctx); bytes != nil {
		return &probeTracker{bytes: bytes}
	}
	if !o.s.observes(tag) {
		if o.s.witness(tag) {
			return &witnessTracker{o: o}
		}
		return nil
	}
	target, udp := targetFrom(ctx)
	t := &connTracker{
		o:       o,
		tag:     tag,
		target:  target,
		server:  destString(server),
		conn:    connFrom(ctx),
		udp:     udp,
		started: o.nowNano(),
		network: o.generation.Load(),
	}
	if _, ok := o.lastServer.Load(tag); !ok && t.server != "" {
		o.lastServer.Store(tag, t.server)
	}
	o.track(t)
	return t
}

func (o *Observer) track(t *connTracker) {
	o.trackMu.Lock()
	o.trackers[t] = struct{}{}
	o.tracked++
	if o.tracked%256 == 0 {
		now := o.nowNano()
		for old := range o.trackers {
			if old.abandoned(now) {
				delete(o.trackers, old)
				delete(o.watching, old)
				old.watched.Store(false)
			}
		}
	}
	o.trackMu.Unlock()
	t.watch()
}

// watch makes the sweep check t until it has no deadline left.
func (o *Observer) watch(t *connTracker) {
	o.trackMu.Lock()
	if t.state.Load() == trackerFinished {
		o.trackMu.Unlock()
		return
	}
	o.watching[t] = struct{}{}
	start := !o.sweeping
	o.sweeping = true
	done := o.done
	o.trackMu.Unlock()
	if start {
		go o.sweepLoop(done)
	}
}

func (o *Observer) untrack(t *connTracker) {
	o.trackMu.Lock()
	delete(o.trackers, t)
	delete(o.watching, t)
	o.trackMu.Unlock()
	t.watched.Store(false)
}

// sweepLoop runs only while some connection has a deadline to check, so a
// device with idle long-lived connections is not woken up.
func (o *Observer) sweepLoop(done chan struct{}) {
	ticker := time.NewTicker(o.s.passive.sweepInterval)
	defer ticker.Stop()
	var batch []*connTracker
	for {
		select {
		case <-done:
			o.trackMu.Lock()
			o.sweeping = false
			o.trackMu.Unlock()
			return
		case <-ticker.C:
		}
		o.trackMu.Lock()
		if len(o.watching) == 0 {
			o.sweeping = false
			o.trackMu.Unlock()
			return
		}
		batch = batch[:0]
		for t := range o.watching {
			batch = append(batch, t)
		}
		o.trackMu.Unlock()
		now := o.nowNano()
		for _, t := range batch {
			lastRead, lastWrite := t.lastRead.Load(), t.lastWrite.Load()
			watching, drop := t.sweep(now)
			switch {
			case drop:
				o.untrack(t)
			case !watching:
				o.trackMu.Lock()
				delete(o.watching, t)
				o.trackMu.Unlock()
				t.watched.Store(false)
				// Traffic during the sweep may have started a new deadline.
				if t.lastRead.Load() != lastRead || t.lastWrite.Load() != lastWrite {
					t.watch()
				}
			}
		}
	}
}

// failure and success skip connections started before the last network
// change: they tell nothing about outbounds in the new network.
func (o *Observer) failure(t *connTracker, kind failureKind) {
	o.mu.Lock()
	if t.network == o.e.generation {
		o.e.onFailure(t.tag, kind, t.target, t.conn, o.now())
	}
	o.mu.Unlock()
	o.wake()
}

func (o *Observer) success(t *connTracker) {
	o.mu.Lock()
	if t.network != o.e.generation {
		o.mu.Unlock()
		return
	}
	queued := len(o.e.queue)
	o.e.onSuccess(t.tag, o.now())
	more := len(o.e.queue) > queued
	o.mu.Unlock()
	if more {
		o.wake()
	}
}

func (o *Observer) witnessed() {
	o.mu.Lock()
	queued := len(o.e.queue)
	o.e.sawSuccess("witness", o.now())
	more := len(o.e.queue) > queued
	o.mu.Unlock()
	if more {
		o.wake()
	}
}

func (o *Observer) finished(t *connTracker) {
	o.mu.Lock()
	st := &o.e.path(t.tag).stats
	if t.bytesRead.Load() > 0 {
		st.connsOK++
	} else if t.bytesWritten.Load() > 0 {
		st.connsFailed++
	}
	st.bytesUp += t.bytesWritten.Load()
	st.bytesDown += t.bytesRead.Load()
	if ttfb := t.ttfb.Load(); ttfb > 0 {
		st.ttfb.add(time.Duration(ttfb))
	}
	o.mu.Unlock()
}

// ---- extension.Observatory --------------------------------------------------

// GetObservation implements extension.Observatory.
func (o *Observer) GetObservation(context.Context) (proto.Message, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	tags := make([]string, 0, len(o.e.paths))
	for tag := range o.e.paths {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	result := &observatory.ObservationResult{}
	for _, tag := range tags {
		p := o.e.paths[tag]
		st := &observatory.OutboundStatus{
			Alive:           p.status != statusDown && p.status != statusSuspect,
			Delay:           p.lastRTT.Milliseconds(),
			LastErrorReason: p.lastError,
			OutboundTag:     tag,
		}
		if !p.lastSuccess.IsZero() {
			st.LastSeenTime = p.lastSuccess.Unix()
		}
		if !p.lastProbe.IsZero() {
			st.LastTryTime = p.lastProbe.Unix()
		}
		result.Status = append(result.Status, st)
	}
	return result, nil
}

// ---- extension.OutboundSelectorProvider ---------------------------------------

// OutboundSelector implements extension.OutboundSelectorProvider.
func (o *Observer) OutboundSelector(balancerTag string, settings *serial.TypedMessage) (extension.OutboundSelector, error) {
	cfg := &SelectorConfig{}
	if settings != nil {
		inst, err := settings.GetInstance()
		if err != nil {
			return nil, err
		}
		c, ok := inst.(*SelectorConfig)
		if !ok {
			return nil, errors.New("leastCost settings must be ", string(proto.MessageName(cfg)))
		}
		cfg = c
	}
	o.mu.Lock()
	s := o.e.addSelector(balancerTag, newSelectorSettings(cfg))
	o.mu.Unlock()
	return &selectorHandle{o: o, s: s}, nil
}

type selectorHandle struct {
	o *Observer
	s *selector
}

// Select implements extension.OutboundSelector.
func (h *selectorHandle) Select(candidates []string) string {
	now := h.o.now()
	nano := now.UnixNano()
	if prev := h.o.e.lastDemand.Swap(nano); prev == 0 || now.Sub(time.Unix(0, prev)) > h.o.s.schedule.pauseWhenIdle {
		h.o.wake()
	}
	if snap := h.s.snapshot.Load(); snap == nil || !equalStrings(*snap, candidates) {
		h.o.mu.Lock()
		h.o.e.updateCandidates(h.s, candidates, now)
		h.o.mu.Unlock()
		h.o.wake()
	}
	return h.s.current()
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---- host API ---------------------------------------------------------------

// NetworkChanged tells the observer that the device moved to another network.
// id identifies the network for remembering working outbounds; an empty id
// means the host cannot tell networks apart.
func (o *Observer) NetworkChanged(id string) {
	o.mu.Lock()
	o.e.onNetworkChange(id, o.now())
	o.generation.Store(o.e.generation)
	o.mu.Unlock()
	o.wake()
}

// NetworkLost pauses all decisions until NetworkChanged.
func (o *Observer) NetworkLost() {
	o.mu.Lock()
	o.e.onNetworkLost(o.now())
	o.mu.Unlock()
}

// Pin keeps the balancer on tag until Release.
func (o *Observer) Pin(balancer, tag string) bool {
	o.mu.Lock()
	ok := o.e.pin(balancer, tag, o.now())
	o.mu.Unlock()
	o.wake()
	return ok
}

// Release lets the balancer choose again.
func (o *Observer) Release(balancer string) bool {
	o.mu.Lock()
	ok := o.e.release(balancer, o.now())
	o.mu.Unlock()
	o.wake()
	return ok
}

// DrainEvents returns kept events as a JSON array and the number of events
// dropped because the buffer was full.
func (o *Observer) DrainEvents() ([]byte, uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.e.events.drain()
}

// Status returns the current state of outbounds and balancers as JSON.
func (o *Observer) Status() []byte {
	o.mu.Lock()
	view := o.e.status(o.now())
	o.mu.Unlock()
	out, _ := json.Marshal(view)
	return out
}

// FromInstance returns the health observatory of a running instance, or nil.
func FromInstance(v *core.Instance) *Observer {
	o, _ := v.GetFeature(extension.ObservatoryType()).(*Observer)
	return o
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		return New(ctx, config.(*Config))
	}))
}
