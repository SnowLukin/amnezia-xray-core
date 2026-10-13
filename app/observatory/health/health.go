// Package health observes outbounds through the connections users already
// make, probes them only when there is a reason, and chooses outbounds for
// balancers with the leastCost strategy.
package health

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/xtls/xray-core/common/safe"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/app/metrics"
	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/features/stats"
	"google.golang.org/protobuf/proto"
)

// Observer is the healthObservatory feature. It keeps no reference to its
// Config: the initial sets in it take up to 1 MiB per provider, and once
// applied they live on as the applied answer.
type Observer struct {
	s   *settings
	ctx context.Context

	dispatcher routing.Dispatcher
	ohm        outbound.Manager
	stats      stats.Manager

	failed      atomic.Bool
	panicCursor uint64

	mu      sync.Mutex
	e       *engine
	kick    chan struct{}
	done    chan struct{}
	started bool
	// Routed UDP associations, counted once when their link ends.
	udpFlows           map[*udpFlow]struct{}
	udpEvidencePending map[*udpFlow]struct{}

	trackMu sync.Mutex
	// Every open connection, for statistics when it ends.
	trackers map[*connTracker]struct{}
	closures map[*closureTracker]struct{}
	// Connections with a deadline the sweep has to check.
	watching map[*connTracker]struct{}
	sweeping bool
	tracked  uint64

	// The engine's network generation, readable without the lock.
	generation atomic.Uint64
	// Measurements have an epoch even when routing ignores a network change.
	qualityEpoch atomic.Uint64
	qualityInfo  qualitySocketReader
	// Names this run of the core, so that a host request meant for a
	// previous run is told apart.
	run string

	providers []*providerRuntime
	// Ends probes and provider requests still running when the observer closes.
	runCtx  context.Context
	stopRun context.CancelFunc
	// Serializes changes of provider outbounds in the outbound manager.
	applyMu        sync.Mutex
	applyClosed    bool
	parseOutbounds func(json.RawMessage) ([]*core.OutboundHandlerConfig, error)
	newHandler     func(*core.OutboundHandlerConfig) (outbound.Handler, error)
	parseRouting   func(json.RawMessage) (*router.Config, error)
	replaceRules   func(*router.Config, bool) error

	clock func() time.Time
	// Connection times count from here on the monotonic clock, so a jump of
	// the wall clock does not time out every open dial at once.
	base time.Time
	once sync.Once
}

// New creates the observer.
func New(ctx context.Context, config *Config) (*Observer, error) {
	if err := config.ValidateFeatures(); err != nil {
		return nil, err
	}
	if err := config.ValidateMeasurementContext(); err != nil {
		return nil, err
	}
	if err := validateQuietShare(config.GetReport()); err != nil {
		return nil, err
	}
	if err := config.ValidateProbeSteps(); err != nil {
		return nil, err
	}
	o := newObserver(ctx, config)
	err := core.RequireFeatures(ctx, func(dispatcher routing.Dispatcher, ohm outbound.Manager, r routing.Router, sm stats.Manager) error {
		o.dispatcher = dispatcher
		o.ohm = ohm
		o.stats = sm
		// The signature has to follow app/router: a mismatch fails this check
		// quietly and leaves provider routing unsupported.
		if rr, ok := r.(interface {
			ReplaceRules(*router.Config, bool) error
		}); ok {
			o.replaceRules = rr.ReplaceRules
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if o.s.features.Metrics && core.MustFromContext(ctx).GetFeature((*metrics.MetricsHandler)(nil)) == nil {
		return nil, errors.New("healthObservatory.telemetry.metrics requires metrics exporter registered before healthObservatory")
	}
	return o, nil
}

func newObserver(ctx context.Context, config *Config) *Observer {
	s := newSettings(config)
	o := &Observer{
		s:              s,
		ctx:            ctx,
		e:              newEngine(s, time.Now().UnixNano()),
		kick:           make(chan struct{}, 1),
		parseOutbounds: loadOutbounds,
		parseRouting:   loadRouting,
		clock:          time.Now,
		done:           make(chan struct{}),
	}
	// A second back, so that zero stays free to mean "not yet".
	o.base = o.clock().Add(-time.Second)
	var id [8]byte
	_, _ = rand.Read(id[:])
	o.run = hex.EncodeToString(id[:])
	o.e.run = o.run
	o.e.frozen = &o.failed
	o.panicCursor, _, _ = safe.Snapshot(0)
	o.runCtx, o.stopRun = context.WithCancel(ctx)
	o.newHandler = o.createHandler
	collectorStart := time.Time{}
	if s.features.Report || s.features.Quality || s.features.ProbeMeasurements {
		collectorStart = o.clock()
	}
	if s.features.Report {
		o.e.report.begin(collectorStart)
	}
	o.e.censusNetworkStarted(collectorStart)
	if !s.passive.skipQuality || s.features.ProbeMeasurements || (s.features.Report && (s.features.HostLabels || s.features.LabelRequest)) {
		o.e.quality = newQualitySummary(collectorStart)
		o.e.quality.reporting = s.features.Report
		if !s.features.Report {
			o.e.quality.open = qualityCollection{}
		}
	}
	o.qualityEpoch.Store(o.e.qualityEpoch)
	// The number of the kept set starts from the wall clock in
	// milliseconds, so it grows across runs too: a host that asks with the
	// number it kept from the run before gets the set of this one.
	seq := uint64(max(o.clock().UnixMilli(), 0))
	for i, ps := range s.providers {
		o.e.providers[i].seq = seq
		o.providers = append(o.providers, &providerRuntime{
			p:        o.e.providers[i],
			cfg:      ps,
			built:    map[string]*core.OutboundHandlerConfig{},
			draining: map[outbound.Handler]*drainingOutbound{},
		})
	}
	o.e.serverNameFor = o.serverName
	o.e.changeLabelNetwork("", o.now())
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
	if o.started {
		o.mu.Unlock()
		return nil
	}
	o.started = true
	o.mu.Unlock()
	// Outside the lock: like the apply of a set, the observer never holds it
	// across a call into the outbound manager.
	o.seedProviderSets()
	o.applyInitial()
	running.Store(o)
	go o.guard("health.go:Observer.loop", func() { o.loop(o.done) })
	return nil
}

// applyInitial applies the initial set of every provider that has one
// before the loop starts, the way it applies an answer: the core starts on
// the set the connect brought or the host kept, without a request. A set
// that does not apply is reported, and the provider goes on with the
// outbounds of the config and its triggers. Like the start switches, an
// applied initial set is no decision for the report.
func (o *Observer) applyInitial() {
	for _, rt := range o.providers {
		raw := rt.cfg.initial
		if len(raw) == 0 {
			continue
		}
		// Up to 1 MiB per provider: from here on the set lives as the
		// applied one.
		rt.cfg.initial = nil
		a, err := o.initialAnswer(rt, raw)
		if err != nil {
			o.emit("provider_apply", map[string]any{"provider": rt.cfg.tag, "trigger": triggerInitial.String(),
				"is_ok": false, "error": cutText(err.Error(), maxFailureText)})
			continue
		}

		func() {
			o.mu.Lock()
			defer o.mu.Unlock()

			o.e.baseFromInitial(rt.p, a)

		}()
		// The headers lie under those the host set before the start, name by
		// name, which gen zero tells.
		if _, err := o.applySet(rt, a, &refreshJob{p: rt.p, trigger: triggerInitial}); err != nil {
			continue
		}

		func() {
			o.mu.Lock()
			defer o.mu.Unlock()

			o.e.startFromInitial(rt.p, a.age, a.nextRefreshIn, a.hasNextRefreshIn, o.now())

		}()
	}
}

// initialAnswer reads the initial set of a provider. One the host saved
// tells by receivedAt how old it is.
func (o *Observer) initialAnswer(rt *providerRuntime, raw []byte) (*providerAnswer, error) {
	// A set the host kept is the answer with the headers and the clock of
	// ProviderSet, which may take it past maxBytes by keptSetRoom.
	if limit := rt.cfg.maxBytes + keptSetRoom; int64(len(raw)) > limit {
		return nil, fmt.Errorf("initial larger than %d bytes", limit)
	}
	a, err := parseInitial(raw)
	if err != nil {
		if !isAnswerError(err) {
			err = fmt.Errorf("invalid answer: %w", err)
		}
		return nil, err
	}
	if !a.receivedAt.IsZero() {
		a.age = min(max(o.now().Round(0).Sub(a.receivedAt), 0), maxLease)
	}
	return a, nil
}

// seedProviderSets gives every provider the outbounds of the config with its
// prefix, which it owns until its first answer, so the first request already
// tells the backend about them. The outbounds of the config are registered
// by the time the features start.
func (o *Observer) seedProviderSets() {
	if o.ohm == nil {
		return
	}
	handlers := o.ohm.ListHandlers(o.ctx)
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, rt := range o.providers {
		var tags []string
		for _, h := range handlers {
			if t := h.Tag(); strings.HasPrefix(t, rt.cfg.prefix) {
				tags = append(tags, t)
			}
		}
		sort.Strings(tags)
		rt.p.tags = tags
	}
}

// Close implements common.Closable.
func (o *Observer) Close() error {
	running.CompareAndSwap(o, nil)
	o.once.Do(func() { close(o.done) })

	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		o.finishUDPFlowsLocked()
		o.e.interruptCensus("close", o.now())

	}()
	o.stopRun()
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
	defer timer.Stop()
	for {
		if o.failed.Load() {
			return
		}
		var now time.Time
		var jobs []*probeJob
		var refreshes []*refreshJob
		var wake time.Time
		var label *labelJob
		var labelWake time.Time
		var downs []downMark
		var sleeps []sleepJob
		var unpinned bool
		func() {
			o.mu.Lock()
			defer o.mu.Unlock()

			safe.TestPanic("health", "loop")
			o.collectPanicsLocked()
			now = o.now()
			jobs, refreshes, wake = o.e.step(now)
			label, labelWake = o.e.nextLabelRequest(o.runCtx, now)
			wake = soonestOf(wake, labelWake)
			downs = o.e.takeDowns()
			sleeps = o.e.takeSleeps()
			unpinned = o.e.takeUnpinned()

		}()
		// Ending a connection reports back to the observer, so it happens
		// outside the lock.
		for _, d := range downs {
			o.closePending(d)
		}
		if len(sleeps) > 0 {
			go o.guard("health.go:Observer.sleepOutbounds", func() { o.sleepOutbounds(sleeps) })
		}
		if unpinned {
			go o.guard("health.go:Observer.endOverdueDrains", func() { o.endOverdueDrains() })
		}
		for _, j := range jobs {
			go o.guard("health.go:Observer.runProbe", func() { o.runProbe(j) })
		}
		if label != nil {
			go o.guard("health.go:Observer.runLabelRequest", func() { o.runLabelRequest(label) })
		}
		for _, r := range refreshes {
			go o.guard("health.go:Observer.runRefresh", func() { o.runRefresh(r) })
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
	if o.failed.Load() {
		return
	}
	ctx, cancel := context.WithCancel(o.runCtx)

	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		j.cancel = cancel
		j.qualityEpoch = o.e.qualityEpoch
		if j.invalidated || j.forgotten {
			cancel()
		}

	}()
	defer cancel()
	if j.isUDP {
		r := o.probeUDPWithContext(ctx, j.tag, o.s.udpProbe)

		func() {
			o.mu.Lock()
			defer o.mu.Unlock()

			o.e.onUDPProbeResult(j, r, o.now())

		}()
		o.wake()
		return
	}
	ctx = o.probeJobContext(ctx, j)
	var r probeResult
	if j.reason == reasonConnectivity {
		c := o.s.connectivity
		r = o.probeWithContext(ctx, c.outbound, &probeSettings{steps: []probeStepSettings{{
			name: "connectivity", destinations: c.destinations, method: "GET", timeout: c.timeout, progressTimeout: c.timeout,
		}}})
	} else {
		r = o.probeWithContext(ctx, j.tag, j.profile)
	}

	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		o.e.onProbeResult(j, r, o.now())

	}()
	o.wake()
}

// ---- outbound.ConnectionObserver ------------------------------------------

// Observes implements outbound.ConnectionObserver: the outbounds it judges,
// the witnesses of the network and the outbound of the network check, whose
// probes have to be told apart from user traffic.
func (o *Observer) Observes(tag string) bool {
	return o.s.observes(tag) || o.s.witness(tag) || (o.s.connectivity != nil && tag == o.s.connectivity.outbound)
}

func (o *Observer) AllowDispatch(ctx context.Context, tag string, handlerProbeOnly bool) bool {
	if !handlerProbeOnly && !o.s.isProbeOnly(tag) {
		return true
	}
	state := probeStateFrom(ctx)
	if state == nil || state.credential == nil {
		return false
	}
	c := state.credential
	o.mu.Lock()
	defer o.mu.Unlock()
	current := !c.job.invalidated && !c.job.forgotten && c.job.generation == o.e.generation && c.job.candidateEpoch == o.e.candidateEpoch
	if c.job.reason == reasonCandidate {
		current = current && o.e.candidateCurrent(c.job)
	}
	return handlerProbeOnly && o.e.isProbeOnly(tag) && c.job.tag == tag && c.job.isProbeOnly &&
		(c.job.reason == reasonCandidate || c.job.reason == reasonHost || (c.job.reason == reasonCensus && o.e.censusJobCurrent(c.job))) && o.e.running[c.job.id] == c.job && current &&
		o.e.paths[tag] == c.path && c.path != nil && c.path.udpIncarnation == c.incarnation && !o.e.retiring[tag] && !o.e.replacing[tag]
}

// StartDial implements outbound.ConnectionObserver.
func (o *Observer) StartDial(ctx context.Context, tag string, server net.Destination, abort func()) outbound.DialTracker {
	if state := probeStateFrom(ctx); state != nil {
		return &probeTracker{state: state}
	}
	// Metadata uses its own epoch even when routing ignores network changes.
	labelEpoch := o.qualityEpoch.Load()
	if !o.s.observes(tag) {
		if o.s.witness(tag) {
			w := &witnessTracker{o: o, labelEpoch: labelEpoch}
			if !o.s.passive.skipQuality {
				_, udp := targetFrom(ctx)
				if !udp {
					w.quality = o.newConnectionQuality(false, 0)
					w.quality.started = o.nowNano()
				}
			}
			return w
		}
		return nil
	}
	target, udp := targetFrom(ctx)
	// Capture the measurement epoch before any traffic activity clock reads.
	quality := o.newConnectionQuality(udp, 0)
	registry := o.connectionRegistry(tag)
	activity, proofSignal := o.connectionSignals(tag)
	proofSignal = proofSignal && (!udp || o.s.features.passiveTCP())
	var connectionPath *path
	var connectionEpoch uint64
	var udpEpoch uint64
	var udpIncarnation uint64
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		connectionPath = o.e.path(tag)
		connectionEpoch = connectionPath.controllerEpoch
		udpEpoch, udpIncarnation = o.e.udpEpoch, connectionPath.udpIncarnation
		o.e.noteDemand(o.now())
		o.e.used(tag, o.now())

	}()
	if o.s.census != nil {
		o.wake()
	}
	if !o.s.features.passiveTCP() && !o.s.features.Quality && !o.s.features.Report && !o.s.features.Metrics {
		proof := !udp && o.s.features.UdpReplies
		proofSignal = proofSignal && !udp
		metadata := !udp && o.s.features.LabelRequest
		if registry || proof || activity || proofSignal || metadata {
			return o.newClosureTracker(tag, abort, proof, registry, activity, proofSignal, connectionPath, connectionEpoch, udpEpoch, udpIncarnation, labelEpoch, metadata)
		}
		return nil
	}
	// Capture the measurement epoch before the start timestamp. A network
	// change during construction must not label an older dial as new traffic.
	t := &connTracker{
		o:          o,
		tag:        tag,
		target:     target,
		server:     destString(server),
		conn:       connFrom(ctx),
		udp:        udp,
		activity:   activity,
		proof:      proofSignal,
		abort:      abort,
		started:    o.nowNano(),
		network:    connectionEpoch,
		labelEpoch: labelEpoch,
	}
	if quality != nil {
		quality.started = t.started
	}
	t.quality = quality
	if _, ok := o.e.lastServer.Load(tag); !ok && t.server != "" {
		o.e.lastServer.Store(tag, t.server)
	}

	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		t.udpPath = connectionPath
		t.udpEpoch = udpEpoch
		t.udpIncarnation = udpIncarnation
		o.e.pathAt(tag, o.now())

	}()
	o.track(t)
	return t
}

func (o *Observer) track(t *connTracker) {

	func() {
		o.trackMu.Lock()
		defer o.trackMu.Unlock()

		if o.trackers == nil {
			o.trackers = make(map[*connTracker]struct{})
		}
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

	}()
	t.watch()
}

// watch makes the sweep check t until it has no deadline left.
func (o *Observer) watch(t *connTracker) {
	if o.s.passive.sweepInterval == 0 || !t.needsSweep() {
		t.watched.Store(false)
		return
	}
	o.trackMu.Lock()
	if t.state.Load() == trackerFinished {
		o.trackMu.Unlock()
		return
	}
	if o.watching == nil {
		o.watching = make(map[*connTracker]struct{})
	}
	o.watching[t] = struct{}{}
	start := !o.sweeping
	o.sweeping = true
	done := o.done
	o.trackMu.Unlock()
	if start {
		go o.guard("health.go:Observer.sweepLoop", func() { o.sweepLoop(done) })
	}
}

// closePending ends the connections of an outbound found down that started
// before the decision and have received nothing: applications retry on the
// outbound chosen now instead of waiting for their own timeout, and the
// memory the stuck connections hold is freed. Connections that received
// something keep going: an address that refuses new connections may still
// serve old ones. At the decision it notes what they have received
// (d.watch), and at the mark stallTimeout later ends those that received
// nothing since (d.silent), see silence.go.
func (o *Observer) closePending(d downMark) {
	before := int64(d.at.Sub(o.base))
	var aborts []func()
	o.trackMu.Lock()
	for t := range o.trackers {
		if t.tag != d.tag || t.abort == nil || t.started > before || t.state.Load() == trackerFinished {
			continue
		}
		switch {
		case d.silent:
			if !t.silentSince(before) {
				continue
			}
		case d.pending && t.firstRead.Load() == 0 && !t.spliced.Load():
		default:
			if d.watch {
				t.noteReceived(before)
			}
			continue
		}
		if t.mark(reportedAborted) {
			aborts = append(aborts, t.abort)
		}
	}
	for t := range o.closures {
		if abort := t.closeFor(d, before); abort != nil {
			aborts = append(aborts, abort)
		}
	}
	o.trackMu.Unlock()
	for _, abort := range aborts {
		abort()
	}
	if len(aborts) == 0 {
		return
	}
	f := map[string]any{"outbound": d.tag, "connections": len(aborts), "reason": d.reason}
	if d.silent {
		f["is_silent"] = true
	}

	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		o.e.emit(o.now(), "pending_closed", f)

	}()
}

func (o *Observer) untrack(t *connTracker) {

	func() {
		o.trackMu.Lock()
		defer o.trackMu.Unlock()

		delete(o.trackers, t)
		delete(o.watching, t)
		// Nothing ends an untracked connection, and abort would keep its whole
		// session reachable. closePending reads it under trackMu.
		t.abort = nil

	}()
	t.watched.Store(false)
}

// A sweep batch larger than this is let go after the pass instead of being
// kept for the next one.
const sweepBatchKeep = 256

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
		stop := func() bool {
			o.mu.Lock()
			defer o.mu.Unlock()
			udpPending := o.sweepUDPEvidenceLocked(o.now())
			o.trackMu.Lock()
			defer o.trackMu.Unlock()
			if len(o.watching) == 0 && !udpPending {
				o.sweeping = false
				return true
			}
			batch = batch[:0]
			for t := range o.watching {
				batch = append(batch, t)
			}
			return false
		}()
		if stop {
			return
		}

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
		// Kept between passes, the batch would hold ended connections
		// reachable for as long as the sweep runs, which is under load.
		clear(batch)
		if cap(batch) > sweepBatchKeep {
			batch = nil
		}
	}
}

// failure and success skip connections started before the last network
// change, and failure skips dials that waited through a network pause:
// neither tells anything about outbounds now.
func (o *Observer) failure(t *connTracker, kind failureKind) {
	if !o.s.features.failure(kind) {
		return
	}
	var stale bool
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		o.e.used(t.tag, o.now())
		stale = false
		if kind.pathLevel() {
			p := o.e.paths[t.tag]
			stale = o.e.startedInPause(o.base.Add(time.Duration(t.started))) ||
				(p != nil && t.started < p.lastAnsweredStart)
		}
		if p := o.e.paths[t.tag]; p != nil && t.network == p.controllerEpoch && !stale {
			o.e.onFailure(t.tag, kind, t.target, t.conn, o.now())
		}

	}()
	o.wake()
}

func (o *Observer) answered(t *connTracker) {

	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		o.collectQualityInitial(t.quality, t.tag, false)
		if t.proof || o.s.features.passiveTCP() {
			o.noteControllerReplyLocked(t.tag, t.started, t.network, t.udpPath)
		}

	}()
}

func (o *Observer) success(t *connTracker) {
	if !t.udp {
		o.labelSucceeded(t.labelEpoch)
	}
	o.connectionSuccess(t.tag, t.started, t.network, t.udpPath, t.udpEpoch, t.udpIncarnation,
		!t.udp && o.s.features.UdpReplies, t.proof || o.s.features.passiveTCP())
}

// noteControllerReplyLocked and connectionSuccess are shared by full and
// lightweight trackers. Collector presence never grants controller proof.
func (o *Observer) noteControllerReplyLocked(tag string, started int64, epoch uint64, snapshot *path) {
	p := o.e.paths[tag]
	if p != nil && (snapshot == nil || p == snapshot) && p.controllerEpoch == epoch {
		p.lastAnsweredStart = max(p.lastAnsweredStart, started)
	}
}

func (o *Observer) connectionSuccess(tag string, started int64, epoch uint64, snapshot *path, udpEpoch, incarnation uint64, tcpProof, controllerProof bool) {
	if !tcpProof && !controllerProof {
		return
	}
	o.mu.Lock()
	p := o.e.paths[tag]
	if p == nil || (snapshot != nil && p != snapshot) || p.controllerEpoch != epoch {
		o.mu.Unlock()
		return
	}
	queued := len(o.e.queue)
	if tcpProof && udpEpoch == o.e.udpEpoch && p.udpIncarnation == incarnation {
		p.lastTCPSuccess = o.now()
		o.sweepUDPEvidenceLocked(o.now())
	}
	if controllerProof {
		o.e.onSuccess(tag, o.now())
	}
	more := len(o.e.queue) > queued
	o.mu.Unlock()
	if more || o.s.labelRequest != nil {
		o.wake()
	}
}

func (o *Observer) witnessed(labelEpoch uint64) {
	var queued int
	var more bool
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		o.e.noteLabelSuccess(labelEpoch)
		queued = len(o.e.queue)
		o.e.sawSuccess("witness", o.now())
		more = len(o.e.queue) > queued

	}()
	if more || o.s.labelRequest != nil {
		o.wake()
	}
}

func (o *Observer) finished(t *connTracker) {
	var now time.Time
	var woke bool
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		o.collectQualityEnd(t.quality, t.tag, false, &t.bytesRead)
		now = o.now()
		if o.s.features.Report || o.s.features.Metrics {
			st := &o.e.pathAt(t.tag, now).stats
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
		}
		// The loop plans the sleep of an outbound that has just woken; nothing
		// else may wake it after the last connection ends. The end of one the
		// observer ended, of an outbound found down, may let that outbound sleep
		// now: the use starts its retries afresh, and the loop asks at once
		// rather than when a retry that had grown is due.
		woke = o.e.used(t.tag, now)

	}()
	if woke || t.has(reportedAborted) {
		o.wake()
	}
}

// ---- outbound.ProcessObserver -----------------------------------------------

var _ outbound.ProcessObserver = (*Observer)(nil)

// ProcessStarted implements outbound.ProcessObserver: the proxy of an
// outbound that can sleep processes a connection, which may have woken it
// whatever routed it there.
func (o *Observer) ProcessStarted(tag string) {
	var woke bool
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		woke = o.e.processStarted(tag, o.now())

	}()
	if woke {
		o.wake()
	}
}

// ProcessEnded implements outbound.ProcessObserver.
func (o *Observer) ProcessEnded(tag string) {
	var last bool
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		last = o.e.processEnded(tag, o.now())

	}()
	if last {
		o.wake()
	}
}

// ---- extension.Observatory --------------------------------------------------

// GetObservation implements extension.Observatory.
func (o *Observer) GetObservation(context.Context) (proto.Message, error) {
	if !o.s.features.Metrics {
		return &observatory.ObservationResult{}, nil
	}
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
	if err := validateSelectorFeatures(cfg, o.s.features); err != nil {
		return nil, err
	}
	var s *selector
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		s = o.e.addSelectorAt(balancerTag, newSelectorSettings(cfg), o.now())

	}()
	return &selectorHandle{o: o, s: s}, nil
}

type selectorHandle struct {
	o *Observer
	s *selector
}

// Select implements extension.OutboundSelector.
func (h *selectorHandle) Select(candidates []string) string {
	if h.o.failed.Load() {
		return h.s.current()
	}
	now := h.o.now()
	nano := now.UnixNano()
	if prev := h.o.e.lastDemand.Swap(nano); h.o.s.schedule.idleAfter > 0 && (prev == 0 || now.Sub(time.Unix(0, prev)) > h.o.s.schedule.idleAfter) {
		h.o.wake()
	}
	if snap := h.s.snapshot.Load(); snap == nil || !equalStrings(*snap, candidates) {
		h.o.mu.Lock()
		h.o.e.updateCandidates(h.s, candidates, now)
		h.o.mu.Unlock()
		h.o.wake()
	}
	tag := h.s.current()
	if h.o.s.isProbeOnly(tag) {
		return ""
	}
	h.o.mu.Lock()
	usable := h.o.e.expiredLeaseUsable(tag)
	h.o.mu.Unlock()
	if !usable {
		return ""
	}
	return tag
}

// FallbackBalancer implements the optional router hook. Selection keeps the
// source's health and demand accounting; the router asks the target normally.
func (h *selectorHandle) FallbackBalancer() string {
	h.o.mu.Lock()
	defer h.o.mu.Unlock()
	if h.o.e.selectors[h.s.tag] != h.s || !h.s.exhausted || h.s.pinned != "" {
		return ""
	}
	return h.s.cfg.fallbackBalancer
}

// FallbackSelected starts one interval only after the router selected a target
// outbound. A configuration reload may change the target meanwhile; a stale
// confirmation must not start an interval for the new target.
func (h *selectorHandle) FallbackSelected(to string) {
	if h.o.failed.Load() {
		return
	}
	h.o.mu.Lock()
	defer h.o.mu.Unlock()
	if !h.o.failed.Load() && h.o.e.selectors[h.s.tag] == h.s && h.s.cfg.fallbackBalancer == to {
		h.o.e.beginFallback(h.s, h.o.now())
	}
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
func (o *Observer) NetworkChanged(id string) { o.networkChanged(id, nil) }

// NetworkLost pauses all decisions until NetworkChanged.
func (o *Observer) NetworkLost() {

	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		o.e.cancelLabelRequest(o.now())
		o.e.labelProof = false
		o.e.onNetworkLost(o.now())

	}()
}

// SetMemoryLow tells the observer whether the process is short of memory.
// While it is, the observer puts off what only improves the choice: returns
// to cheaper outbounds, probes after a network change and timed provider
// refreshes. It lets go at once of what it can: drains without connections,
// the tunnels of outbounds no balancer chooses, state about outbounds nothing
// offers. Finding failures and moving away from them go on as usual, and the
// event log keeps every event. It is safe to call from any goroutine;
// repeating the current value does nothing.
func (o *Observer) SetMemoryLow(low bool) {
	var changed bool
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		changed = o.e.setMemoryLow(low, o.now())

	}()
	if !changed {
		return
	}
	if low {
		o.settleAllDrains()
	}
	o.wake()
}

// Pin keeps the balancer on tag until Release.
// run is the run of the core the request is meant for, as Status reports
// it; a request for another run is ignored. An empty run skips the check.
func (o *Observer) Pin(balancer, tag, run string) bool {
	var ok bool
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		ok = o.forThisRun("pin", balancer, run) && o.e.pin(balancer, tag, o.now())

	}()
	o.wake()
	return ok
}

// Release lets the balancer choose again. run works as in Pin.
func (o *Observer) Release(balancer, run string) bool {
	var ok bool
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		ok = o.forThisRun("release", balancer, run) && o.e.release(balancer, o.now())

	}()
	o.wake()
	return ok
}

func (o *Observer) forThisRun(what, balancer, run string) bool {
	if run == "" || run == o.run {
		return true
	}
	o.e.emit(o.now(), "stale_request", map[string]any{"request": what, "balancer": balancer, "run": run})
	return false
}

// DrainEvents returns kept events as a JSON array and the number of events
// dropped because the buffer was full.
func (o *Observer) DrainEvents() ([]byte, uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.collectPanicsLocked()
	return o.e.events.drain()
}

// Status returns the current state of outbounds and balancers as JSON.
// It is local diagnostics: network identity, server_name and last_error may
// contain private addresses. Send Report or TakeReport to analytics instead.
func (o *Observer) Status() []byte {
	var view statusView
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		o.collectPanicsLocked()
		view = o.e.status(o.now())
		_, _, view.BrokenSubsystems = safe.Snapshot(0)
		if o.failed.Load() && !slices.Contains(view.BrokenSubsystems, "app/observatory/health") {
			view.BrokenSubsystems = append(view.BrokenSubsystems, "app/observatory/health")
		}
		view.Run = o.run

	}()
	out, _ := json.Marshal(view)
	return out
}

// TakeReport returns the report of the current window as JSON when one is
// due and starts a new window; nil when none is due. A window with decisions
// is due once it lasts the report interval. final asks for the last report
// of this run before the host stops the core: it is due when the core made
// any decision or sampled this run, and nothing is due after it.
func (o *Observer) TakeReport(final bool) []byte {
	var v *reportView
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		if final {
			o.finishUDPFlowsLocked()
			o.e.interruptCensus("close", o.now())
		}
		o.collectPanicsLocked()
		v = o.e.takeReport(final, o.now())

	}()
	return o.marshalReport(v)
}

// PeekReport returns the report the host would get from TakeReport(true) now,
// with report_reason snapshot, and closes nothing; nil when it would get
// none. A host keeps it to send the window when the process is killed
// before the stop.
func (o *Observer) PeekReport() []byte {
	var v *reportView
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		o.collectPanicsLocked()
		v = o.e.snapshotReport(o.now())

	}()
	return o.marshalReport(v)
}

func (o *Observer) marshalReport(v *reportView) []byte {
	if v == nil {
		return nil
	}
	v.Run = o.run
	out, _ := json.Marshal(v)
	return out
}

// FromInstance returns the health observatory of a running instance, or nil.
func FromInstance(v *core.Instance) *Observer {
	o, _ := v.GetFeature(extension.ObservatoryType()).(*Observer)
	return o
}

var running atomic.Pointer[Observer]

// Current returns the health observatory of the core running in this
// process, or nil. It serves hosts that start the core through a library
// that keeps the instance to itself; with several instances it is the one
// started last.
func Current() *Observer {
	return running.Load()
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		return New(ctx, config.(*Config))
	}))
}

// NeedsObservation keeps optional user hooks off while retaining core-owned
// probe hooks. Selection already records user demand for the minimal profile.
func (o *Observer) NeedsObservation(ctx context.Context, tag string) bool {
	if probeStateFrom(ctx) != nil {
		return true
	}
	if o.s.witness(tag) {
		return true
	}
	if !o.s.observes(tag) {
		return false
	}
	f := o.s.features
	if f.passiveTCP() || f.Quality || f.Report || f.Metrics || f.UdpReplies || f.LabelRequest {
		return true
	}
	activity, proof := o.connectionSignals(tag)
	if activity || proof {
		return true
	}
	if f.TunnelSleep && o.ohm != nil && sleeperOf(o.ohm.GetHandler(tag)) != nil {
		return true
	}
	return o.connectionRegistry(tag)
}
