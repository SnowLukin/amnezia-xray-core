package health

import (
	"math"
	"math/rand"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type probeReason int

const (
	reasonConnectivity probeReason = iota
	reasonConfirm
	reasonFailover
	reasonNetwork
	reasonRemembered
	reasonRecheck
	reasonUpgrade
	// Last: probes of the host give way to those of the observer.
	reasonHost
)

func (r probeReason) String() string {
	switch r {
	case reasonConnectivity:
		return "connectivity"
	case reasonConfirm:
		return "confirm"
	case reasonFailover:
		return "failover"
	case reasonNetwork:
		return "network_change"
	case reasonRemembered:
		return "remembered"
	case reasonRecheck:
		return "recheck"
	case reasonUpgrade:
		return "upgrade"
	case reasonHost:
		return "host"
	default:
		return "unknown"
	}
}

// probeJob is one probe of an outbound. Jobs are shared: one probe of a tag
// for a reason answers every balancer waiting for it.
type probeJob struct {
	id         uint64
	tag        string
	reason     probeReason
	selector   string
	notBefore  time.Time
	generation uint64
	// The request of the host that asked for the probe.
	request uint64
}

type probeResult struct {
	ok    bool
	rtt   time.Duration
	bytes int64
	err   string
	// The observer ended the probe on close; it tells nothing about the outbound.
	canceled bool
}

// Outbounds no balancer offers any more are forgotten after this long.
const staleAfter = 30 * time.Minute

// While memory is low a network check in doubt is repeated at most this often.
const lowMemoryRecheck = 2 * time.Minute

// engine makes every decision. It never blocks and never reads the clock:
// the caller passes the time and runs the probes the engine asks for.
// It is not safe for concurrent use; the observer guards it with a mutex.
type engine struct {
	s         *settings
	rnd       *rand.Rand
	paths     map[string]*path
	selectors map[string]*selector
	events    *eventLog
	// Last server address dialed per tag, for outbounds without a server
	// name. Dials write it without the observer lock.
	lastServer sync.Map

	queue   []*probeJob
	running map[uint64]*probeJob
	nextID  uint64

	// When the spacing after each recent probe start ends.
	spacedUntil   []time.Time
	starts        []time.Time
	startsByName  map[string][]time.Time
	hostStarts    []time.Time
	serverNameFor func(tag string) string

	// Probe requests of the host, open ones and the latest closed ones.
	requests  []*probeRequest
	requestID uint64

	generation uint64
	networkID  string
	networks   map[string]map[string]string
	netOrder   []string
	// The network was remembered when the host reported it.
	networkKnown bool

	lastDemand atomic.Int64

	networkDown bool
	// When the last pause ended: dials that started before it have waited for
	// a network that was not there.
	resumedAt          time.Time
	checkFails         int
	checkFailingSince  time.Time
	nextConnectivity   time.Time
	lastConnectivityOK time.Time
	// The check kept failing for maxPause and decisions resumed. What they
	// rule out comes back once a check passes.
	doubt bool
	// The check failed while outbounds worked; it is not used in this network.
	connectivityIgnored bool
	lastSuccess         time.Time
	pendingSuspicion    map[string]verdict
	// Outbounds found down whose waiting connections the observer is to end.
	downs []downMark

	// Costs that providers sent for their outbounds, by balancer and outbound.
	costs map[string]map[string]float64
	// Outbounds that left a provider set: they keep their connections but
	// are no longer candidates.
	retiring map[string]bool
	// Outbounds whose handler a provider is swapping right now.
	replacing map[string]bool
	providers []*provider

	lastGC time.Time
	// The host reported that memory is low: work that only improves the
	// choice waits, finding failures does not.
	memoryLow bool

	report report
}

func newEngine(s *settings, seed int64) *engine {
	e := &engine{
		s:                s,
		rnd:              rand.New(rand.NewSource(seed)),
		paths:            map[string]*path{},
		selectors:        map[string]*selector{},
		events:           newEventLog(s.maxEvents, s.maxBytes),
		running:          map[uint64]*probeJob{},
		startsByName:     map[string][]time.Time{},
		networks:         map[string]map[string]string{},
		pendingSuspicion: map[string]verdict{},
		serverNameFor:    func(tag string) string { return tag },
		costs:            map[string]map[string]float64{},
		retiring:         map[string]bool{},
		replacing:        map[string]bool{},
	}
	for _, ps := range s.providers {
		e.providers = append(e.providers, newProvider(ps))
	}
	return e
}

func (e *engine) cost(s *selector, tag string) float64 {
	if c, ok := e.costs[s.tag][tag]; ok {
		return c
	}
	return s.cfg.cost(tag)
}

func (e *engine) jitter(d time.Duration, j float64) time.Duration {
	if j <= 0 || d <= 0 {
		return d
	}
	return time.Duration(float64(d) * (1 + j*(2*e.rnd.Float64()-1)))
}

func (e *engine) path(tag string) *path {
	p := e.paths[tag]
	if p == nil {
		p = &path{tag: tag}
		e.paths[tag] = p
	}
	return p
}

func (e *engine) emit(now time.Time, typ string, fields map[string]any) {
	e.events.add(now, typ, fields)
	e.report.count(typ, fields)
}

func (e *engine) noteDemand(now time.Time) {
	e.lastDemand.Store(now.UnixNano())
}

func (e *engine) idle(now time.Time) bool {
	last := e.lastDemand.Load()
	return last == 0 || now.Sub(time.Unix(0, last)) > e.s.schedule.pauseWhenIdle
}

func (e *engine) active(tag string) bool {
	for _, s := range e.selectors {
		if s.current() == tag {
			return true
		}
	}
	return false
}

// ---- passive evidence -----------------------------------------------------

func (e *engine) onFailure(tag string, kind failureKind, target string, conn uint32, now time.Time) {
	if e.networkDown {
		return
	}
	if e.s.passive.dialOnly && !kind.pathLevel() {
		return
	}
	p := e.path(tag)
	if p.status == statusDown && p.quarantined(now) {
		return
	}
	if p.status == statusSuspect {
		if _, ok := e.pendingSuspicion[p.tag]; ok {
			// Waits for the network check; the network may be what failed.
			return
		}
		if e.queued(p.tag, reasonConfirm) {
			// The probe confirms weak evidence; strong evidence needs no proof.
			if p.addFailure(failure{at: now, kind: kind, target: target, conn: conn}, e.s.passive.window) &&
				p.evaluate(&e.s.passive).strong {
				e.dropQueued(p.tag, reasonConfirm)
				e.markDown(p, "traffic", now)
			}
			return
		}
	}
	if !p.addFailure(failure{at: now, kind: kind, target: target, conn: conn}, e.s.passive.window) {
		return
	}
	v := p.evaluate(&e.s.passive)
	if !v.suspect {
		return
	}
	// Strong evidence does not wait for the cooldown that false alarms of
	// weak evidence earned.
	if !v.strong && now.Before(p.cooldownUntil) {
		return
	}
	e.suspect(p, v, now)
}

// awaiting reports whether a decision about the outbound is already on its way.
func (e *engine) awaiting(tag string) bool {
	if _, ok := e.pendingSuspicion[tag]; ok {
		return true
	}
	return e.queued(tag, reasonConfirm)
}

func (e *engine) onSuccess(tag string, now time.Time) {
	p := e.path(tag)
	e.sawSuccess("traffic", now)
	if p.status == statusDown && p.quarantined(now) {
		// Old connections may still work on an address that refuses new ones.
		return
	}
	if p.status == statusSuspect {
		// Traffic answered before the suspicion was proven: the same false
		// alarm as a confirming probe that passes, without spending the probe.
		e.dropQueued(p.tag, reasonConfirm)
		e.backOffWeakEvidence(p, now)
	}
	p.success(now)
	if p.status != statusUp {
		p.quarantineLevel = 0
		p.provisional = false
		if p.setStatus(statusUp, "traffic", now) {
			e.emitPath(p, now)
		}
	}
	e.remember(tag)
}

func (e *engine) emitPath(p *path, now time.Time) {
	f := map[string]any{"outbound": p.tag, "state": p.status.String(), "reason": p.reason}
	if p.status == statusDown {
		f["quarantine_ms"] = p.quarantineUntil.Sub(now).Milliseconds()
		f["quarantine_level"] = p.quarantineLevel
		if p.provisional {
			f["provisional"] = true
		}
	}
	e.emit(now, "path_state", f)
}

func (e *engine) suspect(p *path, v verdict, now time.Time) {
	if !e.active(p.tag) {
		// Old connections on an outbound that is no longer chosen. Strong
		// evidence still rules it out; weak evidence is not worth a probe.
		if v.strong {
			e.markDown(p, "traffic", now)
		} else {
			p.failures = p.failures[:0]
		}
		return
	}
	p.setStatus(statusSuspect, "traffic", now)
	e.emit(now, "path_state", map[string]any{
		"outbound": p.tag, "state": "suspect", "reason": "traffic",
		"failures": v.failures, "targets": v.targets, "kinds": v.kinds, "strong": v.strong,
	})
	if e.s.connectivity != nil && !e.connectivityIgnored && !e.doubt && now.Sub(e.lastConnectivityOK) > e.s.connectivity.timeout*4 {
		e.pendingSuspicion[p.tag] = v
		e.enqueueConnectivity(now)
		return
	}
	e.decideSuspect(p, v, now)
}

func (e *engine) decideSuspect(p *path, v verdict, now time.Time) {
	confirm := false
	for _, s := range e.selectors {
		if s.current() != p.tag {
			continue
		}
		switch s.cfg.confirm {
		case ConfirmMode_ALWAYS:
			confirm = true
		case ConfirmMode_AUTO:
			if !v.strong {
				confirm = true
			}
		}
	}
	if confirm {
		e.enqueue(p.tag, reasonConfirm, "", now)
		return
	}
	e.markDown(p, "traffic", now)
}

// ---- state changes --------------------------------------------------------

func (e *engine) markDown(p *path, reason string, now time.Time) {
	if p.status == statusDown && p.quarantined(now) {
		// Ruled out already; a late probe or more traffic adds nothing, and a
		// second failover would abandon the candidate being probed.
		if p.provisional && !e.doubt {
			p.provisional = false
			p.quarantineLevel++
		}
		return
	}
	rest := e.jitter(e.s.schedule.quarantine.delay(p.quarantineLevel), e.s.schedule.quarantine.jitter)
	// While the check keeps failing the network may be what failed, so the
	// rest does not lengthen the next one.
	p.provisional = e.doubt
	if !e.doubt {
		p.quarantineLevel++
	}
	p.quarantineUntil = now.Add(rest)
	p.setStatus(statusDown, reason, now)
	p.failures = p.failures[:0]
	delete(e.pendingSuspicion, p.tag)
	e.emitPath(p, now)
	e.noteDown(p, now)
	for _, s := range e.sortedSelectors() {
		if s.returnedTo == p.tag && now.Sub(s.returnedAt) < s.cfg.upgrade.penaltyWindow {
			base := maxDuration(s.cfg.upgrade.interval, s.cfg.minHold)
			s.penalty[p.tag] = minFloat(s.penaltyFor(p.tag)*s.cfg.upgrade.penaltyMultiplier,
				float64(s.cfg.upgrade.maxPenalty)/float64(base))
			e.emit(now, "upgrade_penalty", map[string]any{"balancer": s.tag, "outbound": p.tag, "multiplier": s.penalty[p.tag]})
			s.returnedTo = ""
		}
		if s.current() == p.tag {
			e.failover(s, now)
		}
	}
}

// downMark is an outbound found down: its connections that started before
// at and have received nothing will receive nothing.
type downMark struct {
	tag    string
	at     time.Time
	reason string
}

// noteDown asks the observer to end what waits on p. While the network
// itself is in doubt, waiting connections may still be answered once it is
// back, so they are kept.
func (e *engine) noteDown(p *path, now time.Time) {
	if p.status == statusDown && !p.provisional && !e.s.passive.keepPending {
		e.downs = append(e.downs, downMark{tag: p.tag, at: now, reason: p.reason})
	}
}

func (e *engine) takeDowns() []downMark {
	d := e.downs
	e.downs = nil
	return d
}

func (e *engine) markUp(p *path, reason string, now time.Time) {
	p.success(now)
	p.quarantineLevel = 0
	p.quarantineUntil = time.Time{}
	p.provisional = false
	if p.setStatus(statusUp, reason, now) {
		e.emitPath(p, now)
	}
	e.remember(p.tag)
}

// ---- selection --------------------------------------------------------------

type selector struct {
	tag string
	cfg *selectorSettings

	candidates []string
	snapshot   atomic.Pointer[[]string]
	active     atomic.Pointer[string]

	started     bool
	activeSince time.Time
	pinned      string
	// The start pin releases itself at this time; zero waits for the host.
	pinnedUntil time.Time
	exhausted   bool
	// The network changed while pinned; the release rechecks the choice.
	recheckOnRelease bool

	// Candidates of the failover under way: every one tried, and the ones
	// whose probe has not answered yet.
	failoverTried   map[string]bool
	failoverProbing map[string]bool

	upgradeAt        time.Time
	upgradeTarget    string
	upgradeSuccesses int
	// The longer wait for a penalized target was already served.
	penaltyServed bool

	// A failed return multiplies the wait before the next return to that
	// outbound. A return that holds for penaltyWindow clears it.
	penalty    map[string]float64
	returnedTo string
	returnedAt time.Time
}

func (s *selector) current() string {
	if p := s.active.Load(); p != nil {
		return *p
	}
	return ""
}

func (s *selector) setActive(tag string) {
	s.active.Store(&tag)
}

func (s *selector) penaltyFor(tag string) float64 {
	if v, ok := s.penalty[tag]; ok {
		return v
	}
	return 1
}

func (s *selector) hasCandidate(tag string) bool {
	for _, c := range s.candidates {
		if c == tag {
			return true
		}
	}
	return false
}

func (e *engine) addSelector(tag string, cfg *selectorSettings) *selector {
	if s, ok := e.selectors[tag]; ok {
		s.cfg = cfg
		return s
	}
	s := &selector{tag: tag, cfg: cfg, penalty: map[string]float64{}}
	s.setActive("")
	e.selectors[tag] = s
	return s
}

// ranked returns usable candidates from cheapest to most expensive. Within
// one cost, outbounds known to work go first, then candidate order.
func (e *engine) ranked(s *selector, now time.Time, skip func(string) bool) []string {
	type item struct {
		tag   string
		cost  float64
		known int
		order int
	}
	var items []item
	for i, c := range s.candidates {
		if skip != nil && skip(c) {
			continue
		}
		p := e.path(c)
		if !p.usable(now) {
			continue
		}
		known := 1
		if p.status == statusUp {
			known = 0
		}
		items = append(items, item{c, e.cost(s, c), known, i})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].cost != items[j].cost {
			return items[i].cost < items[j].cost
		}
		if items[i].known != items[j].known {
			return items[i].known < items[j].known
		}
		return items[i].order < items[j].order
	})
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.tag
	}
	return out
}

func (e *engine) updateCandidates(s *selector, candidates []string, now time.Time) {
	for t := range e.replacing {
		if s.offers(t) && !slices.Contains(candidates, t) {
			// Taken in the moment the provider swaps the handler: the next
			// connection brings the complete list.
			return
		}
	}
	s.candidates = make([]string, 0, len(candidates))
	for _, c := range candidates {
		if !e.retiring[c] {
			s.candidates = append(s.candidates, c)
		}
	}
	snap := append([]string(nil), candidates...)
	s.snapshot.Store(&snap)
	lostProbe := false
	for t := range s.failoverProbing {
		if !s.hasCandidate(t) {
			delete(s.failoverProbing, t)
			lostProbe = true
		}
	}
	if lostProbe {
		e.dropUnclaimedFailover()
	}
	if s.upgradeTarget != "" && !s.hasCandidate(s.upgradeTarget) {
		s.upgradeTarget = ""
	}
	cur := s.current()
	switch {
	case !s.started:
		s.started = true
		e.start(s, now)
	case s.pinned != "":
	case cur != "" && e.retiring[cur] && slices.Contains(candidates, cur):
		e.leaveRetiring(s, now)
	case cur != "" && !s.hasCandidate(cur):
		e.failover(s, now)
	case s.exhausted:
		if r := e.ranked(s, now, nil); len(r) > 0 {
			e.switchTo(s, r[0], "candidates_changed", now)
		}
	case lostProbe && e.path(cur).status == statusDown:
		e.failover(s, now)
	case s.upgradeAt.IsZero() && s.upgradeTarget == "" && len(s.failoverProbing) == 0:
		// Nothing was cheaper at the last switch; a new cheaper candidate is
		// worth a return.
		e.scheduleUpgrade(s, now, true)
	}
}

func (e *engine) start(s *selector, now time.Time) {
	if s.cfg.startPinned != "" && s.hasCandidate(s.cfg.startPinned) {
		s.pinned = s.cfg.startPinned
		if s.cfg.startPinnedFor > 0 {
			s.pinnedUntil = now.Add(s.cfg.startPinnedFor)
		}
		e.switchTo(s, s.pinned, "start_pinned", now)
		return
	}
	if tag := e.rememberedFor(s); tag != "" && s.hasCandidate(tag) && e.path(tag).usable(now) {
		e.switchTo(s, tag, "start_remembered", now)
		return
	}
	if tag := s.cfg.startWith; tag != "" && s.hasCandidate(tag) && e.path(tag).usable(now) {
		e.switchTo(s, tag, "start_with", now)
		return
	}
	r := e.ranked(s, now, nil)
	if len(r) == 0 {
		e.exhaust(s, now)
		return
	}
	e.switchTo(s, r[0], "start", now)
}

func (e *engine) switchTo(s *selector, tag string, reason string, now time.Time) {
	from := s.current()
	if from != "" && from != tag && !s.activeSince.IsZero() {
		e.path(from).stats.activeTime += now.Sub(s.activeSince)
	}
	if from != "" && from != tag {
		// Connections kept going to the dead outbound while its replacement
		// was being probed.
		e.noteDown(e.path(from), now)
	}
	held := time.Duration(0)
	if !s.activeSince.IsZero() {
		held = now.Sub(s.activeSince)
	}
	// An outbound that rested its quarantine is judged afresh once it is
	// chosen again, otherwise new failures would not count.
	if p := e.path(tag); (p.status == statusDown || p.status == statusSuspect) && !p.quarantined(now) {
		p.setStatus(statusUnknown, "chosen", now)
		p.failures = p.failures[:0]
	}
	s.setActive(tag)
	s.activeSince = now
	s.exhausted = false
	s.failoverTried = nil
	s.failoverProbing = nil
	e.dropUnclaimedFailover()
	s.upgradeTarget = ""
	s.upgradeSuccesses = 0
	if reason == "upgrade" {
		s.returnedTo = tag
		s.returnedAt = now
	}
	e.scheduleUpgrade(s, now, false)
	e.emit(now, "switch", map[string]any{
		"balancer": s.tag, "from": from, "to": tag, "reason": reason,
		"held_ms": held.Milliseconds(), "cost": e.cost(s, tag),
	})
}

func (e *engine) exhaust(s *selector, now time.Time) {
	from := s.current()
	if from != "" && !s.activeSince.IsZero() {
		e.path(from).stats.activeTime += now.Sub(s.activeSince)
	}
	s.setActive("")
	s.activeSince = time.Time{}
	s.exhausted = true
	s.failoverProbing = nil
	e.dropUnclaimedFailover()
	s.upgradeTarget = ""
	e.emit(now, "exhausted", map[string]any{"balancer": s.tag, "from": from, "candidates": len(s.candidates)})
}

func (e *engine) failover(s *selector, now time.Time) {
	if s.pinned != "" {
		e.emit(now, "pinned_down", map[string]any{"balancer": s.tag, "outbound": s.pinned})
		return
	}
	if s.failoverTried == nil {
		s.failoverTried = map[string]bool{}
	}
	if cur := s.current(); cur != "" {
		s.failoverTried[cur] = true
	}
	if !s.cfg.switchAfterProbe {
		next := e.ranked(s, now, func(t string) bool { return s.failoverTried[t] })
		if len(next) == 0 {
			e.exhaust(s, now)
			return
		}
		e.switchTo(s, next[0], "failover", now)
		return
	}
	if !e.probeCandidates(s, now) {
		e.exhaust(s, now)
	}
}

// leaveRetiring moves the balancer off an outbound that left its provider
// set. Unlike a failover the outbound still works, so the balancer stays on
// it while there is nothing else to take.
func (e *engine) leaveRetiring(s *selector, now time.Time) {
	if s.pinned != "" {
		return
	}
	if e.path(s.current()).status == statusDown {
		e.failover(s, now)
		return
	}
	if s.failoverTried == nil {
		s.failoverTried = map[string]bool{}
	}
	s.failoverTried[s.current()] = true
	if !s.cfg.switchAfterProbe {
		if next := e.ranked(s, now, func(t string) bool { return s.failoverTried[t] }); len(next) > 0 {
			e.switchTo(s, next[0], "retired", now)
			return
		}
	} else if e.probeCandidates(s, now) {
		return
	}
	// Staying; once the drain ends, a failover considers every candidate
	// again, also the ones that failed here and rested since.
	s.failoverTried = nil
}

// probeCandidates moves the balancer to the cheapest candidate that passed
// its probe, once no cheaper candidate is open: being probed or not tried
// yet. Until then it probes the cheapest open candidates, up to concurrent
// at a time, so that dead candidates do not cost a probe timeout each in
// turn. It reports false when no candidate is left.
func (e *engine) probeCandidates(s *selector, now time.Time) bool {
	cur := s.current()
	ranked := e.ranked(s, now, func(t string) bool { return t == cur })
	if tag := e.failoverChoice(s, ranked); tag != "" {
		reason := "failover"
		if e.retiring[cur] {
			reason = "retired"
		}
		e.switchTo(s, tag, reason, now)
		return true
	}
	if s.failoverProbing == nil {
		s.failoverProbing = map[string]bool{}
	}
	names := map[string]bool{}
	for t := range s.failoverProbing {
		names[e.serverName(t)] = true
	}
	open := math.Inf(1)
	for _, t := range ranked {
		if len(s.failoverProbing) >= e.s.schedule.concurrent {
			break
		}
		c := e.cost(s, t)
		switch {
		case s.failoverProbing[t]:
		case s.failoverTried[t]:
			if e.path(t).status == statusUp {
				// It passed and waits for cheaper ones; nothing after it is better.
				return true
			}
			continue
		case names[e.serverName(t)]:
			// Its turn comes when the probe of its server name has answered.
		case s.cfg.isLazy(t) && open < c:
			// Lazy outbounds are tried only once everything cheaper is down.
		default:
			s.failoverTried[t] = true
			s.failoverProbing[t] = true
			names[e.serverName(t)] = true
			e.enqueue(t, reasonFailover, s.tag, now)
		}
		open = math.Min(open, c)
	}
	return len(s.failoverProbing) > 0
}

// failoverChoice returns the cheapest candidate that passed its probe in the
// failover under way, unless a cheaper candidate is still open.
func (e *engine) failoverChoice(s *selector, ranked []string) string {
	open := math.Inf(1)
	for _, t := range ranked {
		c := e.cost(s, t)
		switch {
		case s.failoverProbing[t] || !s.failoverTried[t]:
			open = math.Min(open, c)
		case e.path(t).status == statusUp:
			if open < c {
				return ""
			}
			return t
		}
	}
	return ""
}

// dropUnclaimedFailover forgets failover probes that have not started and
// that no balancer waits for any more.
func (e *engine) dropUnclaimedFailover() {
	kept := e.queue[:0]
	for _, j := range e.queue {
		if j.reason != reasonFailover || e.claimed(j.tag) {
			kept = append(kept, j)
		}
	}
	e.queue = kept
}

func (e *engine) claimed(tag string) bool {
	for _, s := range e.selectors {
		if s.failoverProbing[tag] {
			return true
		}
	}
	return false
}

// refreshCandidates applies a change of retiring outbounds to every balancer.
func (e *engine) refreshCandidates(now time.Time) {
	for _, s := range e.sortedSelectors() {
		if snap := s.snapshot.Load(); snap != nil && s.started {
			e.updateCandidates(s, *snap, now)
		}
	}
}

func (e *engine) scheduleUpgrade(s *selector, now time.Time, soon bool) {
	s.upgradeAt = time.Time{}
	cur := s.current()
	if cur == "" || s.pinned != "" || !e.hasCheaper(s, cur) {
		return
	}
	if soon {
		s.upgradeAt = maxTime(now, s.activeSince.Add(s.cfg.minHold))
		return
	}
	u := s.cfg.upgrade
	wait := u.interval
	if u.randomPhase {
		wait = time.Duration(e.rnd.Float64() * float64(u.interval))
	}
	s.upgradeAt = maxTime(now.Add(wait), s.activeSince.Add(s.cfg.minHold))
}

func (e *engine) hasCheaper(s *selector, cur string) bool {
	c := e.cost(s, cur)
	for _, t := range s.candidates {
		if e.cost(s, t) < c && !s.cfg.isLazy(t) {
			return true
		}
	}
	return false
}

func (e *engine) upgradeTargetFor(s *selector, now time.Time) string {
	cur := s.current()
	c := e.cost(s, cur)
	for _, t := range e.ranked(s, now, func(t string) bool { return t == cur || s.cfg.isLazy(t) }) {
		if e.cost(s, t) < c {
			return t
		}
	}
	return ""
}

// ---- probes -----------------------------------------------------------------

// queued reports whether a probe of tag for reason is waiting or running.
// Probes made in a previous network do not count: their results are dropped.
func (e *engine) queued(tag string, reason probeReason) bool {
	same := func(j *probeJob) bool {
		return j.tag == tag && j.reason == reason && (reason == reasonConnectivity || j.generation == e.generation)
	}
	for _, j := range e.queue {
		if same(j) {
			return true
		}
	}
	for _, j := range e.running {
		if same(j) {
			return true
		}
	}
	return false
}

func (e *engine) dropQueued(tag string, reason probeReason) {
	kept := e.queue[:0]
	for _, j := range e.queue {
		if j.tag != tag || j.reason != reason {
			kept = append(kept, j)
		}
	}
	e.queue = kept
}

// enqueue returns the new job, or nil when one is already waiting or running.
func (e *engine) enqueue(tag string, reason probeReason, selectorTag string, at time.Time) *probeJob {
	if e.queued(tag, reason) {
		return nil
	}
	e.nextID++
	j := &probeJob{id: e.nextID, tag: tag, reason: reason, selector: selectorTag, notBefore: at, generation: e.generation}
	e.queue = append(e.queue, j)
	return j
}

func (e *engine) enqueueConnectivity(now time.Time) {
	if e.queued("", reasonConnectivity) {
		return
	}
	e.nextID++
	e.queue = append(e.queue, &probeJob{id: e.nextID, reason: reasonConnectivity, notBefore: now, generation: e.generation})
}

// budgetWait returns how long the job has to wait for the probe budget, or
// -1 while it waits for a running probe to end: all probe slots are busy or
// one probes its server name.
func (e *engine) budgetWait(j *probeJob, now time.Time) time.Duration {
	if len(e.running) >= e.s.schedule.concurrent {
		return -1
	}
	if j.reason == reasonConnectivity {
		return 0
	}
	name := e.serverName(j.tag)
	for _, r := range e.running {
		if r.reason != reasonConnectivity && e.serverName(r.tag) == name {
			// Simultaneous handshakes to one server name are a censor signal.
			return -1
		}
	}
	var wait time.Duration
	// At most concurrent probes start within one spacing: with one slot that
	// is a pause after every start, with more the starts may come together.
	spaced := e.spacedUntil[:0]
	for _, t := range e.spacedUntil {
		if t.After(now) {
			spaced = append(spaced, t)
		}
	}
	e.spacedUntil = spaced
	if n := len(spaced); n >= e.s.schedule.concurrent {
		ends := slices.Clone(spaced)
		slices.SortFunc(ends, func(a, b time.Time) int { return a.Compare(b) })
		wait = ends[n-e.s.schedule.concurrent].Sub(now)
	}
	e.starts = pruneTimes(e.starts, now)
	if len(e.starts) >= e.s.schedule.perMinute {
		if w := e.starts[0].Add(time.Minute).Sub(now); w > wait {
			wait = w
		}
	}
	byName := pruneTimes(e.startsByName[name], now)
	e.startsByName[name] = byName
	if len(byName) >= e.s.schedule.perServerName {
		if w := byName[0].Add(time.Minute).Sub(now); w > wait {
			wait = w
		}
	}
	if j.reason == reasonHost {
		// The host gets at most half of the probes of a server name, so a
		// failover after its probes still finds the name in the budget.
		e.hostStarts = pruneTimes(e.hostStarts, now)
		if n := len(e.hostStarts); n >= e.s.schedule.hostProbes.perMinute {
			wait = max(wait, e.hostStarts[n-e.s.schedule.hostProbes.perMinute].Add(time.Minute).Sub(now))
		}
		if share := max(e.s.schedule.perServerName/2, 1); len(byName) >= share {
			wait = max(wait, byName[len(byName)-share].Add(time.Minute).Sub(now))
		}
	}
	return wait
}

func (e *engine) serverName(tag string) string {
	p := e.path(tag)
	if p.serverName == "" {
		p.serverName = e.serverNameFor(tag)
	}
	return p.serverName
}

func pruneTimes(ts []time.Time, now time.Time) []time.Time {
	cut := 0
	for cut < len(ts) && now.Sub(ts[cut]) >= time.Minute {
		cut++
	}
	return ts[cut:]
}

// tick does the timed work and returns probes to start now and when to call
// it again (zero means only on new events).
func (e *engine) tick(now time.Time) ([]*probeJob, time.Time) {
	var wake time.Time
	soonest := func(t time.Time) {
		if !t.IsZero() && (wake.IsZero() || t.Before(wake)) {
			wake = t
		}
	}

	idle := e.idle(now)
	for _, s := range e.sortedSelectors() {
		if s.pinned == "" || s.pinnedUntil.IsZero() {
			continue
		}
		if now.Before(s.pinnedUntil) {
			soonest(s.pinnedUntil)
			continue
		}
		e.releaseBy(s.tag, "timer", now)
	}
	if (e.networkDown || e.doubt) && !e.nextConnectivity.IsZero() && !idle {
		if !now.Before(e.nextConnectivity) {
			e.nextConnectivity = time.Time{}
			e.enqueueConnectivity(now)
		} else {
			soonest(e.nextConnectivity)
		}
	}

	if !e.networkDown {
		for _, s := range e.sortedSelectors() {
			if s.returnedTo != "" && now.Sub(s.returnedAt) >= s.cfg.upgrade.penaltyWindow {
				delete(s.penalty, s.returnedTo)
				s.returnedTo = ""
			}
			if s.exhausted && !idle {
				soonest(e.recheckExhausted(s, now))
			}
			if !idle && !e.memoryLow {
				soonest(e.maybeUpgrade(s, now))
			}
		}
	}

	if now.Sub(e.lastGC) >= staleAfter {
		e.gc(now, staleAfter)
	}
	soonest(e.serveRequests(now))

	var start []*probeJob
	// Once a probe of the observer waits for the budget, probes of the host
	// wait behind it.
	waiting := false
	sort.SliceStable(e.queue, func(i, j int) bool { return e.queue[i].reason < e.queue[j].reason })
	kept := e.queue[:0]
	for _, j := range e.queue {
		if j.generation != e.generation && j.reason != reasonConnectivity {
			continue
		}
		if j.reason != reasonConnectivity && (e.networkDown || e.putOff(j, idle)) {
			kept = append(kept, j)
			continue
		}
		if now.Before(j.notBefore) {
			soonest(j.notBefore)
			kept = append(kept, j)
			continue
		}
		if j.reason == reasonHost && waiting {
			kept = append(kept, j)
			continue
		}
		w := e.budgetWait(j, now)
		if w != 0 {
			if w > 0 {
				soonest(now.Add(w))
			}
			waiting = waiting || j.reason != reasonHost
			kept = append(kept, j)
			continue
		}
		e.begin(j, now)
		start = append(start, j)
	}
	e.queue = kept
	return start, wake
}

// putOff tells whether a probe waits for the first user connection after a
// network change, or for memory: it only improves the choice, and nobody
// needs its answer before that.
func (e *engine) putOff(j *probeJob, idle bool) bool {
	switch j.reason {
	case reasonNetwork, reasonRemembered:
		return idle || e.memoryLow
	case reasonUpgrade:
		return e.memoryLow
	}
	return false
}

func (e *engine) sortedSelectors() []*selector {
	out := make([]*selector, 0, len(e.selectors))
	for _, s := range e.selectors {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].tag < out[j].tag })
	return out
}

func (e *engine) begin(j *probeJob, now time.Time) {
	e.running[j.id] = j
	if j.reason == reasonConnectivity {
		return
	}
	e.spacedUntil = append(e.spacedUntil, now.Add(e.jitter(e.s.schedule.spacing, e.s.schedule.spacingJitter)))
	e.starts = append(e.starts, now)
	if j.reason == reasonHost {
		e.hostStarts = append(e.hostStarts, now)
	}
	name := e.serverName(j.tag)
	e.startsByName[name] = append(e.startsByName[name], now)
	p := e.path(j.tag)
	p.probing = true
	p.lastProbe = now
	running := 0
	for _, r := range e.running {
		if r.reason != reasonConnectivity {
			running++
		}
	}
	f := map[string]any{"outbound": j.tag, "reason": j.reason.String(), "balancer": j.selector, "running": running}
	if j.request != 0 {
		f["request"] = j.request
	}
	e.emit(now, "probe_start", f)
}

func (e *engine) recheckExhausted(s *selector, now time.Time) time.Time {
	var wake time.Time
	for _, c := range e.ranked(s, time.Unix(1<<40, 0), nil) {
		p := e.path(c)
		if e.queued(c, reasonRecheck) {
			return time.Time{}
		}
		if now.Before(p.quarantineUntil) {
			if wake.IsZero() || p.quarantineUntil.Before(wake) {
				wake = p.quarantineUntil
			}
			continue
		}
		e.enqueue(c, reasonRecheck, s.tag, now)
		return time.Time{}
	}
	return wake
}

func (e *engine) maybeUpgrade(s *selector, now time.Time) time.Time {
	if s.upgradeAt.IsZero() || s.exhausted || s.pinned != "" || s.upgradeTarget != "" || len(s.failoverProbing) > 0 {
		return time.Time{}
	}
	if now.Before(s.upgradeAt) {
		return s.upgradeAt
	}
	target := e.upgradeTargetFor(s, now)
	if target == "" {
		// Cheaper outbounds rest in quarantine; look again when the first rest ends.
		var wake time.Time
		cur := e.cost(s, s.current())
		for _, c := range s.candidates {
			p := e.path(c)
			if e.cost(s, c) < cur && !s.cfg.isLazy(c) && p.quarantined(now) {
				if wake.IsZero() || p.quarantineUntil.Before(wake) {
					wake = p.quarantineUntil
				}
			}
		}
		s.upgradeAt = wake
		return wake
	}
	if pen := s.penaltyFor(target); pen > 1 && !s.penaltyServed {
		delay := time.Duration(float64(maxDuration(s.cfg.upgrade.interval, s.cfg.minHold)) * (pen - 1))
		s.penaltyServed = true
		s.upgradeAt = now.Add(delay)
		e.emit(now, "upgrade_delayed", map[string]any{"balancer": s.tag, "outbound": target, "delay_ms": delay.Milliseconds()})
		return s.upgradeAt
	}
	s.penaltyServed = false
	s.upgradeTarget = target
	s.upgradeSuccesses = 0
	e.emit(now, "upgrade_start", map[string]any{"balancer": s.tag, "from": s.current(), "to": target})
	e.enqueue(target, reasonUpgrade, s.tag, now)
	return time.Time{}
}

func (e *engine) onProbeResult(j *probeJob, r probeResult, now time.Time) {
	delete(e.running, j.id)
	if r.canceled {
		if p := e.paths[j.tag]; p != nil {
			p.probing = false
		}
		return
	}
	if j.reason == reasonConnectivity {
		e.onConnectivity(r, now)
		return
	}
	p := e.path(j.tag)
	p.probing = false
	p.stats.probes++
	p.stats.probeBytes += r.bytes
	if !r.ok {
		p.stats.probesFailed++
		p.lastError = r.err
	} else {
		p.lastRTT = r.rtt
		p.lastError = ""
	}
	stale := j.generation != e.generation
	f := map[string]any{
		"outbound": j.tag, "reason": j.reason.String(), "balancer": j.selector, "ok": r.ok,
		"rtt_ms": r.rtt.Milliseconds(), "bytes": r.bytes, "error": r.err, "stale": stale,
	}
	if j.request != 0 {
		f["request"] = j.request
	}
	e.emit(now, "probe_result", f)
	if stale {
		return
	}
	if r.ok {
		e.sawSuccess("probe", now)
	} else if e.networkDown {
		// Without a network the failure says nothing about the outbound.
		if again := e.enqueue(j.tag, j.reason, j.selector, now); again != nil {
			again.request = j.request
		}
		return
	}
	e.answerRequests(j, r, now)
	switch j.reason {
	case reasonConfirm:
		if p.status != statusSuspect {
			return
		}
		if r.ok {
			e.clearedSuspicion(p, now)
			return
		}
		p.falseAlarms = 0
		e.markDown(p, "probe", now)
	case reasonNetwork:
		if r.ok {
			e.markUp(p, "probe", now)
			return
		}
		e.markDown(p, "probe", now)
	case reasonFailover:
		if r.ok {
			e.markUp(p, "probe", now)
		} else {
			e.markDown(p, "probe", now)
		}
		for _, s := range e.sortedSelectors() {
			if !s.failoverProbing[j.tag] {
				continue
			}
			delete(s.failoverProbing, j.tag)
			cur := s.current()
			switch {
			case !s.exhausted && e.path(cur).status == statusDown:
				e.failover(s, now)
			case e.retiring[cur]:
				e.leaveRetiring(s, now)
			case s.pinned == "":
				// The outbound works again; a candidate that passed still moves
				// the balancer, nothing more is probed.
				if tag := e.failoverChoice(s, e.ranked(s, now, func(t string) bool { return t == cur })); tag != "" {
					e.switchTo(s, tag, "failover", now)
				}
			}
		}
	case reasonRecheck:
		if !r.ok {
			e.markDown(p, "probe", now)
			return
		}
		e.markUp(p, "probe", now)
		for _, s := range e.sortedSelectors() {
			if s.exhausted && s.hasCandidate(j.tag) {
				e.switchTo(s, j.tag, "recovered", now)
			}
		}
	case reasonRemembered:
		if !r.ok {
			e.markDown(p, "probe", now)
			return
		}
		e.markUp(p, "probe", now)
		for _, s := range e.sortedSelectors() {
			if s.pinned == "" && s.current() != j.tag && e.rememberedFor(s) == j.tag && s.hasCandidate(j.tag) {
				e.switchTo(s, j.tag, "remembered", now)
			}
		}
	case reasonUpgrade:
		e.onUpgradeResult(j, r, p, now)
	case reasonHost:
		e.onHostProbeResult(p, r.ok, now)
	}
}

// abandonReturns gives up the returns to an outbound that failed.
func (e *engine) abandonReturns(tag string, now time.Time) {
	for _, s := range e.sortedSelectors() {
		if s.upgradeTarget != tag {
			continue
		}
		s.upgradeTarget = ""
		e.emit(now, "upgrade_failed", map[string]any{"balancer": s.tag, "outbound": tag, "successes": s.upgradeSuccesses})
		e.scheduleUpgrade(s, now, false)
	}
	e.dropQueued(tag, reasonUpgrade)
}

// clearedSuspicion handles a confirming probe that found the outbound
// working: a false alarm, after which weak evidence waits longer.
func (e *engine) clearedSuspicion(p *path, now time.Time) {
	e.backOffWeakEvidence(p, now)
	e.markUp(p, "probe", now)
}

func (e *engine) backOffWeakEvidence(p *path, now time.Time) {
	if now.After(p.cooldownUntil.Add(maxSuspectCooldown)) {
		// The last false alarm was long ago; start counting again.
		p.falseAlarms = 0
	}
	p.falseAlarms++
	cool := e.s.schedule.suspectCooldown << (p.falseAlarms - 1)
	if cool <= 0 || cool > maxSuspectCooldown {
		cool = maxSuspectCooldown
	}
	p.cooldownUntil = now.Add(cool)
}

func (e *engine) onUpgradeResult(j *probeJob, r probeResult, p *path, now time.Time) {
	var waiting []*selector
	for _, s := range e.sortedSelectors() {
		if s.upgradeTarget == j.tag {
			waiting = append(waiting, s)
		}
	}
	if !r.ok {
		e.markDown(p, "upgrade_probe", now)
		e.abandonReturns(j.tag, now)
		return
	}
	p.success(now)
	if p.status != statusUp {
		p.setStatus(statusUp, "probe", now)
		e.emitPath(p, now)
	}
	next := false
	for _, s := range waiting {
		if !s.hasCandidate(j.tag) {
			s.upgradeTarget = ""
			continue
		}
		s.upgradeSuccesses++
		if s.upgradeSuccesses >= s.cfg.upgrade.successes {
			e.switchTo(s, j.tag, "upgrade", now)
			continue
		}
		next = true
	}
	if next {
		spacing := e.s.schedule.spacing
		for _, s := range waiting {
			if s.upgradeTarget == j.tag && s.cfg.upgrade.spacing > spacing {
				spacing = s.cfg.upgrade.spacing
			}
		}
		e.enqueue(j.tag, reasonUpgrade, j.selector, now.Add(e.jitter(spacing, e.s.schedule.spacingJitter)))
	}
}

func (e *engine) onConnectivity(r probeResult, now time.Time) {
	e.emit(now, "connectivity", map[string]any{"ok": r.ok, "rtt_ms": r.rtt.Milliseconds(), "error": r.err})
	if r.ok {
		e.lastConnectivityOK = now
		e.connectivityPassed(now)
		e.resumePending(now)
		return
	}
	if e.checkFailingSince.IsZero() {
		e.checkFailingSince = now
	}
	e.checkFails++
	if e.lastSuccess.After(e.checkFailingSince) {
		e.ignoreConnectivity(now)
		return
	}
	e.nextConnectivity = now.Add(e.jitter(e.s.connectivity.recheck.delay(e.checkFails-1), e.s.connectivity.recheck.jitter))
	if e.doubt {
		if e.memoryLow {
			e.nextConnectivity = maxTime(e.nextConnectivity, now.Add(lowMemoryRecheck))
		}
		return
	}
	if paused := now.Sub(e.checkFailingSince); paused >= e.s.connectivity.maxPause {
		e.networkDown = false
		e.doubt = true
		e.emit(now, "connectivity_doubt", map[string]any{"failed_checks": e.checkFails, "paused_ms": paused.Milliseconds()})
		e.resumePending(now)
		return
	}
	if e.networkDown {
		return
	}
	e.emit(now, "network_down", map[string]any{"error": r.err})
	e.networkDown = true
	for _, p := range e.paths {
		p.failures = p.failures[:0]
		if p.status == statusSuspect {
			p.setStatus(statusUnknown, "network_down", now)
		}
	}
	e.pendingSuspicion = map[string]verdict{}
}

// sawSuccess handles proof that some outbound works.
func (e *engine) sawSuccess(how string, now time.Time) {
	e.lastSuccess = now
	if e.endPause(now) {
		e.emit(now, "network_up", map[string]any{"by": how})
	}
	if e.checkFails > 0 {
		// Either the network is back or the check cannot reach its
		// destination here; the next check tells which.
		e.nextConnectivity = time.Time{}
		e.enqueueConnectivity(now)
	}
}

// endPause resumes decisions after a pause and reports whether there was one.
func (e *engine) endPause(now time.Time) bool {
	if !e.networkDown {
		return false
	}
	e.networkDown = false
	e.resumedAt = now
	return true
}

// startedInPause tells whether a dial that began at started was already
// waiting when the last pause ended.
func (e *engine) startedInPause(started time.Time) bool {
	return started.Before(e.resumedAt)
}

func (e *engine) connectivityPassed(now time.Time) {
	resumed := e.networkDown || e.doubt
	e.endPause(now)
	e.doubt = false
	e.checkFails = 0
	e.checkFailingSince = time.Time{}
	e.nextConnectivity = time.Time{}
	if !resumed {
		e.restoreProvisional(now)
		return
	}
	e.emit(now, "network_up", map[string]any{"by": "connectivity"})
	e.restoreProvisional(now)
	for _, s := range e.sortedSelectors() {
		cur := s.current()
		if cur == "" || s.pinned != "" || now.Sub(e.path(cur).lastSuccess) < e.s.connectivity.timeout*4 {
			continue
		}
		e.enqueue(cur, reasonNetwork, s.tag, now)
	}
}

// restoreProvisional brings back outbounds ruled out while the check was
// failing: it passes now, so the network was what failed.
func (e *engine) restoreProvisional(now time.Time) {
	var tags []string
	for tag, p := range e.paths {
		if p.provisional {
			tags = append(tags, tag)
		}
	}
	if len(tags) == 0 {
		return
	}
	sort.Strings(tags)
	for _, tag := range tags {
		p := e.paths[tag]
		p.provisional = false
		if p.status != statusDown {
			continue
		}
		p.quarantineUntil = time.Time{}
		p.failures = p.failures[:0]
		p.setStatus(statusUnknown, "network_up", now)
		e.emitPath(p, now)
	}
	for _, s := range e.sortedSelectors() {
		if s.pinned != "" {
			continue
		}
		if s.exhausted {
			if r := e.ranked(s, now, nil); len(r) > 0 {
				e.switchTo(s, r[0], "network_up", now)
			}
			continue
		}
		e.scheduleUpgrade(s, now, true)
	}
}

func (e *engine) ignoreConnectivity(now time.Time) {
	e.emit(now, "connectivity_ignored", map[string]any{"failed_checks": e.checkFails})
	e.connectivityIgnored = true
	e.endPause(now)
	e.doubt = false
	e.checkFails = 0
	e.checkFailingSince = time.Time{}
	e.nextConnectivity = time.Time{}
	for _, p := range e.paths {
		if p.provisional && p.status == statusDown {
			p.quarantineLevel++
		}
		p.provisional = false
	}
	e.resumePending(now)
}

func (e *engine) resumePending(now time.Time) {
	pending := e.pendingSuspicion
	e.pendingSuspicion = map[string]verdict{}
	for tag, v := range pending {
		if p := e.path(tag); p.status == statusSuspect {
			e.decideSuspect(p, v, now)
		}
	}
}

// ---- host signals -------------------------------------------------------------

// onNetworkChange forgets what was learned in the previous network.
func (e *engine) onNetworkChange(id string, now time.Time) {
	known := false
	if id != "" {
		_, known = e.networks[id]
	}
	e.providersOnNetworkChange(known, now)
	e.networkID = id
	e.networkKnown = known
	e.endPause(now)
	e.doubt = false
	e.checkFails = 0
	e.checkFailingSince = time.Time{}
	e.nextConnectivity = time.Time{}
	e.connectivityIgnored = false

	forget := false
	for _, s := range e.selectors {
		if s.cfg.onNetworkChange != NetworkChangeMode_IGNORE {
			forget = true
		}
	}
	if !forget {
		e.emit(now, "network_change", map[string]any{"network": id, "known": known, "generation": e.generation, "ignored": true})
		return
	}
	e.generation++
	e.pendingSuspicion = map[string]verdict{}
	e.emit(now, "network_change", map[string]any{"network": id, "known": known, "generation": e.generation})
	for _, p := range e.paths {
		p.failures = p.failures[:0]
		p.quarantineLevel = 0
		p.quarantineUntil = time.Time{}
		p.provisional = false
		p.cooldownUntil = time.Time{}
		p.falseAlarms = 0
		p.probing = false
		p.setStatus(statusUnknown, "network_change", now)
	}
	e.queue = e.queue[:0]
	for _, s := range e.sortedSelectors() {
		s.failoverTried = nil
		s.failoverProbing = nil
		s.upgradeTarget = ""
		s.penaltyServed = false
		if s.cfg.onNetworkChange != NetworkChangeMode_IGNORE {
			s.penalty = map[string]float64{}
			s.returnedTo = ""
		}
		if s.cfg.onNetworkChange != NetworkChangeMode_RECHECK {
			continue
		}
		if s.pinned != "" {
			s.recheckOnRelease = true
			continue
		}
		e.recheckChoice(s, known, now)
	}
}

// recheckChoice probes the choice of the balancer in a new network, or the
// outbound that last worked in it.
func (e *engine) recheckChoice(s *selector, known bool, now time.Time) {
	if s.exhausted {
		if r := e.ranked(s, now, nil); len(r) > 0 {
			e.switchTo(s, r[0], "network_change", now)
		}
		return
	}
	if tag := e.rememberedFor(s); tag != "" && tag != s.current() && s.hasCandidate(tag) {
		e.enqueue(tag, reasonRemembered, s.tag, now)
		return
	}
	if cur := s.current(); cur != "" {
		e.enqueue(cur, reasonNetwork, s.tag, now)
	}
	u := s.cfg.upgrade
	e.scheduleUpgrade(s, now, u.onNetworkChange && (!u.unknownNetworksOnly || !known))
}

func (e *engine) onNetworkLost(now time.Time) {
	if !e.networkDown {
		e.emit(now, "network_down", map[string]any{"error": "host"})
	}
	e.networkDown = true
	e.doubt = false
	e.nextConnectivity = time.Time{}
	e.pendingSuspicion = map[string]verdict{}
}

// setMemoryLow switches to or from low memory and reports whether that
// changed anything.
func (e *engine) setMemoryLow(low bool, now time.Time) bool {
	if e.memoryLow == low {
		return false
	}
	e.memoryLow = low
	if low {
		e.gc(now, 0)
		e.dropHostProbesForMemory()
		e.keepRequests()
	}
	e.emit(now, "memory_low", map[string]any{"on": low})
	return true
}

func (e *engine) remember(tag string) {
	if !e.s.remember || e.networkID == "" {
		return
	}
	for _, s := range e.selectors {
		if s.current() != tag {
			continue
		}
		m := e.networks[e.networkID]
		if m == nil {
			m = map[string]string{}
			e.networks[e.networkID] = m
			e.netOrder = append(e.netOrder, e.networkID)
			if len(e.netOrder) > e.s.maxNetworks {
				delete(e.networks, e.netOrder[0])
				e.netOrder = e.netOrder[1:]
			}
		}
		m[s.tag] = tag
	}
}

func (e *engine) rememberedFor(s *selector) string {
	if !e.s.remember || e.networkID == "" {
		return ""
	}
	return e.networks[e.networkID][s.tag]
}

func (e *engine) pin(selectorTag, tag string, now time.Time) bool {
	s := e.selectors[selectorTag]
	if s == nil || !s.hasCandidate(tag) {
		return false
	}
	s.pinned = tag
	s.pinnedUntil = time.Time{}
	// A return or a failover under way would move the balancer off the pin
	// once its probe answers.
	if target := s.upgradeTarget; target != "" {
		s.upgradeTarget = ""
		if !e.returningTo(target) {
			e.dropQueued(target, reasonUpgrade)
		}
	}
	s.upgradeSuccesses = 0
	s.upgradeAt = time.Time{}
	s.failoverTried = nil
	s.failoverProbing = nil
	e.dropUnclaimedFailover()
	e.emit(now, "pin", map[string]any{"balancer": selectorTag, "outbound": tag})
	if s.current() != tag {
		e.switchTo(s, tag, "pin", now)
	}
	return true
}

func (e *engine) returningTo(tag string) bool {
	for _, s := range e.selectors {
		if s.upgradeTarget == tag {
			return true
		}
	}
	return false
}

func (e *engine) release(selectorTag string, now time.Time) bool {
	return e.releaseBy(selectorTag, "host", now)
}

func (e *engine) releaseBy(selectorTag, by string, now time.Time) bool {
	s := e.selectors[selectorTag]
	if s == nil || s.pinned == "" {
		return false
	}
	e.emit(now, "release", map[string]any{"balancer": selectorTag, "outbound": s.pinned, "by": by})
	s.pinned = ""
	s.pinnedUntil = time.Time{}
	recheck := s.recheckOnRelease
	s.recheckOnRelease = false
	cur := s.current()
	switch {
	case cur == "":
	case e.retiring[cur]:
		// A provider took the pinned outbound out of its set meanwhile.
		e.leaveRetiring(s, now)
		return true
	case e.path(cur).status == statusDown || !s.hasCandidate(cur):
		e.failover(s, now)
		return true
	}
	e.scheduleUpgrade(s, now, false)
	if recheck {
		e.recheckChoice(s, e.networkKnown, now)
	}
	return true
}

// gc forgets outbounds that no balancer has offered for stale, so rotating
// tags do not grow the state.
func (e *engine) gc(now time.Time, stale time.Duration) {
	e.lastGC = now
	offered := map[string]bool{}
	for _, s := range e.selectors {
		for _, c := range s.candidates {
			offered[c] = true
		}
		// Outbounds leaving a provider set are out of the candidates while
		// they drain, and a balancer may stay on one.
		if snap := s.snapshot.Load(); snap != nil {
			for _, c := range *snap {
				offered[c] = true
			}
		}
		offered[s.current()] = true
	}
	for tag, p := range e.paths {
		if !offered[tag] && !p.probing && now.Sub(p.statusSince) >= stale {
			e.forget(tag)
		}
	}
	// A dial records the server before the outbound has a path.
	e.lastServer.Range(func(tag, _ any) bool {
		if e.paths[tag.(string)] == nil {
			e.lastServer.Delete(tag)
		}
		return true
	})
	for name, ts := range e.startsByName {
		if len(pruneTimes(ts, now)) == 0 {
			delete(e.startsByName, name)
		}
	}
}

// forget drops everything kept about an outbound that is gone.
func (e *engine) forget(tag string) {
	delete(e.paths, tag)
	e.lastServer.Delete(tag)
	for _, s := range e.selectors {
		delete(s.penalty, tag)
	}
	e.queue = slices.DeleteFunc(e.queue, func(j *probeJob) bool { return j.tag == tag })
}

// keepSelectors forgets balancers that the routing no longer has, with what
// they remembered per network.
func (e *engine) keepSelectors(keep map[string]bool) {
	for tag := range e.selectors {
		if keep[tag] {
			continue
		}
		delete(e.selectors, tag)
		delete(e.costs, tag)
		for id, m := range e.networks {
			delete(m, tag)
			if len(m) == 0 {
				delete(e.networks, id)
				e.netOrder = slices.DeleteFunc(e.netOrder, func(n string) bool { return n == id })
			}
		}
	}
	e.dropUnclaimedFailover()
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
