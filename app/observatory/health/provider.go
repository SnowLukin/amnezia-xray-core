package health

import (
	"slices"
	"time"
)

type refreshTrigger int

// Ordered by urgency: when several triggers want a refresh at once, the
// refresh is named after the first one.
const (
	triggerHost refreshTrigger = iota
	triggerExhausted
	triggerMinHealthy
	triggerCostAbove
	triggerNetwork
	triggerNextRefresh
	triggerMaxAge
	triggerInterval
	triggerCount
)

func (t refreshTrigger) String() string {
	switch t {
	case triggerHost:
		return "host"
	case triggerExhausted:
		return "exhausted"
	case triggerMinHealthy:
		return "min_healthy"
	case triggerCostAbove:
		return "cost_above"
	case triggerNetwork:
		return "network_change"
	case triggerNextRefresh:
		return "next_refresh"
	case triggerMaxAge:
		return "max_age"
	case triggerInterval:
		return "interval"
	default:
		return "unknown"
	}
}

// urgent triggers wait only for the shorter exhaustedMinInterval.
func (t refreshTrigger) urgent() bool {
	return t == triggerHost || t == triggerExhausted
}

// provider is the decision state of one provider. Like the rest of the
// engine it never reads the clock and never blocks.
type provider struct {
	cfg *providerSettings

	started bool
	// When the current set was fetched, or when the provider started.
	setAt       time.Time
	nextAt      time.Time
	nextTrigger refreshTrigger

	host           bool
	networkPending bool
	networkAt      time.Time
	// The minHealthy trigger fires once when the balancer drops below the
	// threshold and again only after it got back above it.
	minHealthyArmed bool
	aboveSince      time.Time
	// A host or network refresh that failed is retried with the backoff.
	retrying bool
	retry    refreshTrigger

	fetching    bool
	lastAttempt time.Time
	attempts    []time.Time
	// An outbound the last apply added that the balancer has not offered yet.
	expect string
	// Failed refreshes in a row and when the next attempt may start. The
	// attempt time is kept apart from the success time, so a failing refresh
	// cannot run again at once.
	failures     int
	backoffUntil time.Time
	skipped      map[refreshTrigger]string

	version   string
	members   int
	draining  int
	lastError string
	refreshes int64
}

func newProvider(cfg *providerSettings) *provider {
	return &provider{cfg: cfg, minHealthyArmed: true, skipped: map[refreshTrigger]string{}}
}

// refreshJob is one refresh: requests through the via outbounds in turn and
// the apply of the answer.
type refreshJob struct {
	p       *provider
	trigger refreshTrigger
	// Failed refreshes before this one.
	retry int
}

type refreshOutcome struct {
	ok  bool
	err string
	// The set differs from the previous one and was applied.
	changed       bool
	nextRefreshIn time.Duration
}

func (e *engine) provider(tag string) *provider {
	for _, p := range e.providers {
		if p.cfg.tag == tag {
			return p
		}
	}
	return nil
}

// tickProviders returns refreshes to start now and when to call it again.
func (e *engine) tickProviders(now time.Time) ([]*refreshJob, time.Time) {
	var jobs []*refreshJob
	var wake time.Time
	soonest := func(t time.Time) {
		if !t.IsZero() && (wake.IsZero() || t.Before(wake)) {
			wake = t
		}
	}
	idle := e.idle(now)
	for _, p := range e.providers {
		if !p.started {
			p.started = true
			p.setAt = now
			e.scheduleRefresh(p, 0, now)
		}
		if p.fetching {
			continue
		}
		trigger, ok, next := e.wantedRefresh(p, now)
		if !idle {
			soonest(next)
		}
		if !ok {
			continue
		}
		// Without user traffic nobody needs fresh outbounds now; the next
		// connection wakes the observer again. The host is always answered.
		if idle && trigger != triggerHost {
			e.skipRefresh(p, trigger, "idle", 0, now)
			continue
		}
		if e.networkDown && trigger != triggerHost {
			e.skipRefresh(p, trigger, "network_down", 0, now)
			continue
		}
		if at, why := p.allowedAt(trigger, now); now.Before(at) {
			e.skipRefresh(p, trigger, why, at.Sub(now), now)
			soonest(at)
			continue
		}
		jobs = append(jobs, e.beginRefresh(p, trigger, now))
	}
	return jobs, wake
}

// wantedRefresh returns the most urgent trigger that wants a refresh now
// and, either way, the earliest time a waiting trigger becomes due.
func (e *engine) wantedRefresh(p *provider, now time.Time) (refreshTrigger, bool, time.Time) {
	var want [triggerCount]bool
	var wake time.Time
	soonest := func(t time.Time) {
		if wake.IsZero() || t.Before(wake) {
			wake = t
		}
	}
	want[triggerHost] = p.host
	if p.retrying {
		want[p.retry] = true
	}
	s := e.selectors[p.cfg.balancer]
	if s != nil && s.started && !e.networkDown {
		// The balancer learns about added outbounds from the next connection
		// through it; until then it is exhausted only by the old set.
		if p.expect != "" && s.offers(p.expect) {
			p.expect = ""
		}
		if p.cfg.whenExhausted && s.exhausted && p.expect == "" {
			want[triggerExhausted] = true
		}
		if p.cfg.minHealthy > 0 {
			if e.healthy(s) >= p.cfg.minHealthy {
				p.minHealthyArmed = true
			} else if p.minHealthyArmed {
				want[triggerMinHealthy] = true
			}
		}
		if p.cfg.costAbove {
			if cur := s.current(); cur != "" && e.cost(s, cur) >= p.cfg.costThreshold {
				if p.aboveSince.IsZero() {
					p.aboveSince = maxTime(s.activeSince, p.setAt)
				}
				if due := p.aboveSince.Add(p.cfg.costAboveAfter); now.Before(due) {
					soonest(due)
				} else {
					want[triggerCostAbove] = true
				}
			} else {
				p.aboveSince = time.Time{}
			}
		}
	}
	if p.networkPending {
		if due := p.networkAt.Add(p.cfg.networkSettle); now.Before(due) {
			soonest(due)
		} else if p.cfg.onNetwork != RefreshOnNetwork_REFRESH_UNKNOWN_NOT_CHEAPEST || (s != nil && e.notCheapest(s)) {
			want[triggerNetwork] = true
		} else {
			p.networkPending = false
		}
	}
	if !p.nextAt.IsZero() {
		if now.Before(p.nextAt) {
			soonest(p.nextAt)
		} else {
			want[p.nextTrigger] = true
		}
	}
	if p.cfg.maxAge > 0 {
		if due := p.setAt.Add(p.cfg.maxAge); now.Before(due) {
			soonest(due)
		} else {
			want[triggerMaxAge] = true
		}
	}
	for t := refreshTrigger(0); t < triggerCount; t++ {
		if want[t] {
			return t, true, wake
		}
	}
	return 0, false, wake
}

// healthy counts candidates that are working or not judged yet.
func (e *engine) healthy(s *selector) int {
	n := 0
	for _, c := range s.candidates {
		p := e.paths[c]
		if p == nil || p.status == statusUp || p.status == statusUnknown {
			n++
		}
	}
	return n
}

// notCheapest reports whether the balancer keeps an outbound while a
// cheaper candidate exists, which after a network change means the cheaper
// ones do not work there.
func (e *engine) notCheapest(s *selector) bool {
	cur := s.current()
	if cur == "" || s.pinned != "" {
		return false
	}
	c := e.cost(s, cur)
	for _, t := range s.candidates {
		if e.cost(s, t) < c {
			return true
		}
	}
	return false
}

// allowedAt returns when a refresh for the trigger may start and which
// limit holds it back until then.
func (p *provider) allowedAt(t refreshTrigger, now time.Time) (time.Time, string) {
	var at time.Time
	why := ""
	later := func(t time.Time, reason string) {
		if t.After(at) {
			at, why = t, reason
		}
	}
	gap := p.cfg.minInterval
	if t.urgent() {
		gap = p.cfg.exhaustedMinInterval
	}
	if !p.lastAttempt.IsZero() && gap > 0 {
		later(p.lastAttempt.Add(gap), "min_interval")
	}
	if p.failures > 0 {
		later(p.backoffUntil, "backoff")
	}
	cut := 0
	for cut < len(p.attempts) && now.Sub(p.attempts[cut]) >= time.Hour {
		cut++
	}
	p.attempts = p.attempts[cut:]
	if p.cfg.maxPerHour > 0 && len(p.attempts) >= p.cfg.maxPerHour {
		later(p.attempts[0].Add(time.Hour), "max_per_hour")
	}
	return at, why
}

// skipRefresh reports a held back trigger once per reason, so a trigger that
// waits does not fill the event log.
func (e *engine) skipRefresh(p *provider, t refreshTrigger, why string, wait time.Duration, now time.Time) {
	if p.skipped[t] == why {
		return
	}
	p.skipped[t] = why
	f := map[string]any{"provider": p.cfg.tag, "trigger": t.String(), "reason": why}
	if wait > 0 {
		f["wait_ms"] = wait.Milliseconds()
	}
	e.emit(now, "provider_skip", f)
}

func (e *engine) beginRefresh(p *provider, t refreshTrigger, now time.Time) *refreshJob {
	p.fetching = true
	p.lastAttempt = now
	p.attempts = append(p.attempts, now)
	p.host = false
	p.networkPending = false
	p.minHealthyArmed = false
	p.retrying = false
	clear(p.skipped)
	return &refreshJob{p: p, trigger: t, retry: p.failures}
}

func (e *engine) scheduleRefresh(p *provider, hint time.Duration, now time.Time) {
	p.nextAt = time.Time{}
	switch {
	case hint > 0:
		p.nextAt = now.Add(e.jitter(hint, p.cfg.jitter))
		p.nextTrigger = triggerNextRefresh
	case p.cfg.interval > 0:
		p.nextAt = now.Add(e.jitter(p.cfg.interval, p.cfg.jitter))
		p.nextTrigger = triggerInterval
	}
}

func (e *engine) onRefreshDone(j *refreshJob, r refreshOutcome, now time.Time) {
	p := j.p
	p.fetching = false
	p.refreshes++
	if r.ok {
		p.setAt = now
		// Host requests made while the refresh ran are answered by it.
		p.host = false
		p.lastError = ""
		if !p.aboveSince.IsZero() {
			p.aboveSince = now
		}
		e.scheduleRefresh(p, r.nextRefreshIn, now)
		// The same dead set again: back off as after a failure, so an
		// exhausted balancer does not fetch it every exhaustedMinInterval.
		if r.changed || j.trigger != triggerExhausted {
			p.failures = 0
			p.backoffUntil = time.Time{}
			return
		}
	} else {
		p.lastError = r.err
		switch j.trigger {
		case triggerHost, triggerNetwork:
			p.retrying = true
			p.retry = j.trigger
		case triggerMinHealthy:
			p.minHealthyArmed = true
		}
	}
	p.failures++
	p.backoffUntil = now.Add(e.jitter(p.cfg.backoff.delay(p.failures-1), p.cfg.backoff.jitter))
}

// requestRefresh asks the provider with the tag, or every provider when the
// tag is empty, to refresh as soon as the limits allow.
func (e *engine) requestRefresh(tag string) bool {
	found := false
	for _, p := range e.providers {
		if tag == "" || p.cfg.tag == tag {
			p.host = true
			found = true
		}
	}
	return found
}

func (e *engine) providersOnNetworkChange(known bool, now time.Time) {
	for _, p := range e.providers {
		p.aboveSince = time.Time{}
		switch p.cfg.onNetwork {
		case RefreshOnNetwork_REFRESH_NEVER:
			continue
		case RefreshOnNetwork_REFRESH_UNKNOWN, RefreshOnNetwork_REFRESH_UNKNOWN_NOT_CHEAPEST:
			if known {
				p.networkPending = false
				continue
			}
		}
		p.networkPending = true
		p.networkAt = now
	}
}

// resolveVia returns the outbound to send a provider request through: the
// choice of a leastCost balancer or the outbound itself. It returns a reason
// instead when there is no point in trying.
func (e *engine) resolveVia(via string, now time.Time) (string, string) {
	if via == "" {
		return "", ""
	}
	if s := e.selectors[via]; s != nil {
		if cur := s.current(); cur != "" {
			return cur, ""
		}
		return "", "balancer exhausted"
	}
	if p := e.paths[via]; p != nil && p.quarantined(now) {
		return "", "outbound down"
	}
	return via, ""
}

// providerChange is an applied set as the balancers see it.
type providerChange struct {
	// Every outbound of the set and the costs the provider sent for them.
	tags  []string
	costs map[string]float64
	// New tags, tags that left the set, tags that came back before their
	// drain ended, and tags whose settings changed.
	added, retire, revive, reset []string
}

// applyProviderSet updates the balancers after a provider changed its
// outbounds.
func (e *engine) applyProviderSet(p *provider, c providerChange, now time.Time) {
	clear(e.replacing)
	m := e.costs[p.cfg.balancer]
	if m == nil {
		m = map[string]float64{}
		e.costs[p.cfg.balancer] = m
	}
	for _, tag := range c.tags {
		if v, ok := c.costs[tag]; ok {
			m[tag] = v
		} else {
			delete(m, tag)
		}
	}
	for _, tag := range c.retire {
		e.retiring[tag] = true
	}
	for _, tag := range c.revive {
		delete(e.retiring, tag)
	}
	for _, tag := range c.reset {
		e.resetPath(tag, now)
	}
	p.expect = ""
	if len(c.added) > 0 {
		p.expect = c.added[0]
	}
	p.members = len(c.tags)
	e.refreshCandidates(now)
}

// offers reports whether the last candidates the balancer was given hold tag,
// retiring ones included.
func (s *selector) offers(tag string) bool {
	snap := s.snapshot.Load()
	return snap != nil && slices.Contains(*snap, tag)
}

// replacing marks outbounds whose handler is about to be swapped. Between
// the removal of the old handler and the registration of the new one a
// connection may see the tag missing; that is no reason to leave it.
func (e *engine) replacingTags(tags []string) {
	for _, t := range tags {
		e.replacing[t] = true
	}
}

// resetPath forgets what was learned about an outbound whose settings changed.
func (e *engine) resetPath(tag string, now time.Time) {
	// The new settings may point to another server.
	e.lastServer.Delete(tag)
	p := e.paths[tag]
	if p == nil {
		return
	}
	p.failures = p.failures[:0]
	p.quarantineLevel = 0
	p.quarantineUntil = time.Time{}
	p.provisional = false
	p.cooldownUntil = time.Time{}
	p.falseAlarms = 0
	p.serverName = ""
	if p.setStatus(statusUnknown, "replaced", now) {
		e.emitPath(p, now)
	}
}

// dropProviderOutbound forgets an outbound removed after its drain. The
// balancers stop offering it before the next connection asks.
func (e *engine) dropProviderOutbound(tag string, now time.Time) {
	delete(e.retiring, tag)
	for _, m := range e.costs {
		delete(m, tag)
	}
	for _, s := range e.sortedSelectors() {
		snap := s.snapshot.Load()
		if snap == nil || !slices.Contains(*snap, tag) {
			continue
		}
		next := slices.DeleteFunc(slices.Clone(*snap), func(t string) bool { return t == tag })
		if s.started {
			e.updateCandidates(s, next, now)
		} else {
			s.snapshot.Store(&next)
		}
	}
	e.forget(tag)
}
