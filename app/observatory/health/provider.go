package health

import (
	"net/http"
	"slices"
	"time"
)

type refreshTrigger int

// Ordered by urgency: when several triggers want a refresh at once, the
// refresh is named after the first one that may start.
const (
	triggerHost refreshTrigger = iota
	triggerExhausted
	triggerExpiry
	triggerMinHealthy
	triggerCostAbove
	triggerNetwork
	triggerNextRefresh
	triggerMaxAge
	triggerInterval
	triggerCount
	// The initial set of the config, applied at the start. No trigger
	// wants it; it only names the apply.
	triggerInitial
)

func (t refreshTrigger) String() string {
	switch t {
	case triggerHost:
		return "host"
	case triggerExhausted:
		return "exhausted"
	case triggerExpiry:
		return "expiry"
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
	case triggerInitial:
		return "initial"
	default:
		return "unknown"
	}
}

// urgent triggers wait only for the shorter exhaustedMinInterval. An expiry
// is urgent as its leases tell.
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
	// A busy backend asked every trigger to wait until then. It is no
	// failure: the backoff does not grow.
	retryAfterUntil time.Time
	skipped         map[refreshTrigger]string

	version string
	// Every outbound of the set by its full tag, in the order of the answer;
	// before the first answer the outbounds of the config with the prefix.
	tags      []string
	draining  int
	lastError string
	refreshes int64
	// Numbers the refreshes for the request body, so that the backend tells
	// a request sent again through another via from a new one.
	refreshSeq uint64
	// The previous refresh failed: when, with which HTTP status or 0, and its
	// class, or the reason a well-formed answer was refused. A success
	// clears them.
	lastFailedAt time.Time
	lastStatus   int
	lastClass    string

	// Leases of the outbounds of the set by full tag; an outbound without
	// one has no end. At most one per outbound of the set.
	leases map[string]*lease
	// How far the clock the last answer counts from is ahead of the device,
	// and which clock that is: server, date or device; none when the answer
	// named no lease.
	skew  time.Duration
	clock string

	// Headers an answer or the host put over those of the config, by
	// canonical name. Their generation grows with every change, so that an
	// answer or a refusal a request with older headers brought is told
	// apart.
	rotated    map[string]string
	headersGen uint64
	// The backend refused the headers: every trigger but the host waits
	// until authRetryAt, or until the headers change when it is zero.
	authFailed  bool
	authRetryAt time.Time

	// The last applied answer for the host to keep, and the number of what
	// ProviderSet returns. Kept while memory is low as well: it is state,
	// not work, and without it a killed process loses a rotated token.
	last lastAnswer
	seq  uint64

	// The base config the core started with, as the backend named it: that
	// of the config or of a fresh initial set; empty when neither named one.
	// The base the last applied answer named, and the one the host was told
	// of last, until an answer names the base of the core again.
	base         string
	answerBase   string
	baseReported string
}

func newProvider(cfg *providerSettings) *provider {
	return &provider{cfg: cfg, minHealthyArmed: true, skipped: map[refreshTrigger]string{}, base: cfg.baseVersion}
}

// refreshJob is one refresh: requests through the via outbounds in turn and
// the apply of the answer.
type refreshJob struct {
	p       *provider
	trigger refreshTrigger
	// Failed refreshes before this one.
	retry   int
	refresh uint64
	// The body of a POST, gathered when the refresh starts.
	request *providerRequest
	// Leases the refresh asks about, whatever its trigger.
	asked []askedLease
	// The headers every via sends, and their generation.
	headers    map[string]string
	headersGen uint64
}

type refreshOutcome struct {
	ok  bool
	err string
	// The set differs from the previous one and was applied.
	changed       bool
	nextRefreshIn time.Duration
	// The backend asked to wait this long, at most backoff.max.
	retryAfter time.Duration
	// The HTTP status of the last attempt or 0, and the class of the
	// failure or the reason the answer was refused.
	status int
	class  string
	// The code of an error body of the protocol, and the via entry that
	// brought it.
	code string
	via  string
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
		// A lease ends during a request as well.
		soonest(e.tickLeases(p, now, idle))
		if p.fetching {
			continue
		}
		want, expiry, next := e.wantedRefresh(p, now)
		if !idle {
			soonest(next)
		}
		// Each trigger that wants a refresh goes through the waits that are
		// not limits, and the first one through starts it: an idle
		// exhausted balancer must not hide an expiry, which does not wait
		// for traffic.
		trigger, ok := refreshTrigger(0), false
		for t := refreshTrigger(0); t < triggerCount && !ok; t++ {
			if !want[t] {
				continue
			}
			if why := e.heldBack(p, t, expiry, idle, now); why != "" {
				var wait time.Duration
				if why == "auth" && !p.authRetryAt.IsZero() {
					wait = p.authRetryAt.Sub(now)
					soonest(p.authRetryAt)
				}
				e.skipRefresh(p, t, why, wait, now)
				continue
			}
			trigger, ok = t, true
		}
		if !ok {
			continue
		}
		urgent := trigger.urgent() || (trigger == triggerExpiry && expiry.urgent)
		if at, why := p.allowedAt(urgent, now); now.Before(at) {
			e.skipRefresh(p, trigger, why, at.Sub(now), now)
			soonest(at)
			continue
		}
		jobs = append(jobs, e.beginRefresh(p, trigger, now))
	}
	return jobs, wake
}

// heldBack returns why a trigger that wants a refresh waits whatever the
// limits allow, or "" when it does not.
func (e *engine) heldBack(p *provider, t refreshTrigger, expiry expiryWant, idle bool, now time.Time) string {
	switch {
	case t == triggerHost:
		// The host is always answered.
		return ""
	case idle && !(t == triggerExpiry && expiry.fresh):
		// Without user traffic nobody needs fresh outbounds now; the next
		// connection wakes the observer again. The first request for a
		// lease does not wait: an outbound would end under a sleeping
		// device. A retry after a failure does, or a dead backend would
		// wake the device every backoff.
		return "idle"
	case e.networkDown:
		return "network_down"
	case e.pinnedBalancer(p.cfg.balancer):
		// The host keeps the balancer where it is, often while it verifies
		// the outbound of the start pin, and stops the core when that fails:
		// a refresh then only loads the backend, once for every attempt of
		// a host that starts the core again. The release kicks the loop,
		// and the timer of a start pin ends it within the same turn.
		return "pinned"
	case e.memoryLow && !t.urgent() && !(t == triggerExpiry && expiry.urgent):
		// A refresh is the heaviest work of the observer. With low memory
		// only the host, a balancer with nothing left and a lease that
		// ended wait for one.
		return "memory"
	case p.authHeld(now):
		// The same headers would be refused again. Last, so that a
		// trigger that waits for traffic anyway sets no wake for the end
		// of the pause.
		return "auth"
	}
	return ""
}

// pinnedBalancer tells whether the balancer is pinned, by the host or by
// startPinned.
func (e *engine) pinnedBalancer(tag string) bool {
	s := e.selectors[tag]
	return s != nil && s.pinned != ""
}

// wantedRefresh returns the triggers that want a refresh now, what the
// leases want, and, either way, the earliest time a waiting trigger becomes
// due. Leases keep their own time, see tickLeases.
func (e *engine) wantedRefresh(p *provider, now time.Time) ([triggerCount]bool, expiryWant, time.Time) {
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
	expiry := p.expiryWant(now)
	want[triggerExpiry] = expiry.want
	return want, expiry, wake
}

// healthy counts candidates that are working or not judged yet. One whose
// lease ended is gone for minHealthy, however well it works.
func (e *engine) healthy(s *selector) int {
	n := 0
	for _, c := range s.candidates {
		if e.expired[c] {
			continue
		}
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
	c := e.rankCost(s, cur)
	for _, t := range s.candidates {
		if e.rankCost(s, t) < c {
			return true
		}
	}
	return false
}

// allowedAt returns when a refresh for a trigger may start and which limit
// holds it back until then. An urgent one waits only for the shorter
// exhaustedMinInterval.
func (p *provider) allowedAt(urgent bool, now time.Time) (time.Time, string) {
	var at time.Time
	why := ""
	later := func(t time.Time, reason string) {
		if t.After(at) {
			at, why = t, reason
		}
	}
	gap := p.cfg.minInterval
	if urgent {
		gap = p.cfg.exhaustedMinInterval
	}
	if !p.lastAttempt.IsZero() && gap > 0 {
		later(p.lastAttempt.Add(gap), "min_interval")
	}
	if p.failures > 0 {
		later(p.backoffUntil, "backoff")
	}
	// The backend asked for it: even the host and an exhausted balancer wait,
	// since asking sooner only adds to the load that made it busy.
	later(p.retryAfterUntil, "retry_after")
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
	p.refreshSeq++
	j := &refreshJob{p: p, trigger: t, retry: p.failures, refresh: p.refreshSeq, asked: p.askLeases(now),
		headers: p.requestHeaders(), headersGen: p.headersGen}
	if p.authFailed {
		// One refresh with refused headers per pause: a failure on the way
		// does not let the triggers try them again at once.
		e.holdAuth(p, now)
	}
	if p.cfg.method == http.MethodPost {
		j.request = e.providerRequest(p, j, now)
	}
	return j
}

// startFromInitial dates the initial set the provider starts with. A set
// the host kept is as old as it was when it was saved: maxAge and
// nextRefreshIn count from when the backend sent it, so an old set is
// refreshed soon and a fresh one is not asked for again at the start.
func (e *engine) startFromInitial(p *provider, age, nextRefreshIn time.Duration, now time.Time) {
	p.started = true
	p.setAt = now.Add(-age)
	e.scheduleRefresh(p, max(nextRefreshIn-age, 0), now)
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
	p.answeredLeases(j.asked, r.ok)
	if r.ok {
		p.setAt = now
		// Host requests made while the refresh ran are answered by it.
		p.host = false
		p.authFailed, p.authRetryAt = false, time.Time{}
		p.lastError = ""
		p.lastFailedAt, p.lastStatus, p.lastClass = time.Time{}, 0, ""
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
		p.lastFailedAt, p.lastStatus, p.lastClass = now, r.status, r.class
		if j.trigger == triggerMinHealthy {
			p.minHealthyArmed = true
		}
		if r.authRefused() {
			if j.headersGen != p.headersGen {
				// The host replaced the refused headers while the request
				// ran: the new ones go as soon as the limits allow.
				p.retrying, p.retry = true, j.trigger
				return
			}
			e.holdAuth(p, now)
			e.emit(now, "provider_auth_failed", map[string]any{"provider": p.cfg.tag, "trigger": j.trigger.String(),
				"status": r.status, "via": r.via, "code": r.code})
			return
		}
		if j.trigger == triggerHost || j.trigger == triggerNetwork {
			p.retrying = true
			p.retry = j.trigger
		}
		if r.retryAfter > 0 {
			// A pause the backend chose is no failure. The spread only adds,
			// so no client comes back sooner than asked, and clients told
			// the same pause at once come back apart.
			p.retryAfterUntil = now.Add(time.Duration(float64(r.retryAfter) * (1 + p.cfg.backoff.jitter*e.rnd.Float64())))
			return
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
	// Every outbound of the set, in the order of the answer, and the costs
	// the provider sent for them.
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
	p.tags = c.tags
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
	// The new handler holds nothing until it is used, and its proxy may
	// sleep where the old one could not. The old handler drains apart.
	// Counting a use sets the answer to a sleep asked meanwhile aside.
	p.awake, p.asleep, p.sleeper = false, false, sleeperUnknown
	p.sleepTries, p.nextSleepTry = 0, time.Time{}
	p.uses++
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
	for _, p := range e.providers {
		e.dropLease(p, tag)
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
