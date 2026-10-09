package health

import (
	"slices"
	"strings"
	"time"

	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy"
)

// An outbound such as WireGuard holds a tunnel, a network stack and their
// goroutines while idle, and can release them and create them again with
// its next connection (outbound.Sleeper). The observer decides when, since
// it knows what uses an outbound: the choice of every balancer, the probes
// it runs or is about to run, the requests of the host and of providers, and
// the connections the handler gives the proxy (outbound.ProcessObserver). An
// outbound none of them holds sleeps sleep.after after its last use, at once
// while memory is low; a chosen one never does. The outbound has the last
// word: the observer follows the connections through the tunnel only of
// outbounds it judges, and forgets one idle for long, while Sleep refuses
// whenever a connection uses the tunnel.

type sleeperKind int8

const (
	sleeperUnknown sleeperKind = iota
	sleeperYes
	sleeperNo
)

// sleepJob asks the handler of one outbound to sleep, or only whether its
// proxy can.
type sleepJob struct {
	tag string
	p   *path
	// The uses of the path when the job was planned.
	uses  uint64
	sleep bool
	// idle, or memory when memory is low and sleep.after has not passed.
	reason string
}

type sleepResult int8

const (
	// No handler is registered under the tag.
	sleepGone sleepResult = iota
	// The proxy holds nothing to release.
	sleepCannot
	// The proxy can sleep; the job did not ask it to.
	sleepCan
	// The proxy can sleep, but something took it after the decision.
	sleepHeld
	// It refused, or the observer follows a connection through it.
	sleepBusy
	// It released what it held.
	sleepDone
)

// used notes that an outbound is in use at now: chosen or left by a
// balancer, probed, connected through, or asked through by a provider. It
// reports whether the outbound has just woken or been used for the first
// time with sleep on: the loop then has a sleep to plan.
func (e *engine) used(tag string, now time.Time) bool {
	if tag == "" {
		return false
	}
	p := e.paths[tag]
	if p == nil {
		// A via outbound nothing observes gets no path for this: the
		// observer sees none of its traffic and does not put it to sleep.
		if !e.s.observes(tag) {
			return false
		}
		p = e.pathAt(tag, now)
	}
	woke := !p.awake
	p.awake, p.asleep = true, false
	p.usedAt = now
	p.uses++
	p.sleepTries, p.nextSleepTry = 0, time.Time{}
	return woke && e.s.sleepAfter > 0 && p.sleeper != sleeperNo
}

// processStarted counts a connection the proxy of an outbound that can
// sleep processes, as its handler tells. It is a use whatever routed it
// there: a rule straight at the tag, a fallbackTag or a dialerProxy chain
// wake the proxy as well, before it reports the connection through its
// tunnel, and only for an outbound the observer judges. It reports whether
// the loop has a sleep to plan.
func (e *engine) processStarted(tag string, now time.Time) bool {
	woke := e.used(tag, now)
	if p := e.paths[tag]; p != nil {
		p.processes++
	}
	return woke
}

// processEnded counts the end of a connection processStarted counted, a
// use as well. It reports whether that was the last one and nothing else
// holds the outbound: the loop then plans the sleep, which nothing else
// would. Whatever else holds it, a balancer that chose it for one, lets go
// in a turn of the loop, so the end of every connection through a chosen
// outbound does not turn it.
func (e *engine) processEnded(tag string, now time.Time) bool {
	p := e.paths[tag]
	if p == nil || p.processes == 0 {
		// Forgotten meanwhile, with its handler.
		return false
	}
	p.processes--
	e.used(tag, now)
	return p.processes == 0 && e.s.sleepAfter > 0 && !e.keptAwake(tag, e.idle(now))
}

// fetchingVia counts the provider requests under way through an outbound:
// a request is a use at its start and at its end, and the outbound does not
// sleep in between. A count that reaches zero is dropped, as a via that is
// a balancer resolves to whatever it chose, and rotating tags of a provider
// would pile up otherwise.
func (e *engine) fetchingVia(tag string, delta int, now time.Time) {
	if n := e.fetchVia[tag] + delta; n > 0 {
		e.fetchVia[tag] = n
	} else {
		delete(e.fetchVia, tag)
	}
	e.used(tag, now)
}

// planSleeps puts the outbounds due to sleep into the next batch and
// returns when the next one is due; zero when none is, so once everything
// sleeps there is no timer. An outbound whose proxy may or may not sleep is
// asked at once whether it can, so one that cannot costs no wake later.
// Nothing is planned while a batch is under way: its end kicks the loop.
func (e *engine) planSleeps(now time.Time) time.Time {
	after := e.s.sleepAfter
	if after <= 0 || e.sleepsOut {
		return time.Time{}
	}
	var wake time.Time
	idle := e.idle(now)
	for tag, p := range e.paths {
		if !p.awake || p.sleeping || p.sleeper == sleeperNo {
			continue
		}
		if p.sleeper == sleeperUnknown {
			e.sleeps = append(e.sleeps, sleepJob{tag: tag, p: p, uses: p.uses})
			p.sleeping = true
			continue
		}
		if e.keptAwake(tag, idle) {
			// Whatever holds it uses it again when it lets go.
			continue
		}
		reason := "idle"
		due := maxTime(p.usedAt.Add(after), p.nextSleepTry)
		if e.memoryLow && now.Before(due) {
			// A refusal still waits for its retry: the connection that
			// made it may go on for long.
			due, reason = p.nextSleepTry, "memory"
		}
		if now.Before(due) {
			wake = soonestOf(wake, due)
			continue
		}
		e.sleeps = append(e.sleeps, sleepJob{tag: tag, p: p, uses: p.uses, sleep: true, reason: reason})
		p.sleeping = true
	}
	slices.SortFunc(e.sleeps, func(a, b sleepJob) int { return strings.Compare(a.tag, b.tag) })
	return wake
}

// keptAwake tells whether something the observer knows of uses the outbound
// or is about to. A probe that tick holds back now, while the network is
// down, the device idles or memory is low, is not about to, and neither is
// a return while memory is low: the probe wakes the outbound when it
// starts, and until then the tunnel would idle in memory for nothing.
func (e *engine) keptAwake(tag string, idle bool) bool {
	// A connection goes through it, a drain closes an outbound that left
	// its set, a provider is swapping the handler, a provider request goes
	// through it, a request of the host waits for an answer about it, also
	// while its probe waits.
	if p := e.paths[tag]; p != nil && p.processes > 0 {
		return true
	}
	if e.retiring[tag] || e.replacing[tag] || e.fetchVia[tag] > 0 || e.waitedFor(tag) {
		return true
	}
	if (e.s.connectivity != nil && tag == e.s.connectivity.outbound) || e.s.witness(tag) {
		return true
	}
	for _, s := range e.selectors {
		// The choice, a pin included, and the target of a return. While a
		// failover probes, every candidate it tried as well: one that
		// passed waits there for the cheaper ones to answer.
		if s.current() == tag || (s.upgradeTarget == tag && !e.memoryLow) ||
			(len(s.failoverProbing) > 0 && (s.failoverProbing[tag] || s.failoverTried[tag])) {
			return true
		}
	}
	for _, j := range e.queue {
		if j.tag == tag && !e.heldBackNow(j, idle) {
			return true
		}
	}
	for _, j := range e.running {
		if j.tag == tag {
			return true
		}
	}
	return false
}

// takeSleeps hands the planned batch to the observer, which asks the
// handlers outside its lock.
func (e *engine) takeSleeps() []sleepJob {
	if len(e.sleeps) == 0 {
		return nil
	}
	batch := e.sleeps
	e.sleeps = nil
	e.sleepsOut = true
	return batch
}

// onSlept applies what the handlers of a batch answered.
func (e *engine) onSlept(batch []sleepJob, results []sleepResult, now time.Time) {
	e.sleepsOut = false
	for i, j := range batch {
		p := j.p
		if e.paths[j.tag] != p {
			// Forgotten meanwhile: its handler left.
			continue
		}
		p.sleeping = false
		if p.uses != j.uses {
			// Used after the decision, or its handler was replaced: the
			// answer may not hold, and the outbound waits from the use.
			continue
		}
		switch results[i] {
		case sleepGone:
			// Nothing registered under the tag holds a tunnel; a handler
			// registered later is asked anew once it is used.
			p.awake = false
			p.sleeper = sleeperUnknown
		case sleepCannot:
			p.sleeper = sleeperNo
		case sleepCan:
			p.sleeper = sleeperYes
		case sleepHeld:
			// No refusal: planSleeps takes it up again once let go, as
			// when it finds it held.
			p.sleeper = sleeperYes
		case sleepBusy:
			p.sleeper = sleeperYes
			p.sleepTries++
			p.nextSleepTry = now.Add(sleepRetry(e.s.sleepAfter, p.sleepTries))
		case sleepDone:
			p.sleeper = sleeperYes
			p.awake, p.asleep = false, true
			e.emit(now, "outbound_sleep", map[string]any{
				"outbound": j.tag, "idle_ms": now.Sub(p.usedAt).Milliseconds(), "reason": j.reason,
			})
		}
	}
}

// sleepRetry is the wait before an outbound that refused to sleep tries
// times in a row is asked again. Its next use is not waited for: after the
// balancer left it there may be none, and the connections it carries end by
// themselves. Asking every sleep.after for good would not do either.
func sleepRetry(after time.Duration, tries int) time.Duration {
	d := max(after, minSleepRetry)
	for i := 1; i < tries && d < maxSleepRetry; i++ {
		d *= 2
	}
	return min(d, maxSleepRetry)
}

// awakeSleeper tells whether an outbound may hold a tunnel that only the
// observer would release: forgotten, it would never sleep.
func (e *engine) awakeSleeper(p *path) bool {
	return e.s.sleepAfter > 0 && p.awake && p.sleeper != sleeperNo
}

// sleepOutbounds asks the handlers of a batch outside the observer lock:
// releasing a tunnel may take a while, and decisions must not wait for it.
func (o *Observer) sleepOutbounds(batch []sleepJob) {
	var open map[string]int64
	results := make([]sleepResult, len(batch))
	for i, j := range batch {
		if j.sleep && open == nil {
			open = o.oldestConnections()
		}
		results[i] = o.sleepOutbound(j, open)
	}
	o.mu.Lock()
	o.e.onSlept(batch, results, o.now())
	o.mu.Unlock()
	o.wake()
}

func (o *Observer) sleepOutbound(j sleepJob, open map[string]int64) sleepResult {
	var h outbound.Handler
	if o.ohm != nil {
		h = o.ohm.GetHandler(j.tag)
	}
	if h == nil {
		return sleepGone
	}
	s := sleeperOf(h)
	switch {
	case s == nil:
		return sleepCannot
	case !j.sleep:
		return sleepCan
	}
	if _, ok := open[j.tag]; ok {
		return sleepBusy
	}
	if !o.mayStillSleep(j) {
		return sleepHeld
	}
	if s.Sleep() {
		return sleepDone
	}
	return sleepBusy
}

// mayStillSleep checks again, right before the call, what planSleeps
// decided under the lock: a balancer may have chosen the outbound since, or
// a probe or a request taken it, and its tunnel would then cost the next
// connection a handshake. The lock is not held across Sleep, which may take
// long; a use that comes during the call is set aside by onSlept.
func (o *Observer) mayStillSleep(j sleepJob) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	e := o.e
	return e.paths[j.tag] == j.p && j.p.uses == j.uses && !e.keptAwake(j.tag, e.idle(o.now()))
}

// sleeperOf returns what puts the outbound of a handler to sleep, or nil when
// its proxy holds nothing to release. Every handler of the outbound manager
// has Sleep and answers true for a proxy without one, so the proxy behind it
// tells which outbounds sleep; the call goes through the handler.
func sleeperOf(h outbound.Handler) outbound.Sleeper {
	g, ok := h.(proxy.GetOutbound)
	if !ok {
		return nil
	}
	ps, ok := g.GetOutbound().(outbound.Sleeper)
	if !ok {
		return nil
	}
	if hs, ok := h.(outbound.Sleeper); ok {
		return hs
	}
	return ps
}
