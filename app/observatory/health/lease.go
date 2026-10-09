package health

import (
	"cmp"
	"sort"
	"time"
)

// lease is how long the backend lets the core use one outbound of its set.
// It is kept as what was left of it when the answer that named it last
// arrived, not as a moment on the clock of the device, which may be off by
// any amount.
type lease struct {
	// expiresAt of the answer: it names the value of the lease in force,
	// which an answer may shorten as well as renew.
	end time.Time
	// When the answer that named it last arrived, with the monotonic reading.
	// Each lease has its own: a moment shared by the provider would stretch
	// a lease that a later answer repeated unchanged.
	at time.Time
	// What was left of it then: end less the time the answer counts from,
	// at most maxLease either way.
	ttl time.Duration
	// From at: when a refresh renews it, spread per value, and when that
	// renewal becomes urgent. An answer with the same or an earlier end
	// keeps both points where they were.
	renewIn  time.Duration
	urgentIn time.Duration
	// A refresh asked the backend about this value and got an answer, so it
	// is not asked about again.
	asked bool
	// The last refresh that asked about it failed.
	failed  bool
	expired bool
	// It came ended in the initial set: the device kept the set longer
	// than the lease.
	saved bool
}

// elapsed tells how long ago the lease was named, by the longer of the two
// clocks: the monotonic one stands while the device sleeps and the wall
// clock jumps. A lease then ends early rather than late; an early end costs
// one refresh, and the outbound stays the last resort meanwhile.
func (l *lease) elapsed(now time.Time) time.Duration {
	return longerElapsed(now.Sub(l.at), now.Round(0).Sub(l.at.Round(0)))
}

// longerElapsed takes the longer of what the monotonic and the wall clock
// counted, never below zero. It stands apart from elapsed for the tests: a
// time made up in a test has no monotonic reading, and Go then counts by
// the wall clock alone, so the two clocks never disagree there.
func longerElapsed(mono, wall time.Duration) time.Duration {
	return max(mono, wall, 0)
}

// left is what is left of the lease, below zero once it ended.
func (l *lease) left(now time.Time) time.Duration {
	return l.ttl - l.elapsed(now)
}

// due tells whether a refresh is to ask about the lease: it ended, or with
// renewal on it reached its renewal point. urgent: it ended or reached the
// urgent point.
func (l *lease) due(renew bool, now time.Time) (due, urgent bool) {
	if l.expired {
		return true, true
	}
	if !renew {
		return false, false
	}
	elapsed := l.elapsed(now)
	return elapsed >= l.renewIn, elapsed >= l.urgentIn
}

// leaseAnswer is what an applied answer says about the leases of its set.
type leaseAnswer struct {
	// expiresAt by the full tag, for outbounds of the set only.
	ends       map[string]time.Time
	serverTime time.Time
	// The Date of the answer plus its Age; zero without a Date.
	date time.Time
	// When the answer arrived; zero takes the time of the apply.
	at time.Time
	// Names the clock instead of the one serverTime and date tell: saved
	// for a set the host kept, whose serverTime is moved on by its age.
	clock string
	// The initial set of the config: a lease that ended there is asked
	// about once, since no backend has answered about it in this run.
	initial bool
}

type leaseCounts struct {
	leases, renewed, expired int
	// An end came, went or changed: the set the host keeps is out of date.
	ends bool
}

// askedLease is a lease a refresh asks the backend about: one that ended
// and was not asked about yet.
type askedLease struct {
	tag string
	end time.Time
}

// leaseRef returns what the leases of an answer count from and which clock
// that is: the later of serverTime and the Date of the answer plus its Age,
// or the wall clock of the device without either. A cache sends the Date
// of the answer it keeps and the time it kept it as Age; an old serverTime
// of a cached file only shortens the leases.
func leaseRef(serverTime, date, wall time.Time) (time.Time, string) {
	switch {
	case !serverTime.IsZero() && !date.After(serverTime):
		return serverTime, "server"
	case !date.IsZero():
		return date, "date"
	}
	return wall, "device"
}

// acceptLeases takes the leases of an applied answer, the whole truth about
// the outbounds of its set: a later end renews a lease, the same one brings
// only a fresher clock, an earlier one shortens it, and an outbound without
// one has no lease. Leases of outbounds that left the set go with them. It
// reports whether an outbound expired or came back, which the balancers
// have yet to see.
func (e *engine) acceptLeases(p *provider, tags []string, a leaseAnswer, now time.Time) (leaseCounts, bool) {
	at := a.at
	if at.IsZero() {
		at = now
	}
	wall := at.Round(0)
	ref, clock := leaseRef(a.serverTime, a.date, wall)
	clock = cmp.Or(a.clock, clock)
	var c leaseCounts
	moved := false
	inSet := make(map[string]bool, len(tags))
	for _, tag := range tags {
		inSet[tag] = true
		end, ok := a.ends[tag]
		if !ok {
			if p.leases[tag] != nil {
				c.ends = true
			}
			moved = e.dropLease(p, tag) || moved
			continue
		}
		c.leases++
		l := p.leases[tag]
		was := l != nil && l.expired
		// Bounded below too: an end centuries off the clock, such as the
		// zero time a backend sends for a time it never set, saturates the
		// difference, and what is left of the lease would overflow into a
		// lease that never ends.
		ttl := min(max(end.Sub(ref), -maxLease), maxLease)
		if l == nil || end.After(l.end) {
			// A later end that passed already renews nothing, and a lease
			// that ended stays so: no second provider_expired for it, on
			// every answer of a backend whose ends trail its clock.
			if l != nil && ttl > 0 {
				c.renewed++
			}
			c.ends = true
			l = &lease{end: end, expired: was && ttl <= 0}
			if p.leases == nil {
				p.leases = map[string]*lease{}
			}
			p.leases[tag] = l
			if p.cfg.renewAt > 0 {
				// Spread per value: clients that got their leases at once
				// do not renew them at once.
				l.urgentIn = time.Duration(p.cfg.urgentAt * float64(ttl))
				l.renewIn = min(e.jitter(time.Duration(p.cfg.renewAt*float64(ttl)), p.cfg.renewJitter), l.urgentIn)
			}
		} else {
			// The same value or a shorter one: the renewal stays where it
			// was, or an answer for another trigger would put it off again
			// and again. A shorter end is the value from now on, so that a
			// later answer that extends it past that end renews it, though
			// not past the end it had before.
			gone := l.elapsed(at)
			l.renewIn, l.urgentIn = max(l.renewIn-gone, 0), max(l.urgentIn-gone, 0)
			if end.Before(l.end) {
				l.end = end
				c.ends = true
			}
		}
		// A fresh clock of the server is more exact than what the device
		// counted, which may have slept.
		l.at, l.ttl = at, ttl
		switch {
		case l.ttl > 0:
			l.expired = false
		case a.initial:
			l.saved = true
		default:
			// The backend has just answered and gave no more: asking it
			// again about this value would be a loop.
			l.asked = true
		}
		if was && !l.expired {
			delete(e.expired, tag)
			moved = true
		}
	}
	for tag := range p.leases {
		if !inSet[tag] {
			c.ends = true
			moved = e.dropLease(p, tag) || moved
		}
	}
	// The clock tells about leases, as provider_apply does: an answer
	// without one has none to tell.
	p.skew, p.clock = 0, ""
	if c.leases > 0 {
		p.skew, p.clock = ref.Sub(wall), clock
	}
	moved = e.expireLeases(p, now) || moved
	for _, l := range p.leases {
		if l.expired {
			c.expired++
		}
	}
	return c, moved
}

// dropLease forgets the lease of an outbound and reports whether it had
// ended.
func (e *engine) dropLease(p *provider, tag string) bool {
	delete(p.leases, tag)
	if !e.expired[tag] {
		return false
	}
	delete(e.expired, tag)
	return true
}

// expireLeases marks the leases of p that ran out and reports whether one
// did. An outbound whose lease ended stays a candidate, the last resort.
func (e *engine) expireLeases(p *provider, now time.Time) bool {
	var ended []string
	for tag, l := range p.leases {
		if !l.expired && l.left(now) <= 0 {
			ended = append(ended, tag)
		}
	}
	if len(ended) == 0 {
		return false
	}
	sort.Strings(ended)
	e.leaseEnds++
	for _, tag := range ended {
		l := p.leases[tag]
		l.expired = true
		e.expired[tag] = true
		e.emit(now, "provider_expired", map[string]any{"provider": p.cfg.tag, "outbound": tag,
			"active": e.active(tag), "renewal": renewalOf(p, l)})
	}
	return true
}

// renewalOf tells why a lease ended without a renewal: it came ended in the
// initial set, the backend answered without one, the last refresh that asked
// failed, renewal is off, or no refresh that asked has ended, also when one
// is under way: what it brings is not known yet.
func renewalOf(p *provider, l *lease) string {
	switch {
	case l.saved:
		return "saved"
	case l.failed:
		return "failed"
	case l.asked:
		return "refused"
	case p.cfg.renewAt == 0:
		return "off"
	}
	return "pending"
}

// tickLeases ends the leases of p that ran out, moves the balancers off
// those outbounds and returns when the next lease reaches its renewal
// point or its end. Those wakes come without user traffic too: an outbound
// must stop being chosen once its lease is over, and a lease that ends
// under a sleeping device is renewed. That is one wake for the renewal and
// one for the end per value of a lease; the urgent point wakes only for a
// renewal that waits for memory or a limit, and not for a retry while idle.
func (e *engine) tickLeases(p *provider, now time.Time, idle bool) time.Time {
	if e.expireLeases(p, now) {
		e.refreshCandidates(now)
	}
	var wake time.Time
	soonest := func(in time.Duration) {
		if at := now.Add(in); wake.IsZero() || at.Before(wake) {
			wake = at
		}
	}
	for _, l := range p.leases {
		if l.expired {
			continue
		}
		soonest(l.left(now))
		if l.asked || p.cfg.renewAt == 0 {
			continue
		}
		switch elapsed := l.elapsed(now); {
		case elapsed < l.renewIn:
			soonest(l.renewIn - elapsed)
		case elapsed < l.urgentIn && (!l.failed || !idle):
			soonest(l.urgentIn - elapsed)
		}
	}
	return wake
}

// expiryWant tells whether the leases of p want a refresh: one ended or
// reached its renewal point, and no refresh that got an answer asked about
// that value. urgent: one ended or reached its urgent point. fresh: some of
// them has not failed a refresh yet; the first request for a lease value
// does not wait for user traffic, a retry does. firstUrgent: one of them is
// urgent and has not failed a refresh yet; that first request does not wait
// for a pin either.
type expiryWant struct {
	want, urgent, fresh, firstUrgent bool
}

func (p *provider) expiryWant(now time.Time) expiryWant {
	var w expiryWant
	for _, l := range p.leases {
		if l.asked {
			continue
		}
		due, urgent := l.due(p.cfg.renewAt > 0, now)
		if !due {
			continue
		}
		w.want = true
		w.urgent = w.urgent || urgent
		w.fresh = w.fresh || !l.failed
		w.firstUrgent = w.firstUrgent || (urgent && !l.failed)
	}
	return w
}

// askLeases lists the leases a refresh that starts now asks the backend
// about, whatever its trigger: the ones due and not asked about yet.
func (p *provider) askLeases(now time.Time) []askedLease {
	var out []askedLease
	for tag, l := range p.leases {
		if due, _ := l.due(p.cfg.renewAt > 0, now); due && !l.asked {
			out = append(out, askedLease{tag: tag, end: l.end})
		}
	}
	return out
}

// answeredLeases settles the leases a refresh asked about. After an answer
// the backend knows about them: a lease it did not renew is not asked about
// again, also when it shortened it, so a backend that keeps a lease short
// gets no stream of requests. After a failure, a Retry-After or a refused
// answer they are asked about again, after the backoff or the pause.
func (p *provider) answeredLeases(asked []askedLease, ok bool) {
	for _, a := range asked {
		l := p.leases[a.tag]
		if l == nil || l.end.After(a.end) {
			// Renewed by the answer, or gone with its outbound.
			continue
		}
		l.asked = ok
		l.failed = !ok
	}
}
