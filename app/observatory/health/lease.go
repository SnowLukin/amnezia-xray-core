package health

import (
	"sort"
	"time"
)

// lease is how long the backend lets the core use one outbound of its set.
// It is kept as what was left of it when the answer that named it last
// arrived, not as a moment on the clock of the device, which may be off by
// any amount.
type lease struct {
	// expiresAt of the answer: it names the value of the lease.
	end time.Time
	// When the answer that named it last arrived, with the monotonic reading.
	// Each lease has its own: a moment shared by the provider would stretch
	// a lease that a later answer repeated unchanged.
	at time.Time
	// What was left of it then: end less the time the answer counts from,
	// at most maxLease.
	ttl time.Duration
	// A refresh asked the backend about this value and got an answer, so it
	// is not asked about again.
	asked bool
	// The last refresh that asked about it failed.
	failed  bool
	expired bool
}

// elapsed tells how long ago the lease was named, by the longer of the two
// clocks: the monotonic one stands while the device sleeps and the wall
// clock jumps. A lease then ends early rather than late; an early end costs
// one refresh, and the outbound stays the last resort meanwhile.
func (l *lease) elapsed(now time.Time) time.Duration {
	return max(now.Sub(l.at), now.Round(0).Sub(l.at.Round(0)), 0)
}

// left is what is left of the lease, below zero once it ended.
func (l *lease) left(now time.Time) time.Duration {
	return l.ttl - l.elapsed(now)
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
}

type leaseCounts struct {
	leases, renewed, expired int
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
// the outbounds of its set: a later end renews a lease, the same or an
// earlier one brings only a fresher clock, and an outbound without one has
// no lease. Leases of outbounds that left the set go with them. It reports
// whether an outbound expired or came back, which the balancers have yet to
// see.
func (e *engine) acceptLeases(p *provider, tags []string, a leaseAnswer, now time.Time) (leaseCounts, bool) {
	at := a.at
	if at.IsZero() {
		at = now
	}
	wall := at.Round(0)
	ref, clock := leaseRef(a.serverTime, a.date, wall)
	p.skew, p.clock = ref.Sub(wall), clock
	var c leaseCounts
	moved := false
	inSet := make(map[string]bool, len(tags))
	for _, tag := range tags {
		inSet[tag] = true
		end, ok := a.ends[tag]
		if !ok {
			moved = e.dropLease(p, tag) || moved
			continue
		}
		c.leases++
		l := p.leases[tag]
		was := l != nil && l.expired
		if l == nil || end.After(l.end) {
			if l != nil {
				c.renewed++
			}
			l = &lease{end: end}
			if p.leases == nil {
				p.leases = map[string]*lease{}
			}
			p.leases[tag] = l
		}
		// A fresh clock of the server is more exact than what the device
		// counted, which may have slept.
		l.at, l.ttl = at, min(end.Sub(ref), maxLease)
		if l.ttl > 0 {
			l.expired = false
		} else {
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
			moved = e.dropLease(p, tag) || moved
		}
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
	for _, tag := range ended {
		l := p.leases[tag]
		l.expired = true
		e.expired[tag] = true
		e.emit(now, "provider_expired", map[string]any{"provider": p.cfg.tag, "outbound": tag,
			"active": e.active(tag), "renewal": renewalOf(l)})
	}
	return true
}

// renewalOf tells why a lease ended without a renewal: the backend answered
// without one, the last refresh that asked failed, or no refresh asked yet.
func renewalOf(l *lease) string {
	switch {
	case l.failed:
		return "failed"
	case l.asked:
		return "refused"
	}
	return "pending"
}

// tickLeases ends the leases of p that ran out, moves the balancers off
// those outbounds and returns when the next lease ends. That wake comes
// without user traffic too: an outbound must stop being chosen once its
// lease is over, and it is one wake per lease value.
func (e *engine) tickLeases(p *provider, now time.Time) time.Time {
	if e.expireLeases(p, now) {
		e.refreshCandidates(now)
	}
	var wake time.Time
	for _, l := range p.leases {
		if l.expired {
			continue
		}
		if at := now.Add(l.left(now)); wake.IsZero() || at.Before(wake) {
			wake = at
		}
	}
	return wake
}

// expiryWant tells whether the leases of p want a refresh: one ended and no
// refresh that got an answer asked about it. Such a refresh is urgent. fresh
// tells that some of them has not failed a refresh yet: the first request
// for a lease value does not wait for user traffic, a retry does.
type expiryWant struct {
	want, urgent, fresh bool
}

func (p *provider) expiryWant() expiryWant {
	var w expiryWant
	for _, l := range p.leases {
		if l.asked || !l.expired {
			continue
		}
		w.want, w.urgent = true, true
		w.fresh = w.fresh || !l.failed
	}
	return w
}

// askLeases lists the leases a refresh that starts now asks the backend
// about, whatever its trigger: the ones that ended and were not asked about.
func (p *provider) askLeases() []askedLease {
	var out []askedLease
	for tag, l := range p.leases {
		if !l.asked && l.expired {
			out = append(out, askedLease{tag: tag, end: l.end})
		}
	}
	return out
}

// answeredLeases settles the leases a refresh asked about. After an answer
// the backend knows about them: a lease it did not renew is not asked about
// again, so a backend that keeps a lease short gets no stream of requests.
// After a failure, a Retry-After or a refused answer they are asked about
// again, after the backoff or the pause.
func (p *provider) answeredLeases(asked []askedLease, ok bool) {
	for _, a := range asked {
		l := p.leases[a.tag]
		if l == nil || !l.end.Equal(a.end) {
			// Renewed by the answer, or gone with its outbound.
			continue
		}
		l.asked = ok
		l.failed = !ok
	}
}
