package health

import "time"

// An outbound found down leads nowhere, also for the connections that
// started before and have received something: a dead WireGuard server
// answers none of them. Long-lived flows such as DoH or push connections
// then keep the tunnel awake, and its device repeats the handshake to the
// dead address for as long as they last, while their applications wait for
// a timeout of their own. closePending ends at once only the connections
// that have received nothing yet; for the others the observer notes at the
// decision what each has received, and stallTimeout later ends those that
// received nothing since: they cannot progress, and their applications
// reconnect through the outbound chosen now. A connection that received
// anything since is kept: the path works for it, as an address that refuses
// new connections may still serve old ones.
//
// This holds for every decision but one made while the network check was
// failing, which ends nothing: the network may be what failed. Failures
// about the path need no proof, and a probe goes to a destination of ours:
// one that fails tells that the path is dead, also when it confirmed
// failures that may have been about destinations. Once the core has left
// the path, a connection silent on it for stallTimeout does not progress,
// and its application is better off on the outbound chosen now.
//
// The check costs one wake of the loop per decision and none in silence,
// and reads the counters the trackers keep anyway, nothing per packet.

// silenceCheck is the check of one decision.
type silenceCheck struct {
	mark       downMark
	due        time.Time
	generation uint64
	// A failover away from the outbound was still under way when the check
	// was due: it runs once the balancers moved off, see movedOff.
	held bool
}

// watchSilence plans the check of a decision. A later decision about the
// same outbound replaces the earlier one: the observer notes the
// connections anew for it.
func (e *engine) watchSilence(d downMark) {
	for i := range e.silences {
		if e.silences[i].mark.tag == d.tag {
			e.silences[i] = silenceCheck{mark: d, due: d.at.Add(d.silentAfter), generation: e.generation}
			return
		}
	}
	e.silences = append(e.silences, silenceCheck{mark: d, due: d.at.Add(d.silentAfter), generation: e.generation})
}

type silenceVerdict int8

const (
	silenceDrop silenceVerdict = iota
	silenceHold
	silenceEnd
)

// dueSilences hands the observer the checks that are due and returns when
// the next one is; zero when none waits for its time.
func (e *engine) dueSilences(now time.Time) time.Time {
	var wake time.Time
	kept := e.silences[:0]
	for _, c := range e.silences {
		switch {
		case c.held && !e.downSince(c.mark):
			continue
		case c.held:
			kept = append(kept, c)
			continue
		case now.Before(c.due):
			wake = soonestOf(wake, c.due)
			kept = append(kept, c)
			continue
		}
		switch e.silenceVerdict(c) {
		case silenceEnd:
			e.downs = append(e.downs, downMark{tag: c.mark.tag, at: c.mark.at, reason: c.mark.reason, silent: true})
		case silenceHold:
			c.held = true
			kept = append(kept, c)
		}
	}
	clear(e.silences[len(kept):])
	e.silences = kept
	return wake
}

// downSince tells whether the outbound of the mark has stayed down since,
// without coming back in between.
func (e *engine) downSince(d downMark) bool {
	p := e.paths[d.tag]
	return p != nil && p.status == statusDown && !p.statusSince.After(d.at) && !p.provisional
}

// silenceVerdict decides a check that is due. Silence proves nothing when
// the outbound came back since, or after a network change, or when the
// network itself failed meanwhile: then everything is kept. An outbound a
// balancer chooses keeps its connections too: pinned to it, the balancer
// stays, and any other way back to it means it works. While a failover away
// from it is still under way, new connections still go to it, and those
// ended now would come back to it: the check waits for the balancers to
// move off.
func (e *engine) silenceVerdict(c silenceCheck) silenceVerdict {
	if !e.downSince(c.mark) || c.generation != e.generation || e.networkDown || e.doubt || e.resumedAt.After(c.mark.at) {
		return silenceDrop
	}
	verdict := silenceEnd
	for _, s := range e.selectors {
		if s.current() != c.mark.tag {
			continue
		}
		if len(s.failoverProbing) == 0 {
			return silenceDrop
		}
		verdict = silenceHold
	}
	return verdict
}

// movedOff runs a check of tag held for a failover now that a balancer has
// moved off it.
func (e *engine) movedOff(tag string, now time.Time) {
	for i := range e.silences {
		if c := &e.silences[i]; c.held && c.mark.tag == tag {
			c.held = false
			c.due = now
		}
	}
}

// noteReceived notes what the connection has received by the time its
// outbound was found down at mark, in the units of started. One that cannot
// tell, a spliced connection on a platform whose kernel does not count, is
// not noted, and silence never ends it. The observer calls it under trackMu.
func (t *connTracker) noteReceived(mark int64) {
	n, ok := t.received()
	if !ok {
		t.silentMark = 0
		return
	}
	t.silentMark, t.silentRead = mark, n
}

// silentSince tells whether the connection, noted at mark, has received
// nothing since. lastRead tells of what came between the decision and the
// note; the count, of what came after once lastRead stops at successBytes.
// The observer calls it under trackMu.
func (t *connTracker) silentSince(mark int64) bool {
	if t.silentMark != mark || t.lastRead.Load() > mark {
		return false
	}
	n, ok := t.received()
	return ok && n == t.silentRead
}

// received returns what the connection has received so far: once it is
// spliced, the bytes move past the counters, and the kernel tells them.
func (t *connTracker) received() (uint64, bool) {
	n := uint64(t.bytesRead.Load())
	if !t.spliced.Load() {
		return n, true
	}
	rc := t.socket.Load()
	if rc == nil {
		return 0, false
	}
	rx, ok := kernelReceived(*rc)
	return n + rx, ok
}
