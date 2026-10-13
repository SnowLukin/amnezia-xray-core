package health

import "time"

// An outbound that can sleep, such as WireGuard, refuses while a connection
// goes through it. The flows a phone opens through it while a balancer
// chooses it, push connections, iCloud and the like, outlive the choice and
// are silent most of the time: on a path that works nothing ends them, and
// they would keep the tunnel, its keepalives and their battery cost for
// good. With sleep.closeIdleAfter, once no balancer has chosen such an
// outbound for sleep.afterLastUse and nothing but its connections holds it, the
// observer ends those that have received nothing for closeIdleAfter. Their
// applications reconnect through the outbound chosen now, and the outbound
// sleeps soon after the rest are gone: the shortest sleep.afterLastUse later, as a
// connection that ended may still be closing inside the tunnel.
//
// It is the one place where the observer ends connections on a path that
// works, so it stays lazy. A check notes what the connections have received
// at the first turn after the outbound was left or used, and runs again
// once, when the first of them may have been silent long enough; none runs
// while there is nothing to wait for. Each check reads the counters the
// trackers keep anyway, in one pass over the open connections.

// idleNet follows the network for the checks: silence tells that an
// application idles only while the network worked all along.
type idleNet struct {
	// When the host last reported a network change.
	changed time.Time
	// The first turn of the loop that found the network up and out of
	// doubt after one that did not; zero while it is not.
	settled time.Time
}

// noteNetwork follows the network at every turn of the loop. A turn that
// finds it back counts from then, which is never before it came back; a
// pause that began and ended between two turns shows in resumedAt.
func (e *engine) noteNetwork(now time.Time) {
	switch {
	case e.networkDown || e.doubt:
		e.idleNet.settled = time.Time{}
	case e.idleNet.settled.IsZero():
		e.idleNet.settled = now
	}
}

// networkSettled tells since when the network has worked without a change,
// or false while it is down or in doubt.
func (e *engine) networkSettled() (time.Time, bool) {
	if e.networkDown || e.doubt || e.idleNet.settled.IsZero() {
		return time.Time{}, false
	}
	return maxTime(maxTime(e.idleNet.settled, e.idleNet.changed), e.resumedAt), true
}

// planIdleCheck plans the check of an outbound that keptAwake holds and
// returns when it is due; zero when it is planned now or none waits for a
// time. A planned check goes into the batch of sleeps.
func (e *engine) planIdleCheck(tag string, p *path, now time.Time, idle bool) time.Time {
	if e.heldAwake(tag, idle) {
		// Chosen again, probed, asked through: it waits sleep.afterLastUse from
		// there, whatever a check ended before.
		p.idleClosed = false
		return time.Time{}
	}
	due, closeFrom, ok := e.idleCheckDue(p, now)
	if !ok {
		return time.Time{}
	}
	if now.Before(due) {
		return due
	}
	e.sleeps = append(e.sleeps, sleepJob{tag: tag, p: p, uses: p.uses, idleCheck: true, closeFrom: closeFrom})
	p.sleeping = true
	return time.Time{}
}

// idleCheckDue tells when the connections of an outbound held by nothing
// else are to be checked, and from when the check may end them: sleep.afterLastUse
// after its last use, which leaving it is, and closeIdleAfter after the
// network settled, so that silence a network change or an outage caused
// ends nothing. Once a check ended connections, the ends that follow,
// uses as they are, restart no wait. A use since the last check has the
// next one note the connections at once. False when no check is to come
// for now: the network is down or in doubt, or no connection the last
// check kept can ever be found silent, as a spliced one whose kernel does
// not count.
func (e *engine) idleCheckDue(p *path, now time.Time) (due, closeFrom time.Time, ok bool) {
	n := e.s.closeIdleAfter
	if n <= 0 || p.sleeper != sleeperYes {
		return time.Time{}, time.Time{}, false
	}
	settled, ok := e.networkSettled()
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	closeFrom = settled.Add(n)
	if !p.idleClosed {
		closeFrom = maxTime(closeFrom, p.usedAt.Add(n))
	}
	switch {
	case p.idleUses != p.uses:
		return now, closeFrom, true
	case p.idleNext.IsZero():
		return time.Time{}, time.Time{}, false
	}
	return maxTime(closeFrom, p.idleNext), closeFrom, true
}

// idleCheckStillDue checks again, right before the check, what
// planIdleCheck decided under the lock: a balancer may have chosen the
// outbound since, a probe or a request taken it, or the network failed. The
// lock is not held across the check, which ends connections that report
// back to the observer.
func (e *engine) idleCheckStillDue(j *sleepJob, now time.Time) bool {
	p := j.p
	if e.paths[j.tag] != p || p.uses != j.uses || p.processes == 0 || e.heldAwake(j.tag, e.idle(now)) {
		return false
	}
	due, closeFrom, ok := e.idleCheckDue(p, now)
	j.closeFrom = closeFrom
	return ok && !now.Before(due)
}

// onIdleChecked applies what a check found. The ends of the connections it
// ended are uses that may have come before: the check stands for the uses
// it saw, and the next turn notes the rest anew.
func (e *engine) onIdleChecked(j sleepJob, now time.Time) {
	if j.closed > 0 {
		e.emit(now, "idle_closed", map[string]any{"outbound": j.tag, "connections": j.closed})
	}
	p := j.p
	if e.paths[j.tag] != p {
		return
	}
	p.sleeping = false
	if !j.checked {
		// Taken meanwhile: the next turn plans afresh.
		return
	}
	p.idleUses, p.idleNext = j.uses, j.next
	if j.closed > 0 {
		p.idleClosed = true
	}
}

// closeIdle runs the checks of a batch outside the observer lock: it ends
// the connections of each outbound that have received nothing for
// closeIdleAfter, once the check may, and notes in the job when one it
// kept may first have been silent that long. One pass over the open
// connections serves every check of the batch.
func (o *Observer) closeIdle(batch []sleepJob) {
	var checks map[string]*sleepJob
	o.mu.Lock()
	now := o.now()
	for i := range batch {
		j := &batch[i]
		if !j.idleCheck || !o.e.idleCheckStillDue(j, now) {
			continue
		}
		j.checked = true
		if checks == nil {
			checks = map[string]*sleepJob{}
		}
		checks[j.tag] = j
	}
	o.mu.Unlock()
	if len(checks) == 0 {
		return
	}
	n := int64(o.s.closeIdleAfter)
	at := int64(now.Sub(o.base))
	var aborts []func()
	o.trackMu.Lock()
	for t := range o.trackers {
		j := checks[t.tag]
		if j == nil || t.abort == nil || t.state.Load() == trackerFinished || t.has(reportedAborted) {
			continue
		}
		since, ok := t.idleSince(at, o.s.passive.successBytes)
		switch {
		case !ok:
			// It cannot tell, and is kept: nothing to wait for.
		case at-since >= n && !now.Before(j.closeFrom):
			if t.mark(reportedAborted) {
				aborts = append(aborts, t.abort)
				j.closed++
			}
		default:
			if due := o.base.Add(time.Duration(since + n)); j.next.IsZero() || due.Before(j.next) {
				j.next = due
			}
		}
	}
	for t := range o.closures {
		j := checks[t.tag]
		if j == nil || t.abort == nil || t.state.Load() == trackerFinished || t.aborted.Load() {
			continue
		}
		since, ok := t.idleSince(at)
		if !ok {
			continue
		}
		if at-since >= n && !now.Before(j.closeFrom) {
			if t.aborted.CompareAndSwap(false, true) {
				aborts = append(aborts, t.abort)
				j.closed++
			}
		} else if due := o.base.Add(time.Duration(since + n)); j.next.IsZero() || due.Before(j.next) {
			j.next = due
		}
	}
	o.trackMu.Unlock()
	// Ending a connection reports back to the observer.
	for _, abort := range aborts {
		abort()
	}
}

// idleSince tells since when the connection has received nothing, at now
// in the units of started, or false when it cannot tell: a spliced
// connection on a platform whose kernel does not count. Below successBytes
// lastRead tells. Past it the tracker no longer times reads, and a spliced
// connection moves its bytes past the counters: what it has received then
// tells against the note of a previous check, which this one takes anew
// when anything came since, as silence.go does. The observer calls it under
// trackMu.
func (t *connTracker) idleSince(now, successBytes int64) (int64, bool) {
	n, ok := t.received()
	if !ok {
		return 0, false
	}
	if !t.spliced.Load() && n < uint64(successBytes) {
		return max(t.lastRead.Load(), t.started), true
	}
	if t.idleMark != 0 && n == t.idleRead && t.lastRead.Load() <= t.idleMark {
		return t.idleMark, true
	}
	t.idleMark, t.idleRead = now, n
	return now, true
}
