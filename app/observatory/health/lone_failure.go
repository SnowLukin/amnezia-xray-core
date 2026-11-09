package health

import "time"

const loneFailureInterval = 5 * time.Minute

func (e *engine) hasPathProbe(tag string) bool {
	current := func(j *probeJob) bool {
		return j.tag == tag && !j.invalidated && j.generation == e.generation
	}
	for _, j := range e.queue {
		if current(j) {
			return true
		}
	}
	for _, j := range e.running {
		if current(j) {
			return true
		}
	}
	return false
}

func (e *engine) queueLoneFailure(p *path, now time.Time) {
	if !e.loneFailureEligible(p, now) || p.probing || e.hasPathProbe(p.tag) ||
		(!p.lastLoneAt.IsZero() && now.Sub(p.lastLoneAt) < loneFailureInterval) {
		return
	}
	if j := e.enqueue(p.tag, reasonLone, "", now); j != nil {
		j.loneDown = p.downID
		j.loneSuccess = p.lastSuccess
	}
}

func (e *engine) loneFailureEligible(p *path, now time.Time) bool {
	since := maxTime(maxTime(p.lastSuccess, p.created), maxTime(e.idleNet.changed, e.resumedAt))
	return e.s.passive.loneFailureAfter > 0 && !e.networkDown && !e.doubt &&
		e.active(p.tag) && !e.retiring[p.tag] && !e.replacing[p.tag] &&
		p.status != statusSuspect && p.status != statusDown &&
		now.Sub(since) > e.s.passive.loneFailureAfter
}

func (e *engine) loneNeedsConnectivity(now time.Time) bool {
	return e.s.connectivity != nil && !e.connectivityIgnored &&
		(e.lastConnectivityOK.Before(e.idleNet.changed) ||
			now.Sub(e.lastConnectivityOK) > e.s.connectivity.timeout*4)
}

func (e *engine) loneFailureCurrent(j *probeJob, now time.Time) bool {
	p := e.paths[j.tag]
	return p != nil && !j.invalidated && j.generation == e.generation &&
		p.downID == j.loneDown && p.lastSuccess.Equal(j.loneSuccess) &&
		e.loneFailureEligible(p, now)
}

func (e *engine) cancelLoneFailure(tag string) {
	e.dropQueued(tag, reasonLone)
	for _, j := range e.running {
		if j.tag == tag && j.reason == reasonLone && !j.invalidated {
			j.invalidate()
			if p := e.paths[tag]; p != nil && j.generation == e.generation {
				p.probing = false
			}
		}
	}
}

func (e *engine) cancelLoneFailures() {
	for tag := range e.paths {
		e.cancelLoneFailure(tag)
	}
}
