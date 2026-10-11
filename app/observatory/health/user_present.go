package health

import (
	"slices"
	"time"
)

// UserPresent queues checks of stale selected paths after the user unlocks
// the device. It returns without waiting for any network result.
func (o *Observer) UserPresent() {
	o.mu.Lock()
	o.e.userPresent(o.now())
	o.mu.Unlock()
	o.wake()
}

func (e *engine) userPresent(now time.Time) {
	if e.networkDown || e.doubt {
		return
	}
	seen := map[string]bool{}
	for _, s := range e.sortedSelectors() {
		tag := s.current()
		if s.pinned != "" || s.exhausted || tag == "" || seen[tag] {
			continue
		}
		seen[tag] = true
		p := e.paths[tag]
		if p == nil || e.retiring[tag] || e.replacing[tag] {
			continue
		}
		if !e.wakeProofStale(p, now) || (!p.lastWakeAt.IsZero() && now.Sub(p.lastWakeAt) < e.s.wake.minInterval) ||
			e.queued(tag, reasonWake) {
			e.report.counts.WakeSkipped++
			continue
		}
		if p.probing || (e.hasPathProbe(tag) && !e.replaceDeferredWake(tag, now)) {
			continue
		}
		if j := e.enqueue(tag, reasonWake, "", now); j != nil {
			j.wakeDown = p.downID
			j.wakeSuccess = p.lastSuccess
		}
	}
}

func (e *engine) replaceDeferredWake(tag string, now time.Time) bool {
	current := func(j *probeJob) bool {
		return j.tag == tag && !j.invalidated && j.generation == e.generation
	}
	for _, j := range e.running {
		if current(j) {
			return false
		}
	}
	idle := e.idle(now)
	found := false
	for _, j := range e.queue {
		if !current(j) {
			continue
		}
		if (j.reason != reasonNetwork && j.reason != reasonRemembered) || !e.putOff(j, idle) {
			return false
		}
		found = true
	}
	if !found {
		return false
	}
	e.queue = slices.DeleteFunc(e.queue, current)
	return true
}

func (e *engine) wakeProofStale(p *path, now time.Time) bool {
	return p.lastSuccess.IsZero() || now.Sub(p.lastSuccess) > e.s.wake.freshness
}

func (e *engine) wakeProbeCurrent(j *probeJob, now time.Time) bool {
	p := e.paths[j.tag]
	if p == nil || j.invalidated || j.generation != e.generation || e.networkDown || e.doubt ||
		e.retiring[j.tag] || e.replacing[j.tag] || p.downID != j.wakeDown ||
		!p.lastSuccess.Equal(j.wakeSuccess) || !e.wakeProofStale(p, now) {
		return false
	}
	for _, s := range e.selectors {
		if s.current() == j.tag && s.pinned == "" && !s.exhausted {
			return true
		}
	}
	return false
}

func (e *engine) cancelWakeProbe(tag string) {
	e.dropQueued(tag, reasonWake)
	for _, j := range e.running {
		if j.tag == tag && j.reason == reasonWake && !j.invalidated {
			j.invalidated = true
			if p := e.paths[tag]; p != nil && j.generation == e.generation {
				p.probing = false
			}
		}
	}
}

func (e *engine) cancelWakeProbes() {
	for tag := range e.paths {
		e.cancelWakeProbe(tag)
	}
}
