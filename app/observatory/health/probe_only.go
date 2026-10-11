package health

import (
	"slices"
	"sort"
	"time"
)

func (e *engine) isProbeOnly(tag string) bool {
	return e.s.isProbeOnly(tag)
}

func (e *engine) probeOnlyThreshold(s *selector) int {
	threshold := 0
	for _, p := range e.providers {
		if p.cfg.balancer == s.tag {
			threshold = max(threshold, p.cfg.minHealthy)
		}
	}
	return threshold
}

func (e *engine) probeOnlyNeeded(s *selector) bool {
	if s == nil {
		return false
	}
	threshold := e.probeOnlyThreshold(s)
	return s.started && threshold > 0 && e.healthy(s) < threshold
}

func (e *engine) probeOnlyTags(s *selector) []string {
	var tags []string
	for _, tag := range s.inventory {
		if e.isProbeOnly(tag) && !e.retiring[tag] && !e.replacing[tag] {
			tags = append(tags, tag)
		}
	}
	sort.SliceStable(tags, func(i, j int) bool { return e.cost(s, tags[i]) < e.cost(s, tags[j]) })
	return tags
}

func (e *engine) candidateVerified(p *path, now time.Time) bool {
	return p != nil && p.candidateEpoch == e.candidateEpoch && !p.candidateVerifiedAt.IsZero() &&
		now.Sub(p.candidateVerifiedAt) < e.s.schedule.probeOnly.recheckInterval &&
		p.lastProbeResult.ok && p.lastProbeResult.reason == reasonCandidate.String()
}

func (e *engine) probeOnlyVerified(s *selector, now time.Time) int {
	if s == nil {
		return 0
	}
	verified := 0
	for _, tag := range e.probeOnlyTags(s) {
		if e.candidateVerified(e.paths[tag], now) {
			verified++
		}
	}
	return verified
}

func (e *engine) probeOnlyRefreshReady(s *selector, now time.Time) bool {
	if s == nil {
		return true
	}
	if e.probeOnlyVerified(s, now) > 0 {
		return true
	}
	for _, tag := range e.probeOnlyTags(s) {
		p := e.paths[tag]
		if p == nil || p.candidateEpoch != e.candidateEpoch || !p.candidateChecked ||
			!p.candidateVerifiedAt.IsZero() || e.queued(tag, reasonCandidate) {
			return false
		}
	}
	return true
}

func (e *engine) resetProbeOnly(tag string) {
	if p := e.paths[tag]; p != nil {
		p.candidateChecked = false
		p.candidateVerifiedAt = time.Time{}
		p.candidateEpoch = e.candidateEpoch
	}
}

func (e *engine) cancelCandidates(selectorTag string) {
	e.queue = slices.DeleteFunc(e.queue, func(j *probeJob) bool {
		return j.reason == reasonCandidate && (selectorTag == "" || j.selector == selectorTag)
	})
	for _, j := range e.running {
		if j.reason == reasonCandidate && (selectorTag == "" || j.selector == selectorTag) {
			j.invalidated = true
			if p := e.paths[j.tag]; p != nil {
				p.probing = false
			}
		}
	}
}

func (e *engine) candidateCurrent(j *probeJob) bool {
	s := e.selectors[j.selector]
	return !j.invalidated && j.generation == e.generation && j.candidateEpoch == e.candidateEpoch &&
		e.isProbeOnly(j.tag) && !e.retiring[j.tag] && !e.replacing[j.tag] && s != nil &&
		slices.Contains(s.inventory, j.tag) && e.probeOnlyNeeded(s)
}

func (e *engine) updateProbeOnly(s *selector, now time.Time) time.Time {
	if !e.probeOnlyNeeded(s) {
		if s.probeOnlyActive {
			s.probeOnlyActive = false
			e.cancelCandidates(s.tag)
		}
		return time.Time{}
	}
	tags := e.probeOnlyTags(s)
	if !s.probeOnlyActive {
		s.probeOnlyActive = true
		for _, tag := range tags {
			p := e.pathAt(tag, now)
			if p.candidateEpoch != e.candidateEpoch || p.candidateVerifiedAt.IsZero() {
				e.resetProbeOnly(tag)
			}
		}
	}
	if e.networkDown {
		return time.Time{}
	}
	for _, j := range e.running {
		if j.reason == reasonCandidate {
			return time.Time{}
		}
	}
	for _, j := range e.queue {
		if j.reason == reasonCandidate && j.selector == s.tag {
			return time.Time{}
		}
	}
	var wake time.Time
	for _, tag := range tags {
		p := e.pathAt(tag, now)
		if p.candidateVerifiedAt.IsZero() || p.candidateEpoch != e.candidateEpoch {
			continue
		}
		due := p.candidateVerifiedAt.Add(e.s.schedule.probeOnly.recheckInterval)
		if !now.Before(due) {
			e.enqueue(tag, reasonCandidate, s.tag, now)
			return time.Time{}
		}
		wake = soonestOf(wake, due)
	}
	if e.probeOnlyVerified(s, now) >= e.s.schedule.probeOnly.keepVerified {
		return wake
	}
	for _, tag := range tags {
		p := e.pathAt(tag, now)
		if p.candidateEpoch != e.candidateEpoch || !p.candidateChecked {
			e.enqueue(tag, reasonCandidate, s.tag, now)
			return wake
		}
	}
	return wake
}

func (e *engine) onCandidateResult(j *probeJob, r probeResult, p *path, now time.Time) {
	p.candidateChecked = true
	p.candidateEpoch = e.candidateEpoch
	p.candidateVerifiedAt = time.Time{}
	state := statusDown
	if r.ok {
		state = statusUp
		p.candidateVerifiedAt = now
	}
	if p.setStatus(state, reasonCandidate.String(), now) {
		e.emitPath(p, now)
	}
}
