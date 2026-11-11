package health

import (
	"slices"
	"time"
)

// updateStandby keeps the outbounds of the balancer with probePolicy
// standby checked while it does not choose them: one that came into
// the inventory, from the base config or a set, is checked first, every one
// again in a new network and then once probes.standby.recheckInterval after
// the last probe that judged it. Like candidates, the checks wait for the
// user and for memory, go one at a time and spend the probe budget.
func (e *engine) updateStandby(s *selector, now time.Time) time.Time {
	seen := s.standbySeen
	s.standbySeen = nil
	var tags []string
	for _, tag := range s.inventory {
		if !s.cfg.isStandby(tag) || e.isProbeOnly(tag) {
			continue
		}
		if s.standbySeen == nil {
			s.standbySeen = make(map[string]bool)
		}
		s.standbySeen[tag] = true
		if !seen[tag] {
			e.pathAt(tag, now).standbyAt = time.Time{}
		}
		tags = append(tags, tag)
	}
	if len(tags) == 0 || !s.started || e.networkDown || e.idle(now) || e.memoryLow {
		e.cancelStandbyQueue(s.tag)
		return time.Time{}
	}
	for _, j := range e.queue {
		if j.reason == reasonStandby && j.selector == s.tag {
			return time.Time{}
		}
	}
	var wake time.Time
	for _, tag := range tags {
		p := e.pathAt(tag, now)
		if tag == s.current() || p.probing || e.expired[tag] || e.retiring[tag] || e.replacing[tag] {
			continue
		}
		due := p.standbyAt.Add(e.s.schedule.standbyInterval)
		if p.standbyAt.IsZero() || p.standbyEpoch != e.candidateEpoch || !now.Before(due) {
			e.enqueue(tag, reasonStandby, s.tag, now)
			return time.Time{}
		}
		wake = soonestOf(wake, due)
	}
	return wake
}

// standbyCurrent tells whether a queued check is still wanted: the
// balancer has the outbound and has not chosen it since.
func (e *engine) standbyCurrent(j *probeJob) bool {
	s := e.selectors[j.selector]
	return s != nil && s.cfg.isStandby(j.tag) && s.current() != j.tag && slices.Contains(s.inventory, j.tag) &&
		!e.retiring[j.tag] && !e.replacing[j.tag]
}

func (e *engine) cancelStandbyQueue(selectorTag string) {
	e.queue = slices.DeleteFunc(e.queue, func(j *probeJob) bool {
		return j.reason == reasonStandby && j.selector == selectorTag
	})
}
