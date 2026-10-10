package health

import (
	"slices"
	"time"
)

// Weak evidence waits for a confirming probe, strong evidence does not: when
// failures about the path come while the probe runs, the outbound goes down
// at once and the balancers fail over. On a network that hiccups, such as
// the Wi-Fi of a phone under load, those failures may last a second while
// the probe gets through. Its answer comes after them and is the newest
// proof about the path, so a pass proves the down false and rescues the
// outbound: it is up again without the rest of its quarantine, at the
// quarantine level it had before, and the penalty the down gave a return to
// it is taken back. Its connections that went silent are kept: the check
// stallTimeout after the decision is dropped, and the switch away from it
// ends nothing, as it is no longer down. Those it ended at the decision
// stay ended.
//
// The failover the down started goes on: a balancer that moved or still
// moves away comes back by a return, soon after minHoldTime and through its
// returnToCheaper probes, so a path that fails for a second does not move new
// connections twice within it. A balancer left with nothing takes the
// outbound back at once, as after a recheck. The rescue changes no choice by
// itself, so it is no decision of the report.
//
// Only a confirming probe that was running when the outbound went down
// rescues it: one still waiting for its turn is dropped at the down, and
// one made in a previous network proves nothing about this one. Probes for
// other reasons go their usual way, which already brought an outbound up
// when they passed, whenever they started: a failover probe of it for
// another balancer does so as for any candidate, at level zero, and moves
// only that balancer. Only the answer of a confirming probe was dropped
// once the outbound was no longer suspect.

// Returns to an outbound that came back after a failover from it spread
// over this long after minHoldTime, or over returnToCheaper.interval when that is
// shorter. One server that hiccups rescues itself on every device that saw
// the hiccup at about the same moment; returns at minHoldTime sharp would bring
// their probes to it in one wave.
const returnSpread = 5 * time.Minute

// rescueMark is what a down did that the pass of a confirming probe running
// at the time takes back.
type rescueMark struct {
	job uint64
	// The quarantine level before the down.
	level int
	// Returns the down penalized, as they were before it.
	penalties []penaltyMark
}

type penaltyMark struct {
	selector   string
	penalty    float64
	had        bool
	returnedAt time.Time
}

// rescueMarkFor returns the mark of a down of tag now, or nil when no
// confirming probe of it runs that could undo it. At most one runs, as two
// probes of one server name never do.
func (e *engine) rescueMarkFor(tag string, level int) *rescueMark {
	for _, j := range e.running {
		if j.tag == tag && j.reason == reasonConfirm && !j.invalidated && j.generation == e.generation {
			return &rescueMark{job: j.id, level: level}
		}
	}
	return nil
}

// notePenalty keeps the penalty of s for tag before the down raises it.
func (m *rescueMark) notePenalty(s *selector, tag string) {
	if m == nil {
		return
	}
	v, had := s.penalty[tag]
	m.penalties = append(m.penalties, penaltyMark{selector: s.tag, penalty: v, had: had, returnedAt: s.returnedAt})
}

// takeRescue returns the mark the result of j may act on and forgets it:
// whatever j brings, it rescues nothing later.
func (e *engine) takeRescue(j *probeJob) *rescueMark {
	p := e.paths[j.tag]
	if p == nil || p.rescue == nil || p.rescue.job != j.id {
		return nil
	}
	m := p.rescue
	p.rescue = nil
	return m
}

// rescue undoes the down of p that the confirming probe of m proved false.
func (e *engine) rescue(p *path, m *rescueMark, now time.Time) {
	// For the weak evidence the probe was sent for, this is a false alarm
	// like any other: the next such suspicion waits longer.
	e.backOffWeakEvidence(p, now)
	p.success(now)
	p.quarantineLevel = m.level
	p.quarantineUntil = time.Time{}
	p.provisional = false
	p.setStatus(statusUp, "rescued", now)
	e.emitPath(p, now)
	e.remember(p.tag)
	// Silence proves nothing about a path that works; the checks would
	// find it up and keep everything, but each would cost a wake.
	e.downs = slices.DeleteFunc(e.downs, func(d downMark) bool { return d.tag == p.tag })
	e.silences = slices.DeleteFunc(e.silences, func(c silenceCheck) bool { return c.mark.tag == p.tag })
	for _, pm := range m.penalties {
		s := e.selectors[pm.selector]
		if s == nil {
			continue
		}
		if pm.had {
			s.penalty[p.tag] = pm.penalty
		} else {
			delete(s.penalty, p.tag)
		}
		if s.returnedTo == "" {
			// The return holds as far as this down goes: the window that
			// clears its penalty runs on.
			s.returnedTo, s.returnedAt = p.tag, pm.returnedAt
		}
	}
	for _, s := range e.sortedSelectors() {
		if s.pinned != "" || !s.hasCandidate(p.tag) {
			continue
		}
		cur := s.current()
		switch {
		case s.exhausted:
			e.switchTo(s, p.tag, "recovered", now)
		case cur != "" && cur != p.tag && !s.cfg.isLazy(p.tag) && e.rankCost(s, p.tag) < e.rankCost(s, cur):
			// It moved away already; it returns soon after minHoldTime. A
			// failover still under way returns from where it lands.
			e.scheduleReturn(s, now)
		}
	}
}

// scheduleReturn plans the return of s to a cheaper outbound that turned out
// to work after the balancer failed over from it: once minHoldTime ends, at a
// random point within returnSpread when the returnToCheapers of s have a random
// phase, as a timed return would. The point is never before minHoldTime.
func (e *engine) scheduleReturn(s *selector, now time.Time) {
	e.scheduleReturnToCheaper(s, now, true)
	u := s.cfg.returnToCheaper
	if s.returnToCheaperAt.IsZero() || !u.randomPhase {
		return
	}
	spread := min(s.cfg.returnInterval(s.current()), returnSpread)
	s.returnToCheaperAt = s.returnToCheaperAt.Add(time.Duration(e.rnd.Float64() * float64(spread)))
}
