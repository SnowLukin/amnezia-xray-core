package health

import (
	"slices"
	"time"
)

// A confirmation already running or an audit queued for this down may prove
// the decision false. Its mark restores the prior quarantine and return
// penalties and cancels checks of silent connections. Failover still goes on;
// a working path returns through the usual hold and return probes. A result
// from another network or an earlier down cannot use the mark.

// Returns to an outbound that came back after a failover from it spread
// over this long after minHoldTime, or over returnToCheaper.interval when that is
// shorter. One server that hiccups rescues itself on every device that saw
// the hiccup at about the same moment; returns at minHoldTime sharp would bring
// their probes to it in one wave.
const returnSpread = 5 * time.Minute

// rescueMark is what a pass of the assigned confirmation or audit takes back.
type rescueMark struct {
	job    uint64
	downID uint64
	by     string
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

// rescueMarkFor returns the mark of this down, or nil without its probe.
func (e *engine) rescueMarkFor(tag string, level int) *rescueMark {
	for _, j := range e.running {
		if j.tag == tag && j.reason == reasonConfirm && !j.invalidated && j.generation == e.generation {
			return &rescueMark{job: j.id, downID: e.path(tag).downID, by: "confirm", level: level}
		}
	}
	for _, j := range e.queue {
		if j.tag == tag && j.reason == reasonAudit && !j.invalidated && j.generation == e.generation && j.auditDown == e.path(tag).downID {
			return &rescueMark{job: j.id, downID: j.auditDown, by: "audit", level: level}
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
	if p == nil || p.rescue == nil || p.rescue.job != j.id || p.rescue.downID != p.downID {
		return nil
	}
	m := p.rescue
	p.rescue = nil
	return m
}

// rescue undoes the down of p that m's probe proved false.
func (e *engine) rescue(p *path, m *rescueMark, now time.Time) {
	if m.by == "confirm" {
		// The next weak suspicion waits longer after a false alarm.
		e.backOffWeakEvidence(p, now)
	} else {
		p.stats.auditsPassed++
	}
	p.success(now)
	p.quarantineLevel = m.level
	p.quarantineUntil = time.Time{}
	p.provisional = false
	p.setStatus(statusUp, "rescued", now)
	p.rescuedBy = m.by
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
