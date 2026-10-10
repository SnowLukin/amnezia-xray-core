package health

import (
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"sort"
	"time"
)

type outboundGroup struct {
	name        string
	members     []string
	limit       int
	failed      int
	down        bool
	provisional bool
	until       time.Time
	level       int
	position    int
	lastFailed  string
	lastWorking string
	lastFailure string
}

func (s *selectorSettings) groupEntry(tag string) *costEntrySettings {
	for i := range s.costEntries {
		c := &s.costEntries[i]
		if c.matches(tag) {
			if c.groupLimit > 0 {
				return c
			}
			return nil
		}
	}
	return nil
}

func (s *selector) groupFor(tag string) *outboundGroup {
	if c := s.cfg.groupEntry(tag); c != nil {
		g := s.groups[c.match]
		if g != nil && slices.Contains(g.members, tag) {
			return g
		}
	}
	return nil
}

func (s *selector) groupDown(tag string) bool {
	g := s.groupFor(tag)
	return g != nil && g.down
}

func (e *engine) syncGroups(s *selector) {
	next := map[string]*outboundGroup{}
	for _, tag := range s.candidates {
		c := s.cfg.groupEntry(tag)
		if c == nil {
			continue
		}
		g := next[c.match]
		if g == nil {
			g = s.groups[c.match]
			if g == nil || g.limit != c.groupLimit {
				g = &outboundGroup{name: c.match, limit: c.groupLimit}
			} else {
				g.members = nil
			}
			next[c.match] = g
		}
		g.members = append(g.members, tag)
	}
	for _, g := range next {
		sort.Strings(g.members)
		g.members = slices.Compact(g.members)
		sum := sha256.Sum256([]byte(e.run + "\x00" + g.name))
		g.position = int(binary.BigEndian.Uint64(sum[:8]) % uint64(len(g.members)))
		if !slices.Contains(g.members, g.lastWorking) {
			g.lastWorking = ""
		}
		if !slices.Contains(g.members, g.lastFailed) {
			g.lastFailed = ""
			g.failed = 0
		}
	}
	s.groups = next
}

func (g *outboundGroup) start(s *selector) int {
	if g.lastFailed != "" && (g.failed > 0 || g.down) {
		return (slices.Index(g.members, g.lastFailed) + 1) % len(g.members)
	}
	if i := slices.Index(g.members, s.current()); i >= 0 {
		return i
	}
	if i := slices.Index(g.members, g.lastWorking); i >= 0 {
		return i
	}
	return g.position
}

// Prices always take precedence, including prices changed by a provider.
// At the same price a group stays together and follows its tag ring.
func (e *engine) rankedGroups(s *selector, now time.Time, skip func(string) bool) []string {
	return e.groupOrder(s, now, skip, reasonFailover)
}

func (e *engine) groupOrder(s *selector, now time.Time, skip func(string) bool, reason probeReason) []string {
	type item struct {
		tag, key     string
		cost         float64
		expired      bool
		known, order int
	}
	items := []item{}
	for i, t := range s.candidates {
		if skip != nil && skip(t) {
			continue
		}
		p := e.pathAt(t, now)
		g := s.groupFor(t)
		if !p.usable(now) || (g != nil && g.down && reason != reasonRecheck && reason != reasonHost) {
			continue
		}
		it := item{tag: t, key: t, cost: e.cost(s, t), expired: e.expired[t], known: 1, order: i}
		if p.status == statusUp {
			it.known = 0
		}
		if g != nil {
			it.key = g.members[0]
			it.known = 0
			start := g.start(s)
			if reason == reasonHost && g.down {
				start = g.position
			}
			if reason == reasonReturnToCheaper {
				start = g.position
				if i := slices.Index(g.members, g.lastWorking); i >= 0 {
					start = i
				}
			}
			it.order = (slices.Index(g.members, t) - start + len(g.members)) % len(g.members)
		}
		items = append(items, it)
	}
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.expired != b.expired {
			return !a.expired
		}
		if a.cost != b.cost {
			return a.cost < b.cost
		}
		if a.known != b.known {
			return a.known < b.known
		}
		if a.key != b.key {
			return a.key < b.key
		}
		return a.order < b.order
	})
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.tag
	}
	return out
}

func (e *engine) groupFailure(tag string, now time.Time) {
	for _, s := range e.sortedSelectors() {
		g := s.groupFor(tag)
		if g == nil || g.down || (s.current() != tag && !s.failoverProbing[tag]) {
			continue
		}
		g.failed++
		g.lastFailed = tag
		p := e.path(tag)
		g.lastFailure = p.lastClass
		if p.reason == "traffic" && len(p.failures) > 0 {
			g.lastFailure = p.failures[len(p.failures)-1].kind.String()
		}
		if g.lastFailure == "" {
			g.lastFailure = p.reason
		}
		if g.failed >= g.limit {
			e.groupQuarantine(s, g, now)
		}
	}
}

func (e *engine) groupQuarantine(s *selector, g *outboundGroup, now time.Time) {
	changed := !g.down
	g.down = true
	g.provisional = e.doubt
	g.until = now.Add(e.jitter(e.s.schedule.quarantine.delay(g.level), e.s.schedule.quarantine.jitter))
	if !e.doubt {
		g.level++
	}
	if target := s.returnToCheaperTarget; target != "" && s.groupFor(target) == g {
		s.returnToCheaperTarget = ""
		s.returnToCheaperSuccesses = 0
		e.queue = slices.DeleteFunc(e.queue, func(j *probeJob) bool { return j.reason == reasonReturnToCheaper && !e.returningTo(j.tag) })
	}
	if changed {
		e.emitGroup(s, g, now)
	}
}

func (e *engine) groupSuccess(tag string, now time.Time) {
	for _, s := range e.sortedSelectors() {
		g := s.groupFor(tag)
		if g == nil || !s.hasCandidate(tag) {
			continue
		}
		wasDown := g.down
		g.failed = 0
		g.provisional = false
		g.lastFailed = ""
		g.lastWorking = tag
		g.down = false
		g.until = time.Time{}
		g.level = 0
		if wasDown {
			e.emitGroup(s, g, now)
		}
	}
}

func (e *engine) emitGroup(s *selector, g *outboundGroup, now time.Time) {
	state := "up"
	if g.down {
		state = "down"
	}
	e.emit(now, "group_state", map[string]any{"balancer": s.tag, "group": g.name, "state": state, "members": len(g.members), "failed": g.failed, "quarantine_ms": max(g.until.Sub(now).Milliseconds(), 0)})
}

func (e *engine) sameGroup(a, b string) bool {
	for _, s := range e.selectors {
		if g := s.groupFor(a); g != nil && g == s.groupFor(b) {
			return true
		}
	}
	return false
}

func (e *engine) groupProbeClaimed(j *probeJob) bool {
	s := e.selectors[j.selector]
	return s != nil && s.hasCandidate(j.tag) && s.groupDown(j.tag) &&
		(s.exhausted || (!e.expired[j.tag] && !s.cfg.isLazy(j.tag)))
}

func (e *engine) recheckGroups(s *selector, now time.Time) time.Time {
	if s.exhausted || s.pinned != "" || e.memoryLow {
		return time.Time{}
	}
	var wake time.Time
	names := make([]string, 0, len(s.groups))
	for name := range s.groups {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		g := s.groups[name]
		if !g.down {
			continue
		}
		var tag string
		var memberWake time.Time
		for i := range g.members {
			t := g.members[(g.start(s)+i)%len(g.members)]
			if e.expired[t] || s.cfg.isLazy(t) {
				continue
			}
			if p := e.path(t); p.quarantined(now) {
				memberWake = soonestOf(memberWake, p.quarantineUntil)
				continue
			}
			tag = t
			break
		}
		if tag == "" {
			if !memberWake.IsZero() {
				wake = soonestOf(wake, maxTime(g.until, memberWake))
			}
			continue
		}
		if now.Before(g.until) {
			wake = soonestOf(wake, g.until)
			continue
		}
		pending := false
		for _, t := range g.members {
			if e.queued(t, reasonGroup) {
				pending = true
				break
			}
		}
		if !pending {
			e.enqueue(tag, reasonGroup, s.tag, now)
		}
	}
	return wake
}

func (e *engine) onGroupProbeResult(j *probeJob, r probeResult, p *path, now time.Time) {
	if !r.ok {
		waiting := map[*selector]*outboundGroup{}
		for _, s := range e.sortedSelectors() {
			if g := s.groupFor(j.tag); g != nil && g.down {
				waiting[s] = g
			}
		}
		e.markDown(p, "probe", false, now)
		for _, s := range e.sortedSelectors() {
			if g := waiting[s]; g != nil && g.down {
				g.lastFailed = j.tag
				g.lastFailure = p.lastClass
				g.failed++
				e.groupQuarantine(s, g, now)
			}
		}
		return
	}
	e.markUp(p, "probe", now)
	for _, s := range e.sortedSelectors() {
		if !s.hasCandidate(j.tag) || s.pinned != "" {
			continue
		}
		if s.exhausted {
			e.switchTo(s, j.tag, "recovered", now)
		} else if s.cfg.returnInterval(s.current()) > 0 && s.returnToCheaperAt.IsZero() {
			e.scheduleReturnToCheaper(s, now, false)
		}
	}
}

type groupStatusView struct {
	Balancer       string `json:"balancer"`
	Group          string `json:"group"`
	State          string `json:"state"`
	Members        int    `json:"members"`
	Healthy        int    `json:"healthy"`
	Quarantined    int    `json:"quarantined"`
	ClientPosition int    `json:"client_position"`
	Failed         int    `json:"failed"`
	QuarantineMs   int64  `json:"quarantine_ms"`
	LastFailure    string `json:"last_failure,omitempty"`
}

func (e *engine) groupViews(now time.Time) []groupStatusView {
	var views []groupStatusView
	for _, s := range e.sortedSelectors() {
		names := make([]string, 0, len(s.groups))
		for name := range s.groups {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			g := s.groups[name]
			v := groupStatusView{Balancer: s.tag, Group: g.name, State: "up", Members: len(g.members), ClientPosition: g.position, Failed: g.failed, LastFailure: g.lastFailure}
			if g.down {
				v.State = "down"
				v.QuarantineMs = max(g.until.Sub(now).Milliseconds(), 0)
			}
			for _, t := range g.members {
				p := e.paths[t]
				if p == nil {
					continue
				}
				if p.quarantined(now) {
					v.Quarantined++
				}
				if p.status == statusUp && !e.expired[t] && !g.down {
					v.Healthy++
				}
			}
			views = append(views, v)
		}
	}
	return views
}

func (s *selector) groupRepresentatives(tags []string) []string {
	seen := map[*outboundGroup]bool{}
	out := make([]string, 0, len(tags))
	for _, t := range tags {
		g := s.groupFor(t)
		if g != nil {
			if seen[g] {
				continue
			}
			seen[g] = true
		}
		out = append(out, t)
	}
	return out
}
