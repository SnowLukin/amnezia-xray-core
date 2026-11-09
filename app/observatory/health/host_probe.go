package health

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

// ProbeRequest asks to probe outbounds of a leastCost balancer now.
type ProbeRequest struct {
	Balancer string `json:"balancer"`
	IsUDP    bool   `json:"isUdp"`
	// Candidates of the balancer to probe. Empty: the outbound the balancer
	// uses, then its other candidates that are not lazy, cheapest first.
	Outbounds []string `json:"outbounds,omitempty"`
}

// ProbeTicket names an accepted request. Its results appear in Status under
// probe_requests and in ProbeResult.
type ProbeTicket struct {
	ID  uint64 `json:"id"`
	Run string `json:"run"`
	// The outbounds the request covers, in order.
	Outbounds []string `json:"outbounds"`
}

// Results of an outbound in a probe request.
const (
	probePending = "pending"
	probeOK      = "ok"
	probeFailed  = "failed"
	probeDown    = "down"
	probeSkipped = "skipped"
	probeExpired = "expired"
	probeGone    = "gone"
)

// Closed requests kept for the host to read, normally and while memory is low.
const (
	keptProbeRequests          = 4
	keptProbeRequestsLowMemory = 1
)

// A host request is shared only by requests for the same balancer and probe kind.
type probeRequest struct {
	id         uint64
	balancer   string
	isUDP      bool
	udpEpoch   uint64
	generation uint64
	started    time.Time
	// Zero while the request waits.
	ended time.Time
	end   string
	items []probeItem
	// It waited for a probe; a request answered at once writes no event.
	waited bool
}

type probeItem struct {
	udp    *UDPStatus
	tag    string
	result string
	source string
	// The reason of the probe that answered.
	reason probeReason
	rtt    time.Duration
	err    string
	// Age of the proof for a recent answer, rest left for a quarantined one.
	age  time.Duration
	rest time.Duration
}

func (r *probeRequest) open() bool { return r.ended.IsZero() }

func (r *probeRequest) answered() bool {
	for _, it := range r.items {
		if it.result == probePending {
			return false
		}
	}
	return true
}

func (r *probeRequest) tags() []string {
	out := make([]string, len(r.items))
	for i, it := range r.items {
		out[i] = it.tag
	}
	return out
}

// requestProbe accepts a request of the host and returns its id and the
// outbounds it covers. It never waits for a probe.
func (e *engine) requestProbe(balancer string, outbounds []string, now time.Time) (uint64, []string, error) {
	return e.requestProbeKind(balancer, outbounds, false, now)
}

func (e *engine) requestProbeKind(balancer string, outbounds []string, isUDP bool, now time.Time) (uint64, []string, error) {
	if !e.s.features.ProbeRequests {
		return 0, nil, NewCommandError("feature_disabled", "probes.requests.isEnabled is false")
	}
	if isUDP && e.s.udpProbe == nil {
		return 0, nil, NewCommandError("feature_disabled", "UDP probes are disabled")
	}
	s := e.selectors[balancer]
	if s == nil {
		return 0, nil, fmt.Errorf("no such balancer: %s", balancer)
	}
	if !s.started || len(s.candidates) == 0 {
		return 0, nil, fmt.Errorf("no candidates yet: no connection went through %s", balancer)
	}
	limit := e.s.schedule.hostProbes.maxOutbounds
	var tags []string
	if len(outbounds) == 0 {
		tags = e.probePlan(s, now)
	}
	for _, t := range outbounds {
		if !s.hasCandidate(t) {
			return 0, nil, fmt.Errorf("not a candidate: %s", t)
		}
		if !slices.Contains(tags, t) {
			tags = append(tags, t)
		}
	}
	if isUDP {
		for _, tag := range tags {
			if e.isProbeOnly(tag) {
				return 0, nil, fmt.Errorf("UDP probe is not allowed for probe-only outbound: %s", tag)
			}
		}
	}
	tags = s.groupRepresentatives(tags)
	if len(tags) > limit {
		return 0, nil, fmt.Errorf("too many outbounds: at most %d", limit)
	}
	// Settles requests of a network left meanwhile, so none of them is joined.
	e.serveRequests(now)
	r := e.openRequestKind(balancer, isUDP)
	if r == nil {
		e.requestID++
		r = &probeRequest{id: e.requestID, balancer: balancer, generation: e.generation, started: now, isUDP: isUDP, udpEpoch: e.udpEpoch}
		e.requests = append(e.requests, r)
	}
	for _, t := range tags {
		if len(r.items) < limit && !slices.ContainsFunc(r.items, func(it probeItem) bool {
			return it.tag == t || (s.groupFor(t) != nil && s.groupFor(t) == s.groupFor(it.tag))
		}) {
			r.items = append(r.items, probeItem{tag: t, result: probePending})
		}
	}
	if e.serve(s, r, now) {
		e.closeRequest(r, "done", now)
	} else {
		r.waited = true
	}
	ids := r.tags()
	e.keepRequests()
	return r.id, ids, nil
}

// probePlan is what a request without outbounds covers: the outbound the
// balancer uses, the usable candidates from the cheapest, then the ones in
// quarantine, which are answered without a probe. Lazy outbounds and ones
// whose lease ended stay out; a request that lists them probes them.
func (e *engine) probePlan(s *selector, now time.Time) []string {
	cur := s.current()
	skip := func(t string) bool { return t == cur || s.cfg.isLazy(t) || e.expired[t] }
	var plan []string
	if cur != "" {
		plan = append(plan, cur)
	}
	plan = append(plan, e.ranked(s, now, skip)...)
	var resting []string
	if len(s.groups) == 0 {
		resting = e.rankedAt(s, now, time.Unix(1<<40, 0), skip)
	} else {
		resting = e.groupOrderAt(s, now, time.Unix(1<<40, 0), skip, reasonHost)
	}
	representatives := map[*outboundGroup]string{}
	for _, t := range resting {
		if g := s.groupFor(t); g != nil && g.down {
			previous := representatives[g]
			if previous == "" || (e.path(previous).quarantined(now) && !e.path(t).quarantined(now)) {
				representatives[g] = t
			}
		}
	}
	for _, t := range resting {
		if g := s.groupFor(t); g != nil && g.down {
			if representatives[g] == t {
				plan = append(plan, t)
			}
		} else if e.path(t).quarantined(now) {
			plan = append(plan, t)
		}
	}
	plan = s.groupRepresentatives(plan)
	return plan[:min(len(plan), e.s.schedule.hostProbes.maxOutbounds)]
}

func (e *engine) openRequest(balancer string) *probeRequest {
	return e.openRequestKind(balancer, false)
}

func (e *engine) openRequestKind(balancer string, isUDP bool) *probeRequest {
	for _, r := range e.requests {
		if r.open() && r.balancer == balancer && r.isUDP == isUDP {
			return r
		}
	}
	return nil
}

// serveRequests answers what open requests wait for without a probe, ends
// requests that cannot be answered any more and returns when the first open
// one times out.
func (e *engine) serveRequests(now time.Time) time.Time {
	var wake time.Time
	for _, r := range e.requests {
		if !r.open() {
			continue
		}
		s := e.selectors[r.balancer]
		deadline := r.started.Add(e.s.schedule.hostProbes.timeout)
		switch {
		case s == nil:
			e.settle(r, probeGone, "", "gone", now)
		case r.generation != e.generation || (r.isUDP && r.udpEpoch != e.udpEpoch):
			// Probes of the previous network tell nothing about this one.
			e.settle(r, probeExpired, "network_change", "network_change", now)
		case !now.Before(deadline):
			e.settle(r, probeExpired, "timeout", "timeout", now)
		case e.serve(s, r, now):
			e.closeRequest(r, "done", now)
		default:
			if wake.IsZero() || deadline.Before(wake) {
				wake = deadline
			}
		}
	}
	e.dropUnclaimedHost()
	e.keepRequests()
	return wake
}

// serve answers the waiting outbounds of r that need no probe and makes sure
// a probe is on its way for the others. It reports whether r is answered.
func (e *engine) serve(s *selector, r *probeRequest, now time.Time) bool {
	if r.isUDP {
		return e.serveUDP(s, r, now)
	}
	hp := e.s.schedule.hostProbes
	for i := range r.items {
		it := &r.items[i]
		if it.result != probePending {
			continue
		}
		if !s.offers(it.tag) {
			it.result = probeGone
			continue
		}
		p := e.path(it.tag)
		switch {
		case s.groupDown(it.tag) && now.Before(s.groupFor(it.tag).until):
			g := s.groupFor(it.tag)
			it.result, it.source, it.rest = probeDown, "group_quarantine", max(g.until.Sub(now), 0)
		case p.quarantined(now):
			it.result, it.source, it.rest = probeDown, "quarantine", p.quarantineUntil.Sub(now)
		case p.status == statusUp && now.Sub(p.lastSuccess) < hp.reuse:
			it.result, it.source, it.age = probeOK, "recent", now.Sub(p.lastSuccess)
		case e.memoryLow && !e.active(it.tag):
			// Only the search for failures goes on while memory is low.
			it.result, it.source = probeSkipped, "memory"
		default:
			// A probe of the observer that answers first takes its place.
			if j := e.enqueue(it.tag, reasonHost, r.balancer, now); j != nil {
				j.request = r.id
			}
		}
	}
	return r.answered()
}

// answerRequests hands the result of any probe of an outbound to the
// requests waiting for it.
func (e *engine) answerRequests(j *probeJob, res probeResult, now time.Time) {
	for _, r := range e.requests {
		if !r.open() || r.isUDP || j.isUDP || r.generation != j.generation {
			continue
		}
		for i := range r.items {
			it := &r.items[i]
			if it.tag != j.tag || it.result != probePending {
				continue
			}
			it.result, it.source, it.reason, it.rtt, it.err = probeFailed, "probe", j.reason, res.rtt, res.err
			if res.ok {
				it.result = probeOK
			}
		}
		if r.answered() {
			e.closeRequest(r, "done", now)
		}
	}
	e.keepRequests()
}

// settle ends r, giving result to everything it still waits for.
func (e *engine) settle(r *probeRequest, result, source, end string, now time.Time) {
	for i := range r.items {
		if it := &r.items[i]; it.result == probePending {
			it.result, it.source = result, source
		}
	}
	e.closeRequest(r, end, now)
}

func (e *engine) closeRequest(r *probeRequest, end string, now time.Time) {
	r.ended = now
	r.end = end
	if !r.waited {
		return
	}
	results, sources := map[string]int{}, map[string]int{}
	for _, it := range r.items {
		results[it.result]++
		sources[it.source]++
	}
	e.emit(now, "probe_request", map[string]any{
		"is_udp": r.isUDP, "request": r.id, "balancer": r.balancer, "outbounds": len(r.items), "end": end,
		"ms": now.Sub(r.started).Milliseconds(), "probed": sources["probe"], "reused": sources["recent"],
		"ok": results[probeOK], "failed": results[probeFailed], "down": results[probeDown],
		"skipped": results[probeSkipped], "expired": results[probeExpired], "gone": results[probeGone],
	})
}

// keepRequests forgets the oldest closed requests beyond what is kept.
func (e *engine) keepRequests() {
	limit := keptProbeRequests
	if e.memoryLow {
		limit = keptProbeRequestsLowMemory
	}
	closed := 0
	for _, r := range e.requests {
		if !r.open() {
			closed++
		}
	}
	if closed <= limit {
		return
	}
	drop := closed - limit
	e.requests = slices.DeleteFunc(e.requests, func(r *probeRequest) bool {
		if drop > 0 && !r.open() {
			drop--
			return true
		}
		return false
	})
}

// waitedFor reports whether an open request waits for an answer about tag.
func (e *engine) waitedFor(tag string) bool {
	return e.waitedForKind(tag, false)
}

func (e *engine) waitedForKind(tag string, isUDP bool) bool {
	for _, r := range e.requests {
		if !r.open() || r.isUDP != isUDP {
			continue
		}
		for _, it := range r.items {
			if it.tag == tag && it.result == probePending {
				return true
			}
		}
	}
	return false
}

// dropUnclaimedHost forgets host probes that have not started and that no
// request waits for any more.
func (e *engine) dropUnclaimedHost() {
	e.queue = slices.DeleteFunc(e.queue, func(j *probeJob) bool {
		return j.reason == reasonHost && !e.waitedForKind(j.tag, j.isUDP)
	})
}

// dropHostProbesForMemory forgets host probes of outbounds no balancer uses:
// while memory is low only the search for failures goes on.
func (e *engine) dropHostProbesForMemory() {
	e.queue = slices.DeleteFunc(e.queue, func(j *probeJob) bool {
		return j.reason == reasonHost && !e.active(j.tag)
	})
}

// onHostProbeResult applies a probe the host asked for: it is evidence like
// any other probe, but it neither moves a balancer to a cheaper outbound
// nor counts toward a return.
func (e *engine) onHostProbeResult(p *path, r probeResult, now time.Time) {
	if r.ok {
		if p.status == statusSuspect {
			e.dropQueued(p.tag, reasonConfirm)
			if r.pathState() == statusUp {
				// The same false alarm as a confirming probe that passes.
				e.backOffWeakEvidence(p, now)
			}
		}
		if r.pathState() == statusUp {
			e.markUp(p, "host_probe", now)
		} else {
			e.markProbeState(p, r, now)
		}
		for _, s := range e.sortedSelectors() {
			if s.exhausted && s.hasCandidate(p.tag) {
				e.switchTo(s, p.tag, "recovered", now)
			}
		}
		return
	}
	if _, ok := e.pendingSuspicion[p.tag]; ok {
		// Waits for the network check; the network may be what failed.
		return
	}
	e.dropQueued(p.tag, reasonConfirm)
	e.markDown(p, "host_probe", now)
	e.abandonReturns(p.tag, now)
}

type probeItemView struct {
	UDP          *UDPStatus `json:"udp,omitempty"`
	Outbound     string     `json:"outbound"`
	Result       string     `json:"result"`
	Source       string     `json:"source,omitempty"`
	ProbeReason  string     `json:"probe_reason,omitempty"`
	RTTMs        int64      `json:"rtt_ms,omitempty"`
	Error        string     `json:"error,omitempty"`
	AgeMs        int64      `json:"age_ms,omitempty"`
	QuarantineMs int64      `json:"quarantine_ms,omitempty"`
}

type probeRequestView struct {
	Run       string          `json:"run"`
	IsUDP     bool            `json:"is_udp"`
	ID        uint64          `json:"id"`
	Balancer  string          `json:"balancer"`
	AgeMs     int64           `json:"age_ms"`
	IsDone    bool            `json:"is_done"`
	End       string          `json:"end,omitempty"`
	Outbounds []probeItemView `json:"outbounds"`
}

func (r *probeRequest) view(now time.Time) probeRequestView {
	v := probeRequestView{IsUDP: r.isUDP, ID: r.id, Balancer: r.balancer, AgeMs: now.Sub(r.started).Milliseconds(), IsDone: !r.open(),
		End: r.end, Outbounds: make([]probeItemView, len(r.items))}
	for i, it := range r.items {
		iv := probeItemView{UDP: it.udp, Outbound: it.tag, Result: it.result, Source: it.source, Error: it.err,
			AgeMs: it.age.Milliseconds(), QuarantineMs: it.rest.Milliseconds()}
		if it.source == "probe" && !r.isUDP {
			iv.RTTMs = it.rtt.Milliseconds()
			if it.reason != reasonHost {
				iv.ProbeReason = it.reason.String()
			}
		}
		v.Outbounds[i] = iv
	}
	return v
}

// RequestProbe asks the observer to probe outbounds of a leastCost balancer
// now, within its probe limits, and returns at once. A request for a
// balancer and probe kind with a request still open joins it and gets its id. The results
// appear in Status under probe_requests and in ProbeResult; the host tells
// its request by the id and the run of the ticket. A probe the host asks for
// with TCP is evidence like any other: a failed one may move the balancer away.
// UDP results only report measurements.
func (o *Observer) RequestProbe(r ProbeRequest) (ProbeTicket, error) {
	if err := o.FeatureError("probe_requests"); err != nil {
		return ProbeTicket{}, err
	}
	if r.IsUDP {
		if err := o.FeatureError("udp_probe"); err != nil {
			return ProbeTicket{}, err
		}
	}
	var id uint64
	var tags []string
	var err error
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		id, tags, err = o.e.requestProbeKind(r.Balancer, r.Outbounds, r.IsUDP, o.now())

	}()
	if err != nil {
		return ProbeTicket{}, err
	}
	o.wake()
	return ProbeTicket{ID: id, Run: o.run, Outbounds: tags}, nil
}

// ProbeResult returns the request with the id as JSON, as Status shows it,
// and whether it has ended; nil when the observer no longer keeps it.
func (o *Observer) ProbeResult(run string, id uint64) ([]byte, bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if run == "" || run != o.run {
		return nil, false, NewCommandError("stale_run", "probe ticket belongs to another run")
	}
	if !o.s.features.ProbeRequests {
		return nil, false, NewCommandError("feature_disabled", "probe_requests is disabled")
	}
	for _, r := range o.e.requests {
		if r.id == id {
			v := r.view(o.now())
			v.Run = o.run
			out, _ := json.Marshal(v)
			return out, v.IsDone, nil
		}
	}
	return nil, false, NewCommandError("not_found", "probe ticket is no longer available")
}

func (e *engine) serveUDP(s *selector, r *probeRequest, now time.Time) bool {
	for i := range r.items {
		it := &r.items[i]
		if it.result != probePending {
			continue
		}
		if !s.offers(it.tag) {
			it.result = probeGone
			continue
		}
		p := e.path(it.tag)
		switch {
		case e.isProbeOnly(it.tag):
			it.result, it.source = probeSkipped, "probe_only"
		case e.s.udpProbe == nil:
			it.result, it.source = probeSkipped, "disabled"
		case e.retiring[it.tag] || e.replacing[it.tag]:
			it.result, it.source = probeGone, "gone"
		case e.memoryLow:
			it.result, it.source = probeSkipped, "memory"
		case e.networkDown:
			it.result, it.source = probeSkipped, "network"
		case e.idle(now):
			it.result, it.source = probeSkipped, "idle"
		case p.asleep || p.sleeping:
			it.result, it.source = probeSkipped, "asleep"
		case p.status == statusDown || s.groupDown(it.tag):
			it.result, it.source = probeDown, "down"
		case !p.udp.at.IsZero() && now.Sub(p.udp.at) < e.s.schedule.hostProbes.reuse:
			it.source, it.age = "recent", now.Sub(p.udp.at)
			v := p.statusUDP(now)
			it.udp = &v
			it.result = p.udp.state
		default:
			if j := e.enqueueUDP(it.tag, reasonHost, r.balancer, now); j != nil {
				j.request = r.id
			}
		}
		if it.result == probeSkipped || it.source == "recent" {
			e.udpSkip(p, it.source, now)
		}
	}
	return r.answered()
}

func (e *engine) answerUDPRequests(j *probeJob, res udpProbeResult, now time.Time) {
	for _, r := range e.requests {
		if !r.open() || !r.isUDP || r.udpEpoch != j.udpEpoch || r.generation != j.generation {
			continue
		}
		for i := range r.items {
			it := &r.items[i]
			if it.tag != j.tag || it.result != probePending {
				continue
			}
			if res.canceled {
				it.result, it.source = probeExpired, "canceled"
				continue
			}
			it.result, it.source, it.reason = res.state, "probe", j.reason
			it.udp = &UDPStatus{State: res.state, Answered: res.answered, Sent: res.sent, RTTMs: res.rtt.Milliseconds(), JitterMs: res.jitter.Milliseconds(), Reason: j.reason.String()}
			it.err = res.class
		}
		if r.answered() {
			e.closeRequest(r, "done", now)
		}
	}
	e.keepRequests()
}

func (e *engine) expireUDPRequests(tag, source string, now time.Time) {
	for _, r := range e.requests {
		if !r.open() || !r.isUDP {
			continue
		}
		for i := range r.items {
			it := &r.items[i]
			if it.result == probePending && (tag == "" || tag == it.tag) {
				it.result, it.source = probeExpired, source
			}
		}
		if r.answered() {
			e.closeRequest(r, source, now)
		}
	}
	e.keepRequests()
}
