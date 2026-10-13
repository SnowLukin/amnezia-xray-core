package health

import (
	"context"
	"slices"
	"time"
)

const (
	udpSwitchDelay     = 20 * time.Second
	udpSuspectCooldown = 10 * time.Minute
)

type udpSnapshot struct {
	state    string
	answered int
	sent     int
	rtt      time.Duration
	jitter   time.Duration
	at       time.Time
	reason   probeReason
}

func (e *engine) noteUDPSwitch(p *path) {
	if !p.udpSwitchDone {
		p.udpSwitchPending = true
	}
}

func (e *engine) onUDPUserWrite(p *path, now time.Time) bool {
	if e.s.udpProbe == nil || e.paths[p.tag] != p || !e.active(p.tag) || e.isProbeOnly(p.tag) || p.udpSwitchDone {
		return false
	}
	if !p.udpSwitchPending {
		e.noteUDPSwitch(p)
	}
	if !p.udpSwitchDue.IsZero() {
		return false
	}
	p.udpSwitchDue = now.Add(udpSwitchDelay)
	return true
}

func (e *engine) scheduleUDP(now time.Time) time.Time {
	if e.s.udpProbe == nil {
		return time.Time{}
	}
	var next time.Time
	for _, p := range e.paths {
		if !p.udpSwitchPending || p.udpSwitchDone || p.udpSwitchDue.IsZero() || !e.active(p.tag) {
			continue
		}
		if e.idle(now) || e.networkDown || e.memoryLow || p.asleep || p.sleeping || p.status == statusDown || e.retiring[p.tag] || e.replacing[p.tag] {
			j := &probeJob{tag: p.tag, isUDP: true, reason: reasonUDPSwitch}
			e.udpSkip(p, e.udpBlocked(j, e.idle(now)), now)
			continue
		}
		if now.Before(p.udpSwitchDue) {
			next = soonestOf(next, p.udpSwitchDue)
			continue
		}
		e.enqueueUDP(p.tag, reasonUDPSwitch, "", now)
	}
	return next
}

func (e *engine) onUDPSuspect(p *path, flows, targets int, now time.Time) bool {
	if e.paths[p.tag] != p || !e.active(p.tag) || e.isProbeOnly(p.tag) || e.networkDown || e.retiring[p.tag] || e.replacing[p.tag] ||
		(!p.udpSuspectAt.IsZero() && now.Sub(p.udpSuspectAt) < udpSuspectCooldown) {
		return false
	}
	p.udpSuspectAt = now
	e.emit(now, "udp_suspect", map[string]any{"outbound": p.tag, "flows": flows, "targets": targets, "window_ms": e.s.passive.window.Milliseconds()})
	if e.s.udpProbe != nil {
		e.enqueueUDP(p.tag, reasonUDPSuspect, "", now)
	}
	return true
}

func (e *engine) enqueueUDP(tag string, reason probeReason, selector string, at time.Time) *probeJob {
	if e.s.udpProbe == nil || e.isProbeOnly(tag) {
		return nil
	}
	for _, j := range e.queue {
		if j.isUDP && j.tag == tag && e.udpJobCurrent(j) {
			return nil
		}
	}
	for _, j := range e.running {
		if j.isUDP && j.tag == tag && e.udpJobCurrent(j) {
			return nil
		}
	}
	p := e.pathAt(tag, at)
	e.nextID++
	j := &probeJob{id: e.nextID, tag: tag, reason: reason, selector: selector, notBefore: at, generation: e.generation,
		isUDP: true, udpEpoch: e.udpEpoch, udpIncarnation: p.udpIncarnation, udpPath: p}
	e.queue = append(e.queue, j)
	return j
}

func (e *engine) udpJobCurrent(j *probeJob) bool {
	return !j.invalidated && !j.forgotten && j.udpEpoch == e.udpEpoch && e.paths[j.tag] == j.udpPath &&
		j.udpPath != nil && j.udpPath.udpIncarnation == j.udpIncarnation && !e.isProbeOnly(j.tag) && !e.retiring[j.tag] && !e.replacing[j.tag]
}

func (e *engine) udpBlocked(j *probeJob, idle bool) string {
	p := e.paths[j.tag]
	switch {
	case e.s.udpProbe == nil:
		return "disabled"
	case e.isProbeOnly(j.tag):
		return "probe_only"
	case e.retiring[j.tag] || e.replacing[j.tag]:
		return "gone"
	case e.networkDown:
		return "network_down"
	case idle:
		return "idle"
	case e.memoryLow:
		return "memory"
	case p == nil:
		return "gone"
	case p.asleep || p.sleeping:
		return "asleep"
	case p.status == statusDown:
		return "down"
	case j.reason != reasonHost && !e.active(j.tag):
		return "inactive"
	}
	return ""
}

func (e *engine) udpSkip(p *path, reason string, now time.Time) {
	switch reason {
	case "per_hour", "asleep", "idle", "memory", "recent":
	default:
		return
	}
	if p == nil || reason == "" || p.udpSkips[reason] {
		return
	}
	if p.udpSkips == nil {
		p.udpSkips = map[string]bool{}
	}
	p.udpSkips[reason] = true
	e.emit(now, "udp_skip", map[string]any{"outbound": p.tag, "reason": reason})
}

func (e *engine) hasTCPWork(queued []*probeJob) bool {
	if queued == nil {
		queued = e.queue
	}
	for _, j := range queued {
		if !j.isUDP && !j.invalidated {
			return true
		}
	}
	for _, j := range e.running {
		if !j.isUDP && !j.invalidated {
			return true
		}
	}
	return false
}

func (e *engine) pruneUDPStarts(now time.Time) {
	cut := 0
	for cut < len(e.udpStarts) && now.Sub(e.udpStarts[cut]) >= time.Hour {
		cut++
	}
	e.udpStarts = e.udpStarts[cut:]
}

func (e *engine) tickUDP(j *probeJob, now time.Time, idle bool, tcpQueue []*probeJob) (bool, time.Time) {
	if !e.udpJobCurrent(j) {
		return false, time.Time{}
	}
	if why := e.udpBlocked(j, idle); why != "" {
		e.udpSkip(j.udpPath, why, now)
		return why == "idle" || why == "network_down" || why == "memory" || why == "asleep" || why == "down", time.Time{}
	}
	if e.hasTCPWork(tcpQueue) {
		return true, time.Time{}
	}
	if now.Before(j.notBefore) {
		return true, j.notBefore
	}
	e.pruneUDPStarts(now)
	if len(e.udpStarts) >= e.s.udpProbe.perHour {
		e.udpSkip(j.udpPath, "per_hour", now)
		return true, e.udpStarts[0].Add(time.Hour)
	}
	if wait := e.budgetWait(j, now); wait != 0 {
		if wait > 0 {
			return true, now.Add(wait)
		}
		return true, time.Time{}
	}
	e.beginUDP(j, now)
	return false, time.Time{}
}

func (e *engine) beginUDP(j *probeJob, now time.Time) {
	e.running[j.id] = j
	e.udpStarts = append(e.udpStarts, now)
	j.udpSpacingUntil = now.Add(e.jitter(e.s.schedule.spacing, e.s.schedule.spacingJitter))
	e.spacedUntil = append(e.spacedUntil, j.udpSpacingUntil)
	e.starts = append(e.starts, now)
	e.hostStarts = append(e.hostStarts, now)
	name := e.serverName(j.tag)
	e.startsByName[name] = append(e.startsByName[name], now)
	if e.udpStartsByName == nil {
		e.udpStartsByName = make(map[string][]time.Time)
	}
	e.udpStartsByName[name] = append(pruneTimes(e.udpStartsByName[name], now), now)
	p := j.udpPath
	if j.reason == reasonUDPSwitch {
		p.udpSwitchDone, p.udpSwitchPending = true, false
		p.udpSwitchDue = time.Time{}
	}
	clear(p.udpSkips)
	e.used(j.tag, now)
}

func (e *engine) tcpHostShareStarts(name string, starts []time.Time, now time.Time) []time.Time {
	udpStarts := pruneTimes(e.udpStartsByName[name], now)
	if len(udpStarts) == 0 {
		delete(e.udpStartsByName, name)
		return starts
	}
	e.udpStartsByName[name] = udpStarts
	tcpStarts := slices.Clone(starts)
	// UDP keeps the full SNI quota, but does not consume the TCP host share.
	for _, started := range udpStarts {
		if index := slices.Index(tcpStarts, started); index >= 0 {
			tcpStarts = slices.Delete(tcpStarts, index, index+1)
		}
	}
	return tcpStarts
}

func (e *engine) preemptRunningUDP() {
	for _, j := range e.running {
		if !j.isUDP {
			continue
		}
		alreadyInvalidated := j.invalidated
		j.invalidated = true
		// A started UDP probe keeps its quotas but cannot delay ready TCP work.
		if index := slices.Index(e.spacedUntil, j.udpSpacingUntil); !j.udpSpacingUntil.IsZero() && index >= 0 {
			e.spacedUntil = slices.Delete(e.spacedUntil, index, index+1)
		}
		j.udpSpacingUntil = time.Time{}
		if !alreadyInvalidated && j.cancel != nil {
			j.cancel()
		}
	}
}

func (e *engine) onUDPProbeResult(j *probeJob, r udpProbeResult, now time.Time) {
	if e.running[j.id] != j {
		return
	}
	delete(e.running, j.id)
	if r.canceled || !e.udpJobCurrent(j) {
		return
	}
	p := j.udpPath
	p.udp = udpSnapshot{state: r.state, answered: r.answered, sent: r.sent, rtt: r.rtt, jitter: r.jitter, at: now, reason: j.reason}
	p.stats.udpProbes++
	if r.state == "dead" {
		p.stats.udpProbesDead++
	}
	if r.rtt > 0 {
		p.stats.udpRTT.add(r.rtt)
	}
	if r.answered > 1 {
		p.stats.udpJitter.add(r.jitter)
	}
	clear(p.udpSkips)
	e.used(j.tag, now)
	e.emit(now, "udp_probe", map[string]any{"outbound": j.tag, "reason": j.reason.String(), "state": r.state,
		"answered": r.answered, "sent": r.sent, "rtt_ms": r.rtt.Milliseconds(), "jitter_ms": r.jitter.Milliseconds(),
		"destination_index": r.destinationIndex, "class": r.class})
	e.answerUDPRequests(j, r, now)
}

func (e *engine) cancelUDPJobs(tag string) {
	e.queue = slices.DeleteFunc(e.queue, func(j *probeJob) bool { return j.isUDP && (tag == "" || j.tag == tag) })
	for _, j := range e.running {
		if j.isUDP && (tag == "" || j.tag == tag) {
			j.invalidated = true
			if j.cancel != nil {
				j.cancel()
			}
		}
	}
}

func (e *engine) resetUDPPath(p *path) {
	p.udp = udpSnapshot{}
	p.udpSwitchPending, p.udpSwitchDone = e.active(p.tag), false
	p.udpSwitchDue, p.udpSuspectAt = time.Time{}, time.Time{}
	p.lastTCPSuccess = time.Time{}
	clear(p.udpSkips)
	e.resetUDPEvidence(p)
}

func (e *engine) invalidateUDP(tag string, now time.Time) {
	e.cancelUDPJobs(tag)
	e.expireUDPRequests(tag, "replaced", now)
	if p := e.paths[tag]; p != nil {
		p.udpIncarnation++
		e.resetUDPPath(p)
	}
}

func (e *engine) resetUDPNetwork(now time.Time) {
	e.udpEpoch++
	e.cancelUDPJobs("")
	e.expireUDPRequests("", "network_change", now)
	for _, p := range e.paths {
		e.resetUDPPath(p)
	}
}

type probeCredentialKey struct{}
type probeCredential struct {
	job         *probeJob
	path        *path
	incarnation uint64
}

func (o *Observer) probeJobContext(parent context.Context, j *probeJob) context.Context {
	if !j.isProbeOnly || (j.reason != reasonCandidate && j.reason != reasonHost) {
		return parent
	}
	o.mu.Lock()
	p := o.e.paths[j.tag]
	credential := &probeCredential{job: j, path: p}
	if p != nil {
		credential.incarnation = p.udpIncarnation
	}
	o.mu.Unlock()
	return context.WithValue(parent, probeCredentialKey{}, credential)
}

func (o *Observer) firstUserWrite(t *connTracker) {
	// Without UDP probes there is nothing to schedule: the first write of a
	// connection then does not take the observer's lock.
	if o.s.udpProbe == nil {
		return
	}
	o.mu.Lock()
	p := t.udpPath
	changed := p != nil && t.udpEpoch == o.e.udpEpoch && o.e.paths[t.tag] == p && p.udpIncarnation == t.udpIncarnation &&
		o.e.onUDPUserWrite(p, o.now())
	o.mu.Unlock()
	if changed {
		o.wake()
	}
}

func (o *Observer) watchUDPEvidence() {
	if o.s.passive.sweepInterval <= 0 {
		return
	}
	o.trackMu.Lock()
	start := !o.sweeping
	o.sweeping = true
	done := o.done
	o.trackMu.Unlock()
	if start {
		go o.sweepLoop(done)
	}
}
