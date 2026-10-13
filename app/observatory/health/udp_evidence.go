package health

import "time"

const (
	maxUDPEvidence        = 64
	maxUDPEvidencePending = 4096
)

type udpEvidence struct {
	at       time.Time
	target   string
	answered bool
	flow     *udpFlow
}

func (e *engine) resetUDPEvidence(p *path) {
	p.udpEvidence = nil
}

func (o *Observer) currentUDPFlow(f *udpFlow) bool {
	select {
	case <-o.done:
		return false
	default:
	}
	return !o.e.report.final && o.e.paths[f.tag] == f.path &&
		f.epoch == o.e.udpEpoch && f.incarnation == f.path.udpIncarnation &&
		!o.e.retiring[f.tag] && !o.e.replacing[f.tag] && !o.e.isProbeOnly(f.tag)
}

func (o *Observer) udpFirstWrite(f *udpFlow, now time.Time) {
	var current bool
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		current = o.currentUDPFlow(f)
		if current {
			o.pruneUDPEvidenceLocked(f.path, now)
			o.e.onUDPUserWrite(f.path, now)
		}

	}()
	if current {
		if o.s.passive.udpReplyTimeout > 0 {
			o.watchUDPEvidence()
		}
		o.wake()
	}
}

func (o *Observer) udpAnswered(f *udpFlow, now time.Time) {
	if o.s.passive.udpReplyTimeout <= 0 {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.currentUDPFlow(f) {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.bytesDown <= 0 || f.evidenceAnswered {
		return
	}
	o.addUDPEvidenceLocked(f, true, now)
	f.evidenceAnswered = true
	f.evidenceChecked = true
	f.lastEvidenceBytesDown = f.bytesDown
	delete(o.udpEvidencePending, f)
}

// A closed link remains eligible until its original reply deadline. Traffic
// counters still belong to the window in which the link ended.
func (o *Observer) retainUDPEvidenceLocked(f *udpFlow, now time.Time) {
	if o.s.passive.udpReplyTimeout <= 0 || !o.currentUDPFlow(f) {
		return
	}
	o.refreshUDPReplyEvidenceLocked(f, now)
	if f.evidenceAnswered {
		return
	}
	if f.evidenceChecked || f.firstWrite == 0 || f.writes.Load() < 3 {
		return
	}
	deadline := o.base.Add(time.Duration(f.firstWrite)).Add(o.s.passive.udpReplyTimeout)
	if !now.Before(deadline) {
		o.addUDPEvidenceLocked(f, false, deadline)
		f.evidenceChecked = true
		return
	}
	if len(o.udpEvidencePending) >= maxUDPEvidencePending {
		return
	}
	if o.udpEvidencePending == nil {
		o.udpEvidencePending = make(map[*udpFlow]struct{})
	}
	o.udpEvidencePending[f] = struct{}{}
}

func (o *Observer) addUDPEvidenceLocked(f *udpFlow, answered bool, now time.Time) {
	p := f.path
	o.pruneUDPEvidenceLocked(p, now)
	kept := p.udpEvidence[:0]
	for _, old := range p.udpEvidence {
		if old.flow != f && (!answered || !old.answered) {
			kept = append(kept, old)
		}
	}
	clear(p.udpEvidence[len(kept):])
	p.udpEvidence = kept
	if len(p.udpEvidence) == maxUDPEvidence {
		for i, old := range p.udpEvidence {
			if !old.answered {
				p.udpEvidence = append(p.udpEvidence[:i], p.udpEvidence[i+1:]...)
				break
			}
		}
	}
	p.udpEvidence = append(p.udpEvidence, udpEvidence{at: now, target: f.target, answered: answered, flow: f})
}

func (o *Observer) pruneUDPEvidenceLocked(p *path, now time.Time) {
	kept := p.udpEvidence[:0]
	for _, old := range p.udpEvidence {
		if now.Sub(old.at) <= o.s.passive.window {
			kept = append(kept, old)
		}
	}
	clear(p.udpEvidence[len(kept):])
	p.udpEvidence = kept
}

// Runs under Observer.mu, before taking any flow's mutex.
func (o *Observer) sweepUDPEvidenceLocked(now time.Time) bool {
	if o.s.passive.udpReplyTimeout <= 0 || o.e.report.final {
		clear(o.udpEvidencePending)
		return false
	}
	pending := false
	for f := range o.udpFlows {
		pending = o.checkUDPEvidenceLocked(f, now) || pending
	}
	for f := range o.udpEvidencePending {
		if !o.checkUDPEvidenceLocked(f, now) {
			delete(o.udpEvidencePending, f)
		} else {
			pending = true
		}
	}
	for _, p := range o.e.paths {
		o.pruneUDPEvidenceLocked(p, now)
		if p.lastTCPSuccess.IsZero() || now.Sub(p.lastTCPSuccess) > o.s.passive.window {
			continue
		}
		flows := 0
		targets := make(map[string]struct{})
		answered := false
		for _, evidence := range p.udpEvidence {
			if evidence.answered {
				answered = true
				break
			}
			flows++
			if evidence.target != "" {
				targets[evidence.target] = struct{}{}
			}
		}
		if !answered && flows >= 3 && len(targets) >= 2 {
			if o.e.onUDPSuspect(p, flows, len(targets), now) {
				o.wake()
			}
		}
	}
	return pending
}

func (o *Observer) checkUDPEvidenceLocked(f *udpFlow, now time.Time) bool {
	if !o.currentUDPFlow(f) {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	o.refreshUDPReplyEvidenceLocked(f, now)
	if f.evidenceChecked || f.firstWrite == 0 {
		return false
	}
	deadline := o.base.Add(time.Duration(f.firstWrite)).Add(o.s.passive.udpReplyTimeout)
	if now.Before(deadline) {
		return true
	}
	f.evidenceChecked = true
	if f.writes.Load() >= 3 && f.bytesDown == 0 {
		o.addUDPEvidenceLocked(f, false, deadline)
	}
	return false
}

func (o *Observer) refreshUDPReplyEvidenceLocked(f *udpFlow, now time.Time) {
	if f.bytesDown <= f.lastEvidenceBytesDown {
		return
	}
	at := now
	if !f.evidenceAnswered {
		at = o.base.Add(time.Duration(f.firstRead))
	}
	o.addUDPEvidenceLocked(f, true, at)
	f.lastEvidenceBytesDown = f.bytesDown
	f.evidenceChecked = true
	f.evidenceAnswered = true
}
