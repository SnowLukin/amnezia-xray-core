package health

import (
	"github.com/xtls/xray-core/common/safe"
	"maps"
	"time"
)

// guard is a boundary of this observer's runtime workers, never of startup.
func (o *Observer) guard(where string, fn func()) {
	if o.failed.Load() {
		return
	}
	defer safe.Recover(safe.Subsystem, "app/observatory/health/"+where, o.fail)
	fn()
}

// fail does not take engine locks: the panicking worker may be unwinding one.
func (o *Observer) fail() {
	o.failed.Store(true)
	o.stopRun()
	o.wake()
}

// locked runs fn under the observer lock for a connection or the router. A
// panic there may leave the engine half changed: like one of a worker, it
// freezes the observer, and the caller goes on. where is a constant: a
// string built here would cost every connection an allocation.
func (o *Observer) locked(where string, fn func()) {
	defer safe.Recover(safe.Subsystem, where, o.fail)
	o.mu.Lock()
	defer o.mu.Unlock()
	fn()
}

// collectPanicsLocked runs on a healthy caller of Status/report or the loop.
// Recovery itself must not reenter a lock held by the panicking worker.
func (o *Observer) collectPanicsLocked() {
	count, events, _ := safe.Snapshot(o.panicCursor)
	delta := uint64(0)
	if count >= o.panicCursor {
		delta = count - o.panicCursor
	}
	if o.s.features.Report {
		if delta > uint64(len(events)) {
			o.e.report.counts.PanicsRecovered += delta - uint64(len(events))
		}
	}
	for _, failure := range events {
		o.e.emit(o.now(), "panic_recovered", map[string]any{"scope": failure.Scope, "where": failure.Where})
	}
	if delta > 0 && o.s.features.Report {
		o.e.report.decide()
	}
	o.panicCursor = count
}

// rememberSelector runs under the observer lock before preparing a provider
// replacement. Rollback restores controller metadata together with the atomic
// choice, without copying atomic objects or undoing concurrent traffic counters.
func rememberSelector(s *selector) func() {
	state := struct {
		active, pinned, returnTarget, returnedTo, held                                              string
		activeSince, pinnedUntil, fallbackSince, returnAt, returnedAt, heldUntil                    time.Time
		started, startPin, exhausted, fallingBack, probeOnlyActive, recheckOnRelease, penaltyServed bool
		returnSuccesses                                                                             int
		failoverTried, failoverProbing                                                              map[string]bool
		penalty                                                                                     map[string]float64
		inventory, candidates                                                                       []string
		snapshot                                                                                    *[]string
	}{
		active: s.current(), pinned: s.pinned, returnTarget: s.returnToCheaperTarget, returnedTo: s.returnedTo, held: s.held, heldUntil: s.heldUntil,
		activeSince: s.activeSince, pinnedUntil: s.pinnedUntil, fallbackSince: s.fallbackSince, returnAt: s.returnToCheaperAt, returnedAt: s.returnedAt,
		started: s.started, startPin: s.startPin, exhausted: s.exhausted, fallingBack: s.fallingBack, probeOnlyActive: s.probeOnlyActive,
		recheckOnRelease: s.recheckOnRelease, penaltyServed: s.penaltyServed, returnSuccesses: s.returnToCheaperSuccesses,
		failoverTried: maps.Clone(s.failoverTried), failoverProbing: maps.Clone(s.failoverProbing), penalty: maps.Clone(s.penalty),
		inventory: s.inventory, candidates: s.candidates, snapshot: s.snapshot.Load(),
	}
	return func() {
		s.setActive(state.active)
		s.pinned, s.startPin, s.pinnedUntil = state.pinned, state.startPin, state.pinnedUntil
		s.activeSince, s.started, s.exhausted = state.activeSince, state.started, state.exhausted
		s.held, s.heldUntil = state.held, state.heldUntil
		s.fallingBack, s.fallbackSince = state.fallingBack, state.fallbackSince
		s.probeOnlyActive, s.recheckOnRelease = state.probeOnlyActive, state.recheckOnRelease
		s.returnToCheaperTarget, s.returnToCheaperAt, s.returnToCheaperSuccesses = state.returnTarget, state.returnAt, state.returnSuccesses
		s.penaltyServed, s.returnedTo, s.returnedAt = state.penaltyServed, state.returnedTo, state.returnedAt
		s.failoverTried, s.failoverProbing, s.penalty = state.failoverTried, state.failoverProbing, state.penalty
		s.inventory, s.candidates = state.inventory, state.candidates
		s.snapshot.Store(state.snapshot)
	}
}
