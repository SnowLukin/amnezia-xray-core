package health

import (
	"testing"
	"time"
)

// A probe the engine no longer wants ends at once: with maxConcurrent 1 it
// would hold the only slot until its own timeout, and the probes of the new
// state would wait for it.
func TestInvalidatedTCPProbeIsCanceled(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reason     probeReason
		invalidate func(e *engine, now time.Time)
	}{
		{"outbound replaced", reasonRecheck, func(e *engine, now time.Time) { e.invalidateProbeJobs("a1", now) }},
		{"network change", reasonRecheck, func(e *engine, now time.Time) { e.onNetworkChange("wifi-2", now) }},
		{"outbound gone", reasonRecheck, func(e *engine, now time.Time) { e.forget("a1", now) }},
		{"lone failure answered", reasonLone, func(e *engine, now time.Time) { e.cancelLoneFailure("a1") }},
		{"wake answered", reasonWake, func(e *engine, now time.Time) { e.cancelWakeProbe("a1") }},
		{"candidates canceled", reasonCandidate, func(e *engine, now time.Time) { e.cancelCandidates("") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := newTestEngine(t, nil, nil, "a1", "b1")
			canceled := false
			j := &probeJob{id: 1, tag: "a1", reason: tc.reason, selector: "auto", generation: te.generation, cancel: func() { canceled = true }}
			te.running[j.id] = j
			tc.invalidate(te.engine, t0.Add(time.Second))
			if !j.invalidated {
				t.Fatal("job not invalidated")
			}
			if !canceled {
				t.Fatal("invalidated probe keeps running until its timeout")
			}
		})
	}
}
