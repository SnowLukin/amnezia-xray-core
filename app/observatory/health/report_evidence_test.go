package health

import (
	"testing"
	"time"
)

// The kinds of a suspicion are counted by how it ended: down, proven false
// before, or rescued after a down.
func TestReportEvidenceByOutcome(t *testing.T) {
	te := newReportEngine(t, &Config{}, nil, "a1", "b1")
	kinds := map[string]int{"no_response": 2, "dial_timeout": 1}
	suspect := func(tag string, at time.Time) {
		te.emit(at, "path_state", map[string]any{"outbound": tag, "state": "suspect", "reason": "traffic", "kinds": kinds})
	}
	suspect("a1", at(time.Second))
	te.emit(at(2*time.Second), "path_state", map[string]any{"outbound": "a1", "state": "down", "reason": "traffic"})
	te.emit(at(3*time.Second), "path_state", map[string]any{"outbound": "a1", "state": "up", "reason": "rescued", "by": "audit"})
	suspect("b1", at(4*time.Second))
	te.emit(at(5*time.Second), "path_state", map[string]any{"outbound": "b1", "state": "up", "reason": "traffic"})
	te.emit(at(5*time.Second), "panic_recovered", map[string]any{"scope": "connection", "where": "test"})
	v := reportJSON(t, te.takeReport(true, at(6*time.Second)))
	nr, dt := v.Evidence["no_response"], v.Evidence["dial_timeout"]
	if nr == nil || dt == nil || nr.Suspected != 2 || nr.Confirmed != 1 || nr.Rescued != 1 || nr.Refuted != 1 || *nr != *dt {
		t.Fatalf("evidence %+v %+v", nr, dt)
	}
}

// A tag the provider removed while down and later brought back starts clean:
// its first up is no rescue of the old down.
func TestReportEvidenceForgetsRemovedPath(t *testing.T) {
	te := newReportEngine(t, &Config{}, nil, "a1", "b1")
	te.emit(at(time.Second), "path_state", map[string]any{"outbound": "a1", "state": "suspect", "reason": "traffic", "kinds": map[string]int{"no_response": 2}})
	te.emit(at(2*time.Second), "path_state", map[string]any{"outbound": "a1", "state": "down", "reason": "traffic"})
	te.forget("a1", at(3*time.Second))
	te.emit(at(4*time.Second), "path_state", map[string]any{"outbound": "a1", "state": "up", "reason": "rescued", "by": "audit"})
	te.emit(at(4*time.Second), "panic_recovered", map[string]any{"scope": "connection", "where": "test"})
	if e := reportJSON(t, te.takeReport(true, at(5*time.Second))).Evidence["no_response"]; e == nil || e.Rescued != 0 {
		t.Fatalf("evidence %+v", e)
	}
}

// The engine counts every user failure by kind, and the report tells the
// connections of the last minute before it.
func TestReportEvidenceFailuresAndRecent(t *testing.T) {
	te := newReportEngine(t, &Config{}, nil, "a1", "b1")
	te.run(t, at(0), nil)
	te.onSuccess("a1", at(time.Second))
	te.failN(3, "a1", failDialTimeout, at(30*time.Second))
	te.run(t, at(31*time.Second), nil)
	v := reportJSON(t, te.takeReport(true, at(40*time.Second)))
	e := v.Evidence["dial_timeout"]
	if e == nil || e.Failures != 3 || e.Suspected != 1 || e.Confirmed != 1 {
		t.Fatalf("evidence %+v", v.Evidence)
	}
	r := v.Recent
	if r == nil || r.OK != 1 || r.Failed != 3 || *r.LastSuccessAgoMs != 39000 || *r.LastFailureAgoMs != 10000 {
		t.Fatalf("recent %+v", r)
	}
}
