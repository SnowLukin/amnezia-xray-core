package health

import (
	"strings"
	"testing"
	"time"
)

func incidentsOf(t *testing.T, v *reportView) []reportIncident {
	t.Helper()
	return reportJSON(t, v).Incidents
}

func ms(v *int64) int64 {
	if v == nil {
		return -1
	}
	return *v
}

// One outage told by the events as the engine emits them: every duration is
// measured between its own events, the start ends with its first success.
func TestReportIncidentDurations(t *testing.T) {
	te := newReportEngine(t, &Config{}, nil, "a1", "b1")
	te.emit(at(0), "switch", map[string]any{"balancer": "auto", "from": "", "to": "a1", "reason": "start"})
	te.emit(at(700*time.Millisecond), "first_success", map[string]any{"balancer": "auto", "outbound": "a1", "after": "start"})
	te.emit(at(10*time.Second), "path_state", map[string]any{"outbound": "a1", "state": "suspect", "reason": "traffic"})
	te.emit(at(14*time.Second), "path_state", map[string]any{"outbound": "a1", "state": "down", "reason": "traffic"})
	te.emit(at(15*time.Second), "switch", map[string]any{"balancer": "auto", "from": "a1", "to": "b1", "reason": "failover"})
	te.emit(at(15500*time.Millisecond), "first_success", map[string]any{"balancer": "auto", "outbound": "b1", "after": "failover"})
	v := reportJSON(t, te.takeReport(true, at(20*time.Second)))
	if ms(v.StartMs) != 700 {
		t.Fatalf("start_ms %d", ms(v.StartMs))
	}
	if len(v.Incidents) != 1 {
		t.Fatalf("incidents %+v", v.Incidents)
	}
	in := v.Incidents[0]
	if in.Kind != "failover" || in.From != "a1" || in.To != "b1" || in.Outcome != "recovered" || in.DetectMs != 4000 ||
		ms(in.SwitchMs) != 1000 || ms(in.RecoverMs) != 500 || in.TotalMs != 5500 || in.BackendMs != nil {
		t.Fatalf("incident %+v switch %d recover %d", in, ms(in.SwitchMs), ms(in.RecoverMs))
	}
}

// A suspicion the traffic proves false is no outage, and a path that is not
// the chosen one opens none.
func TestReportIncidentFalseAlarmAndOtherPath(t *testing.T) {
	te := newReportEngine(t, &Config{}, nil, "a1", "b1")
	te.emit(at(0), "switch", map[string]any{"balancer": "auto", "from": "", "to": "a1", "reason": "start"})
	te.emit(at(time.Second), "path_state", map[string]any{"outbound": "a1", "state": "suspect", "reason": "traffic"})
	te.emit(at(2*time.Second), "path_state", map[string]any{"outbound": "a1", "state": "up", "reason": "traffic"})
	te.emit(at(3*time.Second), "path_state", map[string]any{"outbound": "b1", "state": "down", "reason": "probe"})
	// A decision, so that the final report is due.
	te.emit(at(3*time.Second), "panic_recovered", map[string]any{"scope": "connection", "where": "test"})
	if got := incidentsOf(t, te.takeReport(true, at(4*time.Second))); len(got) != 0 {
		t.Fatalf("incidents %+v", got)
	}
}

// An exhausted balancer that the backend gave a new outbound: the refresh
// counts from its first attempt to the set applied.
func TestReportIncidentExhaustedWithBackend(t *testing.T) {
	te := newReportEngine(t, &Config{}, nil, "a1", "b1")
	te.emit(at(0), "switch", map[string]any{"balancer": "auto", "from": "", "to": "a1", "reason": "start"})
	te.emit(at(10*time.Second), "path_state", map[string]any{"outbound": "a1", "state": "down", "reason": "traffic"})
	te.emit(at(10*time.Second), "exhausted", map[string]any{"balancer": "auto", "from": "a1"})
	te.emit(at(10*time.Second), "provider_fetch", map[string]any{"provider": "p", "attempt": 1, "is_ok": false, "error": "balancer exhausted"})
	te.emit(at(11500*time.Millisecond), "provider_fetch", map[string]any{"provider": "p", "attempt": 2, "is_ok": true, "ms": int64(1500)})
	te.emit(at(11600*time.Millisecond), "provider_apply", map[string]any{"provider": "p", "is_ok": true, "is_changed": true})
	te.emit(at(11700*time.Millisecond), "switch", map[string]any{"balancer": "auto", "from": "", "to": "p-s2", "reason": "candidates_changed"})
	te.emit(at(12*time.Second), "first_success", map[string]any{"balancer": "auto", "outbound": "p-s2", "after": "candidates_changed"})
	got := incidentsOf(t, te.takeReport(true, at(13*time.Second)))
	if len(got) != 1 {
		t.Fatalf("incidents %+v", got)
	}
	in := got[0]
	if in.Kind != "exhausted" || in.To != "p-s2" || in.DetectMs != 0 || ms(in.SwitchMs) != 1700 || ms(in.RecoverMs) != 300 ||
		in.TotalMs != 2000 || ms(in.BackendMs) != 1600 {
		t.Fatalf("incident %+v switch %d recover %d backend %d", in, ms(in.SwitchMs), ms(in.RecoverMs), ms(in.BackendMs))
	}
}

// The stop cuts an outage short: the final report tells it open; an interval
// report leaves it for the window where it ends.
func TestReportIncidentOpenAtStop(t *testing.T) {
	te := newReportEngine(t, &Config{}, nil, "a1", "b1")
	te.emit(at(0), "switch", map[string]any{"balancer": "auto", "from": "", "to": "a1", "reason": "start"})
	te.emit(at(time.Second), "path_state", map[string]any{"outbound": "a1", "state": "down", "reason": "traffic"})
	te.emit(at(time.Second), "exhausted", map[string]any{"balancer": "auto", "from": "a1"})
	if got := reportJSON(t, te.snapshotReport(at(2*time.Second))).Incidents; len(got) != 0 {
		t.Fatalf("snapshot told an open outage: %+v", got)
	}
	got := incidentsOf(t, te.takeReport(true, at(5*time.Second)))
	if len(got) != 1 || got[0].Outcome != "open" || got[0].Kind != "exhausted" || got[0].TotalMs != 4000 || got[0].SwitchMs != nil {
		t.Fatalf("incidents %+v", got)
	}
}

// At most ten outages a window; the rest only count.
func TestReportIncidentsBounded(t *testing.T) {
	te := newReportEngine(t, &Config{}, nil, "a1", "b1")
	te.emit(at(0), "switch", map[string]any{"balancer": "auto", "from": "", "to": "a1", "reason": "start"})
	from, to := "a1", "b1"
	for i := 0; i < 12; i++ {
		base := at(time.Duration(i+1) * time.Minute)
		te.emit(base, "path_state", map[string]any{"outbound": from, "state": "down", "reason": "traffic"})
		te.emit(base.Add(time.Second), "switch", map[string]any{"balancer": "auto", "from": from, "to": to, "reason": "failover"})
		te.emit(base.Add(2*time.Second), "first_success", map[string]any{"balancer": "auto", "outbound": to, "after": "failover"})
		from, to = to, from
	}
	v := reportJSON(t, te.takeReport(true, at(time.Hour)))
	if len(v.Incidents) != reportIncidents || v.IncidentsDropped != 2 {
		t.Fatalf("incidents %d dropped %d", len(v.Incidents), v.IncidentsDropped)
	}
}

// The engine itself emits first_success once per choice: the end of an
// outage it then reports.
func TestEngineFirstSuccessEndsOutage(t *testing.T) {
	te := newReportEngine(t, &Config{Schedule: quarantine(30 * time.Second)}, returnToCheaperConfig(), "a1", "b1")
	te.onNetworkChange("home", at(0))
	te.run(t, at(0), nil)
	te.onSuccess("a1", at(time.Second))
	te.onSuccess("a1", at(2*time.Second))
	failoverToB(t, te, at(10*time.Second))
	te.onSuccess("b1", at(15*time.Second))
	te.onSuccess("b1", at(16*time.Second))
	successes := 0
	for _, typ := range te.eventTypes() {
		if typ == "first_success" {
			successes++
		}
	}
	if successes != 2 {
		t.Fatalf("first_success events %d", successes)
	}
	v := reportJSON(t, te.takeReport(true, at(20*time.Second)))
	if v.StartMs == nil || len(v.Incidents) != 1 || v.Incidents[0].To != "b1" || v.Incidents[0].Outcome != "recovered" {
		t.Fatalf("start %v incidents %+v", v.StartMs, v.Incidents)
	}
}

// The users waited from the start of the first connection that never got an
// answer, not from the moment the core saw it fail.
func TestReportIncidentCountsFromUnansweredConnection(t *testing.T) {
	te := newReportEngine(t, &Config{}, nil, "a1", "b1")
	te.emit(at(0), "switch", map[string]any{"balancer": "auto", "from": "", "to": "a1", "reason": "start"})
	te.emit(at(17*time.Second), "path_state", map[string]any{"outbound": "a1", "state": "suspect", "reason": "traffic", "evidence_ms": int64(7000)})
	te.emit(at(17*time.Second), "path_state", map[string]any{"outbound": "a1", "state": "down", "reason": "traffic", "evidence_ms": int64(7500)})
	te.emit(at(17*time.Second), "switch", map[string]any{"balancer": "auto", "from": "a1", "to": "b1", "reason": "failover"})
	te.emit(at(18*time.Second), "first_success", map[string]any{"balancer": "auto", "outbound": "b1", "after": "failover"})
	got := incidentsOf(t, te.takeReport(true, at(20*time.Second)))
	if len(got) != 1 || got[0].TotalMs != 8500 || got[0].DetectMs != 7500 {
		t.Fatalf("incidents %+v", got)
	}
}

// The engine tells when the first unanswered connection started and forgets
// it once the outbound works.
func TestPathStateTellsEvidenceStart(t *testing.T) {
	te := newReportEngine(t, &Config{}, nil, "a1", "b1")
	te.run(t, at(0), nil)
	te.noteFailStart("a1", at(2*time.Second))
	te.failN(3, "a1", failDial, at(9*time.Second))
	raw, _ := te.events.drain()
	if !strings.Contains(string(raw), `"evidence_ms":7000`) {
		t.Fatalf("no evidence start in %s", raw)
	}
	te.onSuccess("a1", at(10*time.Second))
	if !te.paths["a1"].failStart.IsZero() {
		t.Fatal("evidence start survived a success")
	}
}
