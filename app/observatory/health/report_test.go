package health

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func newReportEngine(t *testing.T, cfg *Config, sc *SelectorConfig, candidates ...string) *testEngine {
	t.Helper()
	if sc == nil {
		sc = &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}
	}
	te := newTestEngine(t, cfg, sc, candidates...)
	te.report.begin(t0)
	return te
}

// foldReport adds up drained events by the rules hosts followed before the
// core kept the report: the report has to tell the same.
func foldReport(t *testing.T, raw []byte) (reportCounts, bool) {
	t.Helper()
	var events []Event
	if err := json.Unmarshal(raw, &events); err != nil {
		t.Fatal(err)
	}
	c := reportCounts{SwitchReasons: map[string]int64{}, SwitchHistory: []ReportSwitch{}, ProviderTriggers: map[string]int64{}}
	decided := false
	num := func(v any) int64 { f, _ := v.(float64); return int64(f) }
	for _, ev := range events {
		f := ev.Fields
		c.Events++
		switch ev.Type {
		case "panic_recovered":
			c.PanicsRecovered++
			decided = true
		case "switch":
			reason, _ := f["reason"].(string)
			c.Switches++
			c.SwitchReasons[reason]++
			c.SwitchHistory = append(c.SwitchHistory, ReportSwitch{From: fmt.Sprint(f["from"]), To: fmt.Sprint(f["to"]), Reason: reason})
			if len(c.SwitchHistory) > reportSwitches {
				c.SwitchHistory = c.SwitchHistory[1:]
			}
			decided = decided || !slices.Contains([]string{"start", "start_pinned", "start_remembered", "start_with"}, reason)
		case "fallback_balancer":
			if f["is_active"] == true {
				c.FallbackBalancerCount++
			}
			decided = true
		case "exhausted":
			c.Exhausted++
			decided = true
		case "group_state":
			if f["state"] == "down" {
				c.GroupDown++
			}
		case "path_state":
			switch f["state"] {
			case "down":
				c.PathDown++
			case "suspect":
				c.PathSuspect++
			case "degraded":
				c.PathDegraded++
			case "up":
				if f["reason"] == "rescued" {
					c.PathRescued++
					if f["by"] == "audit" {
						c.AuditPassed++
					}
				}
			}
		case "probe_only_promoted":
			c.ProbeOnlyPromoted++
		case "fresh_path_down":
			c.FreshPathDown++
		case "pending_closed":
			c.PendingClosed += num(f["connections"])
		case "idle_closed":
			c.IdleClosed += num(f["connections"])
		case "udp_probe":
			c.UDPProbeCount++
			switch f["state"] {
			case "ok":
				c.UDPProbeOKCount++
			case "lossy":
				c.UDPProbeLossyCount++
			case "dead":
				c.UDPProbeDeadCount++
				if f["reason"] == "udp_suspect" {
					c.UDPSuspectConfirmedCount++
				}
			}
			if num(f["destination_index"]) > 0 {
				c.UDPProbeSecondDestinationCount++
			}
		case "udp_suspect":
			c.UDPSuspectCount++
		case "probe_result":
			c.Probes++
			if f["is_ok"] != true {
				c.ProbesFailed++
				if f["reason"] == "candidate" {
					c.CandidateProbesFailed++
				}
				if f["reason"] == "wake" {
					c.WakeProbeFailed++
				}
				if f["reason"] == "lone" {
					c.LoneProbeFailed++
				}
			}
			c.ProbeBytes += num(f["bytes"])
			if f["reason"] == "host" {
				c.HostProbes++
			}
		case "probe_start":
			switch f["reason"] {
			case "candidate":
				c.CandidateProbes++
			case "audit":
				c.AuditCount++
			case "wake":
				c.WakeProbeCount++
			case "lone":
				c.LoneProbeCount++
			}
		case "probe_request":
			c.HostProbeRequests++
		case "return_start":
			c.ReturnToCheaperAttempts++
		case "return_failed":
			c.ReturnToCheaperFailures++
			decided = true
		case "pinned_down":
			c.PinnedDown++
			decided = decided || f["is_start_pinned"] != true
		case "network_change":
			c.NetworkChanges++
			if f["is_known"] == true {
				c.NetworkKnown++
			}
		case "network_down":
			c.NetworkDown++
		case "provider_fetch":
			c.ProviderFetches++
			if f["is_ok"] != true {
				c.ProviderFetchFailures++
			}
			if trigger, ok := f["trigger"].(string); ok {
				c.ProviderTriggers[trigger]++
			}
			if _, ok := f["retry_after_ms"]; ok {
				c.ProviderRetryAfter++
			}
		case "provider_apply":
			if f["is_ok"] == false {
				c.ProviderRejected++
				decided = true
			} else if f["is_changed"] == true && f["trigger"] != "initial" {
				c.ProviderApplied++
				decided = true
			}
			if f["is_ok"] == true && num(f["renewed"]) > 0 && f["trigger"] != "initial" {
				c.ProviderRenewals++
			}
			decided = true
		case "provider_expired":
			c.ProviderExpired++
			decided = decided || f["renewal"] != "saved"
		case "provider_auth_failed":
			c.ProviderAuthFailed++
			decided = true
		case "provider_base_changed":
			c.ProviderBaseChanged++
			decided = true
		case "pin":
			c.Pins++
		case "release":
			c.Releases++
		case "memory_low":
			if f["is_low"] == true {
				c.MemoryLow++
			}
		case "outbound_sleep":
			c.OutboundSleeps++
		}
	}
	return c, decided
}

func reportJSON(t *testing.T, v *reportView) reportView {
	t.Helper()
	if v == nil {
		t.Fatal("no report")
	}
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out reportView
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// Skip counters and fallback duration are state-derived; their windows are checked
// through actual actions, including TestFallbackBalancerReportWindowsAndSnapshots.
func eventReportCounts(counts reportCounts) map[string]any {
	values := reflect.ValueOf(counts)
	out := make(map[string]any, values.NumField()-2)
	for i := 0; i < values.NumField(); i++ {
		name := values.Type().Field(i).Name
		if name != "AuditSkipped" && name != "WakeSkipped" && name != "FallbackBalancerMs" {
			out[name] = values.Field(i).Interface()
		}
	}
	return out
}

func TestReportAddsUpTheEvents(t *testing.T) {
	cfg := &Config{Schedule: quarantine(30 * time.Second), Events: &EventsConfig{MaxEvents: maxEvents, MaxBytes: maxEventBytes}}
	te := newReportEngine(t, cfg, returnToCheaperConfig(), "a1", "b1")
	te.onNetworkChange("home", at(0))
	failoverToB(t, te, at(time.Second))
	for now := at(5 * time.Minute); now.Before(at(7 * time.Minute)); now = now.Add(time.Second) {
		te.run(t, now, map[string]bool{"a1": false})
	}
	te.onSuccess("b1", at(8*time.Minute))
	te.onNetworkChange("cafe", at(9*time.Minute))
	te.onNetworkChange("home", at(10*time.Minute))
	te.onNetworkLost(at(11 * time.Minute))
	te.onNetworkChange("home", at(12*time.Minute))
	te.setMemoryLow(true, at(13*time.Minute))
	te.setMemoryLow(false, at(14*time.Minute))
	te.pin("auto", "a1", at(15*time.Minute))
	te.release("auto", at(16*time.Minute))
	if _, _, err := te.requestProbe("auto", []string{"a1"}, at(16*time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}
	te.run(t, at(16*time.Minute+2*time.Second), nil)
	now := at(17 * time.Minute)
	te.emit(now, "path_state", map[string]any{"outbound": "a1", "state": "degraded", "reason": "egress"})
	te.emit(now, "group_state", map[string]any{"balancer": "auto", "group": "a", "state": "down", "members": 3, "failed": 3, "quarantine_ms": int64(30000)})
	te.emit(now, "pending_closed", map[string]any{"outbound": "a1", "connections": 3, "reason": "traffic"})
	te.emit(now, "pinned_down", map[string]any{"balancer": "auto", "outbound": "a1"})
	te.emit(now, "pinned_down", map[string]any{"balancer": "auto", "outbound": "a1", "is_start_pinned": true})
	te.emit(now, "exhausted", map[string]any{"balancer": "auto", "from": "a1", "candidates": 2})
	te.emit(now, "provider_fetch", map[string]any{"provider": "p", "trigger": "host", "retry": 0, "attempt": 1,
		"via": "", "outbound": "", "is_ok": true, "status": 200, "ms": int64(40), "bytes": 900})
	te.emit(now, "provider_fetch", map[string]any{"provider": "p", "trigger": "exhausted", "retry": 1, "attempt": 1,
		"via": "auto", "outbound": "", "is_ok": false, "error": "balancer exhausted"})
	te.emit(now, "provider_fetch", map[string]any{"provider": "p", "trigger": "exhausted", "retry": 1, "attempt": 1,
		"via": "", "outbound": "", "is_ok": false, "status": 429, "retry_after_ms": int64(60000), "error": "status 429"})
	te.emit(now, "provider_apply", map[string]any{"provider": "p", "trigger": "host", "is_ok": false, "error": "bad"})
	te.emit(now, "provider_apply", map[string]any{"provider": "p", "trigger": "host", "is_ok": true, "is_changed": false})
	te.emit(now, "provider_apply", map[string]any{"provider": "p", "trigger": "expiry", "is_ok": true, "is_changed": false,
		"leases": 2, "renewed": 2, "expired": 0})
	te.emit(now, "provider_apply", map[string]any{"provider": "p", "trigger": "host", "is_ok": true, "is_changed": true})
	te.emit(now, "provider_base_changed", map[string]any{"provider": "p", "version": "1", "sections": []string{"dns"}})
	te.emit(now, "provider_expired", map[string]any{"provider": "p", "outbound": "p-x1", "was_active": true, "renewal": "failed"})
	te.emit(now, "provider_auth_failed", map[string]any{"provider": "p", "trigger": "host", "status": 401, "via": "direct",
		"code": "unauthorized"})
	te.emit(now, "provider_base_changed", map[string]any{"provider": "p", "from": "b1", "to": "b2"})
	te.emit(now, "idle_closed", map[string]any{"outbound": "b1", "connections": 2})
	te.emit(now, "outbound_sleep", map[string]any{"outbound": "b1", "idle_ms": int64(300_000), "reason": "idle"})
	te.emit(now, "path_state", map[string]any{"outbound": "a1", "state": "up", "reason": "rescued"})
	te.emit(now, "probe_start", map[string]any{"outbound": "a1", "reason": "audit"})
	te.emit(now, "path_state", map[string]any{"outbound": "a1", "state": "up", "reason": "rescued", "by": "audit"})
	for _, reason := range []string{"lone", "wake", "candidate"} {
		te.emit(now, "probe_start", map[string]any{"outbound": "a1", "reason": reason})
		te.emit(now, "probe_result", map[string]any{"outbound": "a1", "reason": reason, "is_ok": false, "bytes": int64(10)})
	}
	te.emit(now, "probe_only_promoted", map[string]any{"outbound": "a1"})
	te.emit(now, "fresh_path_down", map[string]any{"outbound": "a1"})
	for i, state := range []string{"ok", "lossy", "dead"} {
		te.emit(now, "udp_probe", map[string]any{"outbound": "a1", "reason": "udp_suspect", "state": state, "destination_index": i % 2})
	}
	te.emit(now, "udp_suspect", map[string]any{"outbound": "a1", "flows": 3, "targets": 2})
	te.emit(now, "fallback_balancer", map[string]any{"balancer": "auto", "to": "main", "is_active": true})
	te.emit(now, "fallback_balancer", map[string]any{"balancer": "auto", "to": "main", "is_active": false})

	te.emit(now, "panic_recovered", map[string]any{"scope": "connection", "where": "test.go:worker"})
	raw, dropped := te.events.drain()
	if dropped != 0 {
		t.Fatalf("setup: %d events dropped", dropped)
	}
	want, decided := foldReport(t, raw)
	got := reportJSON(t, te.takeReport(true, now))
	got.EventsDropped = 0
	if !reflect.DeepEqual(eventReportCounts(got.reportCounts), eventReportCounts(want)) {
		t.Fatalf("report\n%+v\nevents add up to\n%+v", got.reportCounts, want)
	}
	if !decided || got.Reason != reportStop {
		t.Fatalf("decided %v, reason %q", decided, got.Reason)
	}
	// Every event-derived counter must be exercised by this fixture.
	// Census budget/byte counters are charged directly and verified separately.
	rv := reflect.ValueOf(want)
	for i := 0; i < rv.NumField(); i++ {
		if name := rv.Type().Field(i).Name; name != "EventsDropped" && name != "AuditSkipped" && name != "WakeSkipped" && name != "FallbackBalancerMs" && name != "CensusProbes" && name != "CensusProbeBytes" && rv.Field(i).IsZero() {
			t.Errorf("the scenario leaves %s at zero", name)
		}
	}
}

// The report counts events the log dropped or a drain took away.
func TestReportDoesNotNeedTheEventLog(t *testing.T) {
	te := newReportEngine(t, &Config{Events: &EventsConfig{MaxEvents: 1}}, nil, "a1", "b1")
	emit := func(n int, now time.Time) {
		for i := 0; i < n; i++ {
			te.emit(now, "probe_result", map[string]any{"outbound": "a1", "is_ok": i%2 == 0, "bytes": int64(10)})
		}
	}
	emit(100, at(time.Second))
	te.events.drain()
	emit(100, at(2*time.Second))
	te.emit(at(3*time.Second), "exhausted", map[string]any{"balancer": "auto", "from": "a1", "candidates": 2})
	got := reportJSON(t, te.takeReport(true, at(4*time.Second)))
	// The start switch, 200 probes and the exhausted balancer. The log holds one event:
	// every event but the first after the drain pushed one out.
	if got.Events != 202 || got.Probes != 200 || got.ProbesFailed != 100 || got.ProbeBytes != 2000 || got.Exhausted != 1 {
		t.Fatalf("counters %+v", got.reportCounts)
	}
	if got.EventsDropped != 200 {
		t.Fatalf("events_dropped %d, want 200", got.EventsDropped)
	}
}

func TestReportDecisions(t *testing.T) {
	type emitted struct {
		typ    string
		fields map[string]any
	}
	for _, tc := range []struct {
		name     string
		events   []emitted
		decision bool
	}{
		{"start switches", []emitted{
			{"switch", map[string]any{"from": "", "to": "a1", "reason": "start_pinned"}},
			{"switch", map[string]any{"from": "a1", "to": "a2", "reason": "start_remembered"}},
		}, false},
		{"start with", []emitted{{"switch", map[string]any{"from": "", "to": "b1", "reason": "start_with"}}}, false},
		{"initial set", []emitted{
			{"provider_apply", map[string]any{"trigger": "initial", "is_ok": true, "is_changed": true, "renewed": 0}},
			{"provider_headers", map[string]any{"provider": "p", "by": "initial"}},
			{"provider_expired", map[string]any{"provider": "p", "renewal": "saved"}},
		}, false},
		{"initial set refused", []emitted{{"provider_apply", map[string]any{"trigger": "initial", "is_ok": false, "error": "x"}}}, true},
		{"suspicion and probes", []emitted{
			{"path_state", map[string]any{"outbound": "a1", "state": "suspect"}},
			{"probe_result", map[string]any{"outbound": "a1", "is_ok": false}},
			{"return_start", map[string]any{"balancer": "auto"}},
			{"memory_low", map[string]any{"is_low": true}},
		}, false},
		{"same provider set", []emitted{{"provider_apply", map[string]any{"is_ok": true, "is_changed": false}}}, false},
		{"leases renewed", []emitted{{"provider_apply", map[string]any{"is_ok": true, "is_changed": false, "renewed": 2}}}, false},
		{"failover", []emitted{{"switch", map[string]any{"from": "a1", "to": "b1", "reason": "failover"}}}, true},
		{"exhausted", []emitted{{"exhausted", map[string]any{"balancer": "auto"}}}, true},
		{"failed return", []emitted{{"return_failed", map[string]any{"balancer": "auto"}}}, true},
		{"pinned down", []emitted{{"pinned_down", map[string]any{"balancer": "auto"}}}, true},
		{"pinned down under the start pin", []emitted{
			{"pinned_down", map[string]any{"balancer": "auto", "is_start_pinned": true}},
		}, false},
		{"provider rejected", []emitted{{"provider_apply", map[string]any{"is_ok": false, "error": "x"}}}, true},
		{"provider applied", []emitted{{"provider_apply", map[string]any{"is_ok": true, "is_changed": true}}}, true},

		{"lease ended", []emitted{{"provider_expired", map[string]any{"provider": "p", "renewal": "refused"}}}, true},
		{"headers refused", []emitted{{"provider_auth_failed", map[string]any{"provider": "p", "status": 401}}}, true},
		{"base changed", []emitted{{"provider_base_changed", map[string]any{"provider": "p", "from": "b1", "to": "b2"}}}, true},
		{"headers changed", []emitted{{"provider_headers", map[string]any{"provider": "p", "by": "host"}}}, false},
		{"outbound asleep", []emitted{{"outbound_sleep", map[string]any{"outbound": "b1", "reason": "idle"}}}, false},
		{"down undone", []emitted{{"path_state", map[string]any{"outbound": "a1", "state": "up", "reason": "rescued"}}}, false},
	} {
		te := newReportEngine(t, nil, nil, "a1", "b1")
		for _, ev := range tc.events {
			te.emit(at(time.Second), ev.typ, ev.fields)
		}
		if got := te.takeReport(true, at(time.Minute)) != nil; got != tc.decision {
			t.Errorf("%s: final report %v, want %v", tc.name, got, tc.decision)
		}
	}
}

// A run that stops while the outbound of the start pin is down sends no
// report: the host verifying that outbound reports the failure itself. The
// same stop under a pin of the host does.
func TestPinnedDownUnderTheStartPinIsNoDecision(t *testing.T) {
	for _, tc := range []struct {
		name   string
		start  string
		host   bool
		report bool
	}{
		{"start pin", "a1", false, false},
		{"pin of the host", "", true, true},
		{"pin of the host over the start pin", "a1", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc := &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), StartPinnedOutbound: tc.start, StartPinTimeout: int64(30 * time.Second)}
			te := newReportEngine(t, nil, sc, "a1", "b1")
			if tc.host {
				te.pin("auto", "a1", at(time.Second))
			}
			te.failN(3, "a1", failDial, at(2*time.Second))
			te.run(t, at(3*time.Second), map[string]bool{"a1": false})
			if n := countEvents(te.eventTypes(), "pinned_down"); n == 0 || te.sel.current() != "a1" {
				t.Fatalf("setup: %d pinned_down, on %q", n, te.sel.current())
			}
			if got := te.takeReport(true, at(10*time.Second)) != nil; got != tc.report {
				t.Fatalf("final report %v, want %v", got, tc.report)
			}
		})
	}
}

// Events lost to a full log are not a decision.
func TestReportDroppedEventsAreNoDecision(t *testing.T) {
	te := newReportEngine(t, &Config{Events: &EventsConfig{MaxEvents: 1}}, nil, "a1", "b1")
	for i := 0; i < 50; i++ {
		te.emit(at(time.Second), "path_state", map[string]any{"outbound": "a1", "state": "up"})
	}
	if te.events.droppedTotal == 0 {
		t.Fatal("setup: nothing dropped")
	}
	if v := te.takeReport(true, at(time.Minute)); v != nil {
		t.Fatalf("final report for a run with lost events only: %+v", v)
	}
}

func failoverEvent(te *testEngine, now time.Time) {
	te.emit(now, "switch", map[string]any{"from": "a1", "to": "b1", "reason": "failover"})
}

func reportEvery(d time.Duration) *Config {
	return &Config{Report: &ReportConfig{Interval: int64(d)}}
}

func TestReportIsDueOnceAWindowWithDecisionsIsLongEnough(t *testing.T) {
	te := newReportEngine(t, reportEvery(time.Hour), nil, "a1", "b1")
	if v := te.takeReport(false, at(3*time.Hour)); v != nil {
		t.Fatalf("a window without decisions is due: %+v", v)
	}
	// The window is past the interval, so a decision goes with the next call.
	failoverEvent(te, at(3*time.Hour+time.Minute))
	v := te.takeReport(false, at(3*time.Hour+2*time.Minute))
	if v == nil || v.Reason != reportInterval || v.Index != 1 || v.WindowMs != (3*3600+120)*1000 || v.Switches != 2 {
		t.Fatalf("first report %+v", v)
	}
	failoverEvent(te, at(3*time.Hour+10*time.Minute))
	if v := te.takeReport(false, at(4*time.Hour+time.Minute)); v != nil {
		t.Fatalf("due before the window is an hour long: %+v", v)
	}
	v = te.takeReport(false, at(4*time.Hour+2*time.Minute))
	if v == nil || v.Reason != reportInterval || v.Index != 2 || v.WindowMs != 3600000 || v.Switches != 1 {
		t.Fatalf("second report %+v", v)
	}
	if v := te.takeReport(false, at(9*time.Hour)); v != nil {
		t.Fatalf("a window without decisions is due: %+v", v)
	}
	if v := te.takeReport(true, at(9*time.Hour)); v == nil || v.Reason != reportStop || v.Index != 3 || v.Switches != 0 {
		t.Fatalf("final report after quiet hours of a run with decisions: %+v", v)
	}
}

func TestReportIntervalOff(t *testing.T) {
	te := newReportEngine(t, reportEvery(-1), nil, "a1", "b1")
	failoverEvent(te, at(time.Minute))
	if v := te.takeReport(false, at(1000*time.Hour)); v != nil {
		t.Fatalf("periodic report while it is off: %+v", v)
	}
	if v := te.takeReport(true, at(1000*time.Hour)); v == nil || v.Reason != reportStop {
		t.Fatalf("final report %+v", v)
	}
}

// Low memory sends nothing early: the host keeps a snapshot instead.
func TestReportNotDueForLowMemory(t *testing.T) {
	te := newReportEngine(t, nil, nil, "a1", "b1")
	failoverEvent(te, at(time.Minute))
	te.setMemoryLow(true, at(2*time.Minute))
	if v := te.takeReport(false, at(3*time.Minute)); v != nil {
		t.Fatalf("report due for low memory: %+v", v)
	}
	if v := te.takeReport(false, at(24*time.Hour)); v == nil || v.MemoryLow != 1 {
		t.Fatalf("report after a day: %+v", v)
	}
}

func TestReportFinalOnce(t *testing.T) {
	quiet := newReportEngine(t, reportEvery(time.Minute), nil, "a1", "b1")
	if v := quiet.takeReport(true, at(time.Hour)); v != nil {
		t.Fatalf("final report of a quiet run: %+v", v)
	}
	failoverEvent(quiet, at(2*time.Hour))
	if quiet.takeReport(false, at(3*time.Hour)) != nil || quiet.snapshotReport(at(3*time.Hour)) != nil {
		t.Fatal("a report after the final one")
	}

	te := newReportEngine(t, reportEvery(time.Minute), nil, "a1", "b1")
	failoverEvent(te, at(time.Second))
	if v := te.takeReport(true, at(10*time.Second)); v == nil || v.Reason != reportStop || v.Index != 1 {
		t.Fatalf("final report %+v", v)
	}
	failoverEvent(te, at(20*time.Second))
	if te.takeReport(true, at(time.Hour)) != nil || te.takeReport(false, at(time.Hour)) != nil ||
		te.snapshotReport(at(time.Hour)) != nil {
		t.Fatal("a report after the final one")
	}
}

// A snapshot closes nothing: the report taken after it counts the same.
func TestReportSnapshotKeepsTheWindow(t *testing.T) {
	te := newReportEngine(t, nil, nil, "a1", "b1")
	if v := te.snapshotReport(at(time.Second)); v != nil {
		t.Fatalf("snapshot before any decision: %+v", v)
	}
	failoverEvent(te, at(time.Second))
	te.path("a1").stats.bytesDown += 500
	first := reportJSON(t, te.snapshotReport(at(10*time.Second)))
	second := reportJSON(t, te.snapshotReport(at(10*time.Second)))
	if first.Reason != reportSnapshot || first.Index != 1 || !reflect.DeepEqual(first, second) {
		t.Fatalf("snapshots differ:\n%+v\n%+v", first, second)
	}
	final := reportJSON(t, te.takeReport(true, at(10*time.Second)))
	final.Reason = reportSnapshot
	if !reflect.DeepEqual(first, final) {
		t.Fatalf("snapshot\n%+v\nfinal\n%+v", first, final)
	}
	if first.Paths["a1"].BytesDown != 500 || first.Switches != 2 {
		t.Fatalf("snapshot %+v", first)
	}
}

func TestReportPathsGrowSincePreviousReport(t *testing.T) {
	te := newReportEngine(t, reportEvery(time.Second), nil, "a1", "b1")
	a := te.path("a1")
	a.stats.bytesUp, a.stats.bytesDown, a.stats.connsOK = 100, 1000, 2
	te.path("b1").stats.connsFailed = 1
	failoverEvent(te, at(time.Second))
	v := te.takeReport(false, at(10*time.Second))
	if v == nil {
		t.Fatal("no report")
	}
	// The balancer chose a1 at start and is still on it.
	if p := v.Paths["a1"]; p.ActiveMs != 10_000 || p.BytesUp != 100 || p.BytesDown != 1000 || p.ConnsOK != 2 || p.State != "unknown" {
		t.Fatalf("a1 %+v", p)
	}
	a.stats.bytesDown += 24
	a.stats.probes, a.stats.probeBytes = 1, 4096
	failoverEvent(te, at(11*time.Second))
	v = te.takeReport(false, at(25*time.Second))
	if p := v.Paths["a1"]; p.ActiveMs != 15_000 || p.BytesUp != 0 || p.BytesDown != 24 || p.ConnsOK != 0 || p.Probes != 1 || p.ProbeBytes != 4096 {
		t.Fatalf("a1 grew by %+v", p)
	}
	if p, ok := v.Paths["b1"]; !ok || !reflect.DeepEqual(p, reportPath{State: "unknown", UDPState: "unknown", ProbeSteps: []reportProbeStep{}}) {
		t.Fatalf("b1 without growth %+v, listed %v", p, ok)
	}
	// An outbound forgotten and seen again counts from zero.
	te.forget("b1", t0)
	te.path("b1").stats.connsFailed = 3
	failoverEvent(te, at(26*time.Second))
	v = te.takeReport(false, at(30*time.Second))
	if p := v.Paths["b1"]; p.ConnsFailed != 3 {
		t.Fatalf("b1 seen again %+v", p)
	}
}

func TestReportListsTheFirstPaths(t *testing.T) {
	te := newReportEngine(t, reportEvery(time.Second), nil, "a1", "b1")
	for i := 0; i < reportPaths+4; i++ {
		te.path(fmt.Sprintf("p%02d", i)).stats.connsOK = 1
	}
	failoverEvent(te, at(time.Second))
	v := te.takeReport(false, at(2*time.Second))
	if len(v.Paths) != reportPaths {
		t.Fatalf("%d paths listed, want %d", len(v.Paths), reportPaths)
	}
	if _, ok := v.Paths[fmt.Sprintf("p%02d", reportPaths-3)]; !ok {
		t.Fatal("last included path is missing")
	}
	if _, ok := v.Paths[fmt.Sprintf("p%02d", reportPaths-2)]; ok {
		t.Fatal("first excluded path is listed")
	}
	// A path left out still starts its growth anew, so dropping a tag later
	// does not bring its whole history at once.
	for _, tag := range []string{"a1", "b1", "p00", "p01", "p02", "p03"} {
		te.forget(tag, t0)
	}
	failoverEvent(te, at(3*time.Second))
	v = te.takeReport(false, at(4*time.Second))
	if p := v.Paths[fmt.Sprintf("p%02d", reportPaths+3)]; p.ConnsOK != 0 {
		t.Fatalf("last path reports %d connections taken before the previous report", p.ConnsOK)
	}
}

func TestReportWallClockGoesBack(t *testing.T) {
	te := newReportEngine(t, nil, nil, "a1", "b1")
	failoverEvent(te, at(time.Second))
	v := te.snapshotReport(t0.Add(-time.Hour))
	if v.WindowMs != 0 || v.SessionMs != 0 {
		t.Fatalf("window %d s, session %d s", v.WindowMs, v.SessionMs)
	}
}

// What the window keeps stays the same however many switches pass, and a
// call with nothing due allocates nothing.
func TestReportStaysBounded(t *testing.T) {
	te := newReportEngine(t, nil, nil, "a1", "b1")
	reasons := []string{"failover", "return", "recovered", "candidates_changed", "retired"}
	for i := 0; i < 100_000; i++ {
		te.emit(at(time.Second), "switch", map[string]any{
			"from": fmt.Sprintf("p-%d", i), "to": fmt.Sprintf("p-%d", i+1), "reason": reasons[i%len(reasons)],
		})
	}
	c := te.report.counts
	if len(c.SwitchHistory) != reportSwitches || cap(c.SwitchHistory) > 2*reportSwitches || len(c.SwitchReasons) != len(reasons)+1 {
		t.Fatalf("switch path %d (cap %d), reasons %d", len(c.SwitchHistory), cap(c.SwitchHistory), len(c.SwitchReasons))
	}
	if c.SwitchHistory[reportSwitches-1] != (ReportSwitch{From: "p-99999", To: "p-100000", Reason: reasons[99_999%len(reasons)]}) {
		t.Fatalf("latest switch %+v", c.SwitchHistory[reportSwitches-1])
	}

	o := newObserver(context.Background(), &Config{Features: &FeatureConfig{Report: v2Bool(true)}, Paths: observed("a")})
	if allocs := testing.AllocsPerRun(100, func() {
		if o.TakeReport(false) != nil || o.PeekReport() != nil {
			t.Fatal("a report is due")
		}
	}); allocs != 0 {
		t.Fatalf("%v allocations per call with nothing due", allocs)
	}
}

func TestObserverReport(t *testing.T) {
	o := newObserver(context.Background(), &Config{Features: &FeatureConfig{Report: v2Bool(true)}, Paths: observed("a"), Report: &ReportConfig{Interval: int64(time.Hour)}})
	clock := t0
	o.clock = func() time.Time { return clock }
	o.e.report.begin(clock)
	o.emit("provider_base_changed", map[string]any{"provider": "p", "version": "1", "sections": []string{"dns"}})

	clock = clock.Add(30 * time.Minute)
	if raw := o.TakeReport(false); raw != nil {
		t.Fatalf("due before the interval: %s", raw)
	}
	var snap map[string]any
	if err := json.Unmarshal(o.PeekReport(), &snap); err != nil {
		t.Fatal(err)
	}
	if snap["health_run"] != o.run || snap["report_reason"] != "snapshot" || snap["report_index"] != 1.0 ||
		snap["window_ms"] != 1800000.0 || snap["provider_base_changed"] != 1.0 {
		t.Fatalf("snapshot %v", snap)
	}
	clock = clock.Add(30 * time.Minute)
	var taken map[string]any
	if err := json.Unmarshal(o.TakeReport(false), &taken); err != nil {
		t.Fatal(err)
	}
	if taken["report_reason"] != "interval" || taken["report_index"] != 1.0 || taken["window_ms"] != 3600000.0 {
		t.Fatalf("report %v", taken)
	}
	clock = clock.Add(time.Minute)
	var final map[string]any
	if err := json.Unmarshal(o.TakeReport(true), &final); err != nil {
		t.Fatal(err)
	}
	if final["report_reason"] != "stop" || final["report_index"] != 2.0 || final["session_ms"] != 3660000.0 ||
		final["provider_base_changed"] != 0.0 || final["health_run"] != o.run {
		t.Fatalf("final report %v", final)
	}
	if o.TakeReport(true) != nil || o.PeekReport() != nil {
		t.Fatal("a report after the final one")
	}
}

// readmeEventRow is the row of the events table of the README for typ.
func readmeEventRow(t *testing.T, typ string) string {
	t.Helper()
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(readme), "\n") {
		if strings.HasPrefix(line, "| `"+typ+"` |") {
			return line
		}
	}
	t.Fatalf("README has no row for %s", typ)
	return ""
}

// A network change no balancer forgets on keeps the generation and tells
// so, as the README describes.
func TestIgnoredNetworkChangeDocumented(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), NetworkChangeAction: NetworkChangeMode_IGNORE}, "a1", "b1")
	gen := te.generation
	te.onNetworkChange("x", at(time.Second))
	ev := te.lastOf(t, "network_change")
	if ev.Fields["is_ignored"] != true || te.generation != gen {
		t.Fatalf("network_change %v at generation %d, want it ignored at %d", ev.Fields, te.generation, gen)
	}
	row := readmeEventRow(t, "network_change")
	for k := range ev.Fields {
		if !strings.Contains(row, "`"+k+"`") {
			t.Errorf("README does not describe %q of network_change", k)
		}
	}
}

// Every key of the report is described in the README.
func TestReportFieldsDocumented(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	te := newReportEngine(t, connectivityConfig(), nil, "a1", "b1")
	failoverEvent(te, at(time.Second))
	te.emit(at(time.Second), "provider_fetch", map[string]any{"trigger": "host", "is_ok": true})
	te.path("a1").stats.udp.ports[udpPortDNS] = udpPortTotals{flows: 1, unanswered: 1}
	raw, _ := json.Marshal(te.snapshotReport(at(time.Second)))
	var top map[string]json.RawMessage
	_ = json.Unmarshal(raw, &top)
	var paths map[string]map[string]any
	_ = json.Unmarshal(top["paths"], &paths)
	keys := []string{}
	for k := range top {
		keys = append(keys, k)
	}
	for k := range paths["a1"] {
		keys = append(keys, k)
	}
	if len(top) < 30 || len(paths["a1"]) != reflect.TypeOf(reportPath{}).NumField() {
		t.Fatalf("setup: report %s", raw)
	}
	for _, k := range keys {
		if !strings.Contains(string(readme), "`"+k+"`") {
			t.Errorf("README does not describe %q", k)
		}
	}
}
