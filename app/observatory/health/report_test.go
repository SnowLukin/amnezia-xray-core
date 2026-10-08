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
		sc = &SelectorConfig{Costs: costs("a", 1, "b", 3)}
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
	c := reportCounts{SwitchReasons: map[string]int64{}, SwitchPath: []string{}, ProviderTriggers: map[string]int64{}}
	decided := false
	num := func(v any) int64 { f, _ := v.(float64); return int64(f) }
	for _, ev := range events {
		f := ev.Fields
		c.Events++
		switch ev.Type {
		case "switch":
			reason, _ := f["reason"].(string)
			c.Switches++
			c.SwitchReasons[reason]++
			c.SwitchPath = append(c.SwitchPath, fmt.Sprintf("%v>%v:%s", f["from"], f["to"], reason))
			if len(c.SwitchPath) > reportSwitches {
				c.SwitchPath = c.SwitchPath[1:]
			}
			decided = decided || !slices.Contains([]string{"start", "start_pinned", "start_remembered"}, reason)
		case "exhausted":
			c.Exhausted++
			decided = true
		case "path_state":
			switch f["state"] {
			case "down":
				c.PathDown++
			case "suspect":
				c.PathSuspect++
			}
		case "pending_closed":
			c.PendingClosed += num(f["connections"])
		case "probe_result":
			c.Probes++
			if f["ok"] != true {
				c.ProbesFailed++
			}
			c.ProbeBytes += num(f["bytes"])
			if f["reason"] == "host" {
				c.HostProbes++
			}
		case "probe_request":
			c.HostProbeRequests++
		case "upgrade_start":
			c.UpgradeAttempts++
		case "upgrade_failed":
			c.UpgradeFailures++
			decided = true
		case "pinned_down":
			c.PinnedDown++
			decided = true
		case "network_change":
			c.NetworkChanges++
			if f["known"] == true {
				c.NetworkKnown++
			}
		case "network_down":
			c.NetworkDown++
		case "provider_fetch":
			c.ProviderFetches++
			if f["ok"] != true {
				c.ProviderFetchFailures++
			}
			if trigger, ok := f["trigger"].(string); ok {
				c.ProviderTriggers[trigger]++
			}
		case "provider_apply":
			if f["ok"] == false {
				c.ProviderRejected++
				decided = true
			} else if f["changed"] == true {
				c.ProviderApplied++
				decided = true
			}
		case "provider_restart_needed":
			c.ProviderRestartNeeded++
			decided = true
		case "pin":
			c.Pins++
		case "release":
			c.Releases++
		case "memory_low":
			if f["on"] == true {
				c.MemoryLow++
			}
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

// Every counter of the report matches what the events add up to, the way a
// host read them from the event log.
func TestReportAddsUpTheEvents(t *testing.T) {
	cfg := &Config{Schedule: quarantine(30 * time.Second), Events: &EventsConfig{MaxEvents: maxEvents, MaxBytes: maxEventBytes}}
	te := newReportEngine(t, cfg, upgradeConfig(), "a1", "b1")
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
	te.emit(now, "pending_closed", map[string]any{"outbound": "a1", "connections": 3, "reason": "traffic"})
	te.emit(now, "pinned_down", map[string]any{"balancer": "auto", "outbound": "a1"})
	te.emit(now, "exhausted", map[string]any{"balancer": "auto", "from": "a1", "candidates": 2})
	te.emit(now, "provider_fetch", map[string]any{"provider": "p", "trigger": "host", "retry": 0, "attempt": 1,
		"via": "", "outbound": "", "ok": true, "status": 200, "ms": int64(40), "bytes": 900})
	te.emit(now, "provider_fetch", map[string]any{"provider": "p", "trigger": "exhausted", "retry": 1, "attempt": 1,
		"via": "auto", "outbound": "", "ok": false, "error": "balancer exhausted"})
	te.emit(now, "provider_apply", map[string]any{"provider": "p", "trigger": "host", "ok": false, "error": "bad"})
	te.emit(now, "provider_apply", map[string]any{"provider": "p", "trigger": "host", "ok": true, "changed": false})
	te.emit(now, "provider_apply", map[string]any{"provider": "p", "trigger": "host", "ok": true, "changed": true})
	te.emit(now, "provider_restart_needed", map[string]any{"provider": "p", "version": "1", "sections": []string{"dns"}})

	raw, dropped := te.events.drain()
	if dropped != 0 {
		t.Fatalf("setup: %d events dropped", dropped)
	}
	want, decided := foldReport(t, raw)
	got := reportJSON(t, te.takeReport(true, now))
	got.EventsDropped = 0
	if !reflect.DeepEqual(got.reportCounts, want) {
		t.Fatalf("report\n%+v\nevents add up to\n%+v", got.reportCounts, want)
	}
	if !decided || got.Reason != reportStop {
		t.Fatalf("decided %v, reason %q", decided, got.Reason)
	}
	// The scenario has to reach every counter, or the comparison proves little.
	rv := reflect.ValueOf(want)
	for i := 0; i < rv.NumField(); i++ {
		if name := rv.Type().Field(i).Name; name != "EventsDropped" && rv.Field(i).IsZero() {
			t.Errorf("the scenario leaves %s at zero", name)
		}
	}
}

// The report counts events the log dropped or a drain took away.
func TestReportDoesNotNeedTheEventLog(t *testing.T) {
	te := newReportEngine(t, &Config{Events: &EventsConfig{MaxEvents: 1}}, nil, "a1", "b1")
	emit := func(n int, now time.Time) {
		for i := 0; i < n; i++ {
			te.emit(now, "probe_result", map[string]any{"outbound": "a1", "ok": i%2 == 0, "bytes": int64(10)})
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
		{"suspicion and probes", []emitted{
			{"path_state", map[string]any{"outbound": "a1", "state": "suspect"}},
			{"probe_result", map[string]any{"outbound": "a1", "ok": false}},
			{"upgrade_start", map[string]any{"balancer": "auto"}},
			{"memory_low", map[string]any{"on": true}},
		}, false},
		{"same provider set", []emitted{{"provider_apply", map[string]any{"ok": true, "changed": false}}}, false},
		{"failover", []emitted{{"switch", map[string]any{"from": "a1", "to": "b1", "reason": "failover"}}}, true},
		{"exhausted", []emitted{{"exhausted", map[string]any{"balancer": "auto"}}}, true},
		{"failed return", []emitted{{"upgrade_failed", map[string]any{"balancer": "auto"}}}, true},
		{"pinned down", []emitted{{"pinned_down", map[string]any{"balancer": "auto"}}}, true},
		{"provider rejected", []emitted{{"provider_apply", map[string]any{"ok": false, "error": "x"}}}, true},
		{"provider applied", []emitted{{"provider_apply", map[string]any{"ok": true, "changed": true}}}, true},
		{"restart needed", []emitted{{"provider_restart_needed", map[string]any{"provider": "p"}}}, true},
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
	if v == nil || v.Reason != reportInterval || v.Index != 1 || v.WindowSec != 3*3600+120 || v.Switches != 2 {
		t.Fatalf("first report %+v", v)
	}
	failoverEvent(te, at(3*time.Hour+10*time.Minute))
	if v := te.takeReport(false, at(4*time.Hour+time.Minute)); v != nil {
		t.Fatalf("due before the window is an hour long: %+v", v)
	}
	v = te.takeReport(false, at(4*time.Hour+2*time.Minute))
	if v == nil || v.Reason != reportInterval || v.Index != 2 || v.WindowSec != 3600 || v.Switches != 1 {
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
	if p, ok := v.Paths["b1"]; !ok || p != (reportPath{State: "unknown"}) {
		t.Fatalf("b1 without growth %+v, listed %v", p, ok)
	}
	// An outbound forgotten and seen again counts from zero.
	te.forget("b1")
	te.path("b1").stats.connsFailed = 3
	failoverEvent(te, at(26*time.Second))
	v = te.takeReport(false, at(30*time.Second))
	if p := v.Paths["b1"]; p.ConnsFailed != 3 {
		t.Fatalf("b1 seen again %+v", p)
	}
}

func TestReportListsTheFirstPaths(t *testing.T) {
	te := newReportEngine(t, reportEvery(time.Second), nil, "a1", "b1")
	for i := 0; i < 20; i++ {
		te.path(fmt.Sprintf("p%02d", i)).stats.connsOK = 1
	}
	failoverEvent(te, at(time.Second))
	v := te.takeReport(false, at(2*time.Second))
	if len(v.Paths) != reportPaths {
		t.Fatalf("%d paths listed, want %d", len(v.Paths), reportPaths)
	}
	if _, ok := v.Paths["p13"]; !ok {
		t.Fatal("p13 is among the first 16 tags and missing")
	}
	if _, ok := v.Paths["p14"]; ok {
		t.Fatal("p14 is beyond the first 16 tags and listed")
	}
	// A path left out still starts its growth anew, so dropping a tag later
	// does not bring its whole history at once.
	for _, tag := range []string{"a1", "b1", "p00", "p01", "p02", "p03"} {
		te.forget(tag)
	}
	failoverEvent(te, at(3*time.Second))
	v = te.takeReport(false, at(4*time.Second))
	if p := v.Paths["p19"]; p.ConnsOK != 0 {
		t.Fatalf("p19 reports %d connections taken before the previous report", p.ConnsOK)
	}
}

func TestReportWallClockGoesBack(t *testing.T) {
	te := newReportEngine(t, nil, nil, "a1", "b1")
	failoverEvent(te, at(time.Second))
	v := te.snapshotReport(t0.Add(-time.Hour))
	if v.WindowSec != 0 || v.SessionSec != 0 {
		t.Fatalf("window %d s, session %d s", v.WindowSec, v.SessionSec)
	}
}

// What the window keeps stays the same however many switches pass, and a
// call with nothing due allocates nothing.
func TestReportStaysBounded(t *testing.T) {
	te := newReportEngine(t, nil, nil, "a1", "b1")
	reasons := []string{"failover", "upgrade", "recovered", "candidates_changed", "retired"}
	for i := 0; i < 100_000; i++ {
		te.emit(at(time.Second), "switch", map[string]any{
			"from": fmt.Sprintf("p-%d", i), "to": fmt.Sprintf("p-%d", i+1), "reason": reasons[i%len(reasons)],
		})
	}
	c := te.report.counts
	if len(c.SwitchPath) != reportSwitches || cap(c.SwitchPath) > 2*reportSwitches || len(c.SwitchReasons) != len(reasons)+1 {
		t.Fatalf("switch path %d (cap %d), reasons %d", len(c.SwitchPath), cap(c.SwitchPath), len(c.SwitchReasons))
	}
	if c.SwitchPath[reportSwitches-1] != "p-99999>p-100000:"+reasons[99_999%len(reasons)] {
		t.Fatalf("latest switch %q", c.SwitchPath[reportSwitches-1])
	}

	o := newObserver(context.Background(), &Config{SubjectSelector: []string{"a"}})
	if allocs := testing.AllocsPerRun(100, func() {
		if o.TakeReport(false) != nil || o.Report() != nil {
			t.Fatal("a report is due")
		}
	}); allocs != 0 {
		t.Fatalf("%v allocations per call with nothing due", allocs)
	}
}

func TestObserverReport(t *testing.T) {
	o := newObserver(context.Background(), &Config{SubjectSelector: []string{"a"}, Report: &ReportConfig{Interval: int64(time.Hour)}})
	clock := t0
	o.clock = func() time.Time { return clock }
	o.e.report.begin(clock)
	o.emit("provider_restart_needed", map[string]any{"provider": "p", "version": "1", "sections": []string{"dns"}})

	clock = clock.Add(30 * time.Minute)
	if raw := o.TakeReport(false); raw != nil {
		t.Fatalf("due before the interval: %s", raw)
	}
	var snap map[string]any
	if err := json.Unmarshal(o.Report(), &snap); err != nil {
		t.Fatal(err)
	}
	if snap["health_run"] != o.run || snap["report_reason"] != "snapshot" || snap["report_index"] != 1.0 ||
		snap["window_sec"] != 1800.0 || snap["provider_restart_needed"] != 1.0 {
		t.Fatalf("snapshot %v", snap)
	}
	clock = clock.Add(30 * time.Minute)
	var taken map[string]any
	if err := json.Unmarshal(o.TakeReport(false), &taken); err != nil {
		t.Fatal(err)
	}
	if taken["report_reason"] != "interval" || taken["report_index"] != 1.0 || taken["window_sec"] != 3600.0 {
		t.Fatalf("report %v", taken)
	}
	clock = clock.Add(time.Minute)
	var final map[string]any
	if err := json.Unmarshal(o.TakeReport(true), &final); err != nil {
		t.Fatal(err)
	}
	if final["report_reason"] != "stop" || final["report_index"] != 2.0 || final["session_sec"] != 3660.0 ||
		final["provider_restart_needed"] != 0.0 || final["health_run"] != o.run {
		t.Fatalf("final report %v", final)
	}
	if o.TakeReport(true) != nil || o.Report() != nil {
		t.Fatal("a report after the final one")
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
	te.emit(at(time.Second), "provider_fetch", map[string]any{"trigger": "host", "ok": true})
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
	if len(top) < 30 || len(paths["a1"]) != 9 {
		t.Fatalf("setup: report %s", raw)
	}
	for _, k := range keys {
		if !strings.Contains(string(readme), "`"+k+"`") {
			t.Errorf("README does not describe %q", k)
		}
	}
}
