package health

import (
	"encoding/json"
	"testing"
	"time"
)

func assertSkippedReport(t *testing.T, view *reportView, reason string, index, audit, wake int64) reportView {
	t.Helper()
	got := reportJSON(t, view)
	if got.Reason != reason || int64(got.Index) != index || got.AuditSkipped != audit || got.WakeSkipped != wake {
		t.Fatalf("report reason=%q index=%d audit_skipped_count=%d wake_skipped_count=%d, want %q %d %d %d",
			got.Reason, got.Index, got.AuditSkipped, got.WakeSkipped, reason, index, audit, wake)
	}
	raw, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]int64{"audit_skipped_count": audit, "wake_skipped_count": wake} {
		var count int64
		if err := json.Unmarshal(fields[key], &count); err != nil {
			t.Fatalf("production JSON field %q: %v", key, err)
		}
		if count != want {
			t.Fatalf("production JSON field %q=%d, want %d", key, count, want)
		}
	}
	return got
}

func TestReportAuditSkippedActions(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) *testEngine
	}{
		{"disabled", func(t *testing.T) *testEngine {
			te := newAuditEngine(t, &SelectorConfig{Features: noProbeNoAudit()})
			te.report.begin(t0)
			te.failN(3, "a1", failDial, at(0))
			if te.queued("a1", reasonAudit) || te.sel.current() != "b1" {
				t.Fatal("disabled audit did not fail over without an audit")
			}
			return te
		}},
		{"queued expiry behind running failover", func(t *testing.T) *testEngine {
			te := newReportEngine(t, parallelConfig(1), nil, "a1", "b1")
			te.failN(3, "a1", failDial, at(0))
			jobs, _ := te.tick(at(time.Second))
			if len(jobs) != 1 || jobs[0].reason != reasonFailover || !te.queued("a1", reasonAudit) {
				t.Fatalf("audit is not waiting for running failover: jobs=%v", jobTags(jobs))
			}
			te.tick(at(30 * time.Second))
			if !te.queued("a1", reasonAudit) {
				t.Fatal("audit expired before its 30 second window ended")
			}
			te.tick(at(30*time.Second + time.Nanosecond))
			te.tick(at(31 * time.Second))
			if te.queued("a1", reasonAudit) || te.path("a1").rescue != nil {
				t.Fatal("expired audit remained queued")
			}
			te.onProbeResult(jobs[0], probeResult{ok: true}, at(32*time.Second))
			if te.sel.current() != "b1" {
				t.Fatal("completed failover did not select b1")
			}
			return te
		}},
		{"network cancellation once", func(t *testing.T) *testEngine {
			te := newAuditEngine(t, nil)
			te.report.begin(t0)
			te.failN(3, "a1", failDial, at(0))
			if !te.queued("a1", reasonAudit) {
				t.Fatal("strong down did not queue audit")
			}
			te.onNetworkChange("next", at(time.Second))
			te.onNetworkChange("another", at(2*time.Second))
			if te.queued("a1", reasonAudit) {
				t.Fatal("network change retained audit")
			}
			return te
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := tc.setup(t)
			raw, _ := te.events.drain()
			folded, _ := foldReport(t, raw)
			if folded.AuditSkipped != 0 || folded.WakeSkipped != 0 {
				t.Fatal("skipped actions unexpectedly appear in event counters")
			}
			for range 2 {
				got := assertSkippedReport(t, te.snapshotReport(at(40*time.Second)), reportSnapshot, 1, 1, 0)
				if got.AuditCount != 0 || got.AuditPassed != 0 {
					t.Fatalf("skipped audit reported as started: %+v", got.reportCounts)
				}
			}
			te.events.drain()
			assertSkippedReport(t, te.takeReport(true, at(41*time.Second)), reportStop, 1, 1, 0)
			if te.takeReport(true, at(42*time.Second)) != nil || te.snapshotReport(at(42*time.Second)) != nil {
				t.Fatal("closed final report was offered again")
			}
		})
	}
}

func TestReportWakeSkippedActions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		actions func(*testing.T, *testEngine)
		skipped int64
		started int64
	}{
		{"fresh success", func(t *testing.T, te *testEngine) {
			te.onSuccess("a1", at(3*time.Minute-time.Second))
			te.userPresent(at(3 * time.Minute))
		}, 1, 0},
		{"interval after actual begin", func(t *testing.T, te *testEngine) {
			j := startWake(t, te, at(3*time.Minute))
			te.onProbeResult(j, probeResult{ok: true}, at(3*time.Minute+time.Second))
			te.userPresent(at(7 * time.Minute))
			if te.queued("a1", reasonWake) {
				t.Fatal("wake ignored interval after actual begin")
			}
		}, 1, 1},
		{"repeated while queued", func(t *testing.T, te *testEngine) {
			for i := range 4 {
				te.userPresent(at(3*time.Minute + time.Duration(i)*time.Second))
			}
			if len(te.queue) != 1 || !te.queued("a1", reasonWake) {
				t.Fatal("repeated user presence did not retain one queued wake")
			}
		}, 3, 0},
		{"unrelated queued probe", func(t *testing.T, te *testEngine) {
			if _, _, err := te.requestProbe("auto", []string{"a1"}, at(3*time.Minute)); err != nil {
				t.Fatal(err)
			}
			if !te.queued("a1", reasonHost) {
				t.Fatal("host request did not queue its probe")
			}
			te.userPresent(at(3 * time.Minute))
			if te.queued("a1", reasonWake) {
				t.Fatal("wake queued behind host probe")
			}
		}, 0, 0},
		{"unrelated running probe", func(t *testing.T, te *testEngine) {
			if _, _, err := te.requestProbe("auto", []string{"a1"}, at(3*time.Minute)); err != nil {
				t.Fatal(err)
			}
			jobs, _ := te.tick(at(3 * time.Minute))
			if len(jobs) != 1 || jobs[0].reason != reasonHost {
				t.Fatalf("host probe did not begin: %v", jobTags(jobs))
			}
			te.userPresent(at(3*time.Minute + time.Second))
			if te.queued("a1", reasonWake) {
				t.Fatal("wake queued while host probe is running")
			}
		}, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := newReportEngine(t, &Config{Schedule: tune(&ScheduleConfig{}, scheduleKnobs{idleAfter: time.Hour}),
				Report: &ReportConfig{QuietShare: 1}}, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3),
				Features: probeFirst(false), ReturnToCheaper: tune(&ReturnToCheaperConfig{Interval: int64(time.Hour)}, returnKnobs{fixedPhase: true})}, "a1", "b1")
			if !te.report.quietSample {
				t.Fatal("quiet share did not select the run")
			}
			te.onSuccess("a1", at(0))
			tc.actions(t, te)
			raw, _ := te.events.drain()
			folded, _ := foldReport(t, raw)
			if folded.AuditSkipped != 0 || folded.WakeSkipped != 0 {
				t.Fatal("skipped actions unexpectedly appear in event counters")
			}
			for range 2 {
				got := assertSkippedReport(t, te.snapshotReport(at(8*time.Minute)), reportSnapshot, 1, 0, tc.skipped)
				if got.WakeProbeCount != tc.started || got.WakeProbeFailed != 0 {
					t.Fatalf("wake starts or failures %+v", got.reportCounts)
				}
			}
			te.events.drain()
			assertSkippedReport(t, te.takeReport(true, at(8*time.Minute+time.Second)), reportStop, 1, 0, tc.skipped)
			if te.takeReport(true, at(9*time.Minute)) != nil {
				t.Fatal("final wake counters were offered twice")
			}
		})
	}
}

func TestReportSkippedActionsResetBetweenWindows(t *testing.T) {
	for _, final := range []bool{false, true} {
		t.Run(map[bool]string{false: "interval", true: "final"}[final], func(t *testing.T) {
			te := newReportEngine(t, &Config{Schedule: quarantine(time.Minute), Report: &ReportConfig{Interval: int64(time.Second)}},
				&SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), Features: noProbeNoAudit()}, "a1", "b1")
			te.failN(3, "a1", failDial, at(time.Second))
			if te.sel.current() != "b1" {
				t.Fatal("strong down did not make a failover decision")
			}
			te.onSuccess("b1", at(2*time.Second))
			te.userPresent(at(2 * time.Second))
			first := assertSkippedReport(t, te.snapshotReport(at(3*time.Second)), reportSnapshot, 1, 1, 1)
			assertSkippedReport(t, te.snapshotReport(at(3*time.Second)), reportSnapshot, 1, 1, 1)
			assertSkippedReport(t, te.takeReport(false, at(3*time.Second)), reportInterval, 1, 1, 1)
			assertSkippedReport(t, te.snapshotReport(at(4*time.Second)), reportSnapshot, 2, 0, 0)
			if te.takeReport(false, at(4*time.Second)) != nil {
				t.Fatal("already closed window was taken again")
			}
			te.onSuccess("b1", at(4*time.Second))
			te.userPresent(at(4 * time.Second))
			te.userPresent(at(5 * time.Second))
			te.failN(3, "b1", failDial, at(6*time.Second))
			assertSkippedReport(t, te.snapshotReport(at(7*time.Second)), reportSnapshot, 2, 1, 2)
			reason := reportInterval
			if final {
				reason = reportStop
			}
			assertSkippedReport(t, te.takeReport(final, at(7*time.Second)), reason, 2, 1, 2)
			if first.AuditSkipped != 1 || first.WakeSkipped != 1 {
				t.Fatal("closing later windows changed the earlier snapshot")
			}
			if final {
				if te.snapshotReport(at(8*time.Second)) != nil || te.takeReport(true, at(8*time.Second)) != nil {
					t.Fatal("final window was offered again")
				}
			} else {
				assertSkippedReport(t, te.snapshotReport(at(8*time.Second)), reportSnapshot, 3, 0, 0)
				assertSkippedReport(t, te.takeReport(true, at(8*time.Second)), reportStop, 3, 0, 0)
			}
		})
	}
}
