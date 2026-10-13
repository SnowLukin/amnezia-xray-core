package health

import (
	"encoding/json"
	"testing"
	"time"
)

func newAuditEngine(t *testing.T, sc *SelectorConfig) *testEngine {
	t.Helper()
	if sc == nil {
		sc = &SelectorConfig{SkipProbeBeforeSwitch: true}
	}
	sc.Paths = pathSpecs("a", 1, "b", 3)
	return newTestEngine(t, &Config{Schedule: &ScheduleConfig{
		Quarantine: &Backoff{InitialDelay: int64(time.Minute)},
		IdleAfter:  int64(time.Hour), MinProbeInterval: int64(time.Second),
	}}, sc, "a1", "b1")
}

func startAudit(t *testing.T, te *testEngine) *probeJob {
	t.Helper()
	te.failN(3, "a1", failDial, at(0))
	if p := te.path("a1"); p.status != statusDown || p.quarantineLevel != 1 {
		t.Fatalf("strong evidence left path at %v, level %d", p.status, p.quarantineLevel)
	}
	jobs, _ := te.tick(at(time.Second))
	for _, j := range jobs {
		if j.reason == reasonAudit && j.tag == "a1" {
			return j
		}
	}
	t.Fatalf("audit did not start: %v", jobTags(jobs))
	return nil
}

func TestStrongDownAuditPassAndFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		ok   bool
	}{{"passed", true}, {"failed", false}} {
		t.Run(tc.name, func(t *testing.T) {
			te := newAuditEngine(t, nil)
			j := startAudit(t, te)
			until := te.path("a1").quarantineUntil
			te.onProbeResult(j, probeResult{ok: tc.ok}, at(2*time.Second))
			p := te.path("a1")
			if tc.ok {
				if p.status != statusUp || p.reason != "rescued" || p.rescuedBy != "audit" || p.quarantineLevel != 0 || !p.quarantineUntil.IsZero() || len(te.silences) != 0 {
					t.Fatalf("passed audit left path %+v, silence checks %+v", p, te.silences)
				}
				if te.sel.current() != "b1" {
					t.Fatalf("audit switched back immediately to %q", te.sel.current())
				}
			} else if p.status != statusDown || p.quarantineLevel != 1 || !p.quarantineUntil.Equal(until) {
				t.Fatalf("failed audit changed down: status %v, level %d, until %v", p.status, p.quarantineLevel, p.quarantineUntil)
			}
			if te.report.counts.AuditCount != 1 || te.report.counts.AuditPassed != boolCount(tc.ok) || p.stats.audits != 1 || p.stats.auditsPassed != boolCount(tc.ok) {
				t.Fatalf("audit counters %+v; path %+v", te.report.counts, p.stats)
			}
			view := te.reportView(reportSnapshot, at(3*time.Second), false)
			if got := view.Paths["a1"]; got.Audits != 1 || got.AuditsPassed != boolCount(tc.ok) {
				t.Fatalf("audit report path %+v", got)
			}
			raw, _ := te.events.drain()
			var events []Event
			if err := json.Unmarshal(raw, &events); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, ev := range events {
				if ev.Type == "path_state" && ev.Fields["reason"] == "rescued" {
					found = ev.Fields["by"] == "audit"
				}
			}
			if found != tc.ok {
				t.Fatalf("rescued by audit event found=%v, want %v", found, tc.ok)
			}
		})
	}
}

func TestAuditRestoresReturnPenalty(t *testing.T) {
	te := newAuditEngine(t, nil)
	te.sel.penalty["a1"] = 2
	te.sel.returnedTo = "a1"
	te.sel.returnedAt = at(-time.Second)
	j := startAudit(t, te)
	if te.sel.returnedTo != "" {
		t.Fatal("down did not consume the return mark")
	}
	te.onProbeResult(j, probeResult{ok: true}, at(2*time.Second))
	if te.sel.penalty["a1"] != 2 || te.sel.returnedTo != "a1" || !te.sel.returnedAt.Equal(at(-time.Second)) {
		t.Fatalf("return penalty after rescue: value %v, mark %q at %v", te.sel.penalty["a1"], te.sel.returnedTo, te.sel.returnedAt)
	}
}

func boolCount(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func TestAuditStartsAfterFailoverAndRecheck(t *testing.T) {
	cfg := parallelConfig(3)
	cfg.Paths = []*PathDefinition{{Tag: "c1", Balancer: "other", Cost: 5}}
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	other := te.addSelector("other", newSelectorSettings(&SelectorConfig{Paths: pathSpecs("c1", 5)}))
	te.updateCandidates(other, []string{"c1"}, at(0))
	if other.current() != "c1" {
		t.Fatal("independent recheck selector did not select c1")
	}
	te.serverNames(map[string]string{"a1": "a", "b1": "b", "c1": "c"})
	te.failN(3, "a1", failDial, at(0))
	te.enqueue("c1", reasonRecheck, "", at(0))
	jobs, _ := te.tick(at(time.Second))
	if len(jobs) != 3 || jobs[0].reason != reasonFailover || jobs[1].reason != reasonRecheck || jobs[2].reason != reasonAudit {
		t.Fatalf("probe priority: %+v", jobs)
	}
}

func TestAuditExpiresWhileProbeBudgetIsBusy(t *testing.T) {
	te := newTestEngine(t, parallelConfig(1), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.failN(3, "a1", failDial, at(0))
	jobs, _ := te.tick(at(time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonFailover {
		t.Fatalf("first probe %+v", jobs)
	}
	te.tick(at(30*time.Second + time.Nanosecond))
	if te.report.counts.AuditSkipped != 1 || te.queued("a1", reasonAudit) || te.path("a1").rescue != nil {
		t.Fatalf("expired audit stayed queued; counters %+v", te.report.counts)
	}
	te.onSuccess("a1", at(2*time.Minute))
	te.failN(3, "a1", failDial, at(2*time.Minute+time.Second))
	if !te.queued("a1", reasonAudit) || te.report.counts.AuditCount != 0 {
		t.Fatalf("expired audit blocked the next down: %+v", te.report.counts)
	}
}

func TestNewDownReplacesQueuedAuditWithoutUsingThrottle(t *testing.T) {
	te := newTestEngine(t, parallelConfig(1), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.failN(3, "a1", failDial, at(0))
	var old *probeJob
	for _, j := range te.queue {
		if j.reason == reasonAudit {
			old = j
		}
	}
	if old == nil {
		t.Fatal("first down did not queue audit")
	}
	te.onSuccess("a1", at(2*time.Minute))
	te.failN(3, "a1", failDial, at(2*time.Minute+time.Second))
	var next *probeJob
	for _, j := range te.queue {
		if j.reason == reasonAudit {
			next = j
		}
	}
	if next == nil || next.id == old.id || next.auditDown != te.path("a1").downID || te.report.counts.AuditSkipped != 1 || te.report.counts.AuditCount != 0 {
		t.Fatalf("queued audit replacement: old %+v, next %+v, counts %+v", old, next, te.report.counts)
	}
}

func TestProviderDropCountsQueuedAuditOnce(t *testing.T) {
	te := newAuditEngine(t, nil)
	te.failN(3, "a1", failDial, at(0))
	te.dropProviderOutbound("a1", at(time.Second))
	if te.queued("a1", reasonAudit) || te.paths["a1"] != nil || te.report.counts.AuditSkipped != 1 || te.report.counts.AuditCount != 0 {
		t.Fatalf("dropped outbound audit counts %+v", te.report.counts)
	}
}

func TestAuditThrottleAndOldResultCannotRescueNewDown(t *testing.T) {
	te := newAuditEngine(t, nil)
	j := startAudit(t, te)
	firstDown := te.path("a1").downID
	te.failN(3, "a1", failDial, at(2*time.Minute))
	if te.path("a1").downID <= firstDown {
		t.Fatal("second down did not get a new identity")
	}
	te.onProbeResult(j, probeResult{ok: true}, at(2*time.Minute+time.Second))
	if p := te.path("a1"); p.status != statusDown || p.rescue != nil || p.stats.auditsPassed != 0 {
		t.Fatalf("old audit rescued second down: %+v", p)
	}
	if te.report.counts.AuditCount != 1 {
		t.Fatalf("audit starts %d, want one", te.report.counts.AuditCount)
	}
}

func TestAuditAllowsOnlyOnePerPenaltyWindow(t *testing.T) {
	te := newAuditEngine(t, nil)
	j := startAudit(t, te)
	te.onProbeResult(j, probeResult{ok: true}, at(2*time.Second))
	te.switchTo(te.sel, "a1", "recovered", at(10*time.Second))
	te.failN(3, "a1", failDial, at(20*time.Second))
	if te.queued("a1", reasonAudit) || te.report.counts.AuditSkipped != 1 {
		t.Fatalf("second down got an audit: %+v", te.report.counts)
	}
	if p := te.path("a1"); p.status != statusDown || p.rescue != nil || p.quarantineLevel != 1 {
		t.Fatalf("second down was softened: %+v", p)
	}
}

func TestSharedPathUsesLongestActivePenaltyWindow(t *testing.T) {
	te := newAuditEngine(t, nil)
	otherConfig := &SelectorConfig{SkipProbeBeforeSwitch: true,
		Paths:           []*PathDefinition{{Tag: "a1", Balancer: "other", Cost: 1}, {Tag: "b1", Balancer: "other", Cost: 3}},
		ReturnToCheaper: &ReturnToCheaperConfig{PenaltyWindow: int64(10 * time.Minute)}}
	other := te.addSelector("other", legacyScenarioSelector(otherConfig, nil))
	te.updateCandidates(other, []string{"a1", "b1"}, t0)
	j := startAudit(t, te)
	te.onProbeResult(j, probeResult{ok: true}, at(2*time.Second))
	te.switchTo(te.sel, "a1", "recovered", at(6*time.Minute))
	te.switchTo(other, "a1", "recovered", at(6*time.Minute))
	te.failN(3, "a1", failDial, at(6*time.Minute))
	if te.queued("a1", reasonAudit) || te.report.counts.AuditSkipped != 1 {
		t.Fatalf("shorter window allowed another audit: %+v", te.report.counts)
	}
}

func TestAuditPreservesDegradedProbeResult(t *testing.T) {
	te := newAuditEngine(t, nil)
	j := startAudit(t, te)
	te.onProbeResult(j, probeResult{ok: true, state: statusDegraded}, at(2*time.Second))
	if p := te.path("a1"); p.status != statusDegraded || p.quarantineLevel != 0 || p.stats.auditsPassed != 1 {
		t.Fatalf("degraded audit result: %+v", p)
	}
}

func TestAuditedPathReturnsAfterHoldAndThreeProbes(t *testing.T) {
	sc := &SelectorConfig{SkipProbeBeforeSwitch: true, MinHoldTime: v2Int64(int64(5 * time.Second)),
		ReturnToCheaper: &ReturnToCheaperConfig{Interval: int64(time.Hour), SkipRandomPhase: true,
			ProbesToReturn: 3, ProbeInterval: int64(time.Second)}}
	te := newAuditEngine(t, sc)
	j := startAudit(t, te)
	te.onProbeResult(j, probeResult{ok: true}, at(2*time.Second))
	if jobs, _ := te.tick(at(4 * time.Second)); len(jobs) != 0 || te.sel.current() != "b1" {
		t.Fatalf("return before hold: jobs %+v, current %q", jobs, te.sel.current())
	}
	for i, when := range []time.Duration{5 * time.Second, 8 * time.Second, 61 * time.Second} {
		if i == 2 {
			jobs, wake := te.tick(at(11 * time.Second))
			if len(jobs) != 0 || !te.queued("a1", reasonReturnToCheaper) || !wake.Equal(at(61*time.Second)) || te.sel.current() != "b1" {
				t.Fatalf("third return did not wait for its SNI budget: jobs %+v, wake %v, current %q", jobs, wake, te.sel.current())
			}
		}
		jobs, _ := te.tick(at(when))
		if len(jobs) != 1 || jobs[0].reason != reasonReturnToCheaper || jobs[0].tag != "a1" {
			t.Fatalf("return probe %d: %+v", i+1, jobs)
		}
		te.onProbeResult(jobs[0], probeResult{ok: true}, at(when+time.Second))
		want := "b1"
		if i == 2 {
			want = "a1"
		}
		if got := te.sel.current(); got != want {
			t.Fatalf("after return probe %d current %q, want %q", i+1, got, want)
		}
	}
}

func TestRunningConfirmationGetsNoExtraAudit(t *testing.T) {
	te := newAuditEngine(t, nil)
	j := confirmRunning(t, te)
	strongDown(t, te, at(3*time.Second))
	if te.queued("a1", reasonAudit) {
		t.Fatal("audit queued behind a running confirmation")
	}
	te.onProbeResult(j, probeResult{ok: true}, at(4*time.Second))
	if p := te.path("a1"); p.status != statusUp || p.rescuedBy != "confirm" {
		t.Fatalf("confirmation rescue: %+v", p)
	}
}

func TestWeakAndAlwaysConfirmationDoNotAudit(t *testing.T) {
	weak := newAuditEngine(t, nil)
	for _, target := range []string{"one", "two", "three"} {
		weak.fail("a1", failNoResponse, target, at(0))
	}
	if !weak.queued("a1", reasonConfirm) || weak.queued("a1", reasonAudit) || weak.path("a1").status != statusSuspect {
		t.Fatal("weak evidence did not wait for confirmation")
	}
	always := newAuditEngine(t, &SelectorConfig{SkipProbeBeforeSwitch: true, ConfirmWithProbe: ConfirmMode_ALWAYS})
	always.failN(3, "a1", failDial, at(0))
	if !always.queued("a1", reasonConfirm) || always.queued("a1", reasonAudit) || always.path("a1").status != statusSuspect {
		t.Fatal("always mode did not wait for confirmation")
	}
}

func TestAuditNetworkChangeAndDisabledSetting(t *testing.T) {
	t.Run("queued network change", func(t *testing.T) {
		te := newAuditEngine(t, nil)
		te.failN(3, "a1", failDial, at(0))
		te.onNetworkChange("next", at(time.Second))
		if te.report.counts.AuditSkipped != 1 || te.queued("a1", reasonAudit) {
			t.Fatalf("queued audit survived network change: %+v", te.report.counts)
		}
		te.switchTo(te.sel, "a1", "recovered", at(2*time.Second))
		te.failN(3, "a1", failDial, at(3*time.Second))
		if !te.queued("a1", reasonAudit) || te.report.counts.AuditCount != 0 {
			t.Fatalf("canceled audit blocked the new network: %+v", te.report.counts)
		}
	})
	t.Run("queued handler replacement", func(t *testing.T) {
		te := newAuditEngine(t, nil)
		te.failN(3, "a1", failDial, at(0))
		te.resetPath("a1", at(time.Second))
		if te.report.counts.AuditSkipped != 1 || te.queued("a1", reasonAudit) {
			t.Fatalf("replacement kept old audit: %+v", te.report.counts)
		}
		te.switchTo(te.sel, "a1", "recovered", at(2*time.Second))
		te.failN(3, "a1", failDial, at(3*time.Second))
		if !te.queued("a1", reasonAudit) || te.report.counts.AuditCount != 0 {
			t.Fatalf("canceled audit blocked the replacement: %+v", te.report.counts)
		}
	})
	t.Run("running network change", func(t *testing.T) {
		te := newAuditEngine(t, nil)
		j := startAudit(t, te)
		te.onNetworkChange("next", at(2*time.Second))
		te.onProbeResult(j, probeResult{ok: true}, at(3*time.Second))
		if te.report.counts.AuditPassed != 0 || te.path("a1").reason == "rescued" {
			t.Fatal("stale audit rescued a path")
		}
	})
	t.Run("disabled", func(t *testing.T) {
		te := newAuditEngine(t, &SelectorConfig{SkipProbeBeforeSwitch: true, ShouldSkipDownAudit: true})
		te.failN(3, "a1", failDial, at(0))
		if te.queued("a1", reasonAudit) || te.report.counts.AuditSkipped != 1 {
			t.Fatalf("disabled audit: %+v", te.report.counts)
		}
	})
}
