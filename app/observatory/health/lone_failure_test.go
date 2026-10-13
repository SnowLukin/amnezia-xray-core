package health

import (
	"testing"
	"time"
)

func newLoneEngine(t *testing.T) *testEngine {
	t.Helper()
	te := newTestEngine(t, &Config{Schedule: &ScheduleConfig{IdleAfter: int64(time.Hour)}},
		&SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), SkipProbeBeforeSwitch: true,
			ReturnToCheaper: &ReturnToCheaperConfig{Interval: int64(time.Hour), SkipRandomPhase: true}}, "a1", "b1")
	te.onSuccess("a1", at(0))
	return te
}

func startLone(t *testing.T, te *testEngine, now time.Time) *probeJob {
	t.Helper()
	te.fail("a1", failDial, "", now)
	jobs, _ := te.tick(now)
	for _, j := range jobs {
		if j.reason == reasonLone && j.tag == "a1" {
			return j
		}
	}
	t.Fatalf("lone probe did not start: %v", jobTags(jobs))
	return nil
}

func TestLoneFailureResults(t *testing.T) {
	for _, ok := range []bool{true, false} {
		t.Run(map[bool]string{true: "passed", false: "failed"}[ok], func(t *testing.T) {
			te := newLoneEngine(t)
			j := startLone(t, te, at(3*time.Minute))
			p := te.path("a1")
			if p.status != statusUp || len(p.failures) != 1 {
				t.Fatalf("one failure decided before proof: %+v", p)
			}
			te.onProbeResult(j, probeResult{ok: ok}, at(3*time.Minute+time.Second))
			if len(p.failures) != 0 || p.probing {
				t.Fatalf("probe did not finish evidence: %+v", p)
			}
			if ok {
				if p.status != statusUp || te.sel.current() != "a1" || p.stats.downsStrong != 0 || len(te.downs) != 0 {
					t.Fatalf("successful lone changed path: %+v", p)
				}
			} else {
				if p.status != statusDown || p.quarantineLevel != 1 || te.sel.current() != "b1" || p.stats.downsStrong != 1 || te.queued("a1", reasonAudit) {
					t.Fatalf("failed lone did not decide once: %+v", p)
				}
				down := te.takeDowns()
				if len(down) != 2 || down[0].tag != "a1" || down[1].tag != "a1" ||
					!down[0].pending || !down[0].watch || !down[1].pending || down[1].watch ||
					!down[0].at.Equal(down[1].at) || len(te.silences) != 1 {
					t.Fatalf("down did not close waiting and watch silent connections: %+v %+v", down, te.silences)
				}
				te.dueSilences(at(3*time.Minute + time.Second + te.s.passive.stallTimeout))
				if down := te.takeDowns(); len(down) != 1 || !down[0].silent {
					t.Fatalf("silent connections were not scheduled to close: %+v", down)
				}
			}
			if te.report.counts.LoneProbeCount != 1 || te.report.counts.LoneProbeFailed != boolCount(!ok) || p.stats.loneProbes != 1 || p.stats.loneProbesFailed != boolCount(!ok) {
				t.Fatalf("lone counters: %+v %+v", te.report.counts, p.stats)
			}
			view := te.reportView(reportSnapshot, at(4*time.Minute), false)
			if got := view.Paths["a1"]; got.LoneProbes != 1 || got.LoneProbesFailed != boolCount(!ok) || got.DownsStrong != boolCount(!ok) {
				t.Fatalf("lone report: %+v", got)
			}
			te.onProbeResult(j, probeResult{ok: false}, at(4*time.Minute))
			if te.report.counts.LoneProbeFailed != boolCount(!ok) || p.stats.downsStrong != boolCount(!ok) {
				t.Fatal("duplicate completion changed counters or decision")
			}
		})
	}
}

func TestLoneFailureEligibility(t *testing.T) {
	cases := []struct {
		name   string
		change func(*testing.T, *testEngine)
		kind   failureKind
	}{
		{"fresh", func(t *testing.T, te *testEngine) { te.onSuccess("a1", at(2*time.Minute)) }, failDial},
		{"threshold", func(t *testing.T, te *testEngine) { te.onSuccess("a1", at(time.Minute)) }, failDial},
		{"disabled", func(t *testing.T, te *testEngine) { te.s.passive.loneFailureAfter = 0 }, failDial},
		{"not_selected", func(t *testing.T, te *testEngine) { te.switchTo(te.sel, "b1", "host", at(time.Minute)) }, failDial},
		{"lost", func(t *testing.T, te *testEngine) { te.onNetworkLost(at(time.Minute)) }, failDial},
		{"doubt", func(t *testing.T, te *testEngine) { te.doubt = true }, failDial},
		{"retired", func(t *testing.T, te *testEngine) { te.retiring["a1"] = true }, failDial},
		{"replacing", func(t *testing.T, te *testEngine) { te.replacing["a1"] = true }, failDial},
		{"queued_other_probe", func(t *testing.T, te *testEngine) {
			if _, _, err := te.requestProbe("auto", []string{"a1"}, at(3*time.Minute)); err != nil {
				t.Fatal(err)
			}
		}, failDial},
		{"running_other_probe", func(t *testing.T, te *testEngine) {
			if _, _, err := te.requestProbe("auto", []string{"a1"}, at(3*time.Minute)); err != nil {
				t.Fatal(err)
			}
			jobs, _ := te.tick(at(3 * time.Minute))
			if len(jobs) != 1 || jobs[0].reason != reasonHost || jobs[0].tag != "a1" || te.running[jobs[0].id] != jobs[0] {
				t.Fatal("host probe did not start")
			}
		}, failDial},
		{"no_response", func(*testing.T, *testEngine) {}, failNoResponse},
		{"stall", func(*testing.T, *testEngine) {}, failStall},
		{"partial_stall", func(*testing.T, *testEngine) {}, failPartialStall},
		{"closed", func(*testing.T, *testEngine) {}, failClosed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			te := newLoneEngine(t)
			tc.change(t, te)
			te.fail("a1", tc.kind, "target", at(3*time.Minute))
			if te.queued("a1", reasonLone) {
				t.Fatal("ineligible failure queued a lone probe")
			}
		})
	}
	for _, kind := range []failureKind{failDial, failDialTimeout, failUnverified} {
		t.Run(kind.String(), func(t *testing.T) {
			te := newLoneEngine(t)
			te.fail("a1", kind, "", at(3*time.Minute))
			if !te.queued("a1", reasonLone) {
				t.Fatal("path failure did not queue a lone probe")
			}
		})
	}
	te := newLoneEngine(t)
	te.path("a1").lastSuccess = time.Time{}
	te.fail("a1", failDial, "", at(0))
	if te.queued("a1", reasonLone) {
		t.Fatal("a new path without proof of life queued a lone probe")
	}
}

func TestLoneFailureThrottleAndDuplicates(t *testing.T) {
	te := newLoneEngine(t)
	te.fail("a1", failDial, "", at(3*time.Minute))
	te.fail("a1", failDialTimeout, "", at(3*time.Minute+time.Second))
	if len(te.queue) != 1 || !te.path("a1").lastLoneAt.IsZero() {
		t.Fatal("queue duplicate or throttle consumed before start")
	}
	jobs, _ := te.tick(at(3*time.Minute + time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonLone {
		t.Fatalf("jobs %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(3*time.Minute+2*time.Second))
	te.fail("a1", failDial, "", at(6*time.Minute))
	if te.queued("a1", reasonLone) {
		t.Fatal("lone bypassed five minute interval")
	}
	te.fail("a1", failDial, "", at(8*time.Minute+time.Second))
	if !te.queued("a1", reasonLone) {
		t.Fatal("lone did not resume at interval boundary")
	}
}

func TestLoneFailureQueuedCanceledWithoutThrottle(t *testing.T) {
	te := newLoneEngine(t)
	te.fail("a1", failDial, "", at(3*time.Minute))
	te.onSuccess("a1", at(3*time.Minute+time.Second))
	te.tick(at(4 * time.Minute))
	if te.queued("a1", reasonLone) || te.report.counts.LoneProbeCount != 0 || !te.path("a1").lastLoneAt.IsZero() {
		t.Fatal("canceled queued probe started or consumed throttle")
	}
	te.fail("a1", failDial, "", at(6*time.Minute))
	if !te.queued("a1", reasonLone) {
		t.Fatal("canceled queued probe consumed throttle")
	}
}

func TestLoneFailureStaleResults(t *testing.T) {
	cases := []struct {
		name   string
		change func(*testing.T, *testEngine)
	}{
		{"traffic_success", func(t *testing.T, te *testEngine) { te.onSuccess("a1", at(3*time.Minute+time.Second)) }},
		{"probe_success", func(t *testing.T, te *testEngine) { te.markUp(te.path("a1"), "probe", at(3*time.Minute+time.Second)) }},
		{"network_lost", func(t *testing.T, te *testEngine) {
			te.onNetworkLost(at(3*time.Minute + time.Second))
			te.endPause(at(3*time.Minute + 2*time.Second))
		}},
		{"network_change", func(t *testing.T, te *testEngine) { te.onNetworkChange("next", at(3*time.Minute+time.Second)) }},
		{"network_ignored", func(t *testing.T, te *testEngine) {
			te.sel.cfg.onNetworkChange = NetworkChangeMode_IGNORE
			te.onNetworkChange("next", at(3*time.Minute+time.Second))
		}},
		{"doubt", func(t *testing.T, te *testEngine) { te.doubt = true }},
		{"retired", func(t *testing.T, te *testEngine) { te.retiring["a1"] = true }},
		{"choice_changed", func(t *testing.T, te *testEngine) { te.switchTo(te.sel, "b1", "host", at(3*time.Minute+time.Second)) }},
		{"handler_replaced", func(t *testing.T, te *testEngine) { te.invalidateProbeJobs("a1", at(3*time.Minute+time.Second)) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			te := newLoneEngine(t)
			j := startLone(t, te, at(3*time.Minute))
			tc.change(t, te)
			te.onProbeResult(j, probeResult{ok: false}, at(3*time.Minute+3*time.Second))
			if te.path("a1").status == statusDown || te.path("a1").stats.downsStrong != 0 || len(te.downs) != 0 || len(te.silences) != 0 || te.queued("a1", reasonLone) {
				t.Fatal("stale lone failure ruled out the path or was requeued")
			}
		})
	}
}

func TestLoneFailureOrdinaryStrongEvidenceWins(t *testing.T) {
	for _, running := range []bool{false, true} {
		te := newLoneEngine(t)
		te.fail("a1", failDial, "", at(3*time.Minute))
		var lone *probeJob
		if running {
			jobs, _ := te.tick(at(3 * time.Minute))
			lone = jobs[0]
		}
		te.failN(2, "a1", failDial, at(3*time.Minute+time.Second))
		p := te.path("a1")
		if p.status != statusDown || p.stats.downsStrong != 1 || !te.queued("a1", reasonAudit) || te.queued("a1", reasonLone) {
			t.Fatalf("ordinary evidence changed: %+v", p)
		}
		if lone != nil {
			te.onProbeResult(lone, probeResult{ok: true}, at(3*time.Minute+2*time.Second))
			if p.status != statusDown || p.quarantineLevel != 1 || p.stats.downsStrong != 1 {
				t.Fatal("lone rescued or repeated ordinary strong down")
			}
		}
	}
}

func TestLoneFailureSelectedByAnotherBalancerAndIdle(t *testing.T) {
	te := newLoneEngine(t)
	other := te.addSelector("other", legacyScenarioSelector(&SelectorConfig{Paths: []*PathDefinition{{Tag: "a1", Cost: 1}}, SkipProbeBeforeSwitch: true}, nil))
	te.updateCandidates(other, []string{"a1"}, at(0))
	te.switchTo(te.sel, "b1", "host", at(time.Minute))
	te.s.schedule.idleAfter = time.Minute
	j := startLone(t, te, at(3*time.Minute))
	if j.reason != reasonLone || !te.idle(at(3*time.Minute)) {
		t.Fatal("selected idle path did not get a lone probe")
	}
}

func TestLoneFailureSettingsPresence(t *testing.T) {
	for _, tc := range []struct {
		cfg  *Config
		want time.Duration
	}{
		{&Config{}, 0},
		{&Config{Features: &FeatureConfig{LoneFailureProbe: v2Bool(true)}, Passive: &PassiveConfig{}}, 2 * time.Minute},
		{&Config{Features: &FeatureConfig{LoneFailureProbe: v2Bool(false)}, Passive: &PassiveConfig{LoneFailureAfter: int64(time.Minute)}}, 0},
		{&Config{Features: &FeatureConfig{LoneFailureProbe: v2Bool(true)}, Passive: &PassiveConfig{LoneFailureAfter: int64(4 * time.Minute)}}, 4 * time.Minute},
	} {
		if got := newSettings(tc.cfg).passive.loneFailureAfter; got != tc.want {
			t.Fatalf("loneFailureAfter=%v, want %v", got, tc.want)
		}
	}
}

func TestLoneFailureKeepsOrdinaryAlwaysConfirmation(t *testing.T) {
	te := newLoneEngine(t)
	te.sel.cfg.confirm = ConfirmMode_ALWAYS
	te.failN(3, "a1", failDial, at(3*time.Minute))
	if te.path("a1").status != statusSuspect || te.queued("a1", reasonLone) || !te.queued("a1", reasonConfirm) {
		t.Fatal("three failures bypassed always confirmation")
	}
	jobs, _ := te.tick(at(3 * time.Minute))
	if len(jobs) != 1 || jobs[0].reason != reasonConfirm {
		t.Fatalf("jobs %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: false}, at(3*time.Minute+time.Second))
	if te.path("a1").status != statusDown || te.path("a1").stats.downsStrong != 1 || te.report.counts.LoneProbeCount != 0 {
		t.Fatal("ordinary confirmation did not decide once")
	}
}

func TestLoneFailureQueueRevalidatesChoice(t *testing.T) {
	te := newLoneEngine(t)
	te.fail("a1", failDial, "", at(3*time.Minute))
	te.switchTo(te.sel, "b1", "host", at(3*time.Minute+time.Second))
	te.tick(at(3*time.Minute + 2*time.Second))
	if te.queued("a1", reasonLone) || te.report.counts.LoneProbeCount != 0 {
		t.Fatal("unselected queued lone started")
	}
}

func TestLoneFailureReportWindows(t *testing.T) {
	te := newLoneEngine(t)
	te.s.reportInterval = time.Second
	j := startLone(t, te, at(3*time.Minute))
	te.onProbeResult(j, probeResult{ok: false}, at(3*time.Minute+time.Second))
	first := te.takeReport(false, at(3*time.Minute+2*time.Second))
	if first == nil || first.LoneProbeCount != 1 || first.LoneProbeFailed != 1 || first.Paths["a1"].LoneProbes != 1 || first.Paths["a1"].LoneProbesFailed != 1 {
		t.Fatalf("first report %+v", first)
	}
	second := te.takeReport(true, at(4*time.Minute))
	if second == nil || second.LoneProbeCount != 0 || second.LoneProbeFailed != 0 || second.Paths["a1"].LoneProbes != 0 || second.Paths["a1"].LoneProbesFailed != 0 {
		t.Fatalf("second report repeated lone counters %+v", second)
	}
}

func TestLoneFailurePriorityAndBudgets(t *testing.T) {
	te := newTestEngine(t, parallelConfig(3), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3, "c", 5), SkipProbeBeforeSwitch: true}, "a1", "b1", "c1")
	te.onSuccess("a1", at(0))
	te.fail("a1", failDial, "", at(3*time.Minute))
	te.enqueue("b1", reasonRecheck, "", at(3*time.Minute))
	te.enqueue("c1", reasonFailover, "", at(3*time.Minute))
	jobs, _ := te.tick(at(3 * time.Minute))
	if len(jobs) != 3 || jobs[0].reason != reasonFailover || jobs[1].reason != reasonRecheck || jobs[2].reason != reasonLone {
		t.Fatalf("lone priority: %+v", jobs)
	}

	limited := newLoneEngine(t)
	limited.starts = []time.Time{at(3 * time.Minute)}
	limited.s.schedule.perMinute = 1
	limited.fail("a1", failDial, "", at(3*time.Minute))
	jobs, next := limited.tick(at(3 * time.Minute))
	if len(jobs) != 0 || !limited.queued("a1", reasonLone) || !next.Equal(at(4*time.Minute)) {
		t.Fatalf("lone ignored rate budget: jobs %+v, next %v", jobs, next)
	}

	shared := newTestEngine(t, parallelConfig(2), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), SkipProbeBeforeSwitch: true}, "a1", "b1")
	shared.serverNames(map[string]string{"a1": "same", "b1": "same"})
	shared.onSuccess("a1", at(0))
	shared.fail("a1", failDial, "", at(3*time.Minute))
	shared.enqueue("b1", reasonRecheck, "", at(3*time.Minute))
	jobs, _ = shared.tick(at(3 * time.Minute))
	if len(jobs) != 1 || jobs[0].reason != reasonRecheck || !shared.queued("a1", reasonLone) {
		t.Fatalf("lone ignored server-name budget: %+v", jobs)
	}
}

func TestLoneDegradedSearchesForUpAndKeepsLivePath(t *testing.T) {
	for _, candidateUp := range []bool{true, false} {
		t.Run(map[bool]string{true: "up_available", false: "all_degraded"}[candidateUp], func(t *testing.T) {
			te := newLoneEngine(t)
			te.sel.cfg.switchAfterProbe = true
			j := startLone(t, te, at(3*time.Minute))
			te.onProbeResult(j, degradedProbe(), at(3*time.Minute+time.Second))
			p := te.path("a1")
			if p.status != statusDegraded || len(p.failures) != 0 || p.stats.downsStrong != 0 || te.sel.current() != "a1" || !te.queued("b1", reasonFailover) || te.queued("a1", reasonAudit) || len(te.downs) != 0 {
				t.Fatalf("degraded lone did not search normally: %+v", p)
			}
			jobs, _ := te.tick(at(3*time.Minute + 3*time.Second))
			if len(jobs) != 1 || jobs[0].tag != "b1" || jobs[0].reason != reasonFailover {
				t.Fatalf("candidate jobs %+v", jobs)
			}
			r := degradedProbe()
			if candidateUp {
				r = probeResult{ok: true}
			}
			te.onProbeResult(jobs[0], r, at(3*time.Minute+4*time.Second))
			want := "a1"
			if candidateUp {
				want = "b1"
			}
			if te.sel.current() != want || te.sel.exhausted || !p.usable(at(3*time.Minute+4*time.Second)) || te.report.counts.LoneProbeFailed != 0 {
				t.Fatalf("degraded result: current %q, exhausted %v, path %+v", te.sel.current(), te.sel.exhausted, p)
			}
		})
	}
}
